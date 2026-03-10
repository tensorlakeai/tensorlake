import argparse
import asyncio
import json
import os
import sys
import traceback
from urllib.parse import urlparse

from tensorlake.applications import Function, Image, SDKUsageError, TensorlakeError
from tensorlake.applications.applications import filter_applications
from tensorlake.applications.image import ImageInformation, image_infos
from tensorlake.applications.interface.function import (
    _ApplicationConfiguration,
    _FunctionConfiguration,
)
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.applications.remote.curl_command import example_application_curl_command
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.secrets import list_secret_names
from tensorlake.applications.validation import (
    ValidationMessage,
    format_validation_messages,
    has_error_message,
    validate_loaded_applications,
)
from tensorlake.builder.client_v2 import BuildContext, ImageBuilderV2Client
from tensorlake.cli._common import Context


def _emit(obj):
    print(json.dumps(obj), flush=True)


def _format_error_message(
    prefix: str, error: Exception | BaseException | None = None
) -> str:
    """Return a user-facing error message without leaking exception payloads."""
    if error is None:
        return prefix
    return f"{prefix} ({type(error).__name__})"


def _debug_enabled() -> bool:
    return os.environ.get("TENSORLAKE_DEBUG", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _error_event(prefix: str, error: Exception | BaseException | None = None) -> dict:
    event: dict[str, str] = {
        "type": "error",
        "message": _format_error_message(prefix, error),
    }
    if error is not None:
        # Keep a concise detail line visible by default for actionable debugging.
        event["details"] = f"{type(error).__name__}: {error}"
    if _debug_enabled():
        event["traceback"] = traceback.format_exc()
    return event


def _build_context_from_env() -> Context:
    """Build CLI context from environment variables set by the Rust CLI."""
    return Context.default(
        api_url=os.environ.get("TENSORLAKE_API_URL"),
        api_key=os.environ.get("TENSORLAKE_API_KEY"),
        personal_access_token=os.environ.get("TENSORLAKE_PAT"),
        namespace=os.environ.get("INDEXIFY_NAMESPACE"),
        organization_id=os.environ.get("TENSORLAKE_ORGANIZATION_ID"),
        project_id=os.environ.get("TENSORLAKE_PROJECT_ID"),
        debug=_debug_enabled(),
    )


def _warning_missing_secrets(auth: Context, secrets: list[str]) -> list[str]:
    """Check for missing secrets and return their names."""
    try:
        existing = auth.list_secret_names(page_size=100)
    except Exception:
        return []
    return [s for s in secrets if s not in existing]


def _onprem_enabled() -> bool:
    return os.environ.get("TENSORLAKE_ONPREM", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _parse_build_envs(build_envs: list[str] | None) -> list[tuple[str, str]] | None:
    if not build_envs:
        return None
    result = []
    for item in build_envs:
        if "=" not in item:
            continue
        key, _, val = item.partition("=")
        result.append((key.strip(), val.strip()))
    return result or None


def deploy(
    application_file_path: str,
    parallel_builds: bool,
    upgrade_running_requests: bool,
    build_envs: list[str] | None = None,
):
    """Deploys applications to Tensorlake Cloud, emitting NDJSON events to stdout."""
    _emit(
        {
            "type": "status",
            "message": f"Preparing deployment for applications from {application_file_path}",
        }
    )

    try:
        application_file_path = os.path.abspath(application_file_path)
        load_code(application_file_path)
    except SyntaxError as e:
        _emit(
            {
                "type": "error",
                "message": f"syntax error in {e.filename}, line {e.lineno}: {e.msg}",
            }
        )
        sys.exit(1)
    except ImportError as e:
        _emit(
            _error_event(
                "failed to import application file. make sure all dependencies are installed in your current environment.",
                e,
            )
        )
        sys.exit(1)
    except Exception as e:
        _emit(_error_event(f"failed to load {application_file_path}", e))
        sys.exit(1)

    validation_messages: list[ValidationMessage] = validate_loaded_applications()
    for item in format_validation_messages(validation_messages):
        _emit(
            {
                "type": "validation",
                "severity": item["severity"],
                "message": item["message"],
                "location": item["location"],
            }
        )

    if has_error_message(validation_messages):
        _emit({"type": "validation_failed"})
        sys.exit(1)

    functions: list[Function] = get_functions()

    if _onprem_enabled():
        deploy_applications(
            applications_file_path=application_file_path,
            upgrade_running_requests=upgrade_running_requests,
            load_source_dir_modules=False,
        )
        _emit({"type": "done"})
        return

    auth = _build_context_from_env()

    builder_v2 = ImageBuilderV2Client.from_env()

    missing = _warning_missing_secrets(auth, list(list_secret_names()))
    if missing:
        _emit({"type": "missing_secrets", "count": len(missing)})

    extra_env_vars = _parse_build_envs(build_envs)

    try:
        asyncio.run(_prepare_images_v2(builder_v2, functions, extra_env_vars))
    except KeyboardInterrupt:
        _emit({"type": "error", "message": "build cancelled by user"})
        sys.exit(1)
    except Exception as e:
        _emit(_error_event("build failed", e))
        sys.exit(1)

    _deploy_applications(
        api_client=auth.cloud_client,
        api_url=auth.api_url,
        application_file_path=application_file_path,
        upgrade_running_requests=upgrade_running_requests,
        functions=functions,
    )


async def _prepare_images_v2(
    builder: ImageBuilderV2Client,
    functions: list[Function],
    extra_env_vars: list[tuple[str, str]] | None = None,
):
    images: dict[Image, ImageInformation] = image_infos()
    for application in filter_applications(functions):
        fn_config: _FunctionConfiguration = application._function_config
        app_config: _ApplicationConfiguration = application._application_config

        for image_info in images.values():
            image_info: ImageInformation
            for function in image_info.functions:
                _emit({"type": "build_start", "image": image_info.image.name})
                try:
                    await builder.build(
                        BuildContext(
                            application_name=fn_config.function_name,
                            application_version=app_config.version,
                            function_name=function._function_config.function_name,
                        ),
                        image_info.image,
                        extra_env_vars=extra_env_vars,
                    )
                except (asyncio.CancelledError, KeyboardInterrupt) as error:
                    raise error
                except Exception as error:
                    event = {
                        "type": "build_failed",
                        "image": image_info.image.name,
                        "error": _format_error_message(
                            f"image '{image_info.image.name}' build failed",
                            error,
                        )
                        + ". "
                        f"check your Image() configuration and try again.",
                    }
                    if _debug_enabled():
                        event["traceback"] = traceback.format_exc()
                    _emit(event)
                    sys.exit(1)

    _emit({"type": "build_done"})


def _deploy_applications(
    api_client,
    api_url: str,
    application_file_path: str,
    upgrade_running_requests: bool,
    functions: list[Function],
):
    _emit({"type": "status", "message": "Deploying applications..."})

    try:
        deploy_applications(
            applications_file_path=application_file_path,
            upgrade_running_requests=upgrade_running_requests,
            load_source_dir_modules=False,
            api_client=api_client,
        )

        for application_function in filter_applications(functions):
            application_function: Function
            curl_command: str | None = example_application_curl_command(
                api_url=api_url,
                application=application_function,
                file_paths=None,
            )
            _emit(
                {
                    "type": "deployed",
                    "application": application_function._name,
                    "curl_command": curl_command,
                }
            )
    except SDKUsageError as e:
        _emit(_error_event("invalid usage", e))
        sys.exit(1)
    except TensorlakeError as e:
        _emit(_error_event("failed to deploy applications", e))
        sys.exit(1)
    except Exception as e:
        _emit(_error_event("failed to deploy applications", e))
        sys.exit(1)

    _emit(
        {
            "type": "done",
            "doc_url": "https://docs.tensorlake.ai/applications/quickstart#calling-applications",
        }
    )


def deploy_entrypoint():
    """Entry point for the deploy command (called from Rust CLI via python -m)."""
    parser = argparse.ArgumentParser(
        description="Deploy applications to Tensorlake Cloud"
    )
    parser.add_argument(
        "application_file_path",
        help="Path to the application .py file",
    )
    parser.add_argument(
        "-p",
        "--parallel-builds",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-u",
        "--upgrade-running-requests",
        action="store_true",
        default=False,
        help="Upgrade requests that are already queued or running",
    )
    parser.add_argument(
        "--build-env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        dest="build_envs",
        help="Inject an ENV directive into generated Dockerfiles (repeatable)",
    )
    args = parser.parse_args()

    try:
        deploy(
            application_file_path=args.application_file_path,
            parallel_builds=args.parallel_builds,
            upgrade_running_requests=args.upgrade_running_requests,
            build_envs=args.build_envs or None,
        )
    except SystemExit:
        raise
    except Exception as e:
        _emit(_error_event("deploy failed", e))
        sys.exit(1)


if __name__ == "__main__":
    deploy_entrypoint()
