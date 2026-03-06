import argparse
import asyncio
import json
import os
import sys
import traceback
from urllib.parse import urlparse

from tensorlake.applications import Function, SDKUsageError, TensorlakeError
from tensorlake.applications.applications import filter_applications
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
from tensorlake.builder import collect_application_build_request
from tensorlake.builder.client_v2 import (
    ApplicationImageBuildError,
    ImageBuilderV2Client,
)
from tensorlake.builder.client_v3 import ImageBuilderV3Client
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


def _format_build_failure_message(
    image_name: str, error: Exception | BaseException
) -> str:
    details = str(error).strip()
    if details:
        return (
            f"image '{image_name}' build failed: {details}. "
            "check your Image() configuration and try again."
        )
    return (
        f"image '{image_name}' build failed ({type(error).__name__}). "
        "check your Image() configuration and try again."
    )


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


def mk_builder(version: str, auth: Context):
    default_build_service_path = (
        "/images/v3/applications" if version == "v3" else "/images/v2"
    )
    build_service = (
        os.getenv("TENSORLAKE_BUILD_SERVICE")
        or f"{auth.api_url}{default_build_service_path}"
    )
    parsed = urlparse(build_service)
    build_service_path = parsed.path.rstrip("/") or default_build_service_path
    if version == "v3":
        return ImageBuilderV3Client(
            cloud_client=auth.cloud_client,
            build_service_path=build_service_path,
        )
    return ImageBuilderV2Client(
        cloud_client=auth.cloud_client,
        build_service_path=build_service_path,
        on_build_start=lambda image, _function_name: _emit(
            {"type": "build_start", "image": image.name}
        ),
    )


def deploy(
    application_file_path: str,
    upgrade_running_requests: bool,
    image_builder_version: str = "v2",
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

    missing = _warning_missing_secrets(auth, list(list_secret_names()))
    if missing:
        _emit({"type": "missing_secrets", "count": len(missing)})

    builder = mk_builder(image_builder_version, auth)
    try:
        asyncio.run(_prepare_images(builder, functions))
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


async def _prepare_images(builder, functions: list[Function]):
    for application in filter_applications(functions):
        try:
            await builder.build(
                collect_application_build_request(application, functions)
            )
        except (asyncio.CancelledError, KeyboardInterrupt) as error:
            raise error
        except ApplicationImageBuildError as error:
            _emit(
                {
                    "type": "build_failed",
                    "image": error.image_name,
                    "error": _format_build_failure_message(
                        error.image_name, error.error
                    ),
                }
            )
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
        "-u",
        "--upgrade-running-requests",
        action="store_true",
        default=False,
        help="Upgrade requests that are already queued or running",
    )
    parser.add_argument(
        "--image-builder-version",
        choices=["v2", "v3"],
        default="v2",
        help="Select image builder version",
    )
    args = parser.parse_args()

    try:
        deploy(
            application_file_path=args.application_file_path,
            upgrade_running_requests=args.upgrade_running_requests,
            image_builder_version=args.image_builder_version,
        )
    except SystemExit:
        raise
    except Exception as e:
        _emit(_error_event("deploy failed", e))
        sys.exit(1)


if __name__ == "__main__":
    deploy_entrypoint()
