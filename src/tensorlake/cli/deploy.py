import argparse
import asyncio
import json
import os
import sys
import traceback
from pathlib import Path

from tensorlake.applications import Function, SDKUsageError, TensorlakeError
from tensorlake.applications.applications import filter_applications
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.applications.remote.curl_command import example_application_curl_command
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.remote.images import (
    ApplicationImageBuild as _ApplicationImageBuild,
)
from tensorlake.applications.remote.images import (
    application_image_builds as _application_images,
)
from tensorlake.applications.remote.images import (
    default_application_image_name,
    explicit_application_image_name,
    immutable_image_reference,
)
from tensorlake.applications.secrets import list_secret_names
from tensorlake.applications.validation import (
    ValidationMessage,
    format_validation_messages,
    has_error_message,
    validate_loaded_applications,
)
from tensorlake.cli._common import Context
from tensorlake.image.sandbox_builder import (
    SandboxImageBuildError,
    build_sandbox_application_image,
)
from tensorlake.image.utils import _SDK_VERSION

_DEPLOY_PROTOCOL_VERSION = 1


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


def _function_service_context(auth: Context) -> Context:
    function_service_url = os.environ.get("TENSORLAKE_FUNCTION_SERVICE_URL", "").strip()
    if not function_service_url:
        return auth
    return Context.default(
        api_url=function_service_url,
        api_key=auth.api_key,
        personal_access_token=auth.personal_access_token,
        namespace=auth.namespace,
        organization_id=auth.organization_id,
        project_id=auth.project_id,
        debug=auth.debug,
    )


def _warning_missing_secrets(auth: Context, secrets: list[str]) -> list[str]:
    """Check for missing secrets and return their names."""
    try:
        existing = auth.list_secret_names(page_size=100)
    except Exception:
        return []
    return [s for s in secrets if s not in existing]


def _parse_build_envs(build_envs: list[str]) -> list[tuple[str, str]]:
    parsed_build_envs: list[tuple[str, str]] = []
    for build_env in build_envs:
        key, separator, value = build_env.partition("=")
        if separator != "=" or not key:
            raise ValueError(
                f"invalid --build-env value '{build_env}'; expected KEY=VALUE"
            )
        parsed_build_envs.append((key, value))
    return parsed_build_envs


def deploy(
    application_file_path: str,
    upgrade_running_requests: bool,
    build_envs: list[tuple[str, str]] | None = None,
):
    """Deploys applications to Tensorlake Cloud, emitting NDJSON events to stdout."""
    _emit(
        {
            "type": "protocol",
            "version": _DEPLOY_PROTOCOL_VERSION,
            "sdk_version": _SDK_VERSION,
        }
    )
    if upgrade_running_requests:
        _emit(
            {
                "type": "error",
                "message": (
                    "--upgrade-running-requests is not supported by Function Service; "
                    "existing requests stay pinned to their deployed application version"
                ),
            }
        )
        sys.exit(1)
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

    auth = _build_context_from_env()
    function_service = _function_service_context(auth)

    missing = _warning_missing_secrets(auth, list(list_secret_names()))
    if missing:
        _emit({"type": "missing_secrets", "count": len(missing), "names": missing})

    try:
        function_images = asyncio.run(
            _prepare_images(
                functions,
                context_dir=str(Path(application_file_path).parent),
                build_envs=build_envs,
            )
        )
    except KeyboardInterrupt:
        _emit({"type": "error", "message": "build cancelled by user"})
        sys.exit(1)
    except Exception as e:
        _emit(_error_event("build failed", e))
        sys.exit(1)

    _deploy_applications(
        api_client=function_service.cloud_client,
        api_url=function_service.api_url,
        application_file_path=application_file_path,
        upgrade_running_requests=upgrade_running_requests,
        functions=functions,
        function_images=function_images,
    )


async def _prepare_images(
    functions: list[Function],
    context_dir: str,
    build_envs: list[tuple[str, str]] | None = None,
) -> dict[tuple[str, str], str]:
    image_builds = _application_images(functions)
    function_images: dict[tuple[str, str], str] = {}
    for image_build in image_builds:
        image = image_build.image
        image_name = image_build.registered_name
        _emit({"type": "build_start", "image": image_name})
        try:
            published = await asyncio.to_thread(
                build_sandbox_application_image,
                image,
                registered_name=image_name,
                build_env_vars=build_envs,
                context_dir=context_dir,
                emit=_emit,
                _allow_pat=True,
            )
        except (asyncio.CancelledError, KeyboardInterrupt) as error:
            raise error
        except SandboxImageBuildError as error:
            _emit(
                {
                    "type": "build_failed",
                    "image": image_name,
                    "error": _format_build_failure_message(image_name, error),
                }
            )
            sys.exit(1)

        immutable_ref = immutable_image_reference(published, image_name)
        for function_key in image_build.function_keys:
            function_images[function_key] = immutable_ref

    _emit({"type": "build_done"})
    return function_images


def _deploy_applications(
    api_client,
    api_url: str,
    application_file_path: str,
    upgrade_running_requests: bool,
    functions: list[Function],
    function_images: dict[tuple[str, str], str],
):
    _emit({"type": "status", "message": "Deploying applications..."})

    try:
        deploy_applications(
            applications_file_path=application_file_path,
            upgrade_running_requests=upgrade_running_requests,
            load_source_dir_modules=False,
            api_client=api_client,
            function_images=function_images,
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
        "--build-env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Environment variable to inject into generated Dockerfiles (repeatable)",
    )
    args = parser.parse_args()

    try:
        deploy(
            application_file_path=args.application_file_path,
            upgrade_running_requests=args.upgrade_running_requests,
            build_envs=_parse_build_envs(args.build_env),
        )
    except SystemExit:
        raise
    except Exception as e:
        _emit(_error_event("deploy failed", e))
        sys.exit(1)


if __name__ == "__main__":
    deploy_entrypoint()
