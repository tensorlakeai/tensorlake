import argparse
import json
import os
import sys
import traceback

from tensorlake.applications import Function, SDKUsageError, TensorlakeError
from tensorlake.applications.applications import (
    filter_applications,
    functions_for_application,
)
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.applications.remote.curl_command import example_application_curl_command
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.remote.manifests.function_manifests import ImageRef
from tensorlake.applications.secrets import list_secret_names
from tensorlake.applications.validation import (
    ValidationMessage,
    format_validation_messages,
    has_error_message,
    validate_loaded_applications,
)
from tensorlake.cli._common import Context
from tensorlake.image.sandbox_builder import (
    SandboxImageError,
    build_sandbox_image,
)


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


def deploy(
    application_file_path: str,
    upgrade_running_requests: bool,
) -> None:
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
        _emit({"type": "missing_secrets", "count": len(missing), "names": missing})

    try:
        image_refs = _prepare_images(functions)
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
        image_refs=image_refs,
    )


def _prepare_images(functions: list[Function]) -> dict[str, ImageRef]:
    """Build every unique function image through the sandbox image builder.

    Returns a `{Image._id: ImageRef(kind="sandbox_template", id=template_name)}`
    map that the deploy step plumbs into each function's manifest. The platform
    reads `image_ref` and the dataplane resolves the function's image directly
    to the registered sandbox-template filesystem snapshot.
    """
    image_refs: dict[str, ImageRef] = {}
    seen: set[str] = set()
    for application in filter_applications(functions):
        for fn in functions_for_application(application, functions):
            image = fn._function_config.image
            if image._id in seen:
                continue
            seen.add(image._id)

            _emit({"type": "build_start", "image": image.name})
            try:
                result = build_sandbox_image(image, emit=_emit)
            except SandboxImageError as error:
                _emit(
                    {
                        "type": "build_failed",
                        "image": image.name,
                        "error": _format_build_failure_message(image.name, error),
                    }
                )
                sys.exit(1)

            # Carry the template *name*, not the public id — the dataplane
            # resolves function images through the existing internal
            # `/projects/{ns}/sandbox-templates/by-name/{name}` endpoint, the
            # same path sandbox allocations already use. Falling back to
            # `image.name` keeps the contract stable if the platform stops
            # echoing the name in its create response.
            template_name = result.get("name") or image.name
            if not template_name:
                _emit(
                    {
                        "type": "build_failed",
                        "image": image.name,
                        "error": (
                            f"image '{image.name}' built but neither the platform "
                            "response nor the Image carried a template name"
                        ),
                    }
                )
                sys.exit(1)
            image_refs[image._id] = ImageRef(kind="sandbox_template", id=template_name)

    _emit({"type": "build_done"})
    return image_refs


def _deploy_applications(
    api_client,
    api_url: str,
    application_file_path: str,
    upgrade_running_requests: bool,
    functions: list[Function],
    image_refs: dict[str, ImageRef] | None = None,
):
    _emit({"type": "status", "message": "Deploying applications..."})

    try:
        deploy_applications(
            applications_file_path=application_file_path,
            upgrade_running_requests=upgrade_running_requests,
            load_source_dir_modules=False,
            api_client=api_client,
            image_refs=image_refs,
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
    args = parser.parse_args()

    try:
        deploy(
            application_file_path=args.application_file_path,
            upgrade_running_requests=args.upgrade_running_requests,
        )
    except SystemExit:
        raise
    except Exception as e:
        _emit(_error_event("deploy failed", e))
        sys.exit(1)


if __name__ == "__main__":
    deploy_entrypoint()
