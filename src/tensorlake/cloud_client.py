"""Single Python wrapper around the Rust Cloud SDK (tensorlake._cloud_sdk).

All Python code that needs to communicate with the TensorLake Cloud API
should use CloudClient. The Rust SDK handles HTTP, auth, and serialization.
"""

import importlib

_IMPORT_ERROR: Exception | None = None
_RustClient = None
_RustClientError = None
_AVAILABLE = False

for _module_name in ("tensorlake._cloud_sdk", "_cloud_sdk"):
    try:
        _mod = importlib.import_module(_module_name)
        _RustClient = _mod.CloudApiClient
        _RustClientError = _mod.CloudApiClientError
        _AVAILABLE = True
        _IMPORT_ERROR = None
        break
    except Exception as e:
        _IMPORT_ERROR = e

_API_KEY_ENV_VAR = "TENSORLAKE_API_KEY"


def _raise_as_tensorlake_error(e: Exception) -> None:
    """Convert Rust SDK exceptions into the TensorlakeError hierarchy."""
    # Lazy import to avoid circular dependency with applications package.
    from tensorlake.applications.interface.exceptions import (
        InternalError,
        RemoteAPIError,
        RemoteTransportError,
        SDKUsageError,
        TensorlakeError,
    )

    if isinstance(e, TensorlakeError):
        raise

    if (
        _RustClientError is not None
        and isinstance(e, _RustClientError)
        and len(e.args) > 0
    ):
        kind: str | None = None
        status_code: int | None = None
        message: str = str(e)

        if len(e.args) == 3:
            kind, status_code, message = e.args
        elif len(e.args) == 1 and isinstance(e.args[0], tuple) and len(e.args[0]) == 3:
            kind, status_code, message = e.args[0]

        if status_code == 401:
            raise SDKUsageError(
                "The provided Tensorlake API credentials are not valid. "
                f"Please check your `tensorlake login` status or '{_API_KEY_ENV_VAR}' environment variable."
            ) from None
        elif status_code == 403:
            raise SDKUsageError(
                "The provided Tensorlake API credentials are not authorized for the requested operation."
            ) from None
        elif status_code is not None:
            raise RemoteAPIError(status_code=status_code, message=message) from None
        elif kind == "connection":
            raise RemoteTransportError(
                f"Connection error while communicating with Tensorlake API: {message}"
            ) from None
        elif kind == "sdk_usage":
            raise SDKUsageError(message) from None
        else:
            raise InternalError(message) from e

    raise InternalError(str(e)) from e


class CloudClient:
    """Thin wrapper around the Rust Cloud SDK PyO3 client.

    Provides consistent error handling: all Rust SDK exceptions are converted
    into the TensorlakeError hierarchy.
    """

    def __init__(
        self,
        api_url: str,
        api_key: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = None,
    ):
        if not _AVAILABLE:
            from tensorlake.applications.interface.exceptions import InternalError

            details = (
                f" Import error: {type(_IMPORT_ERROR).__name__}: {_IMPORT_ERROR}"
                if _IMPORT_ERROR is not None
                else ""
            )
            raise InternalError(
                "Rust Cloud SDK client is required but unavailable. "
                f"Build/install it with `make build_rust_py_client`.{details}"
            )
        try:
            self._client = _RustClient(
                api_url=api_url,
                api_key=api_key,
                organization_id=organization_id,
                project_id=project_id,
                namespace=namespace,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def close(self):
        self._client.close()

    def __enter__(self) -> "CloudClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # -- Application operations --

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_running_requests: bool,
    ) -> None:
        try:
            self._client.upsert_application(
                manifest_json=manifest_json,
                code_zip=code_zip,
                upgrade_running_requests=upgrade_running_requests,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def delete_application(self, application_name: str) -> None:
        try:
            self._client.delete_application(application_name=application_name)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def applications_json(self) -> str:
        try:
            return self._client.applications_json()
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def application_manifest_json(self, application_name: str) -> str:
        try:
            return self._client.application_manifest_json(
                application_name=application_name
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Request operations --

    def run_request(
        self,
        application_name: str,
        inputs: list[tuple[str, bytes, str]],
    ) -> str:
        try:
            return self._client.run_request(
                application_name=application_name,
                inputs=inputs,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def wait_on_request_completion(
        self,
        application_name: str,
        request_id: str,
    ) -> None:
        try:
            self._client.wait_on_request_completion(
                application_name=application_name,
                request_id=request_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def request_metadata_json(
        self,
        application_name: str,
        request_id: str,
    ) -> str:
        try:
            return self._client.request_metadata_json(
                application_name=application_name,
                request_id=request_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def request_output_bytes(
        self,
        application_name: str,
        request_id: str,
    ) -> tuple:
        try:
            return self._client.request_output_bytes(
                application_name=application_name,
                request_id=request_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Auth operations --

    def introspect_api_key_json(self) -> str:
        try:
            return self._client.introspect_api_key_json()
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Secrets operations --

    def list_secrets_json(
        self,
        organization_id: str,
        project_id: str,
        page_size: int = 100,
    ) -> str:
        try:
            return self._client.list_secrets_json(
                organization_id=organization_id,
                project_id=project_id,
                page_size=page_size,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Image build operations --

    def start_image_build(
        self,
        build_service_path: str,
        application_name: str,
        application_version: str,
        function_name: str,
        image_name: str,
        image_id: str,
        build_context: bytes,
    ) -> str:
        try:
            return self._client.start_image_build(
                build_service_path,
                application_name,
                application_version,
                function_name,
                image_name,
                image_id,
                build_context,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def create_application_build(
        self,
        build_service_path: str,
        request_json: str,
        image_contexts: list[tuple[str, bytes]],
    ) -> str:
        try:
            return self._client.create_application_build(
                build_service_path,
                request_json,
                image_contexts,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def application_build_info_json(
        self,
        build_service_path: str,
        application_build_id: str,
    ) -> str:
        try:
            return self._client.application_build_info_json(
                build_service_path,
                application_build_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def cancel_application_build(
        self,
        build_service_path: str,
        application_build_id: str,
    ) -> str:
        try:
            return self._client.cancel_application_build(
                build_service_path,
                application_build_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def build_info_json(self, build_service_path: str, build_id: str) -> str:
        try:
            return self._client.build_info_json(build_service_path, build_id)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def cancel_build(self, build_service_path: str, build_id: str) -> None:
        try:
            self._client.cancel_build(build_service_path, build_id)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def stream_build_logs_json(
        self, build_service_path: str, build_id: str
    ) -> list[str]:
        try:
            return self._client.stream_build_logs_json(build_service_path, build_id)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def stream_build_logs_to_stderr(
        self, build_service_path: str, build_id: str
    ) -> None:
        try:
            self._client.stream_build_logs_to_stderr(build_service_path, build_id)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def stream_build_logs_to_stderr_prefixed(
        self,
        build_service_path: str,
        build_id: str,
        prefix: str,
        color: str | None = None,
    ) -> None:
        try:
            self._client.stream_build_logs_to_stderr_prefixed(
                build_service_path,
                build_id,
                prefix,
                color,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)
