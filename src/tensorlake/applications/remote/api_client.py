import json
import os

from pydantic import BaseModel

from tensorlake.applications.interface.exceptions import (
    InternalError,
    RemoteAPIError,
)
from tensorlake.applications.interface.exceptions import (
    RequestError as RequestErrorException,
)
from tensorlake.applications.interface.exceptions import (
    RequestFailed,
    RequestNotFinished,
    SDKUsageError,
    TensorlakeError,
)
from tensorlake.applications.remote.manifests.application import ApplicationManifest

try:
    from tensorlake_rust_cloud_sdk import CloudApiClient as RustCloudApiClient
    from tensorlake_rust_cloud_sdk import CloudApiClientError as RustCloudApiClientError

    _RUST_CLOUD_CLIENT_AVAILABLE = True
except Exception:
    RustCloudApiClient = None
    RustCloudApiClientError = None
    _RUST_CLOUD_CLIENT_AVAILABLE = False

_API_NAMESPACE_FROM_ENV: str | None = os.getenv("INDEXIFY_NAMESPACE", "default")
_API_URL_FROM_ENV: str = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
_API_KEY_ENVIRONMENT_VARIABLE_NAME = "TENSORLAKE_API_KEY"
_API_KEY_FROM_ENV: str | None = os.getenv(_API_KEY_ENVIRONMENT_VARIABLE_NAME)


class Application(BaseModel):
    name: str
    description: str
    tags: dict[str, str]
    version: str
    tombstoned: bool = False
    created_at: int | None = None


class RequestError(BaseModel):
    function_name: str
    message: str


class RequestMetadata(BaseModel):
    id: str
    # dict when failure outcome
    # str when success outcome
    # None when not finished
    outcome: dict | str | None = None
    application_version: str
    created_at: int
    request_error: RequestError | None = None


class RequestInput(BaseModel):
    data: bytes
    content_type: str
    name: str


class RequestOutput(BaseModel):
    serialized_value: bytes
    content_type: str


def _raise_as_tensorlake_error(e: Exception) -> None:
    """Converts various exceptions into TensorlakeError subclasses.

    Re-raises the original TensorlakeError without modifications.
    Raises SDKUsageError if the provided API credentials are not valid or authorized.
    Raises RemoteAPIError for HTTP errors.
    Raises TensorlakeError on other errors.
    """
    if isinstance(e, TensorlakeError):
        raise  # Propagate original TensorlakeError without modifications.

    if (
        RustCloudApiClientError is not None
        and isinstance(e, RustCloudApiClientError)
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
                f"Please check your `tensorlake login` status or '{_API_KEY_ENVIRONMENT_VARIABLE_NAME}' environment variable."
            ) from None
        elif status_code == 403:
            raise SDKUsageError(
                "The provided Tensorlake API credentials are not authorized for the requested operation."
            ) from None
        elif status_code is not None:
            raise RemoteAPIError(status_code=status_code, message=message) from None
        elif kind == "sdk_usage":
            raise SDKUsageError(message) from None
        else:
            raise InternalError(message) from None

    raise InternalError(str(e)) from e


class APIClient:
    def __init__(
        self,
        api_url: str = _API_URL_FROM_ENV,
        api_key: str | None = _API_KEY_FROM_ENV,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _API_NAMESPACE_FROM_ENV,
    ):
        self._namespace: str | None = namespace
        self._api_url: str = api_url
        self._api_key: str | None = api_key
        self._organization_id: str | None = organization_id
        self._project_id: str | None = project_id
        if not _RUST_CLOUD_CLIENT_AVAILABLE:
            raise InternalError(
                "Rust Cloud SDK client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        try:
            self._rust_client = RustCloudApiClient(
                api_url=self._api_url,
                api_key=self._api_key,
                organization_id=self._organization_id,
                project_id=self._project_id,
                namespace=self._namespace,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def __enter__(self) -> "APIClient":
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point where resources are freed."""
        self.close()

    def close(self):
        """Frees resources held by the API client."""
        self._rust_client.close()

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_running_requests: bool,
    ) -> None:
        """Creates or updates an application in the namespace.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        try:
            self._rust_client.upsert_application(
                manifest_json=manifest_json,
                code_zip=code_zip,
                upgrade_running_requests=upgrade_running_requests,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def delete_application(
        self,
        application_name: str,
    ) -> None:
        """
        Deletes an application and all of its requests from the namespace.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        try:
            self._rust_client.delete_application(application_name=application_name)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def applications(self) -> list[Application]:
        """Returns list of all existing applications.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        try:
            response_json: str = self._rust_client.applications_json()
            application_jsons: list[dict] = json.loads(response_json)["applications"]
            return [Application.model_validate(app) for app in application_jsons]
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def application(self, application_name: str) -> ApplicationManifest:
        """Returns manifest json dict for a specific application.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        try:
            response_json: str = self._rust_client.application_manifest_json(
                application_name=application_name
            )
            return ApplicationManifest.model_validate_json(response_json)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def run_request(
        self,
        application_name: str,
        inputs: list[RequestInput],
    ) -> str:
        """Runs a request for a specific application with given inputs.

        Returns the request ID.
        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        try:
            rust_inputs: list[tuple[str, bytes, str]] = [
                (part.name, part.data, part.content_type) for part in inputs
            ]
            return self._rust_client.run_request(
                application_name=application_name,
                inputs=rust_inputs,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def wait_on_request_completion(
        self,
        application_name: str,
        request_id: str,
    ):
        """Waits for a request to complete by connecting to its progress SSE stream.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        try:
            self._rust_client.wait_on_request_completion(
                application_name=application_name,
                request_id=request_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def request_output(
        self,
        application_name: str,
        request_id: str,
    ) -> RequestOutput:
        """Gets the output of a completed request.

        Raises RequestNotFinished if the request is not yet finished.
        Raises RequestFailed if the request has failed.
        Raises RemoteAPIError if failed to get request output from remote API.
        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        try:
            request_metadata_json: str = self._rust_client.request_metadata_json(
                application_name=application_name,
                request_id=request_id,
            )
            request_metadata: RequestMetadata = RequestMetadata.model_validate_json(
                request_metadata_json
            )

            if request_metadata.outcome is None:
                raise RequestNotFinished()

            if isinstance(request_metadata.outcome, dict):
                if request_metadata.request_error is None:
                    raise RequestFailed(request_metadata.outcome["failure"])
                else:
                    raise RequestErrorException(request_metadata.request_error.message)

            serialized_value_raw, content_type = self._rust_client.request_output_bytes(
                application_name=application_name,
                request_id=request_id,
            )
            if isinstance(serialized_value_raw, (bytes, bytearray, memoryview)):
                serialized_value: bytes = bytes(serialized_value_raw)
            elif isinstance(serialized_value_raw, list):
                serialized_value = bytes(serialized_value_raw)
            else:
                raise InternalError(
                    "Unexpected request output payload type from Rust Cloud SDK: "
                    f"{type(serialized_value_raw).__name__}"
                )
            return RequestOutput(
                serialized_value=serialized_value,
                content_type=content_type,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)
