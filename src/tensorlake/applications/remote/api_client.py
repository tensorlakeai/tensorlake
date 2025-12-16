import os
from typing import Any, Callable

import httpx
from httpx_sse import EventSource, ServerSentEvent, connect_sse
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
from tensorlake.utils.retries import exponential_backoff

# Timeout used by default for HTTP requests that don't run customer code
# or download large amounts of data.
_DEFAULT_HTTP_REQUEST_TIMEOUT_SEC = 5.0

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


class RequestOutput(BaseModel):
    serialized_value: bytes
    content_type: str


def _print_retry(e: BaseException, sleep_time: float, retries: int):
    # Print each retry to keep user's UX interactive.
    print(
        f"Retrying remote API request after {sleep_time:.2f} seconds. Retry count: {retries}. Retryable exception: {e}",
    )


# We use _is_retriable_exception to decide which exceptions are retriable.
_RETRIABLE_EXCEPTIONS = (Exception,)


def _is_retriable_exception(e: Exception) -> bool:
    if isinstance(e, RemoteAPIError):
        # 502 Service Unavailable is returned by reverse proxies when the backend server is not available.
        # i.e. when a single replica Server is getting deployed. We also convert all transient httpx exceptions
        # into it.
        if e.status_code == 502:
            return True
        # 503 Service Unavailable is returned by reverse proxies when the backend server is not available.
        # i.e. when a single replica Server is getting deployed. We also convert all transient httpx exceptions
        # into it.
        if e.status_code == 503:
            return True
        # Server timeout or client side timeout.
        if e.status_code == 504:
            return True

    return False


def _raise_as_tensorlake_error(e: Exception) -> None:
    """Converts various exceptions into TensorlakeError subclasses.

    Re-raises the original TensorlakeError without modifications.
    Raises SDKUsageError if the provided API credentials are not valid or authorized.
    Raises RemoteAPIError for HTTP errors.
    Raises TensorlakeError on other errors.
    """
    if isinstance(e, TensorlakeError):
        raise  # Propagate original TensorlakeError without modifications.

    # Convert all transient httpx exceptions into RemoteAPIError with 503 status code
    # which indicates Service Temporarily Unavailable. Similar meaning.
    if isinstance(e, (httpx.NetworkError, httpx.RemoteProtocolError)):
        raise RemoteAPIError(
            status_code=503, message=f"Transient HTTP error: {str(e)}"
        ) from e

    # Convert client side timeout into HTTP timeout error.
    if isinstance(e, httpx.TimeoutException):
        raise RemoteAPIError(
            status_code=504, message=f"Request timed out: {str(e)}"
        ) from e

    if isinstance(e, httpx.HTTPStatusError):
        status_code: int = e.response.status_code

        try:
            response_text: str = e.response.text
        except httpx.ResponseNotRead:
            # This exceptions is raised when response content has not been read yet.
            # because the response is in streaming mode. We have to call .read() first.
            try:
                e.response.read()
                response_text: str = e.response.text
            except Exception:
                response_text: str = "Failed to read response text"

        if status_code == 401:
            raise SDKUsageError(
                "The provided Tensorlake API credentials are not valid. "
                f"Please check your `tensorlake login` status or '{_API_KEY_ENVIRONMENT_VARIABLE_NAME}' environment variable."
            ) from None
        elif status_code == 403:
            raise SDKUsageError(
                "The provided Tensorlake API credentials are not authorized for the requested operation."
            ) from None
        else:
            message: str = f"HTTP request failed: {response_text}"
            raise RemoteAPIError(status_code=status_code, message=message) from e

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
        self._client: httpx.Client = httpx.Client(
            timeout=_DEFAULT_HTTP_REQUEST_TIMEOUT_SEC
        )
        self._namespace: str | None = namespace
        self._api_url: str = api_url
        self._api_key: str | None = api_key
        self._organization_id: str | None = organization_id
        self._project_id: str | None = project_id

    def __enter__(self) -> "APIClient":
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point where resources are freed."""
        self.close()

    def close(self):
        """Frees resources held by the API client."""
        self._client.close()

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
        self._run_request(
            self._client.build_request(
                "POST",
                url=self._endpoint_url(f"v1/namespaces/{self._namespace}/applications"),
                files={"code": code_zip},
                data={
                    "code_content_type": "application/zip",
                    "application": manifest_json,
                    "upgrade_requests_to_latest_code": upgrade_running_requests,
                },
            )
        )

    def delete_application(
        self,
        application_name: str,
    ) -> None:
        """
        Deletes an application and all of its requests from the namespace.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        self._run_request(
            self._client.build_request(
                "DELETE",
                url=self._endpoint_url(
                    f"v1/namespaces/{self._namespace}/applications/{application_name}"
                ),
            )
        )

    def applications(self) -> list[Application]:
        """Returns list of all existing applications.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        applications_response: httpx.Response = self._run_request(
            self._client.build_request(
                "GET",
                url=self._endpoint_url(f"v1/namespaces/{self._namespace}/applications"),
            )
        )
        try:
            application_jsons: list[dict] = applications_response.json()["applications"]

            return [Application.model_validate(app) for app in application_jsons]
        except Exception as e:
            raise InternalError(
                f"failed to parse applications list response: {applications_response.text}"
            ) from e

    def application(self, application_name: str) -> ApplicationManifest:
        """Returns manifest json dict for a specific application.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        application_response: httpx.Response = self._run_request(
            self._client.build_request(
                "GET",
                url=self._endpoint_url(
                    f"v1/namespaces/{self._namespace}/applications/{application_name}"
                ),
            )
        )
        try:
            return ApplicationManifest.model_validate_json(application_response.text)
        except Exception as e:
            raise InternalError(
                f"failed to parse application response: {application_response.text}"
            ) from e

    def run_request(
        self,
        application_name: str,
        input: bytes,
        input_content_type: str,
    ) -> str:
        """Runs a request for a specific application with given input.

        Returns the request ID.
        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        response: httpx.Response = self._run_request(
            self._client.build_request(
                "POST",
                url=self._endpoint_url(
                    f"v1/namespaces/{self._namespace}/applications/{application_name}"
                ),
                headers={
                    "Content-Type": input_content_type,
                    "Accept": "application/json",
                },
                content=input,
            )
        )

        try:
            return response.json()["request_id"]
        except Exception as e:
            raise InternalError(
                f"failed to parse run request response: {response.text}"
            ) from e

    def wait_on_request_completion(
        self,
        application_name: str,
        request_id: str,
    ):
        """Waits for a request to complete by connecting to its progress SSE stream.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """

        def event_processor(sse: ServerSentEvent) -> None | bool:
            event: dict[str, Any] = sse.json()
            # Finish processing when we see RequestFinished event.
            if "RequestFinished" in event:
                return True

        self._run_sse_stream(
            method="GET",
            url=self._endpoint_url(
                f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}/progress"
            ),
            timeout=None,  # No timeout as we wait for customer code completion
            event_processor=event_processor,
        )

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
        request_metadata_response: httpx.Response = self._run_request(
            self._client.build_request(
                "GET",
                url=self._endpoint_url(
                    f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}"
                ),
            )
        )

        try:
            request_metadata: RequestMetadata = RequestMetadata.model_validate_json(
                request_metadata_response.text
            )
        except Exception as e:
            raise InternalError(
                f"failed to parse request metadata response: {request_metadata_response.text}"
            ) from e

        if request_metadata.outcome is None:
            raise RequestNotFinished()

        if isinstance(request_metadata.outcome, dict):
            if request_metadata.request_error is None:
                raise RequestFailed(request_metadata.outcome["failure"])
            else:
                raise RequestErrorException(request_metadata.request_error.message)

        # request.outcome is str at this point so the request is finished successfully and its output is available.
        request_output_response: httpx.Response = self._run_request(
            self._client.build_request(
                "GET",
                url=self._endpoint_url(
                    f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}/output"
                ),
                timeout=None,  # No timeout as we download customer data of any size
            )
        )
        return RequestOutput(
            serialized_value=request_output_response.content,
            content_type=request_output_response.headers.get("Content-Type", ""),
        )

    @exponential_backoff(
        max_retries=5,
        retryable_exceptions=_RETRIABLE_EXCEPTIONS,
        is_retryable=_is_retriable_exception,
        on_retry=_print_retry,
    )
    def _run_request(self, request: httpx.Request) -> httpx.Response:
        """Sends an HTTP request and returns the response.

        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        self._add_auth_headers(request.headers)

        try:
            response: httpx.Response = self._client.send(request)
            response.raise_for_status()
        except Exception as e:
            _raise_as_tensorlake_error(e)

        return response

    @exponential_backoff(
        max_retries=10,  # Give extra retries for SSE streams because they have much higher change of getting disrupted due to transient errors
        retryable_exceptions=_RETRIABLE_EXCEPTIONS,
        is_retryable=_is_retriable_exception,
        on_retry=_print_retry,
    )
    def _run_sse_stream(
        self,
        method: str,
        url: str,
        timeout: float | None,
        event_processor: Callable[[ServerSentEvent], None | Any],
    ) -> Any:
        """Sends an HTTP request to connect to an SSE stream and calls the event processor for each event.

        If the event processor returns non None value then the stream processing stops and the non None value is returned.
        Raises SDKUsageError if the client configuration is not valid for the operation.
        Raises TensorlakeError on other errors.
        """
        auth_headers: dict[str, str] = {}
        self._add_auth_headers(auth_headers)

        try:
            with connect_sse(
                self._client,
                method=method,
                url=url,
                headers=auth_headers,
                timeout=timeout,
            ) as event_source:
                event_source: EventSource
                event_source.response.raise_for_status()
                for sse in event_source.iter_sse():
                    result: Any | None = event_processor(sse)
                    if result is not None:
                        return result
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def _add_auth_headers(self, headers: dict[str, str]) -> None:
        """Adds authentication headers to the headers dict.

        Doesn't raise any exceptions.
        """
        if self._api_key is not None:
            headers["Authorization"] = f"Bearer {self._api_key}"
        # Add X-Forwarded-Organization-Id and X-Forwarded-Project-Id headers when org/project IDs are provided
        # These are needed when using PAT (API keys get org/project via introspection)
        if self._organization_id is not None:
            headers["X-Forwarded-Organization-Id"] = self._organization_id
        if self._project_id is not None:
            headers["X-Forwarded-Project-Id"] = self._project_id

    def _endpoint_url(self, endpoint: str) -> str:
        return f"{self._api_url}/{endpoint}"
