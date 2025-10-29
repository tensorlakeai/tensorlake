import json
import logging
import os
from typing import Iterator, List

import httpx
from httpx_sse import ServerSentEvent, connect_sse
from pydantic import BaseModel
from rich import print  # TODO: Migrate to use click.echo

from tensorlake.applications.interface.exceptions import (
    RemoteAPIError,
)
from tensorlake.applications.interface.exceptions import (
    RequestError as RequestErrorException,
)
from tensorlake.applications.interface.exceptions import (
    RequestFailureException,
    RequestNotFinished,
)
from tensorlake.applications.remote.manifests.application import ApplicationManifest
from tensorlake.utils.http_client import (
    TRANSIENT_HTTPX_ERRORS,
    EventHook,
)
from tensorlake.utils.retries import exponential_backoff


# Model for applications returned by the list endpoint
class ApplicationListItem(BaseModel):
    name: str
    description: str
    tags: dict[str, str]
    version: str
    tombstoned: bool = False
    created_at: int | None = None


_API_NAMESPACE_FROM_ENV: str = os.getenv("INDEXIFY_NAMESPACE", "default")
_API_URL_FROM_ENV: str = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
_API_KEY_FROM_ENV: str = os.getenv("TENSORLAKE_API_KEY")


class DataPayload(BaseModel):
    id: str
    path: str
    size: int
    sha256_hash: str


class RequestError(BaseModel):
    function_name: str
    message: str


class Allocation(BaseModel):
    id: str
    function_name: str
    executor_id: str
    function_executor_id: str
    created_at: int
    # dict when failure outcome
    # str when success outcome
    # None when not finished
    outcome: dict | str | None = None
    attempt_number: int
    execution_duration_ms: int | None = None


class FunctionRun(BaseModel):
    id: str
    name: str
    application: str
    namespace: str
    status: str
    outcome: str
    created_at: int
    application_version: str
    allocations: List[Allocation]


class RequestMetadata(BaseModel):
    id: str
    # dict when failure outcome
    # str when success outcome
    # None when not finished
    outcome: dict | str | None = None
    application_version: str
    created_at: int
    request_error: RequestError | None = None
    function_runs: List[FunctionRun]


class RequestCreatedEvent(BaseModel):
    request_id: str


class RequestFinishedEvent(BaseModel):
    request_id: str


class RequestProgressPayload(BaseModel):
    request_id: str
    function_name: str
    function_run_id: str
    allocation_id: str | None = None
    executor_id: str | None = None
    outcome: str | None = None


class WorkflowEvent(BaseModel):
    event_name: str
    stdout: str | None = None
    stderr: str | None = None
    payload: RequestCreatedEvent | RequestProgressPayload | RequestFinishedEvent

    def __str__(self) -> str:
        stdout = (
            ""
            if self.stdout is None
            else f"[bold red]stdout[/bold red]: \n {self.stdout}\n"
        )
        stderr = (
            ""
            if self.stderr is None
            else f"[bold red]stderr[/bold red]: \n {self.stderr}\n"
        )

        return f"{stdout}{stderr}[bold green]{self.event_name}[/bold green]: {self.payload}"


def log_retries(e: BaseException, sleep_time: float, retries: int):
    print(
        f"Retrying after {sleep_time:.2f} seconds. Retry count: {retries}. Retryable exception: {e.__repr__()}"
    )


class APIClient:
    def __init__(
        self,
        api_url: str = _API_URL_FROM_ENV,
        api_key: str | None = _API_KEY_FROM_ENV,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str = _API_NAMESPACE_FROM_ENV,
        event_hooks: dict[str, list[EventHook]] | None = None,
    ):
        self._client: httpx.Client = httpx.Client(event_hooks=event_hooks)

        self._namespace: str = namespace
        self._api_url: str = api_url
        self._api_key: str | None = api_key
        self._organization_id: str | None = organization_id
        self._project_id: str | None = project_id

    def __enter__(self) -> "APIClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._client.close()

    def _request(self, method: str, **kwargs) -> httpx.Response:
        try:
            # No request timeouts for now.
            # This is only correct for when we're waiting for application request completion.
            request = self._client.build_request(method, timeout=None, **kwargs)
            response = self._client.send(request)
            status_code = response.status_code
            if status_code >= 400:
                raise RemoteAPIError(status_code=status_code, message=response.text)
        except httpx.RequestError as e:
            message = f"Make sure the server is running and accessible at {self._api_url}, {e}"
            raise RemoteAPIError(status_code=503, message=message)
        return response

    def _add_api_key(self, kwargs):
        if self._api_key:
            if "headers" not in kwargs:
                kwargs["headers"] = {}
            kwargs["headers"]["Authorization"] = f"Bearer {self._api_key}"

        # Add X-Forwarded-Organization-Id and X-Forwarded-Project-Id headers when org/project IDs are provided
        # These are needed when using PAT (API keys get org/project via introspection)
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        if self._organization_id:
            kwargs["headers"]["X-Forwarded-Organization-Id"] = self._organization_id
        if self._project_id:
            kwargs["headers"]["X-Forwarded-Project-Id"] = self._project_id

    @exponential_backoff(
        max_retries=5,
        retryable_exceptions=(RemoteAPIError,),
        is_retryable=lambda e: isinstance(e, RemoteAPIError) and e.status_code == 503,
        on_retry=log_retries,
    )
    def _get(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("GET", url=f"{self._api_url}/{endpoint}", **kwargs)

    def _post(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("POST", url=f"{self._api_url}/{endpoint}", **kwargs)

    def _put(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("PUT", url=f"{self._api_url}/{endpoint}", **kwargs)

    def _delete(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("DELETE", url=f"{self._api_url}/{endpoint}", **kwargs)

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_running_requests: bool,
    ):
        response = self._post(
            f"v1/namespaces/{self._namespace}/applications",
            files={"code": code_zip},
            data={
                "code_content_type": "application/zip",
                "application": manifest_json,
                "upgrade_requests_to_latest_code": upgrade_running_requests,
            },
        )
        response.raise_for_status()

    def delete_application(
        self,
        application_name: str,
    ) -> None:
        """
        Deletes an application and all of its requests from the namespace.
        :param application_name: The name of the application to delete.
        WARNING: This operation is irreversible.
        """
        response = self._delete(
            f"v1/namespaces/{self._namespace}/applications/{application_name}",
        )
        response.raise_for_status()

    def applications(self) -> List[ApplicationListItem]:
        """Returns list of all existing applications."""
        return [
            ApplicationListItem(**app)
            for app in self._get(
                f"v1/namespaces/{self._namespace}/applications"
            ).json()["applications"]
        ]

    def application(self, application_name: str) -> ApplicationManifest:
        """Returns manifest json dict for a specific application."""
        return ApplicationManifest(
            **self._get(
                f"v1/namespaces/{self._namespace}/applications/{application_name}"
            ).json()
        )

    def call(
        self,
        application_name: str,
        payload: bytes,
        payload_content_type: str,
    ) -> str:
        kwargs = {
            "headers": {
                "Content-Type": payload_content_type,
                "Accept": "application/json",
            },
            "content": payload,
        }
        response = self._post(
            f"v1/namespaces/{self._namespace}/applications/{application_name}",
            **kwargs,
        )
        return response.json()["request_id"]

    def _parse_request_events_from_sse_event(
        self, sse: ServerSentEvent
    ) -> Iterator[WorkflowEvent]:
        obj = json.loads(sse.data)

        for event_name, event_data in obj.items():
            # Handle bare ID events
            if event_name == "id":
                yield WorkflowEvent(
                    event_name="RequestCreated",
                    payload=RequestCreatedEvent(request_id=event_data),
                )
                continue

            # Handle RequestFinished events
            if event_name == "RequestFinished":
                yield WorkflowEvent(
                    event_name=event_name,
                    payload=RequestFinishedEvent(request_id=event_data["request_id"]),
                )
                continue

            # Handle all other event types
            event_payload = RequestProgressPayload.model_validate(event_data)
            event = WorkflowEvent(event_name=event_name, payload=event_payload)

            yield event

    @exponential_backoff(
        max_retries=10,
        retryable_exceptions=TRANSIENT_HTTPX_ERRORS,
        on_retry=log_retries,
    )
    def wait_on_request_completion(
        self,
        application_name: str,
        request_id: str,
        **kwargs,
    ):
        self._add_api_key(kwargs)
        with connect_sse(
            self._client,
            "GET",
            f"{self._api_url}/v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}/progress",
            **kwargs,
        ) as event_source:
            if not event_source.response.is_success:
                resp = event_source.response.read().decode("utf-8")
                raise Exception(f"failed to wait for request: {resp}")
            for sse in event_source.iter_sse():
                events = self._parse_request_events_from_sse_event(sse)
                for event in events:
                    if event.event_name == "RequestFinished":
                        break

    def _download_request_output(
        self,
        application_name: str,
        request_id: str,
    ) -> tuple[bytes, str]:
        response = self._get(
            f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}/output",
        )
        response.raise_for_status()
        return response.content, response.headers.get("Content-Type", "")

    def request_output(
        self,
        application_name: str,
        request_id: str,
    ) -> tuple[bytes, str]:
        response = self._get(
            f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}",
        )
        response.raise_for_status()
        request = RequestMetadata(**response.json())
        if request.outcome is None:
            raise RequestNotFinished()

        if isinstance(request.outcome, dict):
            if request.request_error is None:
                raise RequestFailureException(request.outcome["failure"])
            else:
                raise RequestErrorException(request.request_error.message)

        # request.outcome is str at this point so the request is finished successfully.
        return self._download_request_output(
            application_name=application_name,
            request_id=request_id,
        )
