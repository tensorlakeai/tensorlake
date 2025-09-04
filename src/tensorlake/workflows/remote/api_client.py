import json
import logging
import os
from typing import Any, Dict, Generator, Iterator, List, Optional, Union

import httpx
from httpx_sse import ServerSentEvent, connect_sse
from pydantic import BaseModel, Field
from rich import print  # TODO: Migrate to use click.echo

from tensorlake.utils.http_client import (
    _TRANSIENT_HTTPX_ERRORS,
    get_httpx_client,
)
from tensorlake.utils.retries import exponential_backoff

from ..interface.exceptions import (
    RemoteAPIException,
    RequestException,
    RequestNotFinished,
)

logger = logging.getLogger("tensorlake")


_API_URL_FROM_ENV: str = os.getenv("INDEXIFY_URL", "https://api.tensorlake.ai")
_API_KEY_FROM_ENV: str = os.getenv("TENSORLAKE_API_KEY")


class GraphOutputMetadata(BaseModel):
    id: str
    num_outputs: int
    compute_fn: str


class RequestProgress(BaseModel):
    pending_tasks: int
    successful_tasks: int
    failed_tasks: int


class RequestError(BaseModel):
    function_name: str
    message: str


class ShallowRequestMetadata(BaseModel):
    id: str
    status: str
    outcome: str
    created_at: int


class Allocation(BaseModel):
    id: str
    server_id: str = Field(alias="executor_id")
    container_id: str = Field(alias="function_executor_id")
    created_at: int
    outcome: Optional[str] = None
    attempt_number: int


class Task(BaseModel):
    id: str
    status: str
    outcome: str
    created_at: int = Field(alias="creation_time_ns")
    allocations: Optional[List[Allocation]] = None


class RequestMetadata(BaseModel):
    id: str
    completed: bool
    status: str
    outcome: str
    failure_reason: str
    outstanding_tasks: int
    request_progress: dict[str, RequestProgress]
    graph_version: str
    created_at: str
    request_error: Optional[RequestError] = None
    outputs: List[GraphOutputMetadata] = []
    created_at: int


class RequestCreatedEvent(BaseModel):
    request_id: str


class RequestFinishedEvent(BaseModel):
    request_id: str


class RequestProgressPayload(BaseModel):
    request_id: str
    fn_name: str
    task_id: str
    allocation_id: Optional[str] = None
    executor_id: Optional[str] = None
    outcome: Optional[str] = None


class WorkflowEvent(BaseModel):
    event_name: str
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    payload: Union[RequestCreatedEvent, RequestProgressPayload, RequestFinishedEvent]

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
        namespace: str = "default",
        api_url: str = _API_URL_FROM_ENV,
        api_key: str | None = _API_KEY_FROM_ENV,
    ):
        self._client: httpx.Client = get_httpx_client(
            config_path=None, make_async=False
        )
        self._namespace: str = namespace
        self._api_url: str = api_url
        self._api_key: str | None = api_key

    def __enter__(self) -> "APIClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._client.close()

    def _request(self, method: str, **kwargs) -> httpx.Response:
        try:
            # No request timeouts for now.
            # This is only correct for when we're waiting for application request completion.
            request = self._client.build_request(method, timeout=None, **kwargs)
            response = self._client.send(request)
            logger.debug(
                "Indexify: %r %r => %r",
                request,
                kwargs.get("data", {}),
                response,
            )
            status_code = response.status_code
            if status_code >= 400:
                raise RemoteAPIException(status_code=status_code, message=response.text)
        except httpx.RequestError as e:
            message = f"Make sure the server is running and accessible at {self._api_url}, {e}"
            raise RemoteAPIException(status_code=503, message=message)
        return response

    def _add_api_key(self, kwargs):
        if self._api_key:
            if "headers" not in kwargs:
                kwargs["headers"] = {}
            kwargs["headers"]["Authorization"] = f"Bearer {self._api_key}"

    @exponential_backoff(
        max_retries=5,
        retryable_exceptions=(RemoteAPIException,),
        is_retryable=lambda e: isinstance(e, RemoteAPIException)
        and e.status_code == 503,
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

    def _close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close()

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_tasks_to_latest_version: bool,
    ):
        response = self._post(
            f"v1/namespaces/{self._namespace}/applications",
            files={"code": code_zip},
            data={
                "code_content_type": "application/zip",
                "application": manifest_json,
                "upgrade_tasks_to_latest_version": upgrade_tasks_to_latest_version,
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

    def tasks(self, application_name: str, request_id: str) -> List[Task]:
        response = self._get(
            f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}/tasks"
        )
        return [Task(**task) for task in response.json()["tasks"]]

    def application_manifests(self) -> List[Dict[str, Any]]:
        """Returns manifest json dicts for all existing applications."""
        return self._get(f"v1/namespaces/{self._namespace}/applications").json()[
            "applications"
        ]

    def application_manifest(self, application_name: str) -> Dict[str, Any]:
        """Returns manifest json dict for a specific application."""
        return self._get(
            f"v1/namespaces/{self._namespace}/applications/{application_name}"
        )

    def logs(
        self, application_name: str, invocation_id: str, allocation_id: str, file: str
    ) -> Optional[str]:
        try:
            response = self._get(
                f"namespaces/{self._namespace}/applications/{application_name}/invocations/{invocation_id}/allocations/{allocation_id}/logs/{file}"
            )
            response.raise_for_status()
            return response.content.decode("utf-8")
        except RemoteAPIException as e:
            print(f"failed to fetch logs: {e}")
            return None

    def requests(self, application_name: str) -> List[RequestMetadata]:
        response = self._get(
            f"v1/namespaces/{self._namespace}/applications/{application_name}/requests"
        )
        requests: List[ShallowRequestMetadata] = []
        for request in response.json()["requests"]:
            requests.append(ShallowRequestMetadata(**request))

        return requests

    def request(self, application_name: str, request_id: str) -> RequestMetadata:
        response = self._get(
            f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}"
        )
        return RequestMetadata(**response.json())

    def call(
        self,
        application_name: str,
        payload: bytes,
        payload_content_type: str,
        block_until_done: bool = False,
    ) -> str:
        if not block_until_done:
            return self._call(application_name, payload, payload_content_type)
        events = self.call_stream(application_name, payload, payload_content_type)
        try:
            while True:
                print(str(next(events)))
        except StopIteration as result:
            # TODO: Once we only support Python >= 3.13, we can just return events.close().
            events.close()
            return result.value

    def _call(
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
            "data": payload,
        }
        response = self._post(
            f"v1/namespaces/{self._namespace}/applications/{application_name}",
            **kwargs,
        )
        return response.json()["request_id"]

    def call_stream(
        self,
        application_name: str,
        payload: bytes,
        payload_content_type: str,
    ) -> Generator[WorkflowEvent, None, str]:
        kwargs = {
            "headers": {
                "Content-Type": payload_content_type,
            },
            "data": payload,
        }
        self._add_api_key(kwargs)
        request_id: Optional[str] = None
        try:
            with connect_sse(
                self._client,
                "POST",
                f"{self._api_url}/v1/namespaces/{self._namespace}/applications/{application_name}",
                **kwargs,
            ) as event_source:
                if not event_source.response.is_success:
                    resp = event_source.response.read().decode("utf-8")
                    raise Exception(f"failed to wait for request: {resp}")
                for sse in event_source.iter_sse():
                    for event in self._parse_request_events_from_sse_event(
                        application_name, sse
                    ):
                        if request_id is None:
                            request_id = event.payload.request_id
                        yield event

        except _TRANSIENT_HTTPX_ERRORS:
            if request_id is None:
                print("request ID is unknown, cannot block until done")
                raise

            self.wait_on_request_completion(application_name, request_id, **kwargs)

        if request_id is None:
            raise Exception("request ID not returned")

        return request_id

    def _parse_request_events_from_sse_event(
        self, application_name: str, sse: ServerSentEvent
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

            # Log failures with their stdout/stderr
            if (
                event.event_name == "TaskCompleted"
                and isinstance(event.payload, RequestProgressPayload)
                and event.payload.outcome == "failure"
            ):
                event.stdout = self.logs(
                    application_name,
                    event.payload.request_id,
                    event.payload.allocation_id,
                    "stdout",
                )

                event.stderr = self.logs(
                    application_name,
                    event.payload.request_id,
                    event.payload.allocation_id,
                    "stderr",
                )

            yield event

    @exponential_backoff(
        max_retries=10,
        retryable_exceptions=_TRANSIENT_HTTPX_ERRORS,
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
            f"{self._api_url}/namespaces/{self._namespace}/applications/{application_name}/invocations/{request_id}/wait",
            **kwargs,
        ) as event_source:
            if not event_source.response.is_success:
                resp = event_source.response.read().decode("utf-8")
                raise Exception(f"failed to wait for request: {resp}")
            for sse in event_source.iter_sse():
                events = self._parse_request_events_from_sse_event(
                    application_name, sse
                )
                for event in events:
                    if event.event_name == "RequestFinished":
                        break

    def _download_outputs(
        self,
        application_name: str,
        request_id: str,
        function_name: str,
        output_metadata: GraphOutputMetadata,
    ) -> List[bytes]:
        outputs: List[bytes] = []
        for i in range(output_metadata.num_outputs):
            response = self._get(
                f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}/output/{function_name}/id/{output_metadata.id}/index/{i}",
            )
            response.raise_for_status()
            outputs.append(response.content)
        return outputs

    def function_outputs(
        self,
        application_name: str,
        request_id: str,
        function_name: str,
    ) -> List[Any]:
        response = self._get(
            f"v1/namespaces/{self._namespace}/applications/{application_name}/requests/{request_id}",
        )
        response.raise_for_status()
        request = RequestMetadata(**response.json())
        if request.status in ["pending", "running"]:
            raise RequestNotFinished()

        if request.request_error is not None:
            raise RequestException(request.request_error.message)

        all_outputs = []
        for output_metadata in request.outputs:
            if output_metadata.compute_fn == function_name:
                all_outputs.extend(
                    self._download_outputs(
                        application_name, request_id, function_name, output_metadata
                    )
                )
        return all_outputs
