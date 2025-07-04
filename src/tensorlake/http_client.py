import json
import logging
import os
from typing import Any, Dict, Generator, Iterator, List, Optional, Union

import httpx
from httpx_sse import ServerSentEvent, connect_sse
from pydantic import BaseModel
from rich import print  # TODO: Migrate to use click.echo

from tensorlake.error import ApiException, GraphStillProcessing
from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.function_errors import InvocationError
from tensorlake.functions_sdk.graph import (
    ComputeGraphMetadata,
    Graph,
)
from tensorlake.functions_sdk.graph_serialization import (
    ZIPPED_GRAPH_CODE_CONTENT_TYPE,
    zip_graph_code,
)
from tensorlake.functions_sdk.object_serializer import get_serializer
from tensorlake.functions_sdk.runtime_definition import InvocationMetadata
from tensorlake.settings import DEFAULT_SERVICE_URL
from tensorlake.utils.http_client import (
    _TRANSIENT_HTTPX_ERRORS,
    get_httpx_client,
    get_sync_or_async_client,
)
from tensorlake.utils.retries import exponential_backoff

logger = logging.getLogger("tensorlake")


class InvocationFinishedEvent(BaseModel):
    invocation_id: str


class InvocationEventPayload(BaseModel):
    invocation_id: str
    fn_name: str
    task_id: str
    allocation_id: Optional[str] = None
    executor_id: Optional[str] = None
    outcome: Optional[str] = None


class InvocationEvent(BaseModel):
    event_name: str
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    payload: Union[InvocationEventPayload, InvocationFinishedEvent]

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


class GraphOutputMetadata(BaseModel):
    id: str
    compute_fn: str


class GraphOutputs(BaseModel):
    invocation: InvocationMetadata
    outputs: List[GraphOutputMetadata]
    cursor: Optional[str] = None


def log_retries(e: BaseException, sleep_time: float, retries: int):
    print(
        f"Retrying after {sleep_time:.2f} seconds. Retry count: {retries}. Retryable exception: {e.__repr__()}"
    )


class TensorlakeClient:
    def __init__(
        self,
        service_url: str = DEFAULT_SERVICE_URL,  # service_url is already set from DEFAULT_SERVICE_URL, which reads from env var
        config_path: Optional[str] = None,
        namespace: str = "default",
        api_key: Optional[str] = None,
        **kwargs,
    ):
        self.service_url = service_url
        self._config_path = config_path
        self._client: httpx.Client = get_httpx_client(config_path)

        self.namespace: str = namespace
        self.compute_graphs: List[Graph] = []
        self.labels: dict = {}
        self._service_url = service_url
        self._timeout = kwargs.get("timeout")
        self._graphs: Dict[str, Graph] = {}
        self._api_key = api_key
        if not self._api_key:
            self._api_key = os.getenv("TENSORLAKE_API_KEY")

    def _request(self, method: str, **kwargs) -> httpx.Response:
        try:
            request = self._client.build_request(
                method, timeout=self._timeout, **kwargs
            )
            response = self._client.send(request)
            logger.debug(
                "Indexify: %r %r => %r",
                request,
                kwargs.get("data", {}),
                response,
            )
            status_code = response.status_code
            if status_code >= 400:
                raise ApiException(status_code=status_code, message=response.text)
        except httpx.RequestError as e:
            message = f"Make sure the server is running and accessible at {self._service_url}, {e}"
            raise ApiException(status_code=503, message=message)
        return response

    @classmethod
    def with_mtls(
        cls,
        cert_path: str,
        key_path: str,
        ca_bundle_path: Optional[str] = None,
        service_url: str = DEFAULT_SERVICE_URL,
        *args,
        **kwargs,
    ) -> "TensorlakeClient":
        """
        Create a client with mutual TLS authentication. Also enables HTTP/2,
        which is required for mTLS.
        NOTE: mTLS must be enabled on the Indexify service for this to work.

        :param cert_path: Path to the client certificate. Resolution handled by httpx.
        :param key_path: Path to the client key. Resolution handled by httpx.
        :param args: Arguments to pass to the httpx.Client constructor
        :param kwargs: Keyword arguments to pass to the httpx.Client constructor
        :return: A client with mTLS authentication

        Example usage:
        ```
        from indexify import IndexifyClient

        client = IndexifyClient.with_mtls(
            cert_path="/path/to/cert.pem",
            key_path="/path/to/key.pem",
        )
        assert client.heartbeat() == True
        ```
        """
        if not (cert_path and key_path):
            raise ValueError("Both cert and key must be provided for mTLS")

        client = get_sync_or_async_client(
            cert_path=cert_path, key_path=key_path, ca_bundle_path=ca_bundle_path
        )

        indexify_client = TensorlakeClient(service_url, *args, **kwargs)
        indexify_client._client = client
        return indexify_client

    def _add_api_key(self, kwargs):
        if self._api_key:
            if "headers" not in kwargs:
                kwargs["headers"] = {}
            kwargs["headers"]["Authorization"] = f"Bearer {self._api_key}"

    @exponential_backoff(
        max_retries=5,
        retryable_exceptions=(ApiException,),
        is_retryable=lambda e: isinstance(e, ApiException) and e.status_code == 503,
        on_retry=log_retries,
    )
    def _get(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("GET", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _post(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("POST", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _put(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("PUT", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _delete(self, endpoint: str, **kwargs) -> httpx.Response:
        self._add_api_key(kwargs)
        return self._request("DELETE", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close()

    def register_compute_graph(
        self,
        graph: Graph,
        code_dir_path: str,
        upgrade_tasks_to_latest_version: bool,
    ):
        graph_metadata: ComputeGraphMetadata = graph.definition()
        graph_code: bytes = zip_graph_code(graph=graph, code_dir_path=code_dir_path)
        response = self._post(
            f"namespaces/{self.namespace}/compute_graphs",
            files={"code": graph_code},
            data={
                "compute_graph": graph_metadata.model_dump_json(exclude_none=True),
                "upgrade_tasks_to_latest_version": upgrade_tasks_to_latest_version,
                "code_content_type": ZIPPED_GRAPH_CODE_CONTENT_TYPE,
            },
        )
        response.raise_for_status()
        self._graphs[graph.name] = graph

    def delete_compute_graph(
        self,
        graph_name: str,
    ) -> None:
        """
        Deletes a graph and all of its invocations from the namespace.
        :param graph_name The name of the graph to delete.
        WARNING: This operation is irreversible.
        """
        response = self._delete(
            f"namespaces/{self.namespace}/compute_graphs/{graph_name}",
        )
        response.raise_for_status()

    def graphs(self) -> List[ComputeGraphMetadata]:
        graphs_json = self._get(f"namespaces/{self.namespace}/compute_graphs").json()[
            "compute_graphs"
        ]
        graphs = []
        for graph in graphs_json:
            graphs.append(ComputeGraphMetadata(**graph))

        return graphs

    def graph(self, name: str) -> ComputeGraphMetadata:
        response = self._get(f"namespaces/{self.namespace}/compute_graphs/{name}")
        return ComputeGraphMetadata(**response.json())

    def namespaces(self) -> List[str]:
        response = self._get("namespaces")
        namespaces_dict = response.json()["namespaces"]
        namespaces = []
        for item in namespaces_dict:
            namespaces.append(item["name"])
        return namespaces

    def create_namespace(self, namespace: str):
        self._post("namespaces", json={"name": namespace})

    def logs(
        self, cg_name: str, invocation_id: str, allocation_id: str, file: str
    ) -> Optional[str]:
        try:
            response = self._get(
                f"namespaces/{self.namespace}/compute_graphs/{cg_name}/invocations/{invocation_id}/allocations/{allocation_id}/logs/{file}"
            )
            response.raise_for_status()
            return response.content.decode("utf-8")
        except ApiException as e:
            print(f"failed to fetch logs: {e}")
            return None

    def invocations(self, graph: str) -> List[InvocationMetadata]:
        response = self._get(
            f"namespaces/{self.namespace}/compute_graphs/{graph}/invocations"
        )
        invocations = []
        for invocation in response.json()["invocations"]:
            invocations.append(InvocationMetadata(**invocation))

        return invocations

    def invocation(self, graph: str, invocation: str) -> InvocationMetadata:
        response = self._get(
            f"namespaces/{self.namespace}/compute_graphs/{graph}/invocations/{invocation}"
        )
        return InvocationMetadata(**response.json())

    def replay_invocations(self, graph: str):
        self._post(f"namespaces/{self.namespace}/compute_graphs/{graph}/replay")

    def invoke_graph_with_object(
        self,
        graph: str,
        block_until_done: bool = False,
        input_encoding: str = "cloudpickle",
        **kwargs,
    ) -> str:
        events = self.stream_invoke_graph_with_object(
            graph, block_until_done, input_encoding, **kwargs
        )
        try:
            while True:
                print(str(next(events)))
        except StopIteration as result:
            # TODO: Once we only support Python >= 3.13, we can just return events.close().
            events.close()
            return result.value

    def stream_invoke_graph_with_object(
        self,
        graph: str,
        block_until_done: bool = False,
        input_encoding: str = "cloudpickle",
        **kwargs,
    ) -> Generator[InvocationEvent, None, str]:
        serializer = get_serializer(input_encoding)
        ser_input = serializer.serialize(kwargs)
        params = {"block_until_finish": block_until_done}
        kwargs = {
            "headers": {
                "Content-Type": serializer.content_type,
            },
            "data": ser_input,
            "params": params,
        }
        self._add_api_key(kwargs)
        invocation_id: Optional[str] = None
        try:
            with connect_sse(
                self._client,
                "POST",
                f"{self.service_url}/namespaces/{self.namespace}/compute_graphs/{graph}/invoke_object",
                **kwargs,
            ) as event_source:
                if not event_source.response.is_success:
                    resp = event_source.response.read().decode("utf-8")
                    raise Exception(f"failed to wait for invocation: {resp}")
                for sse in event_source.iter_sse():
                    for event in self._parse_invocation_events_from_sse_event(
                        graph, sse
                    ):
                        if invocation_id is None:
                            invocation_id = event.payload.invocation_id
                        yield event

        except _TRANSIENT_HTTPX_ERRORS:
            if invocation_id is None:
                print("invocation ID is unknown, cannot block until done")
                raise

            if block_until_done:
                self.wait_on_invocation_completion(graph, invocation_id, **kwargs)

        if invocation_id is None:
            raise Exception("invocation ID not returned")

        return invocation_id

    def _parse_invocation_events_from_sse_event(
        self, graph: str, sse: ServerSentEvent
    ) -> Iterator[InvocationEvent]:
        obj = json.loads(sse.data)

        for event_name, event_data in obj.items():
            # Handle InvocationFinished events
            if event_name == "InvocationFinished":
                yield InvocationEvent(
                    event_name=event_name,
                    payload=InvocationFinishedEvent(invocation_id=event_data["id"]),
                )
                continue

            # Handle legacy 'id' events (backwards compatibility)
            if event_name == "id":
                yield InvocationEvent(
                    event_name="InvocationFinished",  # Normalize event name
                    payload=InvocationFinishedEvent(invocation_id=event_data),
                )
                continue

            # Handle all other event types
            event_payload = InvocationEventPayload.model_validate(event_data)
            event = InvocationEvent(event_name=event_name, payload=event_payload)

            # Log failures with their stdout/stderr
            if (
                event.event_name == "TaskCompleted"
                and isinstance(event.payload, InvocationEventPayload)
                and event.payload.outcome == "Failure"
            ):
                event.stdout = self.logs(
                    graph,
                    event.payload.invocation_id,
                    event.payload.allocation_id,
                    "stdout",
                )

                event.stderr = self.logs(
                    graph,
                    event.payload.invocation_id,
                    event.payload.allocation_id,
                    "stderr",
                )

            yield event

    @exponential_backoff(
        max_retries=10,
        retryable_exceptions=_TRANSIENT_HTTPX_ERRORS,
        on_retry=log_retries,
    )
    def wait_on_invocation_completion(
        self,
        graph: str,
        invocation_id: str,
        **kwargs,
    ):
        self._add_api_key(kwargs)
        with connect_sse(
            self._client,
            "GET",
            f"{self.service_url}/namespaces/{self.namespace}/compute_graphs/{graph}/invocations/{invocation_id}/wait",
            **kwargs,
        ) as event_source:
            if not event_source.response.is_success:
                resp = event_source.response.read().decode("utf-8")
                raise Exception(f"failed to wait for invocation: {resp}")
            for sse in event_source.iter_sse():
                events = self._parse_invocation_events_from_sse_event(graph, sse)
                for event in events:
                    if event.event_name == "InvocationFinished":
                        break

    def _download_output(
        self,
        graph: str,
        invocation_id: str,
        fn_name: str,
        output_id: str,
    ) -> TensorlakeData:
        response = self._get(
            f"namespaces/{self.namespace}/compute_graphs/{graph}/invocations/{invocation_id}/fn/{fn_name}/output/{output_id}",
        )
        response.raise_for_status()
        content_type = response.headers.get("Content-Type")
        if content_type == "application/json":
            encoding = "json"
        else:
            encoding = "cloudpickle"
        return TensorlakeData(id=output_id, payload=response.content, encoder=encoding)

    def graph_outputs(
        self,
        graph: str,
        invocation_id: str,
        fn_name: str,
    ) -> List[Any]:
        """
        Returns the extracted objects by a graph for an ingested object. If the extractor name is provided, only the objects extracted by that extractor are returned.
        If the extractor name is not provided, all the extracted objects are returned for the input object.
        graph: str: The name of the graph
        invocation_id: str: The ID of the invocation.
        fn_name: Optional[str]: The name of the function whose output is to be returned if provided
        return: Union[Dict[str, List[Any]], List[Any]]: The extracted objects. If the extractor name is provided, the output is a list of extracted objects by the extractor. If the extractor name is not provided, the output is a dictionary with the extractor name as the key and the extracted objects as the value. If no objects are found, an empty list is returned.
        """
        response = self._get(
            f"namespaces/{self.namespace}/compute_graphs/{graph}/invocations/{invocation_id}/outputs",
        )
        response.raise_for_status()
        graph_outputs = GraphOutputs(**response.json())
        if graph_outputs.invocation.status in ["Pending", "Running"]:
            raise GraphStillProcessing()

        if graph_outputs.invocation.invocation_error is not None:
            raise InvocationError(graph_outputs.invocation.invocation_error.message)

        graph_metadata: ComputeGraphMetadata = self.graph(graph)
        output_encoder = graph_metadata.nodes[fn_name].output_encoder

        outputs = []
        for output in graph_outputs.outputs:
            if output.compute_fn == fn_name:
                indexify_data = self._download_output(
                    graph, invocation_id, fn_name, output.id
                )
                serializer = get_serializer(output_encoder)
                output = serializer.deserialize(indexify_data.payload)
                outputs.append(output)
        return outputs
