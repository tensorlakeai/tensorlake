import httpx

from ...interface.exceptions import InternalError, SDKUsageError
from ...interface.request_context import RequestMetrics
from ..http_server.handlers.add_metrics import (
    ADD_METRICS_PATH,
    ADD_METRICS_VERB,
    AddCounterRequest,
    AddMetricsRequest,
    AddTimerRequest,
)


class RequestMetricsHTTPClient(RequestMetrics):
    """HTTP client for accessing request metrics in subprocesses and child threads.

    Thread-safe for use in multiple threaded applications.
    """

    def __init__(
        self,
        request_id: str,
        allocation_id: str,
        function_name: str,
        http_client: httpx.Client,
    ):
        self._request_id: str = request_id
        self._allocation_id: str = allocation_id
        self._function_name: str = function_name
        self._http_client: httpx.Client = http_client

    def timer(self, name: str, value: int | float):
        # If we don't validate user supplied inputs here then there will be a Pydantic validation error
        # below which will raise an InternalError instead of SDKUsageError.
        if not isinstance(name, str):
            raise SDKUsageError(f"Timer name must be a string, got: {name}")
        if not isinstance(value, (int, float)):
            raise SDKUsageError(f"Timer value must be a number, got: {value}")

        request_payload: AddMetricsRequest = AddMetricsRequest(
            request_id=self._request_id,
            allocation_id=self._allocation_id,
            function_name=self._function_name,
            timer=AddTimerRequest(name=name, value=value),
            counter=None,
        )
        self._run_add_request(request_payload)

    def counter(self, name: str, value: int = 1):
        # If we don't validate user supplied inputs here then there will be a Pydantic validation error
        # below which will raise an InternalError instead of SDKUsageError.
        if not isinstance(name, str):
            raise SDKUsageError(f"Counter name must be a string, got: {name}")
        if not isinstance(value, int):
            raise SDKUsageError(f"Counter value must be an int, got: {value}")

        request_payload: AddMetricsRequest = AddMetricsRequest(
            request_id=self._request_id,
            allocation_id=self._allocation_id,
            function_name=self._function_name,
            timer=None,
            counter=AddCounterRequest(name=name, value=value),
        )
        self._run_add_request(request_payload)

    def _run_add_request(self, request_payload: AddMetricsRequest) -> None:
        try:
            request: httpx.Request = self._http_client.build_request(
                ADD_METRICS_VERB,
                url=ADD_METRICS_PATH,
                json=request_payload.model_dump(),
            )
            response: httpx.Response = self._http_client.send(request)
            response.raise_for_status()
        except Exception as e:
            raise InternalError(f"Failed to add metrics via HTTP: {e}") from e

    def __getstate__(self):
        raise SDKUsageError("Pickling of RequestMetricsHTTPClient is not supported.")

    def __setstate__(self, state):
        raise SDKUsageError("Unpickling of RequestMetricsHTTPClient is not supported.")
