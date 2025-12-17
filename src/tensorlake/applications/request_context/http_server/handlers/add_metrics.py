from pydantic import BaseModel

from .handler import Handler, Request, Response

ADD_METRICS_PATH: str = "/metrics"
ADD_METRICS_VERB: str = "POST"


class AddTimerRequest(BaseModel):
    name: str
    value: int | float


class AddCounterRequest(BaseModel):
    name: str
    value: int


class AddMetricsRequest(BaseModel):
    request_id: str
    allocation_id: str
    function_name: str
    timer: AddTimerRequest | None
    counter: AddCounterRequest | None


class AddMetricsResponse(BaseModel):
    pass


class BaseAddMetricsHandler(Handler):
    """Base handler for processing add metrics requests."""

    def handle(self, request: Request) -> Response:
        add_metrics_request = AddMetricsRequest.model_validate_json(request.body)
        add_metrics_response: AddMetricsResponse = self._handle(add_metrics_request)
        return Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=add_metrics_response.model_dump_json().encode("utf-8"),
        )

    def _handle(self, request: AddMetricsRequest) -> AddMetricsResponse:
        raise NotImplementedError(
            "BaseAddMetricsHandler subclasses must implement _handle method."
        )
