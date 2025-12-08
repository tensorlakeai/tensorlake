from ....request_context.http_server.handlers.add_metrics import (
    AddMetricsRequest,
    AddMetricsResponse,
    BaseAddMetricsHandler,
)


class LocalAddMetricsHandler(BaseAddMetricsHandler):
    def __init__(self):
        super().__init__()

    def _handle(self, request: AddMetricsRequest) -> AddMetricsResponse:
        _print_metrics(request)
        return AddMetricsResponse()


def _print_metrics(request: AddMetricsRequest) -> None:
    if request.counter is not None:
        print(
            "function_metric",
            {
                "counter_name": request.counter.name,
                "counter_value": request.counter.value,
            },
            flush=True,
        )
    if request.timer is not None:
        print(
            "function_metric",
            {
                "timer_name": request.timer.name,
                "timer_value": request.timer.value,
            },
            flush=True,
        )
