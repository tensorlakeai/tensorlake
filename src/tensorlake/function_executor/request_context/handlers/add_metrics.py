from tensorlake.applications.request_context.http_server.handlers.add_metrics import (
    AddMetricsRequest,
    AddMetricsResponse,
    BaseAddMetricsHandler,
)
from tensorlake.applications.request_context.metrics import (
    print_counter_incremented_event,
    print_timer_recorded_event,
)


class AddMetricsHandler(BaseAddMetricsHandler):
    def __init__(self):
        super().__init__()

    def _handle(self, request: AddMetricsRequest) -> AddMetricsResponse:
        if request.counter is not None:
            print_counter_incremented_event(
                request_id=request.request_id,
                function_name=request.function_name,
                counter_name=request.counter.name,
                counter_value=request.counter.value,
                local_mode=False,
            )
        if request.timer is not None:
            print_timer_recorded_event(
                request_id=request.request_id,
                function_name=request.function_name,
                timer_name=request.timer.name,
                timer_value=request.timer.value,
                local_mode=False,
            )
        return AddMetricsResponse()
