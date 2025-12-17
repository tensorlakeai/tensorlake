import time

from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.request_context.http_server.handlers.progress_update import (
    FunctionProgressUpdateRequest,
    FunctionProgressUpdateResponse,
)
from tensorlake.applications.request_context.progress import (
    print_progress_update,
)

from ..allocation_state_wrapper import AllocationStateWrapper


class AllocationProgress:
    def __init__(
        self, allocation_state: AllocationStateWrapper, logger: InternalLogger
    ):
        self._allocation_state: AllocationStateWrapper = allocation_state
        self._logger: InternalLogger = logger.bind(module=__name__)

    def update(
        self,
        request: FunctionProgressUpdateRequest,
    ) -> FunctionProgressUpdateResponse:
        self._allocation_state.update_progress(
            current=request.current, total=request.total
        )

        print_progress_update(
            request_id=request.request_id,
            function_name=request.function_name,
            current=request.current,
            total=request.total,
            message=request.message,
            attributes=request.attributes,
            local_mode=False,
        )

        # sleep(0) here momentarily to give us a chance to send the progress update from allocation state to Executor.
        # This is solves a hypothetical problem that we might return to running user code and lock GIL and Executor
        # won't get the progress update. A proper fix for this is so add a reply message from Executor to acknowledge
        # progress update received, but this is more involved change.
        time.sleep(0)

        return FunctionProgressUpdateResponse()
