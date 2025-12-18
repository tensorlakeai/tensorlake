from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.request_context.http_server.handlers.request_state.commit_write import (
    BaseCommitWriteHandler,
    CommitWriteRequest,
    CommitWriteResponse,
)
from tensorlake.function_executor.allocation_info import AllocationInfo


class CommitWriteHandler(BaseCommitWriteHandler):
    def __init__(
        self,
        allocation_infos: dict[str, AllocationInfo],
        logger: InternalLogger,
    ):
        super().__init__()
        self._allocation_infos: dict[str, AllocationInfo] = allocation_infos
        self._logger: InternalLogger = logger.bind(module=__name__)

    def _handle(self, request: CommitWriteRequest) -> CommitWriteResponse:
        if request.allocation_id not in self._allocation_infos:
            raise ValueError(
                f"Received commit write request for unknown allocation_id: {request.allocation_id}",
            )

        allocation_info: AllocationInfo = self._allocation_infos[request.allocation_id]
        return allocation_info.runner.run_request_context_operation(request)
