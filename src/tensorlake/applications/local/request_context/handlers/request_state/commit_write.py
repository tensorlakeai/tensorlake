from tensorlake.applications.request_context.http_server.handlers.request_state.commit_write import (
    BaseCommitWriteHandler,
    CommitWriteRequest,
    CommitWriteResponse,
)


class LocalCommitWriteHandler(BaseCommitWriteHandler):
    def __init__(self, request_state_dir_path: str):
        super().__init__()

    def _handle(self, request: CommitWriteRequest) -> CommitWriteResponse:
        # Local fs write is already commited, this is a no-op.
        return CommitWriteResponse()
