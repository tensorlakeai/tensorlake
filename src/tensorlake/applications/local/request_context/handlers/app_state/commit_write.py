from tensorlake.applications.request_context.http_server.handlers.app_state.commit_write import (
    BaseCommitWriteHandler,
    CommitWriteRequest,
    CommitWriteResponse,
)


class LocalCommitWriteHandler(BaseCommitWriteHandler):
    def __init__(self, app_state_dir_path: str):
        super().__init__()

    def _handle(self, request: CommitWriteRequest) -> CommitWriteResponse:
        # Local fs write is already committed, this is a no-op.
        return CommitWriteResponse()
