import os.path

from tensorlake.applications.blob_store import BLOB, BLOBChunk
from tensorlake.applications.request_context.http_server.handlers.app_state.prepare_write import (
    BasePrepareWriteHandler,
    PrepareWriteRequest,
    PrepareWriteResponse,
)
from tensorlake.vendor.nanoid import generate as nanoid

from .file_path import app_state_file_path


class LocalPrepareWriteHandler(BasePrepareWriteHandler):
    def __init__(self, app_state_dir_path: str):
        super().__init__()
        self._app_state_dir_path: str = app_state_dir_path

    def _handle(self, request: PrepareWriteRequest) -> PrepareWriteResponse:
        file_path: str = app_state_file_path(
            self._app_state_dir_path,
            request.state_key,
        )
        return PrepareWriteResponse(
            blob=BLOB(
                id=nanoid(),
                chunks=[
                    BLOBChunk(
                        uri=f"file://{file_path}",
                        size=request.size,
                        etag=None,
                    ),
                ],
            ),
        )
