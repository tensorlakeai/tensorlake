import os.path

from tensorlake.applications.blob_store import BLOB, BLOBChunk
from tensorlake.applications.request_context.http_server.handlers.request_state.prepare_read import (
    BasePrepareReadHandler,
    PrepareReadRequest,
    PrepareReadResponse,
)
from tensorlake.vendor.nanoid import generate as nanoid

from .file_path import request_state_file_path


class LocalPrepareReadHandler(BasePrepareReadHandler):
    def __init__(self, request_state_dir_path: str):
        super().__init__()
        self._request_state_dir_path: str = request_state_dir_path

    def _handle(self, request: PrepareReadRequest) -> PrepareReadResponse:
        file_path: str = request_state_file_path(
            self._request_state_dir_path,
            request.state_key,
        )
        if not os.path.exists(file_path):
            return PrepareReadResponse(blob=None)

        file_size: int = os.path.getsize(file_path)
        return PrepareReadResponse(
            blob=BLOB(
                id=nanoid(),
                chunks=[
                    BLOBChunk(
                        uri=f"file://{file_path}",
                        size=file_size,
                        etag=None,
                    ),
                ],
            ),
        )
