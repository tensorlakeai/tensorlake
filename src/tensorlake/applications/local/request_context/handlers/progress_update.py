import json

from tensorlake.applications.request_context.http_server.handlers.progress_update import (
    BaseProgressUpdateHandler,
    FunctionProgressUpdateRequest,
    FunctionProgressUpdateResponse,
)
from tensorlake.applications.request_context.progress import print_progress_update


class LocalProgressUpdateHandler(BaseProgressUpdateHandler):
    def __init__(self):
        super().__init__()

    def _handle(
        self, request: FunctionProgressUpdateRequest
    ) -> FunctionProgressUpdateResponse:
        print_progress_update(
            request_id=request.request_id,
            function_name=request.function_name,
            current=request.current,
            total=request.total,
            message=request.message,
            attributes=request.attributes,
            local_mode=True,
        )
        return FunctionProgressUpdateResponse()
