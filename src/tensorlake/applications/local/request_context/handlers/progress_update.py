import json

from tensorlake.applications.request_context.http_server.handlers.progress_update import (
    BaseProgressUpdateHandler,
    FunctionProgressUpdateRequest,
    FunctionProgressUpdateResponse,
)


class LocalProgressUpdateHandler(BaseProgressUpdateHandler):
    def __init__(self):
        super().__init__()

    def _handle(
        self, request: FunctionProgressUpdateRequest
    ) -> FunctionProgressUpdateResponse:
        print(
            f"executing step {_format_step(request.current)} of {_format_step(request.total)}{_format_message(request.message, request.attributes)}",
            flush=True,
        )
        return FunctionProgressUpdateResponse()


def _format_message(message: str | None, attributes: dict[str, str] | None) -> str:
    if message is None and attributes is None:
        return ""

    if message is None:
        return json.dumps(attributes)
    elif attributes is None:
        return f": {message}."
    else:
        return f": {message}. {json.dumps(attributes)}"


def _format_step(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return str(value)
