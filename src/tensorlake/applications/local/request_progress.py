import json
from typing import Any

from ..interface.request_context import FunctionProgress


class LocalFunctionProgress(FunctionProgress):
    """FunctionProgress that tracks the progress of a local function call."""

    def __init__(self):
        super().__init__()

    def update(
        self, current: float, total: float, message: str | None = None, **kwargs
    ) -> None:
        print(f"Executing step {current} of {total}{format_message(message, kwargs)}")


def format_message(message: str | None, kwargs: dict[str, Any]) -> str:
    if message is None and not kwargs:
        return ""

    if message is None:
        return json.dumps(kwargs)
    elif not kwargs:
        return f": {message}."
    else:
        return f": {message}. {json.dumps(kwargs)}"
