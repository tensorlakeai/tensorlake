import json
from typing import Any

from ..interface.request_context import FunctionProgress


class LocalFunctionProgress(FunctionProgress):
    """FunctionProgress that tracks the progress of a local function call."""

    def __init__(self):
        super().__init__()

    def update(
        self,
        current: float,
        total: float,
        message: str | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        print(
            f"executing step {current} of {total}{format_message(message, attributes)}"
        )


def format_message(message: str | None, attributes: dict[str, str] | None) -> str:
    if message is None and not attributes:
        return ""

    if message is None:
        return json.dumps(attributes)
    elif not attributes:
        return f": {message}."
    else:
        return f": {message}. {json.dumps(attributes)}"
