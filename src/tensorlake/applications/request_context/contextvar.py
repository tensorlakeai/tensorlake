import contextvars
from typing import Any

from ..interface.exceptions import SDKUsageError

_VARIABLE_NAME = "request_context"
# Don't import RequestContext here to avoid circular dependency.
_current_request_context = contextvars.ContextVar(_VARIABLE_NAME)


def get_current_request_context() -> Any:
    try:
        return _current_request_context.get()
    except LookupError:
        raise SDKUsageError(
            "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
            "Please only call Tensorlake SDK from Tensorlake Functions."
        )


def set_current_request_context(request_context: Any) -> None:
    _current_request_context.set(request_context)
