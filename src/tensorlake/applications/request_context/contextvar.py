import contextvars
from typing import Any

from ..interface.exceptions import RequestFailureException

_VARIABLE_NAME = "request_context"
# Don't import RequestContext here to avoid circular dependency.
_current_request_context = contextvars.ContextVar(_VARIABLE_NAME)


def get_current_request_context() -> Any:
    try:
        return _current_request_context.get()
    except LookupError:
        raise RequestFailureException(
            "No request context is available. It's only available inside a Tensorlake Function call."
            "It's not available in threads spawned by a Tensorlake Function."
        )


def set_current_request_context(request_context: Any) -> None:
    _current_request_context.set(request_context)
