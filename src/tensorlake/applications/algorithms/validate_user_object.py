from ..interface import (
    SDKUsageError,
)
from ..interface.futures import (
    Future,
    ListFuture,
)


def validate_tail_call_user_future(
    function_name: str, tail_call_user_future: Future
) -> None:
    """Validates the Future object returned by user as a tail call.

    Raises SDKUsageError on validation failure.
    """
    # This is a very important check for our UX. We can await for ListFuture
    # in user code but we cannot return it from a function as tail call because
    # there's no Python code to reassemble the list from individual resolved futures.
    if isinstance(tail_call_user_future, ListFuture):
        raise SDKUsageError(
            f"Function '{function_name}' returned {tail_call_user_future}. "
            f"A {tail_call_user_future._kind_str} can only be used as a function argument, not returned from it."
        )
