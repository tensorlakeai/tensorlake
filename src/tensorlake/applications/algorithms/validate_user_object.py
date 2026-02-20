from ..interface import (
    SDKUsageError,
)
from ..interface.futures import (
    FunctionCallFuture,
    Future,
    ListFuture,
    ReduceOperationFuture,
)


def validate_tail_call_user_future(
    function_name: str, tail_call_user_future: Future
) -> None:
    """Validates the Future object returned by user as a tail call.

    Raises SDKUsageError on validation failure.
    """
    if not tail_call_user_future._tail_call:
        tail_call_function_name: str = "<function_name>"
        if isinstance(tail_call_user_future, FunctionCallFuture):
            tail_call_function_name = tail_call_user_future._function_name
        elif isinstance(tail_call_user_future, ReduceOperationFuture):
            tail_call_function_name = tail_call_user_future._function_name
        elif isinstance(tail_call_user_future, ListFuture):
            if tail_call_user_future._metadata.function_name is not None:
                tail_call_function_name = tail_call_user_future._metadata.function_name
        raise SDKUsageError(
            f"Function '{function_name}' returned {tail_call_user_future} was not created using tail call API. "
            f"Please use `{tail_call_function_name}.tail_call(...)` to create the Future."
        )

    # This is a very important check for our UX. We can await for ListFuture
    # in user code but we cannot return it from a function as tail call because
    # there's no Python code to reassemble the list from individual resolved futures.
    if isinstance(tail_call_user_future, ListFuture):
        raise SDKUsageError(
            f"Function '{function_name}' returned {tail_call_user_future}. "
            f"A {tail_call_user_future._kind_str} can only be used as a function argument, not returned from it."
        )
