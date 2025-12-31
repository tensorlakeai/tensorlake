from typing import Any, Set

from ..interface import (
    InternalError,
    SDKUsageError,
)
from ..interface.awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    Future,
    ReduceOperationAwaitable,
)


def validate_user_object(
    user_object: Awaitable | Future | Any, running_awaitable_ids: Set[str]
) -> None:
    """Validates the object produced by user function.

    An object can be produced by either returning it from a user function or passing it as
    an argument to a Future or a blocking operation.

    function_call_ids is set of function call IDs that are already running or finished.
    Raises SDKUsageError if the object is invalid.
    Raises TensorlakeError for other errors.
    """
    if not isinstance(user_object, (Awaitable, Future)):
        return

    # TODO: Allow passing Futures that are already running. This makes our implementation
    # more complex because each running Future can be used as argument in multiple other
    # trees of Awaitables.
    if isinstance(user_object, Future):
        raise SDKUsageError(
            f"Cannot run {user_object}, please pass a not running Awaitable instead."
        )

    awaitable: Awaitable = user_object
    if awaitable.id in running_awaitable_ids:
        raise SDKUsageError(
            f"{awaitable} has an already running Future. "
            "Only not running Awaitable can be passed as function argument or returned from a function."
        )

    if isinstance(awaitable, AwaitableList):
        awaitable: AwaitableList
        for item in awaitable.items:
            validate_user_object(
                user_object=item, running_awaitable_ids=running_awaitable_ids
            )
    elif isinstance(awaitable, ReduceOperationAwaitable):
        awaitable: ReduceOperationAwaitable
        for item in awaitable.inputs:
            validate_user_object(
                user_object=item, running_awaitable_ids=running_awaitable_ids
            )
    elif isinstance(awaitable, FunctionCallAwaitable):
        awaitable: FunctionCallAwaitable
        for arg in awaitable.args:
            validate_user_object(
                user_object=arg, running_awaitable_ids=running_awaitable_ids
            )
        for arg in awaitable.kwargs.values():
            validate_user_object(
                user_object=arg, running_awaitable_ids=running_awaitable_ids
            )
    else:
        raise InternalError(f"Unexpected Awaitable subclass: {type(awaitable)}")
