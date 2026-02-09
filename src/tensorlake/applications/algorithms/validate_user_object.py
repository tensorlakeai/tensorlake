from typing import Any, Set

from ..interface import (
    SDKUsageError,
)
from ..interface.awaitables import (
    Awaitable,
    AwaitableList,
)
from .walk_awaitable_tree import dfs_bottom_up


def validate_user_awaitable_before_running(
    awaitable: Awaitable, running_awaitable_ids: Set[str]
) -> None:
    """Validates awaitable tree produced by a user function to check if it can be run.

    An awaitable can be produced by either returning it from a user function or passing it as
    an argument to a Future or a blocking operation.

    running_awaitable_ids is set of awaitable IDs that are already running or finished.
    Raises SDKUsageError if the object is invalid.
    Raises TensorlakeError for other errors.
    """
    for node in dfs_bottom_up(awaitable, leaf_awaitable_types=()):
        if not isinstance(node, Awaitable):
            raise SDKUsageError(
                f"Cannot run {node}, please pass a not running Awaitable instead."
            )

        node: Awaitable
        if node.id in running_awaitable_ids:
            raise SDKUsageError(
                f"{node} has an already running Future. "
                "Only not running Awaitable can be passed as function argument or returned from a function."
            )


def validate_tail_call_user_object(
    function_name: str, tail_call_user_object: Any
) -> None:
    """Validates the object that user expects to run as tail call.

    Raises SDKUsageError on validation failure.
    """
    # i.e. we don't support Futures as tail calls.
    if not isinstance(tail_call_user_object, Awaitable):
        raise SDKUsageError(
            f"Function '{function_name}' returned {tail_call_user_object} which is not an Awaitable. "
            f"Please return a not running Awaitable instead."
        )

    # This is a very important check for our UX. We can await for AwaitableList
    # in user code but we cannot return it from a function as tail call because
    # there's no Python code to reassemble the list from individual resolved awaitables.
    if isinstance(tail_call_user_object, AwaitableList):
        raise SDKUsageError(
            f"Function '{function_name}' returned {tail_call_user_object}. "
            f"A {tail_call_user_object.kind_str} can only be used as a function argument, not returned from it."
        )
