from typing import Any, Generator

from ..interface.awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    Future,
    ReduceOperationAwaitable,
)
from ..interface.exceptions import InternalError


def dfs_bottom_up(
    root: Awaitable | Future,
    leaf_awaitable_types: tuple[type[Awaitable], ...],
) -> Generator[Awaitable | Future, None, None]:
    """Yields all Awaitables and Futures starting from leafs up to the root aka Post-Order DFS.

    This traversal order is useful when at the moment of processing a node, all its children have
    to be already processed.

    leaf_awaitable_types is a tuple of Awaitable types that should be treated as leafs in the
    traversal. Their children won't be traversed.

    The traversal order is deterministic (always the same for the same tree).

    Doesn't look inside Futures as they are not currently supported in trees of Awaitables.
    Doesn't yield user supplied values.
    Raises InternalError if encounters an unexpected Awaitable type.
    """
    dfs_stack: list[Awaitable | Future] = [root]
    yield_stack: list[Awaitable | Future] = []

    while len(dfs_stack) > 0:
        node: Awaitable | Future = dfs_stack.pop()
        yield_stack.append(node)

        if isinstance(node, leaf_awaitable_types):
            continue
        elif isinstance(node, AwaitableList):
            node: AwaitableList
            for item in node.items:
                if _is_awaitable_tree_node(item):
                    dfs_stack.append(item)
        elif isinstance(node, ReduceOperationAwaitable):
            node: ReduceOperationAwaitable
            for item in node.inputs:
                if _is_awaitable_tree_node(item):
                    dfs_stack.append(item)
        elif isinstance(node, FunctionCallAwaitable):
            node: FunctionCallAwaitable
            for arg in node.args:
                if _is_awaitable_tree_node(arg):
                    dfs_stack.append(arg)
            # Sort dict keys to ensure deterministic traversal order.
            for key in sorted(node.kwargs.keys()):
                arg = node.kwargs[key]
                if _is_awaitable_tree_node(arg):
                    dfs_stack.append(arg)
        elif isinstance(node, Future):
            pass  # Don't look inside Futures as they are not currently supported in trees of Awaitables.
        else:
            raise InternalError(f"Unexpected type of Awaitable tree node: {type(node)}")

    yield from reversed(yield_stack)


def _is_awaitable_tree_node(value: Awaitable | Future | Any) -> bool:
    return isinstance(value, (Awaitable, Future))
