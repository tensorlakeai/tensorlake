from typing import Any, Generator

from ..interface.exceptions import InternalError
from ..interface.futures import (
    FunctionCallFuture,
    Future,
    ListFuture,
    ReduceOperationFuture,
    _InitialMissingType,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)


def dfs_bottom_up_unique_only(root: Future) -> Generator[Future, None, None]:
    """Yields all unique Futures starting from leafs up to the root aka Post-Order DFS.

    This traversal order is useful when at the moment of processing a node, all its children have
    to be already processed.

    The traversal order is deterministic (always the same for the same tree).
    Doesn't yield the same Future referenced in the tree multiple times.
    Doesn't yield user supplied values.

    Raises InternalError if encounters an unexpected Future type.
    """
    seen_future_ids: set[str] = set()
    for future in dfs_bottom_up(root):
        if future._id not in seen_future_ids:
            seen_future_ids.add(future._id)
            yield future


def dfs_bottom_up(root: Future) -> Generator[Future, None, None]:
    """Yields all Futures starting from leafs up to the root aka Post-Order DFS.

    This traversal order is useful when at the moment of processing a node, all its children have
    to be already processed.

    The traversal order is deterministic (always the same for the same tree).
    Doesn't yield user supplied values.

    Raises InternalError if encounters an unexpected Future type.
    """
    dfs_stack: list[Future] = [root]
    yield_stack: list[Future] = []

    while len(dfs_stack) > 0:
        node: Future = dfs_stack.pop()
        yield_stack.append(node)

        if isinstance(node, ListFuture):
            node: ListFuture
            items: list[_TensorlakeFutureWrapper[Future] | Any] | ListFuture = (
                _unwrap_future(node._items)
            )
            if isinstance(items, ListFuture):
                dfs_stack.append(items)
            else:
                for item in node._items:
                    item: Future | Any = _unwrap_future(item)
                    if isinstance(item, Future):
                        dfs_stack.append(item)
        elif isinstance(node, ReduceOperationFuture):
            node: ReduceOperationFuture
            initial: Future | Any | _InitialMissingType = _unwrap_future(node._initial)
            if isinstance(initial, Future):
                dfs_stack.append(initial)

            items: list[_TensorlakeFutureWrapper[Future] | Any] | ListFuture = (
                _unwrap_future(node._items)
            )
            if isinstance(items, ListFuture):
                dfs_stack.append(items)
            else:
                for item in node._items:
                    item: Future | Any = _unwrap_future(item)
                    if isinstance(item, Future):
                        dfs_stack.append(item)
        elif isinstance(node, FunctionCallFuture):
            node: FunctionCallFuture
            for arg in node._args:
                arg: Future | Any = _unwrap_future(arg)
                if isinstance(arg, Future):
                    dfs_stack.append(arg)
            # Sort dict keys to ensure deterministic traversal order.
            for key in sorted(node._kwargs.keys()):
                arg: Future | Any = _unwrap_future(node._kwargs[key])
                if isinstance(arg, Future):
                    dfs_stack.append(arg)
        else:
            raise InternalError(f"Unexpected type of Future tree node: {type(node)}")

    yield from reversed(yield_stack)
