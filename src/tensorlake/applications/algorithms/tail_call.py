from typing import Any

from ..interface.futures import (
    Future,
    ListFuture,
    ReduceOperationFuture,
    _InitialMissing,
    _InitialMissingType,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)


def tail_call_output_future_ids(root: Future) -> set[str]:
    """Propagates tail call mode from the root future to its child futures that are output of the root.

    root: The root future from which to propagate tail call metadata.
    """
    output_future_ids: set[str] = {root._id}

    if not isinstance(root, ReduceOperationFuture):
        return output_future_ids

    items: list[_TensorlakeFutureWrapper[Future] | Any] | ListFuture = _unwrap_future(
        root._items
    )
    if isinstance(items, ListFuture):
        num_items: int = _list_future_num_items(items)
    else:
        num_items = len(root._items)

    initial: Future | Any | _InitialMissingType = _unwrap_future(root._initial)
    if root._initial is _InitialMissing:
        if num_items == 0:
            # SDKUsageError when we run the future.
            # No need to propagate anything.
            return output_future_ids

        if num_items == 1:
            # items[0] is the output future.
            if isinstance(items, ListFuture):
                # No need to propagate further because we only support Map ListFuture which
                # will call a function on the item with tail call mode.
                output_future_ids.add(items._id)
            else:
                first_item: Future | Any = _unwrap_future(root._items[0])
                if isinstance(first_item, Future):
                    output_future_ids.add(first_item._id)
    else:
        if num_items == 0:
            # output is initial.
            if isinstance(initial, Future):
                output_future_ids.add(initial._id)

    return output_future_ids


def _list_future_num_items(future: ListFuture) -> int:
    while True:
        items: list[_TensorlakeFutureWrapper[Future] | Any] | ListFuture = (
            _unwrap_future(future._items)
        )
        if isinstance(items, ListFuture):
            future = items
        else:
            return len(items)
