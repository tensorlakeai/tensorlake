from typing import Any

from ..interface.function import Function
from ..interface.futures import FunctionCallFuture, Future


def derived_function_call_future(
    source: Future, function: Function, *args: Any, **kwargs: Any
) -> FunctionCallFuture:
    """Creates a FunctionCallFuture for the given function with function future settings derived from the source future.

    Raises TensorlakeError on error.
    """
    if source._start_delay is not None:
        return function.future.call_later(source._start_delay, *args, **kwargs)
    elif source._tail_call:
        return function.tail_call(*args, **kwargs)
    else:
        return function.future(*args, **kwargs)
