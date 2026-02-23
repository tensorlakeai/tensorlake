from typing import Any

from ..interface.function import Function
from ..interface.futures import FunctionCallFuture, Future


def derived_function_call_future(
    source: Future, function: Function, *args: Any, **kwargs: Any
) -> FunctionCallFuture:
    """Creates a FunctionCallFuture for the given function with function future settings derived from the source future.

    Raises TensorlakeError on error.
    """
    future: FunctionCallFuture = function.future(*args, **kwargs)
    if source._start_delay is not None:
        future._start_delay = source._start_delay
    return future
