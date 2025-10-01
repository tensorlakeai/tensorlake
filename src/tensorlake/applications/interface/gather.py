from collections.abc import Iterable
from typing import Any, List

from .function import Function
from .function_call import FunctionCall
from .future import FutureList


def gather(items: Iterable[Any | FunctionCall]) -> FutureList:
    """Converts the iterable into a future that resolves into a list of values from the iterable.

    The returned FutureList object can be used in Tensorlake Function call arguments.
    It cannot be returned from a Tensorlake Function.
    """
    return FutureList(items)


def map(function: Function, iterable: Iterable) -> FutureList:
    """Returns a future that resolves into a list with every item transformed using the supplied function.

    Similar to https://docs.python.org/3/library/functions.html#map.
    If the function accepts request context then it should be its first argument
    and have `tensorlake.RequestContext` type annotation.
    """
    function_calls: List[FunctionCall] = []
    for item in iterable:
        args = [item]
        function_calls.append(function(*args))
    return gather(function_calls)
