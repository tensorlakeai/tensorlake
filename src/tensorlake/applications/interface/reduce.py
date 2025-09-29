from collections.abc import Iterable
from typing import Any

from .function import Function
from .function_call import FunctionCall
from .gather import FutureList


class ReducerFunctionCall(FunctionCall):
    def __init__(
        self,
        reducer_function_name: str,
        inputs: FutureList,
    ):
        super().__init__(reducer_function_name)
        # Contains at least one item due to initial + SDK validation.
        self._inputs: FutureList = inputs

    @property
    def inputs(self) -> FutureList:
        # Guaranteed to contain at least a single element.
        return self._inputs

    def __repr__(self) -> str:
        return (
            f"<Tensorlake ReducerFunctionCall(\n"
            f"  reducer_function_name={self.function_name!r},\n"
            f"  inputs={self.inputs!r},\n"
            f")>"
        )


class _InitialMissingType:
    pass


_InitialMissing = _InitialMissingType()


def reduce(
    function: Function,
    iterable: Iterable,
    initial: Any | _InitialMissingType = _InitialMissing,
    /,
) -> ReducerFunctionCall:
    """Calls the supplied function as a reducer.

    Similar to https://docs.python.org/3/library/functools.html#functools.reduce.
    If function accepts request context then it should be the first parameter of
    the function and it needs to have `tensorlake.RequestContext` type annotation.
    """
    # list is mutable so let's always save a copy here.
    inputs: FutureList = (
        iterable if isinstance(iterable, FutureList) else FutureList(list(iterable))
    )
    if len(inputs.items) == 0 and initial is _InitialMissing:
        raise TypeError("reduce() of empty iterable with no initial value")

    if initial is not _InitialMissing:
        inputs.items.insert(0, initial)

    return ReducerFunctionCall(
        reducer_function_name=function.function_config.function_name,
        inputs=inputs,
    )
