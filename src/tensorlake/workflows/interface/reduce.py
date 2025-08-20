from collections.abc import Iterable
from typing import Any, List

from .function import Function
from .function_call import FunctionCall
from .gather import FutureList


class ReducerFunctionCall(FunctionCall):
    def __init__(
        self,
        reducer_function_name: str,
        inputs: FutureList,
        is_initial_missing: bool,
        initial: Any,
    ):
        super().__init__(reducer_function_name)
        self._inputs: FutureList = inputs
        self._is_initial_missing: bool = is_initial_missing
        self._initial: Any = initial

    @property
    def inputs(self) -> FutureList:
        return self._inputs

    @property
    def is_initial_missing(self) -> bool:
        return self._is_initial_missing

    @property
    def initial(self) -> Any:
        return self._initial

    def __repr__(self) -> str:
        return (
            f"<Tensorlake ReducerFunctionCall(\n"
            f"  reducer_function_name={self.function_name!r},\n"
            f"  inputs={self.inputs!r},\n"
            f"  is_initial_missing={self.is_initial_missing!r},\n"
            f"  initial={self.initial!r}\n"
            f")>"
        )


class _InitialMissing:
    pass


def reduce(
    function: Function,
    iterable: Iterable,
    initial: Any | _InitialMissing = _InitialMissing(),
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

    return ReducerFunctionCall(
        reducer_function_name=function.function_config.function_name,
        inputs=inputs,
        is_initial_missing=initial is _InitialMissing,
        initial=initial if initial is not _InitialMissing else None,
    )
