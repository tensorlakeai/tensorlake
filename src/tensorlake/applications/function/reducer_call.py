from typing import Any, List

from ..interface.function import Function
from ..interface.function_call import RegularFunctionCall


def reducer_function_call(
    reducer_function: Function, accumulator: Any, item: Any
) -> RegularFunctionCall:
    """Creates a reducer function call with the provided arguments."""
    args: List[Any] = [accumulator, item]
    return reducer_function(*args)
