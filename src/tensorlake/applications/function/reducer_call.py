from typing import Any, List

from ..interface.function import Function
from ..interface.function_call import RegularFunctionCall
from .function_call import prepend_request_context_placeholder_to_function_args


def reducer_function_call(
    reducer_function: Function, accumulator: Any, item: Any
) -> RegularFunctionCall:
    """Creates a reducer function call with the provided arguments."""
    args: List[Any] = [accumulator, item]
    prepend_request_context_placeholder_to_function_args(reducer_function, args)
    return reducer_function(*args)
