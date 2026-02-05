from typing import Any

from ..interface.awaitables import (
    FunctionCallAwaitable,
    ReduceOperationAwaitable,
)
from ..interface.function import Function
from ..registry import get_function


def reduce_operation_to_function_call_chain(
    awaitable: ReduceOperationAwaitable,
) -> FunctionCallAwaitable:
    """Returns a chain of function calls equivalent to the supplied reduce operation.

    If there's only one input in reduce operation then it returns it as is.
    """
    function: Function = get_function(awaitable.function_name)
    # inputs have at least two items, ReduceOperationAwaitable creation code ensures this.
    #
    # Create a chain of function calls to reduce all inputs one by one.
    # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
    # using string concat function into "abcd".
    previous_function_call_awaitable: FunctionCallAwaitable = function.awaitable(
        awaitable.inputs[0], awaitable.inputs[1]
    )
    for input_item in awaitable.inputs[2:]:
        previous_function_call_awaitable = function.awaitable(
            previous_function_call_awaitable, input_item
        )

    return previous_function_call_awaitable
