from typing import Any, List

from ..interface.function import Function
from ..interface.function_call import RegularFunctionCall
from ..interface.request_context import RequestContext, RequestContextPlaceholder
from ..registry import get_class
from .type_hints import function_arg_type_hint


def prepend_request_context_placeholder_to_function_args(
    function: Function, args: List[Any]
) -> None:
    """Prepend a RequestContextPlaceholder to the beginning of the function arguments if the function expects a RequestContext.

    RequestContext type hint is required to bind the context to the first argument.
    """
    arg_0_type_hints: List[Any] = function_arg_type_hint(function, 0)
    for arg_0_type_hint in arg_0_type_hints:
        if arg_0_type_hint is RequestContext:
            args.insert(0, RequestContextPlaceholder())
            break


def create_self_instance(class_name: str) -> Any:
    # TODO: Raise RequestError with a clear description if the class is not found and class_name is not None.
    # Right now an Exception is raised from get_class without details.
    cls: Any = get_class(class_name)
    instance: Any = cls()  # Creating an instance and calling our empty constructor here
    instance.__tensorlake_original_init__()  # Calling original user constructor here
    return instance


def set_self_arg(function_call: RegularFunctionCall, self_instance: Any) -> None:
    function_call.args.insert(0, self_instance)


def set_request_context_args(
    function_call: RegularFunctionCall, request_context: RequestContext
) -> None:
    for ix, arg in enumerate(function_call.args):
        if isinstance(arg, RequestContextPlaceholder):
            function_call.args[ix] = request_context
    for key, value in function_call.kwargs.items():
        if isinstance(value, RequestContextPlaceholder):
            function_call.kwargs[key] = request_context
