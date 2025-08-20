"""Function call utilities that are not part of the SDK interface."""

import inspect
from dataclasses import dataclass
from types import UnionType
from typing import Any, List, Union, get_args, get_origin

from .interface.exceptions import RequestException
from .interface.file import File
from .interface.function import Function
from .interface.function_call import FunctionCall
from .interface.request_context import RequestContext, RequestContextPlaceholder
from .registry import get_class, get_function
from .user_data_serializer import UserDataSerializer, serializer_by_name

REDUCER_IS_LAST_VALUE_PARAMETER_NAME: str = "is_last_value"
REDUCER_ACCUMULATOR_PARAMETER_NAME: str = "accumulator"


def api_function_call(api: Function | str, payload: Any) -> FunctionCall:
    """Creates a function call for the API function with the provided payload.

    The function call is compliant with API function calling convention.
    """
    if isinstance(api, str):
        api: Function = get_function(api)

    # API function call conventions:
    # [optional ctx, payload]
    args: List[Any] = [payload]

    # RequestContext type hint is required to bind the context to the first argument.
    arg_0_type_hints: List[Any] = function_arg_type_hint(api, 0)
    for arg_0_type_hint in arg_0_type_hints:
        if arg_0_type_hint is RequestContext:
            args.insert(0, RequestContextPlaceholder())
            break

    if api.function_config.class_name is None:
        return api(*args)
    else:
        cls: Any = get_class(api.function_config.class_name)
        return cls().getattr(api.function_config.class_method_name)(*args)


def fill_with_request_context_placeholders(function_call: FunctionCall) -> None:
    """Replaces request contexts in the supplied function call with request context placeholders."""
    for ix, arg in enumerate(function_call.args):
        if isinstance(arg, RequestContext):
            function_call.args[ix] = RequestContextPlaceholder()
    for key, value in function_call.kwargs.items():
        if isinstance(value, RequestContext):
            function_call.kwargs[key] = RequestContextPlaceholder()


def create_self_instance(function_call: FunctionCall) -> Any:
    # TODO: Raise RequestError with a clear description if the class is not found and class_name is not None.
    # Right now an Exception is raised from get_class without details.
    cls: Any = get_class(function_call.class_name)
    instance: Any = cls()  # Creating an instance and calling our empty constructor here
    instance.__tensorlake_original_init__()  # Calling original user constructor here
    return instance


def set_self_arg(function_call: FunctionCall, self_instance: Any) -> None:
    function_call.args.insert(0, self_instance)


def set_request_context_args(
    function_call: FunctionCall, request_context: RequestContext
) -> None:
    for ix, arg in enumerate(function_call.args):
        if isinstance(arg, RequestContextPlaceholder):
            function_call.args[ix] = request_context
    for key, value in function_call.kwargs.items():
        if isinstance(value, RequestContextPlaceholder):
            function_call.kwargs[key] = request_context


def create_default_accumulator_value(function: Function) -> Any:
    function_signature: inspect.Signature = _function_signature(function)

    if REDUCER_ACCUMULATOR_PARAMETER_NAME not in function_signature.parameters:
        raise RequestException(
            f"Function `{function.function_config.function_name}` is missing reducer "
            f"`{REDUCER_ACCUMULATOR_PARAMETER_NAME}` parameter"
        )

    parameter: inspect.Parameter = function_signature.parameters[
        REDUCER_ACCUMULATOR_PARAMETER_NAME
    ]
    if parameter.default is inspect.Parameter.empty:
        raise RequestException(
            f"Function `{function.function_config.function_name}` reducer "
            f"`{REDUCER_ACCUMULATOR_PARAMETER_NAME}` parameter is missing default value"
        )
    # We create a copy of the default accumulator value by serializing and deserializing it.
    # This is required because default parameter values are singletones and it's a common mistake
    # to use them for mutable types.
    user_serializer: UserDataSerializer = serializer_by_name(
        function.function_config.input_serializer
    )
    serialized_default: bytes = user_serializer.serialize(parameter.default)
    return user_serializer.deserialize(
        serialized_default,
        function_kwarg_type_hint(function, REDUCER_ACCUMULATOR_PARAMETER_NAME),
    )


@dataclass
class FunctionOutputs:
    values: List[Any]
    files: List[File]
    function_calls: List[FunctionCall]


def extract_function_outputs(
    function_output: Any,
) -> FunctionOutputs:
    processed_outputs = FunctionOutputs(
        values=[],
        files=[],
        function_calls=[],
    )

    # To return multiple function outputs user needs to wrap them into a list or a tuple.
    # We're not allowing Iterable and other similar abstractions because they match
    # simple types like str or bytes that should be treated as a single value.
    if isinstance(function_output, list) or isinstance(function_output, tuple):
        for item in function_output:
            if isinstance(item, FunctionCall):
                processed_outputs.function_calls.append(item)
            elif isinstance(item, File):
                processed_outputs.files.append(item)
            else:
                processed_outputs.values.append(item)

        return processed_outputs
    # Everything which is not list or a tuple is treated as a single value.
    elif isinstance(function_output, FunctionCall):
        processed_outputs.function_calls.append(function_output)
    elif isinstance(function_output, File):
        processed_outputs.files.append(function_output)
    else:
        processed_outputs.values.append(function_output)

    return processed_outputs


def function_arg_type_hint(function: Function, arg_ix: int) -> List[Any]:
    """Returns the type hint for positional function call argument at the specified index, or None if not found."""
    function_signature: inspect.Signature = _function_signature(function)
    parameters: list[inspect.Parameter] = list(function_signature.parameters.values())
    if arg_ix < 0 or arg_ix >= len(parameters):
        return []
    parameter: inspect.Parameter = parameters[arg_ix]
    if parameter.annotation is inspect.Parameter.empty:
        return []
    return _resolve_type_hint(parameter.annotation)


def function_kwarg_type_hint(function: Function, key: str) -> List[Any]:
    """Returns the type hint for keyword function call argument with the specified key, or None if not found."""
    function_signature: inspect.Signature = _function_signature(function)
    if key not in function_signature.parameters:
        return []
    parameter: inspect.Parameter = function_signature.parameters[key]
    if parameter.annotation is inspect.Parameter.empty:
        return []
    return _resolve_type_hint(parameter.annotation)


def function_return_type_hint(function: Function) -> List[Any]:
    function_signature: inspect.Signature = _function_signature(function)
    if function_signature.return_annotation is inspect.Signature.empty:
        return []

    return _resolve_type_hint(function_signature.return_annotation)


def _function_signature(function: Function) -> inspect.Signature:
    # Common approach to getting the function signatures.
    return inspect.signature(
        function.original_function,
        follow_wrapped=False,
        eval_str=False,
    )


def _resolve_type_hint(type_hint: Any) -> List[Any]:
    """Returns all singular (scalar) types in the provided type hint.

    Recurses only once into top level List or Tuple.
    Also extracts types from all Union types.

    Examples:
        str -> [str]
        List[str] -> str
        Tuple[str, int] -> [str, int]
        str | FunctionCall -> [str, FunctionCall]
        List[str | FunctionCall] -> [str, FunctionCall]
        Tuple[str | FunctionCall, int] -> [str, FunctionCall, int]

    """
    origin = get_origin(type_hint)
    if origin is list:
        return _resolve_list_type_hint(type_hint)
    elif origin is tuple:
        return _resolve_tuple_type_hint(type_hint)
    elif _is_union_origin(origin):
        return _resolve_union_type_hint(type_hint)
    else:
        return [type_hint]


def _resolve_list_type_hint(list_type_hint: Any) -> List[Any]:
    """Returns the singular (scalar) type in the provided list type hint.

    Resolves Unions as List types.

    Examples:
        List[str] -> [str]
        List[str | FunctionCall] -> [str, FunctionCall]
    """
    list_type_arg: Any = get_args(list_type_hint)[0]
    # print(f"Resolving type hint: {list_type_arg}, origin: {get_origin(list_type_arg)}")
    if _is_union_origin(get_origin(list_type_arg)):
        return _resolve_union_type_hint(list_type_arg)
    else:
        return [list_type_arg]


def _resolve_tuple_type_hint(tuple_type_hint: Any) -> List[Any]:
    """Returns all singular (scalar) types in the provided tuple type hint.

    Examples:
        Tuple[str, int] -> [str, int]
        Tuple[str | FunctionCall, int] -> [str, FunctionCall, int]
    """
    types: List[Any] = []
    for t in get_args(tuple_type_hint):
        if _is_union_origin(get_origin(t)):
            types.extend(_resolve_union_type_hint(t))
        else:
            types.append(t)
    return types


def _is_union_origin(origin: Any) -> bool:
    """Returns True if the provided type hint origin is for a union of types."""
    # The first check is for | operator.
    # The second is for Union[] type hint.
    return origin is UnionType or origin is Union


def _resolve_union_type_hint(union_type_hint: Any) -> List[Any]:
    """Returns all singular (scalar) types in the provided union type hint.

    Examples:
        str | FunctionCall -> [str, FunctionCall]
    """
    return [t for t in get_args(union_type_hint)]
