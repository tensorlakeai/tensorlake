import inspect
from types import UnionType
from typing import Any, List, Union, get_args, get_origin

from ..interface.function import Function


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
