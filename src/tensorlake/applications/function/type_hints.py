import inspect
import pickle
from types import UnionType
from typing import Any, List, Union, get_args, get_origin

from ..interface.function import Function


def function_arg_type_hint(function: Function, arg_ix: int) -> List[Any]:
    """Returns the type hint for positional function call argument at the specified index, or None if not found.

    arg_ix can be negative to indicate position from the end of the argument list.
    """
    signature: inspect.Signature = function_signature(function)
    parameters: list[inspect.Parameter] = list(signature.parameters.values())
    if arg_ix >= len(parameters) or arg_ix < -len(parameters):
        return []
    parameter: inspect.Parameter = parameters[arg_ix]
    if parameter.annotation is inspect.Parameter.empty:
        return []
    return _resolve_type_hint(parameter.annotation)


def function_kwarg_type_hint(function: Function, key: str) -> List[Any]:
    """Returns the type hint for keyword function call argument with the specified key, or None if not found."""
    signature: inspect.Signature = function_signature(function)
    if key not in signature.parameters:
        return []
    parameter: inspect.Parameter = signature.parameters[key]
    if parameter.annotation is inspect.Parameter.empty:
        return []
    return _resolve_type_hint(parameter.annotation)


def function_return_type_hint(function: Function) -> List[Any]:
    signature: inspect.Signature = function_signature(function)
    if signature.return_annotation is inspect.Signature.empty:
        return []

    return _resolve_type_hint(signature.return_annotation)


def serialize_type_hints(type_hints: List[Any]) -> bytes:
    return pickle.dumps(type_hints)


def deserialize_type_hints(serialized_type_hints: bytes) -> List[Any]:
    return pickle.loads(serialized_type_hints)


def function_signature(function: Function) -> inspect.Signature:
    """Returns the function signature for the provided Tensorlake Function.

    Raises Exception if the signature cannot be obtained.
    """
    # Common approach to getting the function signatures.
    return inspect.signature(
        function._original_function,
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
