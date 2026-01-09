import inspect
import pickle
from types import UnionType
from typing import Any, List, Union, get_args, get_origin

from ..interface import Function


def parameter_type_hints(parameter: inspect.Parameter) -> List[Any]:
    """Returns the type hint for the provided function parameter.

    Returns empty list if the parameter has no type hint.
    """
    if parameter.annotation is inspect.Parameter.empty:
        return []
    return _resolve_type_hints(parameter.annotation)


def function_arg_type_hint(function: Function, arg_index: int) -> List[Any]:
    """Returns the type hint for function call argument at the specified index.

    Returns empty list if the function has no such positional argument or if the argument has no type hint.
    The index is zero-based, the ordering of parameters is the same as in the function definition.
    arg_index can be negative to indicate position from the end of the argument list.
    """
    signature: inspect.Signature = function_signature(function)
    # signature.parameters is an ordered mapping in parameters definition order.
    parameters: list[inspect.Parameter] = list(signature.parameters.values())
    if arg_index >= len(parameters) or arg_index < -len(parameters):
        return []
    return parameter_type_hints(parameters[arg_index])


def function_kwarg_type_hint(function: Function, key: str) -> List[Any]:
    """Returns the type hint for keyword function call argument with the specified key.

    Returns empty list if the function has no such keyword argument or if the argument has no type hint.
    """
    signature: inspect.Signature = function_signature(function)
    if key not in signature.parameters:
        return []
    return parameter_type_hints(signature.parameters[key])


def function_return_type_hint(function: Function) -> List[Any]:
    signature: inspect.Signature = function_signature(function)
    if signature.return_annotation is inspect.Signature.empty:
        return []

    return _resolve_type_hints(signature.return_annotation)


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


def _resolve_type_hints(type_hint: Any) -> List[Any]:
    """Returns all types in the provided type hint.

    Examples:
        str -> [str]
        List[str] -> [List[str]]
        Tuple[str, int] -> [Tuple[str, int]]
        str | FunctionCall -> [str, FunctionCall]
        List[str | FunctionCall] -> [List[str | FunctionCall]]
        Tuple[str | FunctionCall, int] -> [Tuple[str | FunctionCall, int]]

    """
    if _is_union_origin(get_origin(type_hint)):
        return _resolve_union_type_hint(type_hint)
    else:
        return [type_hint]


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
