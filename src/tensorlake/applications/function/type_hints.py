import inspect
import pickle
from types import UnionType
from typing import Annotated, Any, Dict, List, Set, Tuple, Union, get_args, get_origin

import pydantic

from ..interface import File, Function


def parameter_type_hints(parameter: inspect.Parameter) -> List[Any]:
    """Returns the type hint for the provided function parameter.

    Returns empty list if the parameter has no type hint.
    """
    if parameter.annotation is inspect.Parameter.empty:
        return []
    return _resolve_type_hint(parameter.annotation)


def type_hint_arguments(type_hint: Any) -> List[Any]:
    """Returns the type hints of arguments for the provided type hint.

    Examples:
    For List[str] returns [[str]].
    For Dict[str, int] returns [[str], [int]].
    For Union[str, int] returns [[str, int]].
    For tuple[Union[str, int], bool] returns [[str, int], [bool]].
    For List[int] returns [[int]].
    For Dict[str, List[int]] returns [[str], [int]].
    For Dict[str, List[int] | None] returns [[str], [int, None]].
    For str returns [].
    """
    args: tuple[Any, ...] = get_args(type_hint)
    resolved_args: List[Any] = []
    for arg in args:
        resolved_args.append(_resolve_type_hint(arg))
    return resolved_args


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
    """Simplifies the provided type hint.

    If the provided type hint is a union of types, returns all singular types in the union in a list.
    If the type hint is an annotated type, like Annotated[T, ...] or Optional[T], returns T.
    Otherwise, returns a list with the provided type hint as the only element.

    Examples:
        str -> [str]
        List[str] -> [List[str]]
        Tuple[str, int] -> [Tuple[str, int]]
        str | FunctionCall -> [str, FunctionCall]
        List[str | FunctionCall] -> [List[str | FunctionCall]]
        Tuple[str | FunctionCall, int] -> [Tuple[str | FunctionCall, int]]
    """
    origin: Any = get_origin(type_hint)
    if _is_union_origin(origin):
        # Also handles Optional[T] because its origin is Union[T, None].
        return _resolve_union_type_hint(type_hint)
    elif origin is Annotated:
        return _resolve_type_hint(get_args(type_hint)[0])
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


def is_list_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for a list."""
    # Handles type hints: List[T], list[T], list.
    return (
        get_origin(type_hint) is List
        or get_origin(type_hint) is list
        or type_hint is list
    )


def is_dict_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for a dict."""
    # Handles type hints: Dict[K, V], dict[K, V], dict.
    return (
        get_origin(type_hint) is Dict
        or get_origin(type_hint) is dict
        or type_hint is dict
    )


def is_set_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for a set."""
    # Set is serialized as array by json.dumps.
    # Handles type hints: Set[T], set[T], set.
    return (
        get_origin(type_hint) is Set or get_origin(type_hint) is set or type_hint is set
    )


def is_tuple_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for a tuple."""
    # Handles type hints: Tuple[T], tuple[T], tuple.
    return (
        get_origin(type_hint) is Tuple
        or get_origin(type_hint) is tuple
        or type_hint is tuple
    )


def is_pydantic_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for a Pydantic model class."""
    return inspect.isclass(type_hint) and issubclass(type_hint, pydantic.BaseModel)


def is_file_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for an SDK File."""
    return inspect.isclass(type_hint) and issubclass(type_hint, File)
