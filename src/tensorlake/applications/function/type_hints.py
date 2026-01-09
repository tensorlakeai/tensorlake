import inspect
import pickle
from typing import Any

from ..interface import Awaitable, File, Function


def function_parameters(function: Function) -> list[inspect.Parameter]:
    """Returns the list of function parameters for the provided Tensorlake Function.

    The parameters are in the same order as in the function definition.
    Self parameter for instance methods is not included.
    Raises Exception if the signature cannot be obtained.
    """
    signature: inspect.Signature = function_signature(function)
    first_arg_index: int = 0 if function._function_config.class_name is None else 1
    # signature.parameters is an ordered mapping in parameters definition order.
    return list(signature.parameters.values())[first_arg_index:]


def serialize_type_hint(type_hint: Any) -> bytes:
    return pickle.dumps(type_hint)


def deserialize_type_hint(serialized_type_hint: bytes) -> Any:
    return pickle.loads(serialized_type_hint)


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


def is_file_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for an SDK File."""
    return inspect.isclass(type_hint) and issubclass(type_hint, File)


def is_awaitable_type_hint(type_hint: Any) -> bool:
    """Returns True if the provided type hint is for an SDK Awaitable."""
    return inspect.isclass(type_hint) and issubclass(type_hint, Awaitable)
