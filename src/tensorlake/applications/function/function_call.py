from typing import Any, List

from ..interface import Awaitable, FunctionError, InternalError
from ..registry import get_class


def create_self_instance(class_name: str) -> Any:
    try:
        cls: Any = get_class(class_name)
    except Exception:
        raise InternalError(f"Class {class_name} not found in Tensorlake Application")

    instance: Any = cls()  # Creating an instance and calling our empty constructor here
    instance.__tensorlake_original_init__()  # Calling original user constructor here
    return instance


def set_self_arg(args: List[Any], self_instance: Any) -> None:
    args.insert(0, self_instance)


def create_function_error(
    awaitable: Awaitable, cause: str | None = None
) -> FunctionError:
    # We currently don't provide cause details because except in rare cases we don't know them
    # at function caller side. The cause details are printed in called function's logs instead.
    if cause is None:
        return FunctionError(f"{awaitable} failed")
    else:
        return FunctionError(f"{awaitable} failed: {cause}")
