from traceback import format_exception
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
    awaitable: Awaitable, cause: str | BaseException | None
) -> FunctionError:
    if isinstance(cause, BaseException):
        exception_str: str = "".join(format_exception(cause))
        return FunctionError(f"{awaitable} failed due to exception: \n{exception_str}")
    elif isinstance(cause, str):
        return FunctionError(f"{awaitable} failed: {cause}")
    elif cause is None:
        return FunctionError(f"{awaitable} failed")
