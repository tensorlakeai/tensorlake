from typing import Any

from ..interface.function_call import RegularFunctionCall
from ..interface.request_context import RequestContext
from ..registry import get_class


def create_self_instance(class_name: str) -> Any:
    # TODO: Raise RequestError with a clear description if the class is not found and class_name is not None.
    # Right now an Exception is raised from get_class without details.
    cls: Any = get_class(class_name)
    instance: Any = cls()  # Creating an instance and calling our empty constructor here
    instance.__tensorlake_original_init__()  # Calling original user constructor here
    return instance


def set_self_arg(function_call: RegularFunctionCall, self_instance: Any) -> None:
    function_call.args.insert(0, self_instance)
