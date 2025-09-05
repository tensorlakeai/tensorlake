from typing import Any, List

from ..interface.function import Function
from ..interface.function_call import RegularFunctionCall
from ..registry import get_class, get_function
from .function_call import prepend_request_context_placeholder_to_function_args
from .type_hints import function_arg_type_hint
from .user_data_serializer import function_input_serializer


def api_function_call_with_object_payload(
    api: Function | str, object: Any
) -> RegularFunctionCall:
    """Creates a function call for the API function with the provided payload.

    This is used for API function calls done using SDK.
    The function call is compliant with API function calling convention.
    """
    if isinstance(api, str):
        api: Function = get_function(api)

    # API function call conventions:
    # [optional ctx: tensorlake.RequestContext, payload: Optional type hint]
    args: List[Any] = [object]

    prepend_request_context_placeholder_to_function_args(api, args)

    if api.function_config.class_name is None:
        return api(*args)
    else:
        cls: Any = get_class(api.function_config.class_name)
        return cls().getattr(api.function_config.class_method_name)(*args)


def api_function_call_with_serialized_payload(
    api: Function | str, payload: bytes
) -> RegularFunctionCall:
    """Creates a function call for the API function with the provided serialized payload.

    This is used for API function calls done over HTTP.
    The function call is compliant with API function calling convention.
    The supplied binary payload is deserialized using the input serializer and type hints of the API function.
    """
    if isinstance(api, str):
        api: Function = get_function(api)

    deserialized_payload: Any = function_input_serializer(api).deserialize(
        payload, function_arg_type_hint(api, -1)
    )
    return api_function_call_with_object_payload(api, deserialized_payload)
