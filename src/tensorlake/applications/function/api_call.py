from typing import Any, List

from ..interface.file import File
from ..interface.function import Function
from ..interface.function_call import RegularFunctionCall
from ..registry import get_class
from .type_hints import function_arg_type_hint
from .user_data_serializer import function_input_serializer


def _api_function_call_with_object_payload(
    api: Function, object: Any
) -> RegularFunctionCall:
    """Creates a function call for the API function with the provided payload.

    This is used for API function calls done using SDK.
    The function call is compliant with API function calling convention.
    """
    # API function call conventions:
    # [optional ctx: tensorlake.RequestContext, payload: Optional type hint]
    args: List[Any] = [object]

    if api.function_config.class_name is None:
        return api(*args)
    else:
        # Warning: don't create class instance here as it must be reused by SDK if created once.
        cls: Any = get_class(api.function_config.class_name)
        return getattr(cls, api.function_config.class_method_name)(*args)


def api_function_call_with_serialized_payload(
    api: Function, payload: bytes, payload_content_type: str
) -> RegularFunctionCall:
    """Creates a function call for the API function with the provided serialized payload.

    This is used for API function calls done over HTTP.
    The function call is compliant with API function calling convention.
    The supplied binary payload is deserialized using the input serializer and type hints of the API function.
    """
    # We're using API function payload argument type hint to determine how to deserialize it properly.
    payload_type_hints: List[Any] = function_arg_type_hint(api, -1)
    payload_is_file: bool = False
    for hint in payload_type_hints:
        if hint is File:
            payload_is_file = True

    if payload_is_file:
        deserialized_payload: File = File(
            content_type=payload_content_type, content=payload
        )
    else:
        deserialized_payload: Any = function_input_serializer(api).deserialize(
            payload, payload_type_hints
        )

    return _api_function_call_with_object_payload(api, deserialized_payload)
