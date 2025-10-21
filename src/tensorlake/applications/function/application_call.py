from typing import Any, List

from ..interface.awaitables import FunctionCallAwaitable
from ..interface.file import File
from ..interface.function import Function
from ..registry import get_class
from .type_hints import function_arg_type_hint
from .user_data_serializer import deserialize_value, function_input_serializer


def _application_function_call_with_object_payload(
    application: Function, object: Any
) -> FunctionCallAwaitable:
    """Creates a FunctionCallAwaitable for the application function with the provided payload.

    This is used for application function calls done using SDK.
    The FunctionCallAwaitable is compliant with application function calling convention.
    """
    # Application function calling convention:
    # [payload: Optional type hint]
    if application._function_config.class_name is None:
        return application.awaitable(object)
    else:
        # Warning: don't create class instance here as it must be reused by SDK if created once.
        cls: Any = get_class(application._function_config.class_name)
        return getattr(cls, application._function_config.class_method_name).awaitable(
            object
        )


def application_function_call_with_serialized_payload(
    application: Function, payload: bytes, payload_content_type: str
) -> FunctionCallAwaitable:
    """Creates a FunctionCallAwaitable for the API function with the provided serialized payload.

    This is used for API function calls done over HTTP.
    The FunctionCallAwaitable is compliant with API function calling convention.
    The supplied binary payload is deserialized using the input serializer and type hints of the API function.
    """
    # We're using API function payload argument type hint to determine how to deserialize it properly.
    payload_type_hints: List[Any] = function_arg_type_hint(application, -1)
    deserialized_payload: Any | File = deserialize_value(
        serialized_value=payload,
        content_type=payload_content_type,
        serializer=function_input_serializer(application),
        type_hints=payload_type_hints,
    )
    return _application_function_call_with_object_payload(
        application, deserialized_payload
    )
