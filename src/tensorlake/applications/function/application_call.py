from typing import Any, List

from ..interface.file import File
from ..interface.function import Function
from ..interface.function_call import RegularFunctionCall
from ..registry import get_class
from ..user_data_serializer import UserDataSerializer
from .type_hints import function_arg_type_hint
from .user_data_serializer import function_input_serializer


def _application_function_call_with_object_payload(
    application: Function, object: Any
) -> RegularFunctionCall:
    """Creates a function call for the application function with the provided payload.

    This is used for application function calls done using SDK.
    The function call is compliant with application function calling convention.
    """
    # Application function call conventions:
    # [payload: Optional type hint]
    args: List[Any] = [object]

    if application.function_config.class_name is None:
        return application(*args)
    else:
        # Warning: don't create class instance here as it must be reused by SDK if created once.
        cls: Any = get_class(application.function_config.class_name)
        return getattr(cls, application.function_config.class_method_name)(*args)


def application_function_call_with_serialized_payload(
    application: Function, payload: bytes, payload_content_type: str
) -> RegularFunctionCall:
    """Creates a function call for the API function with the provided serialized payload.

    This is used for API function calls done over HTTP.
    The function call is compliant with API function calling convention.
    The supplied binary payload is deserialized using the input serializer and type hints of the API function.
    """
    # We're using API function payload argument type hint to determine how to deserialize it properly.
    payload_type_hints: List[Any] = function_arg_type_hint(application, -1)
    payload_is_file: bool = False
    for hint in payload_type_hints:
        if hint is File:
            payload_is_file = True

    if payload_is_file:
        deserialized_payload: File = File(
            content_type=payload_content_type, content=payload
        )
    else:
        deserialized_payload: Any = function_input_serializer(application).deserialize(
            payload, payload_type_hints
        )

    return _application_function_call_with_object_payload(
        application, deserialized_payload
    )


def serialize_application_call_payload(
    input_serializer: UserDataSerializer, payload: Any
) -> tuple[bytes, str]:
    """Serializes the application payload using the application function input serializer.

    Returns a tuple of (serialized_payload, content_type).
    """
    if isinstance(payload, File):
        return payload.content, payload.content_type
    else:
        return (
            input_serializer.serialize(payload),
            input_serializer.content_type,
        )
