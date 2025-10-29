from typing import Any, List

from ..interface.awaitables import FunctionCallAwaitable
from ..interface.file import File
from ..interface.function import Function
from ..metadata import ValueMetadata
from ..registry import get_class
from .type_hints import function_arg_type_hint
from .user_data_serializer import deserialize_value, function_input_serializer


def application_function_call(
    application: Function, payload: Any
) -> FunctionCallAwaitable:
    """Creates a FunctionCallAwaitable for the application function with the provided payload.

    The FunctionCallAwaitable is compliant with application function calling convention.
    """
    # Application function calling convention:
    # [payload: Optional type hint]
    if application._function_config.class_name is None:
        return application.awaitable(payload)
    else:
        # Warning: don't create class instance here as it must be reused by SDK if created once.
        cls: Any = get_class(application._function_config.class_name)
        return getattr(cls, application._function_config.class_method_name).awaitable(
            payload
        )


def deserialize_application_function_call_payload(
    application: Function, payload: bytes, payload_content_type: str | None
) -> Any:
    """Deserializes the API function call payload.

    This is used for API function calls done over HTTP.
    The FunctionCallAwaitable is compliant with API function calling convention.
    The supplied binary payload is deserialized using the input serializer and type hints of the API function.
    The payload_content_type is used as File content type when application function expects a File.
    """
    # We're using API function payload argument type hint to determine how to deserialize it properly.
    payload_type_hints: List[Any] = function_arg_type_hint(application, -1)
    if len(payload_type_hints) == 0:
        payload_type_hints = [object]

    last_exception: BaseException | None = None

    for type_hint in payload_type_hints:
        try:
            deserialized_payload: Any | File = deserialize_value(
                serialized_value=payload,
                metadata=ValueMetadata(
                    id="fake_id",
                    cls=type_hint,
                    serializer_name=function_input_serializer(application).name,
                    content_type=payload_content_type,
                ),
            )
        except BaseException as e:
            last_exception = e
            deserialized_payload = None

    if last_exception is not None:
        # If all deserialization attempts failed, raise the last exception.
        raise last_exception

    return deserialized_payload
