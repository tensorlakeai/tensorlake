from dataclasses import dataclass
from typing import Any, List

from tensorlake.vendor.nanoid import generate as nanoid

from ..interface import DeserializationError, File, Function
from ..metadata import ValueMetadata
from ..user_data_serializer import UserDataSerializer
from .type_hints import function_has_kwarg, function_kwarg_type_hint
from .user_data_serializer import (
    deserialize_value,
    function_input_serializer,
    serialize_value,
)


@dataclass
class SerializedKWArg:
    data: bytes
    content_type: str | None


def serialize_application_function_call_kwargs(
    input_serializer: UserDataSerializer, kwargs: dict[str, Any]
) -> dict[str, SerializedKWArg]:
    """Serializes application function call kwargs.

    Returns a dict with serialized value and content type for each argument.

    raises SerializationError if serialization fails.
    """
    # NB: We don't have access to Function here as we might be called from RemoteRunner.
    serialized_kwargs: dict[str, tuple[bytes, str | None]] = {}
    for key, value in kwargs.items():
        data, metadata = serialize_value(
            value=value, value_serializer=input_serializer, value_id=nanoid()
        )
        serialized_kwargs[key] = SerializedKWArg(
            data=data, content_type=metadata.content_type
        )
    return serialized_kwargs


def deserialize_application_function_call_kwargs(
    application: Function, serialized_kwargs: dict[str, SerializedKWArg]
) -> dict[str, Any | File]:
    """Deserializes the API function call kwargs.

    This is used for API function calls done over HTTP.
    The FunctionCallAwaitable is compliant with API function calling convention.
    The supplied binary kwargs are deserialized using the input serializer and type hints of the API function.
    The content type from serialized_kwargs is used as File content type when application function expects a File.

    raises DeserializationError if deserialization fails.
    """
    deserialized_kwargs: dict[str, Any | File] = {}
    for key, serialized_kwarg in serialized_kwargs.items():
        serialized_kwarg: SerializedKWArg
        if not function_has_kwarg(application, key):
            continue  # Allow users to pass unknown kwargs to give them more flexibility.

        deserialized_kwargs[key] = _deserialize_application_function_call_kwarg(
            application=application,
            key=key,
            payload=serialized_kwarg.data,
            payload_content_type=serialized_kwarg.content_type,
        )

    return deserialized_kwargs


def _deserialize_application_function_call_kwarg(
    application: Function,
    key: str,
    payload: bytes,
    payload_content_type: str | None,
) -> Any | File:
    """Deserializes a single application function call kwarg."""
    input_serializer: UserDataSerializer = function_input_serializer(application)
    # We're using API function payload argument type hint to determine how to deserialize it properly.
    payload_type_hints: List[Any] = function_kwarg_type_hint(application, key)
    if len(payload_type_hints) == 0:
        # We are now doing pre-deployment validation for this so this is never supposed to happen.
        raise DeserializationError(
            f"Cannot deserialize application function '{application}' parameter '{key}': parameter is missing type hint."
        )

    last_error: DeserializationError | None = None

    for type_hint in payload_type_hints:
        try:
            return deserialize_value(
                serialized_value=payload,
                metadata=ValueMetadata(
                    id="fake_id",
                    cls=type_hint,
                    serializer_name=input_serializer.name,
                    content_type=payload_content_type,
                ),
            )
        except DeserializationError as e:
            last_error = e

    # If all deserialization attempts failed, raise the last exception.
    raise last_error
