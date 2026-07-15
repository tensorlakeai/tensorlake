from typing import Any

from ..interface import DeserializationError, File, Function
from ..metadata import ValueMetadata
from ..user_data_serializer import (
    APPLICATION_FUNCTION_CALL_SERIALIZER_NAME,
    SDK_FUNCTION_CALL_SERIALIZER_NAME,
    UserDataSerializer,
    serializer_by_name,
)


def function_input_serializer(function: Function, app_call: bool) -> UserDataSerializer:
    """Returns the appropriate user data serializer for inputs of the given function.

    app_call indicates if the inputs are for a top-level application function call.
    Raises InternalError if the serializer is unknown.
    """
    return (
        serializer_by_name(APPLICATION_FUNCTION_CALL_SERIALIZER_NAME)
        if app_call
        else serializer_by_name(SDK_FUNCTION_CALL_SERIALIZER_NAME)
    )


def function_output_serializer(
    function: Function, output_serializer_override: str | None
) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function outputs.

    Raises InternalError if the serializer is unknown.
    """
    if output_serializer_override is not None:
        return serializer_by_name(output_serializer_override)
    if function._application_config is not None:
        return serializer_by_name(APPLICATION_FUNCTION_CALL_SERIALIZER_NAME)
    return serializer_by_name(SDK_FUNCTION_CALL_SERIALIZER_NAME)


def serialize_value(
    value: Any,
    serializer: UserDataSerializer,
    value_id: str,
    type_hint: Any,
) -> tuple[bytes, ValueMetadata]:
    """Serializes the given value using the provided serializer.

    A type hint is either type(value) or a type hint from function signature.

    The returned ValueMetadata has the supplied value_id.
    Raises SerializationError if serialization fails.
    """
    metadata: ValueMetadata = ValueMetadata(
        id=value_id,
        type_hint=None,  # Set below
        serializer_name=None,
        content_type="",  # Set below
    )
    data: bytes = None
    if isinstance(value, File):
        data = value.content
        metadata.content_type = value.content_type
        metadata.type_hint = File
    else:
        data = serializer.serialize(value, type_hint=type_hint)
        metadata.content_type = serializer.content_type
        metadata.serializer_name = serializer.name
        metadata.type_hint = type_hint

    return data, metadata


def deserialize_value_with_metadata(
    serialized_value: bytes | bytearray | memoryview,
    metadata: ValueMetadata,
) -> Any | File:
    """Deserializes the given value using the provided serializer and metadata.

    Raises DeserializationError if deserialization fails.
    Raises InternalError if serializer in the metadata is unknown.
    """
    # metadata.serializer_name is None for Files in some cases.
    # Pass a placeholder serializer then, it's not going to be used anyway.
    serializer: UserDataSerializer = (
        serializer_by_name(SDK_FUNCTION_CALL_SERIALIZER_NAME)
        if metadata.serializer_name is None
        else serializer_by_name(metadata.serializer_name)
    )
    return deserialize_value(
        serialized_value=serialized_value,
        serializer=serializer,
        content_type=metadata.content_type,
        type_hint=metadata.type_hint,
    )


def deserialize_value(
    serialized_value: bytes | bytearray | memoryview,
    serializer: UserDataSerializer,
    content_type: str | None,
    type_hint: Any,
) -> Any | File:
    """Deserializes the given value using the provided serializer and information.

    If type_hint is File, deserializes to File using the provided content_type.
    Otherwise, deserializes to the type hinted by type_hint using the provided serializer.

    Raises DeserializationError if deserialization fails.
    """
    if type_hint is File:
        if content_type is None:
            raise DeserializationError(
                "Deserializing to File requires a content type, but None was provided."
            )
        # Don't pass memoryview or bytearray to users at a cost of memory copy.
        # This is because bytes have much more capabilities than bytearray or memoryview.
        # And it'll be confusing to users to handle the conversion themselves when they need bytes.
        # We postpone this copying until we have to here. Other deserialization approaches don't do this.
        serialized_value: bytes = (
            bytes(serialized_value)
            if isinstance(serialized_value, (memoryview, bytearray))
            else serialized_value
        )
        return File(content=serialized_value, content_type=content_type)
    else:
        return serializer.deserialize(serialized_value, type_hint)
