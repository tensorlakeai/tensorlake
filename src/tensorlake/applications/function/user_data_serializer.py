from typing import Any

from ..interface import DeserializationError, File, Function
from ..metadata import ValueMetadata
from ..user_data_serializer import (
    NON_API_FUNCTION_SERIALIZER_NAME,
    SerializationResult,
    UserDataSerializer,
    serializer_by_name,
)


def function_input_serializer(function: Function) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function inputs.

    Raises InternalError if the serializer is unknown.
    """
    if function._application_config is not None:
        return serializer_by_name(function._application_config.input_deserializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)


def function_output_serializer(
    function: Function, output_serializer_override: str | None
) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function outputs.

    Raises InternalError if the serializer is unknown.
    """
    if output_serializer_override is not None:
        return serializer_by_name(output_serializer_override)
    if function._application_config is not None:
        return serializer_by_name(function._application_config.output_serializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)


def serialize_value(
    value: Any,
    serializer: UserDataSerializer,
    value_id: str,
    type_hints: list[Any],
) -> tuple[bytes, ValueMetadata]:
    """Serializes the given value using the provided serializer.

    The type_hints parameter specifies possible types of the value.
    type_hints must not be empty if the passed serializer is JSON serializer.

    The returned ValueMetadata has the supplied value_id.
    Raises SerializationError if serialization fails.
    """
    metadata: ValueMetadata = ValueMetadata(
        id=value_id,
        type_hint=None,
        has_type_hint=False,
        serializer_name=None,
        content_type=None,
    )
    data: bytes = None
    if isinstance(value, File):
        data = value.content
        metadata.content_type = value.content_type
        metadata.has_type_hint = True
        metadata.type_hint = File
    else:
        result: SerializationResult = serializer.serialize(value, type_hints=type_hints)
        data = result.data
        metadata.serializer_name = serializer.name
        metadata.has_type_hint = result.type_hint_set
        metadata.type_hint = result.type_hint

    return data, metadata


def deserialize_value_with_metadata(
    serialized_value: bytes | bytearray | memoryview,
    metadata: ValueMetadata,
) -> Any | File:
    """Deserializes the given value using the provided serializer and metadata.

    Raises DeserializationError if deserialization fails.
    Raises InternalError if serializer in the metadata is unknown.
    """
    serializer: UserDataSerializer = serializer_by_name(metadata.serializer_name)
    return deserialize_value(
        serialized_value=serialized_value,
        serializer=serializer,
        content_type=metadata.content_type,
        type_hints=[metadata.type_hint] if metadata.has_type_hint else [],
    )


def deserialize_value(
    serialized_value: bytes | bytearray | memoryview,
    serializer: UserDataSerializer,
    content_type: str | None,
    type_hints: list[Any],
) -> Any | File:
    """Deserializes the given value using the provided serializer and information.

    If type_hints contain File, deserializes to File using the provided content_type.
    Otherwise, deserializes to the type hinted by type_hints using the provided serializer.

    Raises DeserializationError if deserialization fails.
    """
    has_file_type_hint: bool = any(type_hint is File for type_hint in type_hints)
    if has_file_type_hint:
        # First we try to deserialize without File type hint. If serialized data doesn't match
        # any non-File type hint, we fall back to File deserialization.
        type_hints = [type_hint for type_hint in type_hints if type_hint is not File]

    last_exception: DeserializationError | None = None
    try:
        return serializer.deserialize(serialized_value, type_hints)
    except DeserializationError as e:
        last_exception = e

    if has_file_type_hint:
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

    raise last_exception
