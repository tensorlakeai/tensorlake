from typing import Any

from ..interface.file import File
from ..interface.function import Function
from ..metadata import ValueMetadata
from ..user_data_serializer import (
    NON_API_FUNCTION_SERIALIZER_NAME,
    UserDataSerializer,
    serializer_by_name,
)


def function_input_serializer(function: Function) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function inputs."""
    if function._application_config is not None:
        return serializer_by_name(function._application_config.input_deserializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)


def function_output_serializer(
    function: Function, output_serializer_override: str | None
) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function outputs."""
    if output_serializer_override is not None:
        return serializer_by_name(output_serializer_override)
    if function._application_config is not None:
        return serializer_by_name(function._application_config.output_serializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)


def serialize_value(
    value: Any, serializer: UserDataSerializer, value_id: str
) -> tuple[bytes, ValueMetadata]:
    """Serializes the given value using the provided serializer.

    The returned ValueMetadata has the supplied value_id.
    """
    metadata: ValueMetadata = ValueMetadata(
        id=value_id,
        cls=type(value),
        serializer_name=None,
        content_type=None,
    )
    data: bytes = None
    if isinstance(value, File):
        data = value.content
        metadata.content_type = value.content_type
    else:
        data = serializer.serialize(value)
        metadata.serializer_name = serializer.name

    return data, metadata


def deserialize_value(
    serialized_value: bytes,
    metadata: ValueMetadata,
) -> Any | File:
    """Deserializes the given value using the provided serializer and type hints."""
    is_file_output: bool = metadata.cls is File

    if is_file_output:
        if metadata.content_type is None:
            raise ValueError(
                "Deserializing to File requires a content type, but None was provided."
            )
        return File(content=serialized_value, content_type=metadata.content_type)
    else:
        if metadata.serializer_name is None:
            raise ValueError(
                "Serializer name is None for non-File value. Cannot deserialize value."
            )
        return serializer_by_name(metadata.serializer_name).deserialize(
            serialized_value, [metadata.cls]
        )
