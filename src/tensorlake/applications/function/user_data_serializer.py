from typing import Any, List

from ..interface.file import File
from ..interface.function import Function
from ..user_data_serializer import (
    NON_API_FUNCTION_SERIALIZER_NAME,
    UserDataSerializer,
    serializer_by_name,
)


def function_input_serializer(function: Function) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function inputs."""
    if function._application_config is not None:
        return serializer_by_name(function._application_config.input_serializer)
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
    value: Any, serializer: UserDataSerializer
) -> tuple[bytes, str | None]:
    """Serializes the given value using the provided serializer.

    Returns a tuple of (serialized_value, content_type).
    The content type is not None only for custom content types set by user
    in File object if the value is File.
    """
    if isinstance(value, File):
        return value.content, value.content_type
    else:
        return (
            serializer.serialize(value),
            None,
        )


def deserialize_value(
    serialized_value: bytes,
    content_type: str | None,
    serializer: UserDataSerializer | None,
    type_hints: List[Any],
) -> Any | File:
    """Deserializes the given value using the provided serializer and type hints."""
    is_file_output: bool = False
    for type_hint in type_hints:
        if type_hint is File:
            is_file_output = True

    if is_file_output:
        if content_type is None:
            raise ValueError(
                "Deserializing to File requires a content type, but None was provided."
            )
        return File(content=serialized_value, content_type=content_type)
    else:
        if serializer is None:
            raise ValueError(
                "Deserializer is None for non-File value. Cannot deserialize value."
            )
        return serializer.deserialize(serialized_value, type_hints)
