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


def serialize_value(value: Any, serializer: UserDataSerializer) -> tuple[bytes, str]:
    """Serializes the given value using the provided serializer."""
    if isinstance(value, File):
        return value.content, value.content_type
    else:
        return (
            serializer.serialize(value),
            serializer.content_type,
        )


def deserialize_value(
    serialized_value: bytes,
    serialized_value_content_type: str,
    serializer: UserDataSerializer,
    type_hints: List[Any],
) -> Any | File:
    """Deserializes the given value using the provided serializer and type hints."""
    is_file_output: bool = False
    for type_hint in type_hints:
        if type_hint is File:
            is_file_output = True

    if is_file_output:
        return File(
            content=serialized_value, content_type=serialized_value_content_type
        )
    else:
        return serializer.deserialize(serialized_value, type_hints)
