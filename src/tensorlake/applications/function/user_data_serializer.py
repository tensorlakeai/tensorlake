from ..interface.function import Function
from ..user_data_serializer import (
    NON_API_FUNCTION_SERIALIZER_NAME,
    UserDataSerializer,
    serializer_by_name,
)


def function_input_serializer(function: Function) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function inputs."""
    if function.application_config is not None:
        return serializer_by_name(function.application_config.input_serializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)


def function_output_serializer(
    function: Function, output_serializer_override: str | None
) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function outputs."""
    if output_serializer_override is not None:
        return serializer_by_name(output_serializer_override)
    if function.application_config is not None:
        return serializer_by_name(function.application_config.output_serializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)
