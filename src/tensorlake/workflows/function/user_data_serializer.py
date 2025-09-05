from ..interface.function import Function
from ..user_data_serializer import (
    NON_API_FUNCTION_SERIALIZER_NAME,
    UserDataSerializer,
    serializer_by_name,
)


def function_input_serializer(function: Function) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function inputs."""
    if function.api_config is not None:
        return serializer_by_name(function.api_config.input_serializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)


def function_output_serializer(function: Function) -> UserDataSerializer:
    """Returns the appropriate user data serializer for the given function outputs."""
    if function.api_config is not None:
        return serializer_by_name(function.api_config.output_serializer)
    return serializer_by_name(NON_API_FUNCTION_SERIALIZER_NAME)
