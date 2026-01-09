import inspect
from dataclasses import dataclass
from typing import Any

from tensorlake.vendor.nanoid import generate as nanoid

from ..interface import DeserializationError, File, Function, InternalError
from ..user_data_serializer import UserDataSerializer
from .type_hints import (
    function_parameters,
    function_signature,
)
from .user_data_serializer import (
    deserialize_value,
    function_input_serializer,
    serialize_value,
)


@dataclass
class ApplicationArgument:
    value: Any | File
    type_hint: Any


@dataclass
class SerializedApplicationArgument:
    data: bytes | memoryview
    content_type: str


def serialize_application_function_call_arguments(
    input_serializer: UserDataSerializer,
    args: list[ApplicationArgument],
    kwargs: dict[str, ApplicationArgument],
) -> tuple[
    list[SerializedApplicationArgument], dict[str, SerializedApplicationArgument]
]:
    """Serializes application function call args and kwargs.

    Returns a dict with serialized value and content type for each argument.

    raises SerializationError if serialization fails.
    """
    # NB: We don't have access to Function here as we might be called from RemoteRunner.
    serialized_args: list[SerializedApplicationArgument] = []
    for arg in args:
        arg: ApplicationArgument
        arg_data, arg_metadata = serialize_value(
            value=arg.value,
            serializer=input_serializer,
            value_id=nanoid(),
            type_hint=arg.type_hint,
        )
        serialized_args.append(
            SerializedApplicationArgument(
                data=arg_data, content_type=arg_metadata.content_type
            )
        )

    serialized_kwargs: dict[str, tuple[bytes, str | None]] = {}
    for kwarg_key, kwarg in kwargs.items():
        kwarg: ApplicationArgument
        kwarg_data, kwarg_metadata = serialize_value(
            value=kwarg.value,
            serializer=input_serializer,
            value_id=nanoid(),
            type_hint=kwarg.type_hint,
        )
        serialized_kwargs[kwarg_key] = SerializedApplicationArgument(
            data=kwarg_data, content_type=kwarg_metadata.content_type
        )

    return serialized_args, serialized_kwargs


def deserialize_application_function_call_arguments(
    application: Function,
    serialized_args: list[SerializedApplicationArgument],
    serialized_kwargs: dict[str, SerializedApplicationArgument],
) -> tuple[list[Any | File], dict[str, Any | File]]:
    """Deserializes the API function call args and kwargs.

    The supplied binary args and kwargs are deserialized using the input serializer and type hints of the API function.
    The content type from serialized_kwargs is used as File content type when application function expects a File.

    raises DeserializationError if deserialization fails.
    """
    input_serializer: UserDataSerializer = function_input_serializer(
        application, app_call=True
    )
    signature: inspect.Signature = function_signature(application)

    deserialized_kwargs: dict[str, Any | File] = {}
    for key, serialized_kwarg in serialized_kwargs.items():
        serialized_kwarg: SerializedApplicationArgument
        if key not in signature.parameters:
            # Allow users to pass unknown args and ignore them instead of failing.
            # This gives them more flexibility i.e. when they migrate they add a new
            # application parameter but their new code is not deployed or used yet.
            continue

        parameter: inspect.Parameter = signature.parameters[key]
        if parameter.annotation is inspect.Parameter.empty:
            # We are now doing pre-deployment validation for this so this is never supposed to happen.
            raise DeserializationError(
                f"Cannot deserialize application function '{application}' keyword argument '{key}': argument is missing type hint."
            )

        deserialized_kwargs[key] = _deserialize_application_function_call_arg(
            deserializer=input_serializer,
            arg_type_hint=parameter.annotation,
            serialized_arg=serialized_kwarg,
        )

    # signature.parameters is an ordered mapping in parameters definition order.
    parameters_in_definition_order: list[inspect.Parameter] = function_parameters(
        application
    )
    deserialized_args: list[Any | File] = []
    for i, serialized_arg in enumerate(serialized_args):
        if i >= len(parameters_in_definition_order):
            # Allow users to pass unknown args and ignore them instead of failing.
            # This gives them more flexibility i.e. when they migrate they add a new
            # application parameter but their new code is not deployed or used yet.
            break

        parameter: inspect.Parameter = parameters_in_definition_order[i]
        if parameter.name in serialized_kwargs:
            # Allow users to pass args that are also in kwargs and ignore them instead of failing.
            # If a user used kwarg for a parameter, it's a more clear intention then when a positional
            # argument is used.
            continue

        if parameter.annotation is inspect.Parameter.empty:
            # This should never happen as we do pre-deployment validation for this.
            raise DeserializationError(
                f"Cannot deserialize positional argument {i} of application function '{application}' because the argument has no type hint."
            )

        deserialized_args.append(
            _deserialize_application_function_call_arg(
                deserializer=input_serializer,
                arg_type_hint=parameter.annotation,
                serialized_arg=serialized_arg,
            )
        )

    return deserialized_args, deserialized_kwargs


def _deserialize_application_function_call_arg(
    deserializer: UserDataSerializer,
    arg_type_hint: Any,
    serialized_arg: SerializedApplicationArgument,
) -> Any | File:
    """Deserializes a single application function call argument.

    arg_type_hint is a list of possible type hints for the argument.
    It must not be empty.
    Raises DeserializationError if deserialization fails.
    """
    return deserialize_value(
        serialized_value=serialized_arg.data,
        serializer=deserializer,
        content_type=serialized_arg.content_type,
        type_hint=arg_type_hint,
    )
