import base64
import inspect
from typing import Any, Dict, List

from pydantic import BaseModel

from ...function.type_hints import (
    function_parameters,
    function_return_type_hint,
    parameter_type_hints,
    serialize_type_hints,
)
from ...function.user_data_serializer import (
    function_input_serializer,
    function_output_serializer,
)
from ...interface.function import Function, _ApplicationConfiguration
from .function import FunctionManifest, create_function_manifest


class EntryPointInputManifest(BaseModel):
    arg_name: str
    type_hints_base64: str


class EntryPointManifest(BaseModel):
    function_name: str
    input_serializer: str
    # The inputs are sorted in order of function arguments.
    inputs: list[EntryPointInputManifest]
    output_serializer: str
    output_type_hints_base64: str


class ApplicationManifest(BaseModel):
    name: str
    description: str
    tags: Dict[str, str]
    version: str
    functions: Dict[str, FunctionManifest]
    entrypoint: EntryPointManifest

    def model_dump_json(self, **kwargs: Any) -> str:
        # exclude_unset=True to avoid sending default None values.
        # This is required for JSONSchema object fields where
        # almost everything is optional. And 'default' fields.
        # If they are set to None by it actually means that None is
        # the default value, not absence of default value.
        if "exclude_unset" not in kwargs:
            kwargs["exclude_unset"] = True
        return super().model_dump_json(**kwargs)


def create_application_manifest(
    application_function: Function, all_functions: List[Function]
) -> ApplicationManifest:
    """Creates ApplicationManifest for the supplied application function.

    Raises TensorlakeError on error.
    """
    app_config: _ApplicationConfiguration = application_function._application_config

    function_manifests: Dict[str, FunctionManifest] = {
        fn._function_config.function_name: create_function_manifest(
            application_function, app_config.version, fn
        )
        for fn in all_functions
    }

    output_type_hints: List[Any] = function_return_type_hint(application_function)
    serialized_output_type_hints: bytes = serialize_type_hints(output_type_hints)
    output_type_hints_base64: str = base64.encodebytes(
        serialized_output_type_hints
    ).decode("utf-8")

    return ApplicationManifest(
        name=application_function._function_config.function_name,
        description=application_function._function_config.description,
        tags=app_config.tags,
        version=app_config.version,
        functions=function_manifests,
        entrypoint=EntryPointManifest(
            function_name=application_function._function_config.function_name,
            input_serializer=function_input_serializer(application_function).name,
            inputs=_input_manifests(application_function),
            # Application functions never have an output serializer override.
            output_serializer=function_output_serializer(
                application_function, None
            ).name,
            output_type_hints_base64=output_type_hints_base64,
        ),
    )


def _input_manifests(application_function: Function) -> list[EntryPointInputManifest]:
    parameters_in_definition_order: list[inspect.Parameter] = function_parameters(
        application_function
    )
    input_manifests: list[EntryPointInputManifest] = []
    for parameter in parameters_in_definition_order:
        parameter: inspect.Parameter
        type_hints: list[Any] = parameter_type_hints(parameter)
        serialized_type_hints: bytes = serialize_type_hints(type_hints)
        type_hints_base64: str = base64.encodebytes(serialized_type_hints).decode(
            "utf-8"
        )

        input_manifests.append(
            EntryPointInputManifest(
                arg_name=parameter.name, type_hints_base64=type_hints_base64
            )
        )

    return input_manifests
