import base64
import inspect
import pickle
from typing import Any, Dict, List

from pydantic import BaseModel

from ...function.type_hints import (
    function_parameters,
    function_signature,
    serialize_type_hint,
)
from ...function.user_data_serializer import (
    function_input_serializer,
    function_output_serializer,
)
from ...interface.function import Function, _ApplicationConfiguration
from .function import FunctionManifest, create_function_manifest


class EntryPointInputManifest(BaseModel):
    arg_name: str
    type_hint: Any


def serialize_input_manifests(manifests: list[EntryPointInputManifest]) -> bytes:
    return pickle.dumps(manifests)


def deserialize_input_manifests(serialized_input_manifests: bytes) -> List[Any]:
    return pickle.loads(serialized_input_manifests)


class EntryPointManifest(BaseModel):
    function_name: str
    input_serializer: str
    # pickled and base64 encoded list[EntryPointInputManifest]
    # The inputs are sorted in order of function arguments.
    inputs_base64: str
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
    app_signature: inspect.Signature = function_signature(application_function)

    function_manifests: Dict[str, FunctionManifest] = {
        fn._function_config.function_name: create_function_manifest(
            application_function, app_config.version, fn
        )
        for fn in all_functions
    }

    inputs: list[EntryPointInputManifest] = _input_manifests(application_function)
    inputs_base64: str = base64.encodebytes(serialize_input_manifests(inputs)).decode(
        "utf-8"
    )

    output_type_hint_base64: str = base64.encodebytes(
        serialize_type_hint(app_signature.return_annotation)
    ).decode("utf-8")

    return ApplicationManifest(
        name=application_function._function_config.function_name,
        description=application_function._function_config.description,
        tags=app_config.tags,
        version=app_config.version,
        functions=function_manifests,
        entrypoint=EntryPointManifest(
            function_name=application_function._function_config.function_name,
            input_serializer=function_input_serializer(
                application_function, app_call=True
            ).name,
            inputs_base64=inputs_base64,
            # Application functions never have an output serializer override.
            output_serializer=function_output_serializer(
                application_function, None
            ).name,
            output_type_hints_base64=output_type_hint_base64,
        ),
    )


def _input_manifests(application_function: Function) -> list[EntryPointInputManifest]:
    parameters_in_definition_order: list[inspect.Parameter] = function_parameters(
        application_function
    )
    input_manifests: list[EntryPointInputManifest] = []
    for parameter in parameters_in_definition_order:
        parameter: inspect.Parameter

        input_manifests.append(
            EntryPointInputManifest(
                arg_name=parameter.name, type_hint=parameter.annotation
            )
        )

    return input_manifests
