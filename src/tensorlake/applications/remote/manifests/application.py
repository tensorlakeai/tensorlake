import base64
from typing import Any, Dict, List

from pydantic import BaseModel

from ...function.type_hints import function_return_type_hint, serialize_type_hints
from ...function.user_data_serializer import (
    function_input_serializer,
    function_output_serializer,
)
from ...interface.function import Function, _ApplicationConfiguration
from .function import FunctionManifest, create_function_manifest


class EntryPointManifest(BaseModel):
    function_name: str
    input_serializer: str
    output_serializer: str
    output_type_hints_base64: str


class ApplicationManifest(BaseModel):
    name: str
    description: str
    tags: Dict[str, str]
    version: str
    functions: Dict[str, FunctionManifest]
    entrypoint: EntryPointManifest


def create_application_manifest(
    application_function: Function, all_functions: List[Function]
) -> ApplicationManifest:
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
            output_serializer=function_output_serializer(
                application_function, None
            ).name,
            output_type_hints_base64=output_type_hints_base64,
        ),
    )
