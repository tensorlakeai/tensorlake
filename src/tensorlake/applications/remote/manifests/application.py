from typing import Dict, List

from ...interface.function import Function, _ApplicationConfiguration
from .function import create_function_manifest
from .manifests import ApplicationManifest, FunctionManifest


def create_application_manifest(
    application_function: Function, all_functions: List[Function]
) -> ApplicationManifest:
    app_config: _ApplicationConfiguration = application_function.application_config

    function_manifests: Dict[str, FunctionManifest] = {
        fn.function_config.function_name: create_function_manifest(
            application_function, app_config.version, fn
        )
        for fn in all_functions
    }

    return ApplicationManifest(
        name=application_function.function_config.function_name,
        description=application_function.function_config.description,
        tags=app_config.tags,
        version=app_config.version,
        functions=function_manifests,
        default_api=application_function.function_config.function_name,
    )
