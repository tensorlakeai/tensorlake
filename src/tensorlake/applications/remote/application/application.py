from typing import Dict, List

from ...interface.application import Application
from ...interface.function import Function
from .function import create_function_manifest
from .manifests import ApplicationManifest, FunctionManifest


def create_application_manifest(
    app: Application, functions: List[Function]
) -> ApplicationManifest:
    function_manifests: Dict[str, FunctionManifest] = {
        fn.function_config.function_name: create_function_manifest(app, fn)
        for fn in functions
    }

    if app.default_api_function is None:
        default_api_function_name: str = ""
        for function in functions:
            # Use the first API function as the default if no default is explicitly set by user.
            if function.api_config is not None:
                default_api_function_name = function.function_config.function_name
                break
    else:
        default_api_function_name: str = (
            app.default_api_function.function_config.function_name
        )

    return ApplicationManifest(
        name=app.name,
        description=app.description,
        tags=app.tags,
        version=app.version,
        functions=function_manifests,
        default_api=default_api_function_name,
    )
