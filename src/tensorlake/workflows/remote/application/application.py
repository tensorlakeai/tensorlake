from typing import Dict, List

from pydantic import BaseModel

from ...interface.application import Application
from ...interface.function import Function
from .function import FunctionManifest, create_function_manifest


class ApplicationManifest(BaseModel):
    name: str
    description: str
    tags: Dict[str, str]
    version: str
    functions: Dict[str, FunctionManifest]


def create_application_manifest(
    app: Application, functions: List[Function]
) -> ApplicationManifest:
    functions: Dict[str, FunctionManifest] = {
        fn.function_config.function_name: create_function_manifest(app, fn)
        for fn in functions
    }

    return ApplicationManifest(
        name=app.name,
        description=app.description,
        tags=app.tags,
        version=app.version,
        functions=functions,
    )
