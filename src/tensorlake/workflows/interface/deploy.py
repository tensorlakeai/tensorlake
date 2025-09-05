from typing import List

from ..application import get_user_defined_or_default_application
from ..registry import get_functions
from ..remote.api_client import APIClient
from ..remote.application.application import (
    ApplicationManifest,
    create_application_manifest,
)
from ..remote.application.code import zip_application_code
from .application import Application
from .function import Function


def deploy(
    application_source_dir_path: str, upgrade_tasks_to_latest_version: bool = False
) -> None:
    """Deploys all the Tensorlake Functions so they are runnable in remote mode (i.e. on Tensorlake Cloud).

    `application_source_dir_path` is a path to application source code directory.
    """
    # TODO: Validate the graph.
    application: Application = get_user_defined_or_default_application()
    functions: List[Function] = get_functions()

    # TODO: We can only generate the manifest once we loaded all of the files from the
    # application code dir into memory because all of them need to get registered first.
    app_manifest: ApplicationManifest = create_application_manifest(
        application, functions
    )
    app_code: bytes = zip_application_code(application, application_source_dir_path)
    with APIClient() as api_client:
        api_client.upsert_application(
            manifest_json=app_manifest.model_dump_json(),
            code_zip=app_code,
            upgrade_tasks_to_latest_version=upgrade_tasks_to_latest_version,
        )
