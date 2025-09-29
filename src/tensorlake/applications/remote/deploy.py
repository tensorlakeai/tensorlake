import os
from typing import Set

from ..application import get_user_defined_or_default_application
from ..interface.application import Application
from ..registry import get_functions
from ..remote.api_client import APIClient
from ..remote.application.application import (
    ApplicationManifest,
    create_application_manifest,
)
from ..remote.application.ignored_code_paths import ignored_code_paths
from ..remote.application.loader import load_application
from ..remote.application.zip import zip_application_code


def deploy(
    application_source_dir_or_file_path: str,
    upgrade_running_requests: bool = True,
    load_application_modules: bool = False,
) -> None:
    """Deploys all the Tensorlake Functions so they are runnable in remote mode (i.e. on Tensorlake Cloud).

    `application_source_dir_or_file_path` is a path to application source code directory or file.
    `upgrade_running_requests` indicates whether to update running requests to use the deployed code.
    `load_application_modules` indicates whether to load all application code modules so that the registry is populated.
                               Should be set to True when called from CLI, False when called programmatically from test code.
    """
    # TODO: Validate the graph.

    # Work with absolute paths to make sure that the path comparisons work correctly.
    application_source_dir_or_file_path: str = os.path.abspath(
        application_source_dir_or_file_path
    )
    application_source_dir_path: str = (
        os.path.dirname(application_source_dir_or_file_path)
        if os.path.isfile(application_source_dir_or_file_path)
        else application_source_dir_or_file_path
    )

    ignored_absolute_paths: Set[str] = ignored_code_paths(application_source_dir_path)

    if load_application_modules:
        load_application(application_source_dir_or_file_path, ignored_absolute_paths)

    # Define default application if the caller didn't define a custom one.
    application: Application = get_user_defined_or_default_application()

    # Now the application is fully loaded into memory so we can use the registry.
    app_manifest: ApplicationManifest = create_application_manifest(
        app=application, functions=get_functions()
    )
    app_code: bytes = zip_application_code(
        application_source_dir_path, ignored_absolute_paths
    )

    with APIClient() as api_client:
        api_client.upsert_application(
            manifest_json=app_manifest.model_dump_json(),
            code_zip=app_code,
            upgrade_running_requests=upgrade_running_requests,
        )
