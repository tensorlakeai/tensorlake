import os
from typing import List, Set

from ..applications import filter_applications
from ..interface.function import Function
from ..registry import get_functions
from ..remote.api_client import APIClient
from ..remote.manifests.application import (
    ApplicationManifest,
    create_application_manifest,
)
from .code.ignored_code_paths import ignored_code_paths
from .code.loader import load_code
from .code.zip import zip_code


def deploy_applications(
    applications_file_path: str,
    upgrade_running_requests: bool = True,
    load_source_dir_modules: bool = False,
) -> None:
    """Deploys all applications in the supplied .py file so they are runnable in remote mode (i.e. on Tensorlake Cloud).

    `application_file_path` is a path to the .py file where the applications are defined.
    `upgrade_running_requests` indicates whether to update running requests to use the deployed code.
    `load_source_dir_modules` indicates whether to load all applications code modules so that the registry is populated.
                               Should be set to True when called from CLI, False when called programmatically from test code.
    """
    # TODO: Validate the graph.

    # Work with absolute paths to make sure that the path comparisons work correctly.
    applications_file_path: str = os.path.abspath(applications_file_path)
    applications_dir_path: str = os.path.dirname(applications_file_path)
    ignored_absolute_paths: Set[str] = ignored_code_paths(applications_dir_path)

    if load_source_dir_modules:
        load_code(applications_file_path)

    # Now the application is fully loaded into memory so we can use the registry.
    functions: List[Function] = get_functions()
    app_code: bytes = zip_code(
        code_dir_path=applications_dir_path,
        ignored_absolute_paths=ignored_absolute_paths,
        all_functions=functions,
    )

    for application in filter_applications(functions):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=application, all_functions=functions
        )

        with APIClient() as api_client:
            api_client.upsert_application(
                manifest_json=app_manifest.model_dump_json(),
                code_zip=app_code,
                upgrade_running_requests=upgrade_running_requests,
            )
