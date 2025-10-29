import os
from typing import List, Set

from ..applications import filter_applications
from ..interface.function import Function
from ..registry import get_functions
from ..remote.api_client import APIClient
from ..remote.api_client_context_manager import APIClientContextManager
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
    api_client: APIClient | None = None,
) -> None:
    """Deploys all applications in the supplied .py file so they are runnable in remote mode (i.e. on Tensorlake Cloud).

    `application_file_path` is a path to the .py file where the applications are defined.
    `upgrade_running_requests` indicates whether to update running requests to use the deployed code.
    `load_source_dir_modules` indicates whether to load the .py file so all applications from it get added to the registry.
                               Should be set to True when called from CLI, False when called programmatically from test code
                               because applications in test code are already loaded into registry.
    `api_client` is an optional APIClient to use for deployment. If not supplied, a new client will be created from environment.
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

    with APIClientContextManager(api_client) as api_client:
        for application in filter_applications(functions):
            app_manifest: ApplicationManifest = create_application_manifest(
                application_function=application, all_functions=functions
            )
            api_client.upsert_application(
                manifest_json=app_manifest.model_dump_json(),
                code_zip=app_code,
                upgrade_running_requests=upgrade_running_requests,
            )
