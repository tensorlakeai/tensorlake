from typing import Any

from ..interface import SDKUsageError
from ..local.runner import LocalRunner
from ..registry import get_function
from ..remote.api_client_context_manager import APIClient
from ..remote.runner import RemoteRunner
from .function import Function, _is_application_function
from .request import Request


def run_local_application(application: Function | str, payload: Any) -> Request:
    """Runs the application function locally with the given payload and returns the request.

    Raises TensorlakeError if failed creating the request.
    """
    if isinstance(application, str):
        try:
            application: Function = get_function(application)
        except Exception as e:
            raise SDKUsageError(f"Function with name '{application}' is not defined")

    if not _is_application_function(application):
        raise SDKUsageError(
            f"{application} is not an application function and cannot be run as an application. "
            "To make it an application function, add @application() decorator to it."
        )

    with LocalRunner(app=application, app_payload=payload) as runner:
        return runner.run()


def run_remote_application(application: Function | str, payload: Any) -> Request:
    """Runs the application function remotely (i.e. on Tensorlake Cloud) with the given payload and returns the request.

    Raises TensorlakeError if failed creating the request.
    """
    if isinstance(application, Function) and not _is_application_function(application):
        raise SDKUsageError(
            f"{application} is not an application function and cannot be run as an application. "
            "To make it an application function, add @application() decorator to it."
        )

    app_name: str = (
        application._function_config.function_name
        if isinstance(application, Function)
        else application
    )

    # We can't get Function object here because the user's client call might not load the function definitions.
    return RemoteRunner(
        application_name=app_name,
        payload=payload,
        api_client=_remote_api_client_singleton,
    ).run()


import atexit

# Use a singleton API client for all remote application runs because we don't want to require users to manage API clients
# or call close on every RemoteRunner or RemoteRequest.
_remote_api_client_singleton: APIClient = APIClient()
atexit.register(_remote_api_client_singleton.close)
