import atexit
import threading

from ..interface import SDKUsageError
from ..local.runner import LocalRunner
from ..registry import get_function
from ..remote.api_client import APIClient
from ..remote.runner import RemoteRunner
from .function import Function, _is_application_function
from .request import Request


def run_local_application(application: Function | str, *args, **kwargs) -> Request:
    """Runs the application function locally with the given arguments and returns the request.

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

    with LocalRunner(
        app=application, app_args=list(args), app_kwargs=dict(kwargs)
    ) as runner:
        return runner.run()


def run_remote_application(application: Function | str, *args, **kwargs) -> Request:
    """Runs the application function remotely (i.e. on Tensorlake Cloud) with the given kwargs and returns the request.

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
        args=list(args),
        kwargs=dict(kwargs),
        api_client=_get_remote_api_client(),
    ).run()


# Use a singleton API client for all remote application runs because we don't want to require users to manage API clients
# or call close on every RemoteRunner or RemoteRequest.
# Lazily initialized to avoid requiring the Rust Cloud SDK at import time.
_remote_api_client_singleton: APIClient | None = None
_remote_api_client_lock = threading.Lock()


def _get_remote_api_client() -> APIClient:
    global _remote_api_client_singleton
    if _remote_api_client_singleton is None:
        with _remote_api_client_lock:
            if _remote_api_client_singleton is None:
                _remote_api_client_singleton = APIClient()
                atexit.register(_remote_api_client_singleton.close)
    return _remote_api_client_singleton
