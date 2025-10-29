from typing import Any

from ..local.runner import LocalRunner
from ..registry import get_function
from ..remote.runner import RemoteRunner
from .function import Function
from .request import Request


def run_local_application(application: Function | str, payload: Any) -> Request:
    """Runs the application function locally with the given payload and returns the request."""
    # TODO: validate the application.
    # TODO: validate that the supplied function is an API function.

    if isinstance(application, str):
        application: Function = get_function(application)

    with LocalRunner(app=application, app_payload=payload) as runner:
        return runner.run()


def run_remote_application(application: Function | str, payload: Any) -> Request:
    """Runs the application function remotely (i.e. on Tensorlake Cloud) with the given payload and returns the request."""
    # TODO: validate the graph.
    # TODO: validate that the supplied function is an API function.
    app_name: str = (
        application._function_config.function_name
        if isinstance(application, Function)
        else application
    )

    # We can't get Function object here because the user's client call might not load the function definitions.
    return RemoteRunner(application_name=app_name, payload=payload).run()
