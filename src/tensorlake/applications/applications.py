from typing import Any, Generator, Iterator

from .interface.function import Function
from .interface.request import Request
from .interface.run import run_local_application, run_remote_application

# Internal utilities for working with applications.


def filter_applications(
    functions: Iterator[Function],
) -> Generator[Function, None, None]:
    """Yields all applications out of the supplied functions."""
    for function in functions:
        function: Function
        if function._application_config is None:
            continue
        yield function


def run_application(application: Function | str, payload: Any, remote: bool) -> Request:
    """Runs the application remotely or locally depending on the `remote` parameter value.

    This is a convenience wrapper around the `run_remote_application` and `run_local_application`.
    It's not part of SDK interface, it's a helper function for writing tests.
    """
    if remote:
        return run_remote_application(application, payload)
    else:
        return run_local_application(application, payload)
