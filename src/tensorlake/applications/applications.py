from collections.abc import Generator, Iterator

from .interface.function import Function, _is_application_function
from .interface.request import Request
from .interface.run import (
    run_local_application,
    run_remote_application,
)

# Internal utilities for working with applications.


def filter_applications(
    functions: Iterator[Function],
) -> Generator[Function, None, None]:
    """Yields all applications out of the supplied functions."""
    for function in functions:
        function: Function
        if not _is_application_function(function):
            continue
        yield function


def functions_for_application(
    application: Function,
    functions: Iterator[Function],
) -> list[Function]:
    """Returns functions included in the application build request.

    This is intentionally broader than the application's reachable function graph.
    Today deploy packaging/runtime ships all non-application Tensorlake functions for
    each application and does not derive a smaller server-side closure. Keeping the
    builder aligned with that behavior is safer than doing client-side graph pruning,
    even though it can build more images than strictly necessary.
    """
    return [
        function
        for function in functions
        if function is application or not _is_application_function(function)
    ]


def run_application(
    application: Function | str, remote: bool, *args, **kwargs
) -> Request:
    """Runs the application remotely or locally depending on the `remote` parameter value.

    This is a convenience wrapper around the `run_remote_application` and `run_local_application`.
    It's not part of SDK interface, it's a helper function for writing tests.
    """
    if remote:
        return run_remote_application(application, *args, **kwargs)
    else:
        return run_local_application(application, *args, **kwargs)
