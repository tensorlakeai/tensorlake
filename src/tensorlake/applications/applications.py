from typing import Generator, Iterator

from .interface.function import Function


def filter_applications(
    functions: Iterator[Function],
) -> Generator[Function, None, None]:
    """Yields all applications out of the supplied functions."""
    for function in functions:
        function: Function
        if function.application_config is None:
            continue
        yield function
