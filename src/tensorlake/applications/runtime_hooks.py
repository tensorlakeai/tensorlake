from collections.abc import Generator
from typing import Any, Callable, List, TypeVar

from .interface.exceptions import InternalError, SDKUsageError

# This module is not part of SDK interface. It contains internal runtime hooks.

# Type vars to make it clear what we expect without importing the corresponding SDK classes.
# This avoids circular dependencies.
Future = TypeVar("Future")


# (Futures, timeout: float | None, return_when: int) -> List[Any]
__wait_futures: (
    Callable[
        [List[Future], float | None, int],
        tuple[List[Any], tuple[List[Future], List[Future]]],
    ]
    | None
) = None


def wait_futures(
    futures: List[Future], timeout: float | None, return_when: int
) -> tuple[List[Future], List[Future]]:
    """Waits for the given futures to complete respecting the timeout and return_when.

    The future's results (value or exception) are set on return.
    Returns a tuple of two lists: (done_futures, not_done_futures).
    This is similar to https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.wait.
    """
    global __wait_futures
    if __wait_futures is None:
        _raise_multiprocessing_usage_error()

    return __wait_futures(futures, timeout, return_when)


def set_wait_futures_hook(hook: Any) -> None:
    global __wait_futures
    if __wait_futures is not None:
        raise InternalError("__wait_futures runtime hook already initialized")

    __wait_futures = hook


def clear_wait_futures_hook() -> None:
    """Clears the __wait_futures runtime hook if set.

    Never raises.
    """
    global __wait_futures
    __wait_futures = None


# (Future) -> Generator[None, None, Any]
__await_future: (
    Callable[
        [Future],
        Generator[None, None, Any],
    ]
    | None
) = None


def await_future(future: Future) -> Generator[None, None, Any]:
    """Returns a generator that yields until the future is completed.

    The future's results (value or exception) are set on generator return.
    This is used to await a Future in an async Function.
    """
    global __await_future
    if __await_future is None:
        _raise_multiprocessing_usage_error()

    return __await_future(future)


def set_await_future_hook(hook: Any) -> None:
    global __await_future
    if __await_future is not None:
        raise InternalError("__await_future runtime hook already initialized")

    __await_future = hook


def clear_await_future_hook() -> None:
    """Clears the __await_future runtime hook if set.

    Never raises.
    """
    global __await_future
    __await_future = None


__run_futures: Callable[[List[Future]], None] = None


def run_futures(futures: List[Future]) -> None:
    """Starts running the given futures in background.

    Future results are set when the futures complete.
    """
    global __run_futures
    if __run_futures is None:
        _raise_multiprocessing_usage_error()

    return __run_futures(futures)


def set_run_futures_hook(hook: Any) -> None:
    global __run_futures
    if __run_futures is not None:
        raise InternalError("__run_futures runtime hook already initialized")

    __run_futures = hook


def clear_run_futures_hook() -> None:
    """Clears the __run_futures runtime hook if set.

    Never raises.
    """
    global __run_futures
    __run_futures = None


def _raise_multiprocessing_usage_error() -> None:
    raise SDKUsageError(
        "Tensorlake SDK is not initialized. If you are using multiprocessing, please note that "
        "only a RequestContext created in the main process can be used in child processes. "
        "Other SDK features are not available in child processes at the moment."
    )
