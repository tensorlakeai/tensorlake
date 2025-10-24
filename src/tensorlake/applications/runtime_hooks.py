from typing import Any, Callable, List, TypeVar

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
        raise RuntimeError(
            "Internal error: __wait_futures runtime hook not initialized"
        )

    return __wait_futures(futures, timeout, return_when)


def set_wait_futures_hook(hook: Any) -> None:
    global __wait_futures
    if __wait_futures is not None:
        raise RuntimeError(
            "Internal error: __wait_futures runtime hook already initialized"
        )

    __wait_futures = hook


def clear_wait_futures_hook() -> None:
    """Clears the __wait_futures runtime hook if set.

    Never raises.
    """
    global __wait_futures
    __wait_futures = None


__run_futures: Callable[[List[Future], float | None], None] = None


def run_futures(futures: List[Future], start_delay: float | None) -> None:
    """Starts running the given futures in background with the given delay.

    Future results are set when the futures complete.
    """
    global __run_futures
    if __run_futures is None:
        raise RuntimeError("Internal error: __run_futures runtime hook not initialized")

    return __run_futures(futures, start_delay)


def set_run_futures_hook(hook: Any) -> None:
    global __run_futures
    if __run_futures is not None:
        raise RuntimeError(
            "Internal error: __run_futures runtime hook already initialized"
        )

    __run_futures = hook


def clear_run_futures_hook() -> None:
    """Clears the __run_futures runtime hook if set.

    Never raises.
    """
    global __run_futures
    __run_futures = None
