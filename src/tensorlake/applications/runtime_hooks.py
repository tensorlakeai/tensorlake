from typing import Any, Callable, List, TypeVar

# This module is not part of SDK interface. It contains internal runtime hooks.

# Type vars to make it clear what we expect without importing the corresponding SDK classes.
# This avoids circular dependencies.
Future = TypeVar("Future")
# Either a FunctionCallFuture or a ReduceOperationFuture
FunctionCall = TypeVar("FunctionCall")


# (Futures, timeout: float | None, return_when: int) -> List[Any]
__wait_futures: Callable[[List[Future], float | None, int], List[Any]] | None = None


def wait_futures(
    futures: List[Future], timeout: float | None, return_when: int
) -> tuple[List[Future], List[Future]]:
    """Waits for the given futures to complete respecting the timeout and return_when.

    The future's results (value or exception) are set on return.
    Returns a tuple of two lists: (done_futures, not_done_futures).
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


__run_function_calls: Callable[[List[FunctionCall]], List[Future]] = None


def run_function_calls(function_calls: List[FunctionCall]) -> List[Future]:
    """Starts running the given function calls and returns their Futures."""
    global __run_function_calls
    if __run_function_calls is None:
        raise RuntimeError(
            "Internal error: __run_function_calls runtime hook not initialized"
        )

    return __run_function_calls(function_calls)


def set_run_function_calls_hook(hook: Any) -> None:
    global __run_function_calls
    if __run_function_calls is not None:
        raise RuntimeError(
            "Internal error: __run_function_calls runtime hook already initialized"
        )

    __run_function_calls = hook
