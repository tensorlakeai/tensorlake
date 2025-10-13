from typing import Any, Callable, List, TypeVar

# This module is not part of SDK interface. It contains internal runtime hooks.

# Type vars to make it clear what we expect without importing the corresponding SDK classes.
# This avoids circular dependencies.
Future = TypeVar("Future")
FunctionCall = TypeVar("FunctionCall")


# (Futures, is_async: bool, timeout: float | None) -> List[Any]
__wait_futures: Callable[[List[Future], bool, float | None], List[Any]] | None = None


def wait_futures(
    futures: List[Future], is_async: bool, timeout: float | None
) -> List[Any]:
    global __wait_futures
    if __wait_futures is None:
        raise RuntimeError(
            "Internal error: __wait_futures runtime hook not initialized"
        )

    return __wait_futures(futures, is_async, timeout)


def set_wait_futures_hook(hook: Any) -> None:
    global __wait_futures
    if __wait_futures is not None:
        raise RuntimeError(
            "Internal error: __wait_futures runtime hook already initialized"
        )

    __wait_futures = hook


__start_function_calls: Callable[[List[FunctionCall]], None] = None


def start_function_calls(function_calls: List[FunctionCall]) -> None:
    global __start_function_calls
    if __start_function_calls is None:
        raise RuntimeError(
            "Internal error: __start_function_calls runtime hook not initialized"
        )

    return __start_function_calls(function_calls)


def set_start_function_calls_hook(hook: Any) -> None:
    global __start_function_calls
    if __start_function_calls is not None:
        raise RuntimeError(
            "Internal error: __start_function_calls runtime hook already initialized"
        )

    __start_function_calls = hook


__start_and_wait_function_calls: Callable[[List[FunctionCall]], List[Any]] = None


def start_and_wait_function_calls(function_calls: List[FunctionCall]) -> List[Any]:
    global __start_and_wait_function_calls
    if __start_and_wait_function_calls is None:
        raise RuntimeError(
            "Internal error: __start_and_wait_function_calls runtime hook not initialized"
        )

    return __start_and_wait_function_calls(function_calls)


def set_start_and_wait_function_calls_hook(hook: Any) -> None:
    global __start_and_wait_function_calls
    if __start_and_wait_function_calls is not None:
        raise RuntimeError(
            "Internal error: __start_and_wait_function_calls runtime hook already initialized"
        )

    __start_and_wait_function_calls = hook
