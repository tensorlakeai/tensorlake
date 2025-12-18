import asyncio
import inspect
import random
import time
from functools import wraps
from typing import Any, Callable, Type, TypeVar

R = TypeVar("R")


def exponential_backoff(
    max_retries: int = 3,
    initial_delay_seconds: float = 0.1,
    max_delay_seconds: float = 15.0,
    jitter_range: tuple[float, float] = (0.5, 1.0),
    retryable_exceptions: tuple[Type[BaseException], ...] = (Exception,),
    is_retryable: Callable[[BaseException], bool] = lambda e: True,
    on_retry: Callable[[BaseException, float, int], None] | None = None,
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    Decorator that implements exponential backoff retry logic.

    Args:
        func: The function to retry.
        max_retries: Maximum number of retry attempts.
        initial_delay_seconds: Initial delay in seconds between retries.
        max_delay_seconds: Maximum delay in seconds between retries.
        jitter_range: Tuple of (min, max) multipliers for jitter to randomize the delay.
        retryable_exceptions: Tuple of exception types that should trigger a retry,
                              any Exception subclass by default.
        is_retryable: Optional callable that determines if an exception is retryable.
        on_retry: Optional callback function called before each retry
                  with (exception, sleep_time, retry_count).

    Returns:
        Wrapped function that implements retry logic.

    Example:
        ```
        @exponential_backoff(retryable_exceptions=(ValueError,))
        def flaky_function() -> str:
            if random.random() < 0.5:
                raise ValueError("Random failure!")
            return "Success"
        ```
    """

    def calculate_sleep_time(retries: int) -> float:
        base_delay = initial_delay_seconds * (2**retries)
        sleep_time = min(base_delay, max_delay_seconds)
        jitter = random.uniform(*jitter_range)
        return sleep_time * jitter

    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> R:
            retries: int = 0

            while True:
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    if not is_retryable(e):
                        raise

                    retries += 1
                    if retries > max_retries:
                        raise

                    sleep_time: float = calculate_sleep_time(retries)
                    if on_retry:
                        on_retry(e, sleep_time, retries)
                    time.sleep(sleep_time)

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> R:
            retries: int = 0

            while True:
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as e:
                    if not is_retryable(e):
                        raise

                    retries += 1
                    if retries > max_retries:
                        raise

                    sleep_time: float = calculate_sleep_time(retries)
                    if on_retry:
                        on_retry(e, sleep_time, retries)
                    await asyncio.sleep(sleep_time)

        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
