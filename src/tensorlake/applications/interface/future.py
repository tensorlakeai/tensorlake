from collections.abc import Iterable
from typing import Any, List

from .exceptions import TensorlakeException

# (Future, is_async: bool, timeout: float | None) -> Any
__runtime_hook_get_future_result = None


class Future:
    """An object representing an ongoing computation in a Tensorlake Application.

    A Future tracks an asynchronous computation in Tensorlake Application
    and provides access to its result.
    """

    def __await__(self):
        """Returns the result of the future when awaited, blocking until it is available.

        Raises an Exception representing the failure if the future fails.
        """
        if __runtime_hook_get_future_result is None:
            raise TensorlakeException(
                "Internal Error: No Tensorlake runtime hook is set for getting future result"
            )
        return __runtime_hook_get_future_result(self, is_async=True, timeout=None)

    def result(self, timeout: float | None = None) -> Any:
        """Returns the result of the future, blocking until it is available.

        Raises an Exception representing the failure if the future fails.
        If timeout is not None and the result does not become available within timeout seconds,
        a TimeoutError is raised.
        """
        if __runtime_hook_get_future_result is None:
            raise TensorlakeException(
                "Internal Error: No Tensorlake runtime hook is set for getting future result"
            )
        return __runtime_hook_get_future_result(self, is_async=False, timeout=timeout)


class FutureList(Future):
    """A list of futures that resolves into a list of values.

    Not if an item is not a Future then it's treated as a ready value.
    """

    def __init__(self, items: Iterable[Any], delay_sec: float | None):
        self._items: List[Any | Future] = list(items)
        self._delay_sec: float | None = delay_sec

    @property
    def items(self) -> List[Any | Future]:
        return self._items

    @property
    def delay_sec(self) -> float | None:
        return self._delay_sec

    def __repr__(self) -> str:
        return f"Tensorlake FutureList(items={self._items!r}, delay_sec={self._delay_sec!r})"

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake FutureList
        # object from a function. FutureList object can only be used in function arguments.
        raise TypeError(
            f"Attempt to pickle a Tensorlake FutureList object. "
            "A Tensorlake FutureList object cannot be returned from a Tensorlake Function. "
            "It can only be used as a Tensorlake Function argument. "
            "A Tensorlake FutureList object gets created by calling `tensorlake.gather` or `tensorlake.map`."
        )
