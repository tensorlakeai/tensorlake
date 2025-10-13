from collections.abc import Iterable
from enum import Enum
from typing import Any, Callable, List

# (Futures, is_async: bool, timeout: float | None) -> List[Any]
__runtime_hook_wait_futures: (
    Callable[[List["Future"], bool, float | None], List[Any]] | None
) = None


class FutureType(Enum):
    FUNCTION_CALL = 1  # Future ID is function call ID.
    FUTURE_LIST = 2  # Future ID is ignored.


class Future:
    """An object representing an ongoing computation in a Tensorlake Application.

    A Future tracks an asynchronous computation in Tensorlake Application
    and provides access to its result.
    """

    def __init__(self, id: str, type: FutureType):
        self._id: str = id
        self._type: FutureType = type

    def __await__(self):
        """Returns the result of the future when awaited, blocking until it is available.

        Raises an Exception representing the failure if the future fails.
        """
        raise NotImplementedError("Future __await__ is not implemented yet")

    def result(self, timeout: float | None = None) -> Any:
        """Returns the result of the future, blocking until it is available.

        Raises an Exception representing the failure if the future fails.
        If timeout is not None and the result does not become available within timeout seconds,
        a TimeoutError is raised.
        """
        global __runtime_hook_wait_futures
        return __runtime_hook_wait_futures([self], is_async=False, timeout=timeout)

    @classmethod
    def gather(cls, items: Iterable[Any | "Future"]) -> "Future":
        """Returns a Future that resolves into a list of values made out of supplied items.

        If an item is not a Future then it's treated as a ready value.
        """
        return FutureList(items)


class FutureList(Future):
    """A list of futures and values that resolves into a list of values.

    If an item is not a Future then it's treated as a ready value.
    This class is used to signal SDK that a value is not a regular value but
    a list of values and futures that user wants to be resolved in some way.
    """

    def __init__(self, items: Iterable[Any]):
        super().__init__(id="future_list", type=FutureType.FUTURE_LIST)
        self._items: List[Any | Future] = list(items)

    def __repr__(self) -> str:
        return f"Tensorlake FutureList(items={self._items!r})"

    def __await__(self):
        raise NotImplementedError("FutureList __await__ is not implemented yet")

    def result(self, timeout: float | None = None) -> List[Any]:
        global __runtime_hook_wait_futures
        futures: List[tuple[int, Future]] = []
        for ix, item in enumerate(self._items):
            if isinstance(item, Future):
                futures.append((ix, item))

        future_values: List[Any] = __runtime_hook_wait_futures(
            [fut for _, fut in futures],
            is_async=False,
            timeout=timeout,
        )

        for (ix, _), value in zip(futures, future_values):
            self._items[ix] = value

        return list(self._items)

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake FutureList
        # object from a function. FutureList object can only be used in function arguments.
        raise TypeError(
            f"Attempt to pickle a Tensorlake FutureList object. "
            "A Tensorlake FutureList object cannot be returned from a Tensorlake Function. "
            "It can only be used as a Tensorlake Function argument. "
            "A Tensorlake FutureList object gets created by calling `tensorlake.gather` or `tensorlake.map`."
        )
