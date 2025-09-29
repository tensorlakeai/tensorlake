from collections.abc import Iterable
from typing import Any, List


class Future:
    """Abstract interface for Tensorlake futures.

    A Future tracks an asynchronous computation in Tensorlake Workflows
    and provides access to its output.
    """

    # This interface is empty right now because we don't allow resolving/awaiting futures yet.
    pass


class FutureList(Future):
    """A list of futures that resolves into a list of values.

    Not if an item is not a Future then it's treated as a ready value.
    """

    def __init__(self, items: Iterable[Any]):
        self._items: List[Any] = list(items)

    @property
    def items(self) -> List[Any]:
        return self._items

    def __repr__(self) -> str:
        return f"Tensorlake FutureList(items={self._items!r})"

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake FutureList
        # object from a function. FutureList object can only be used in function arguments.
        raise TypeError(
            f"Attempt to pickle a Tensorlake FutureList object. "
            "A Tensorlake FutureList object cannot be returned from a Tensorlake Function. "
            "It can only be used as a Tensorlake Function argument. "
            "A Tensorlake FutureList object gets created by calling `tensorlake.gather` or `tensorlake.map`."
        )
