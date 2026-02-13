from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

from ..runtime_hooks import await_future as runtime_hook_await_future
from ..runtime_hooks import run_futures as runtime_hook_run_futures
from ..runtime_hooks import wait_futures as runtime_hook_wait_futures
from .exceptions import InternalError, SDKUsageError, TensorlakeError


def _request_scoped_id() -> str:
    """Generates a unique ID scoped to a single request.

    This ID is used to identify function calls, futures and values (data payloads)
    within a single request.
    """
    # We need full sized nanoid here because we can run a request
    # for months and we don't want to ever collide these IDs between
    # function calls of the same request.
    return nanoid_generate()


class _FutureResultMissingType:
    pass


_FutureResultMissing = _FutureResultMissingType()


class RETURN_WHEN(Enum):
    """Constants used by Future.wait() to control when it should return."""

    FIRST_COMPLETED = 0
    FIRST_FAILURE = 1
    ALL_COMPLETED = 2


class Future:
    """An object representing an ongoing computation in a Tensorlake Application.

    A Future tracks an asynchronous computation in Tensorlake Application
    and provides access to its result.

    All public fields and methods of Future are meant to be used by users in their
    Tensorlake Application code. All internal fields and methods of Future prefixed
    with _ are meant to be used by Tensorlake runtime and should not be used by users
    in their code.
    """

    # Warning: a Future object cannot be copied by value because it'll result in
    # multiple copies of the same Future object that go out of sync. Also we key
    # all operations with Futures by their IDs and our internal data structures
    # might become inconsistent because of this.

    def __init__(self, id: str):
        self._id: str = id
        # Set when running the Future.
        self._start_delay: float | None = None
        self._tail_call: bool = False
        # Up to date result and exception, kept updated by runtime.
        # Lock is not needed because we're not doing any blocking reads/writes here.
        self._result: Any | _FutureResultMissingType = _FutureResultMissing
        self._exception: TensorlakeError | None = None

    def _run(self) -> "Future":
        """Runs this Future as soon as possible.

        Raises TensorlakeError on error.
        """
        self._start_delay = None
        self._tail_call = False
        runtime_hook_run_futures(futures=[self])
        return self

    def _run_later(self, start_delay: float) -> "Future":
        """Runs this Future after the given delay in seconds.

        Raises TensorlakeError on error.
        """
        self._start_delay = start_delay
        self._tail_call = False
        runtime_hook_run_futures(futures=[self])
        return self

    def _run_tail_call(self) -> "Future":
        """Runs this Future as a tail call.

        Raises TensorlakeError on error.
        """
        self._start_delay = None
        self._tail_call = True
        runtime_hook_run_futures(futures=[self])
        return self

    def set_result(self, result: Any):
        """Mark the Future as done and set its result."""
        self._result = result

    def set_exception(self, exception: TensorlakeError):
        """Mark the Future as failed and set its exception."""
        self._exception = exception

    @property
    def exception(self) -> TensorlakeError | None:
        """Returns the exception representing the failure of the future, or None if it is not completed yet or succeeded."""
        return self._exception

    def __await__(self) -> Generator[None, None, Any]:
        """Returns a generator for the result of the future.

        Generator:
            Raises RequestError if a function call represented by the Future raised RequestError.
            Raises FunctionError if a function call represented by the Future failed.
            Raises TimeoutError if the timeout is not None and is expired.
            Raises TensorlakeError on other errors.
        """
        if self.done():
            return self._done_result()
        yield from runtime_hook_await_future(self)
        return self._done_result()

    def result(self, timeout: float | None = None) -> Any:
        """Returns the result of the future, blocking until it is available.

        Raises RequestError if a function call represented by the Future raised RequestError.
        Raises FunctionError if a function call represented by the Future failed.
        Raises TimeoutError if the timeout is not None and is expired.
        Raises TensorlakeError on other errors.
        """
        if self.done():
            return self._done_result()
        runtime_hook_wait_futures(
            [self],
            timeout=timeout,
            return_when=RETURN_WHEN.ALL_COMPLETED,
        )
        return self._done_result()

    def done(self) -> bool:
        """Returns True if the future is done running (either successfully or with failure)."""
        # This logic relies on runtime setting these fields as soon as the future is done.
        return self._result is not _FutureResultMissing or self._exception is not None

    def _done_result(self) -> Any:
        """Returns the result of the future if it's done, otherwise raises an error."""
        if not self.done():
            raise InternalError(
                f"Attempt to get result of a Future that is not done: {self}"
            )
        if self._exception is not None:
            raise self._exception
        return self._result

    @classmethod
    def wait(
        cls,
        futures: Iterable["Future"],
        timeout: float | None = None,
        return_when=RETURN_WHEN.ALL_COMPLETED,
    ) -> tuple[List["Future"], List["Future"]]:
        """Returns when return_when condition is met for the futures.

        return_when can be one of:
        # The futures wait will return when any future finishes.
        RETURN_WHEN.FIRST_COMPLETED = 0
        # The futures wait will return when any future fails.
        # If no failures then it is equivalent to ALL_COMPLETED.
        RETURN_WHEN.FIRST_FAILURE = 1
        # The futures wait will return when all the futures finish.
        RETURN_WHEN.ALL_COMPLETED = 2

        timeout can be used to control the maximum number of seconds to wait before returning.
        Returns a tuple of two lists: (done, not_done) depending on return_when.

        This is similar to concurrent.futures.wait:
        https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.wait

        Raises TensorlakeError on error running the wait operation. Errors of each individual Future are
        accessible via their .exception property and they don't get raised by wait operation.
        """
        return runtime_hook_wait_futures(
            futures=list(futures),
            timeout=timeout,
            return_when=return_when,
        )

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and used a Tensorlake Future
        # embedded inside some other object like a list.
        # Note: this exception will be converted into SerializationError when pickling is attempted.
        raise SDKUsageError(
            f"Attempt to pickle {self}. It cannot be stored inside an object "
            "which is a function argument or returned from a function."
        )

    def __repr__(self) -> str:
        # Shows exact structure of the Future. Used for debug logging.
        return (
            f"{type(self)}(\n" f"  id={self._id!r},\n" f"  done={self.done()!r}\n" f")"
        )

    def __str__(self) -> str:
        """Returns a pretty printed human readable string representation of the Future.

        Used to show the structure of Futures in a concise way in error messages.
        """
        # Use local import to break circular dependency.
        from ._pretty_print import pretty_print

        return pretty_print(self)


class _FutureListKind(Enum):
    MAP_OPERATION = 0


@dataclass
class _FutureListMetadata:
    kind: _FutureListKind
    # Not None for MAP_OPERATION kind.
    function_name: str | None

    @property
    def durability_key(self) -> str:
        if self.kind == _FutureListKind.MAP_OPERATION:
            return f"MAP_OPERATION:{self.function_name}"
        else:
            return f"UNKNOWN_KIND:{self.kind}"


class ListFuture(Future):
    """A Future that represents a list of other Futures.

    Allows to pass a list of Futures and values as a single list argument to a Tensorlake Function.
    Cannot be returned from a Tensorlake Function as a tail call (Server can't resolve it).
    """

    def __init__(
        self,
        id: str,
        items: "list[Any | Future] | ListFuture",
        metadata: _FutureListMetadata,
    ):
        super().__init__(id=id)
        self._items: "list[Any | Future] | ListFuture" = items
        self._metadata: _FutureListMetadata = metadata

    @property
    def _kind_str(self) -> str:
        """Returns a human readable representation of the ListFuture kind."""
        if self._metadata.kind == _FutureListKind.MAP_OPERATION:
            return "Tensorlake Map Operation"
        else:
            return "Tensorlake ListFuture"

    def __repr__(self) -> str:
        # Shows exact structure of the Future. Used for debug logging.
        if isinstance(self._items, list):
            items_repr = ",\n    ".join(repr(item) for item in self._items)
        else:
            items_repr = repr(self._items)
        return (
            f"<{type(self)}(\n"
            f"  id={self._id!r},\n"
            f"  items=[\n    " + items_repr + "\n  ]\n"
            f")>"
        )


def _make_map_operation_future(
    function_name: str,
    items: Iterable[Any | Future] | ListFuture,
) -> ListFuture:
    items: list[Any | Future] | ListFuture = items
    if not isinstance(items, ListFuture):
        items = list(items)  # should be iterable
    return ListFuture(
        id=_request_scoped_id(),
        items=items,
        metadata=_FutureListMetadata(
            kind=_FutureListKind.MAP_OPERATION,
            function_name=function_name,
        ),
    )


class FunctionCallFuture(Future):
    """A Future that represents a call of a Tensorlake Function."""

    def __init__(
        self,
        id: str,
        function_name: str,
        args: List[Any | Future],
        kwargs: Dict[str, Any | Future],
    ):
        super().__init__(id=id)
        self._function_name: str = function_name
        self._args: List[Any | Future] = args
        self._kwargs: Dict[str, Any | Future] = kwargs

    def __repr__(self) -> str:
        # Shows exact structure of the Future. Used for debug logging.
        return (
            f"<{type(self)}(\n"
            f"  id={self._id!r},\n"
            f"  function_name={self._function_name!r},\n"
            f"  args=[\n    "
            + ",\n    ".join(repr(arg) for arg in self._args)
            + "\n  ],\n"
            f"  kwargs={{\n    "
            + ",\n    ".join(f"{k!r}: {v!r}" for k, v in self._kwargs.items())
            + "\n  }}\n"
            f")>"
        )


class _InitialMissingType:
    pass


_InitialMissing = _InitialMissingType()


class ReduceOperationFuture(Future):
    """A Future that represents a reduce operation over a collection."""

    def __init__(
        self,
        id: str,
        function_name: str,
        items: list[Any | Future] | ListFuture,
        initial: Any | Future | _InitialMissingType,
    ):
        super().__init__(id=id)
        self._function_name: str = function_name
        self._items: list[Any | Future] | ListFuture = items
        self._initial: Any | Future | _InitialMissingType = initial

    def __repr__(self) -> str:
        # Shows exact structure of the Future. Used for debug logging.
        return (
            f"<{type(self)}(\n"
            f"  id={self._id!r},\n"
            f"  function_name={self._function_name!r},\n"
            f"  items={self._items!r},\n"
            f"  initial={self._initial!r}\n"
            f")>"
        )


def _make_reduce_operation_future(
    function_name: str,
    items: Iterable[Any | Future] | ListFuture,
    initial: Any | Future | _InitialMissingType,
) -> ReduceOperationFuture:
    items: list[Any | Future] | ListFuture = items
    if not isinstance(items, ListFuture):
        items = list(items)
    return ReduceOperationFuture(
        id=_request_scoped_id(),
        function_name=function_name,
        items=items,
        initial=initial,
    )
