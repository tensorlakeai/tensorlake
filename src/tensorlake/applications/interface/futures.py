import asyncio
from collections.abc import Coroutine, Generator
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, TypeVar, Union

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

from ..runtime_hooks import await_future as runtime_hook_await_future
from ..runtime_hooks import coroutine_to_future as runtime_hook_coroutine_to_future
from ..runtime_hooks import register_coroutine as runtime_hook_register_coroutine
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


FutureType = TypeVar("FutureType", bound="Future")
# Future itself, or a coroutine created by calling an async Tensorlake Function or an asyncio.Task that runs such coroutine.
# Instead of a Future, user can pass any of these. We unwrap the Future in this case.
_TensorlakeFutureWrapper = Union[
    FutureType, Coroutine[Any, Any, FutureType], asyncio.Task
]


class _FutureResultMissingType:
    pass


_FutureResultMissing = _FutureResultMissingType()


class RETURN_WHEN(Enum):
    """Constants used by Future.wait() to control when it should return."""

    FIRST_COMPLETED = 0
    FIRST_FAILURE = 1
    ALL_COMPLETED = 2


class Future:
    """An object representing a computation in a Tensorlake Application.

    A Future tracks an asynchronous computation in Tensorlake Application
    and provides access to its result. A computation is not running on Future creation.
    It is started by calling one of the run() methods, result() or awaiting the Future object.

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
        self._run_hook_was_called: bool = False
        # Set for async function Futures.
        self._coroutine: Coroutine[Any, Any, Any] | None = None
        # Up to date result and exception, kept updated by runtime.
        # Lock is not needed because we're not doing any blocking reads/writes here.
        self._result: Any | _FutureResultMissingType = _FutureResultMissing
        self._exception: TensorlakeError | None = None

    def run(self) -> "Future":
        """Runs this Future as soon as possible and returns it.

        Returns self to start the Future immeditely and save it in i.e. a list.

        Raises SDKUsageError if the Future was already started.
        Raises TensorlakeError on error.
        """
        if self._run_hook_was_called:
            raise SDKUsageError(
                f"Attempt to run Future that is already started: {self}"
            )
        self._run_hook_was_called = True
        runtime_hook_run_futures(futures=[self])
        return self

    def run_later(self, start_delay: float) -> "Future":
        """Runs this Future after the given delay in seconds and returns it.

        Raises TensorlakeError on error.
        """
        if not self._run_hook_was_called:
            self._start_delay = start_delay
        return self.run()

    def __await__(self) -> Generator[None, None, Any]:
        """Runs the Future and returns a generator for the result.

        The same Future can be awaited multiple times. The first await runs the Future,
        subsequent awaits just wait for the result of the Future. This method is intended
        to be used directly by async functions doing non-blocking calls to sync functions
        via function.future().

        Generator:
            Raises RequestError if a function call represented by the Future raised RequestError.
            Raises FunctionError if a function call represented by the Future failed.
            Raises TimeoutError if the timeout is not None and is expired.
            Raises TensorlakeError on other errors.
        """
        if self.done():
            return self._done_result()
        if not self._run_hook_was_called:
            self.run()
        yield from runtime_hook_await_future(self)
        return self._done_result()

    @property
    def exception(self) -> TensorlakeError | None:
        """Returns the exception representing the failure of the future, or None if it is not completed yet or succeeded."""
        return self._exception

    def result(self, timeout: float | None = None) -> Any:
        """Returns the result of the future, blocking until it is available.

        Runs the future if it's not already running.

        Raises RequestError if a function call represented by the Future raised RequestError.
        Raises FunctionError if a function call represented by the Future failed.
        Raises TimeoutError if the timeout is not None and is expired.
        Raises TensorlakeError on other errors.
        """
        if self.done():
            return self._done_result()
        if not self._run_hook_was_called:
            self.run()
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
        `done` list contains futures that are done (successfully or with failure) at the moment of return.
        `not_done` list contains futures that are not done at the moment of return.
        The Futures are started by this method if they were not started before.

        This is similar to concurrent.futures.wait:
        https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.wait

        Raises SDKUsageError if any of the given futures were not started or if any of the given objects is not a Future.
        Raises TensorlakeError on error running the wait operation. Errors of each individual Future are
        accessible via their .exception property and they don't get raised by wait operation.
        """
        for future in futures:
            if not isinstance(future, Future):
                raise SDKUsageError(f"Cannot run a non-Future object {future}.")
            if not future._run_hook_was_called:
                future.run()

        return runtime_hook_wait_futures(
            futures=list(futures),
            timeout=timeout,
            return_when=return_when,
        )

    def _set_result(self, result: Any):
        """Mark the Future as done and set its result."""
        self._result = result

    def _set_exception(self, exception: TensorlakeError):
        """Mark the Future as failed and set its exception."""
        self._exception = exception

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
        items: "list[Any | _TensorlakeFutureWrapper[Future]] | _TensorlakeFutureWrapper[ListFuture]",
        metadata: _FutureListMetadata,
    ):
        super().__init__(id=id)
        self._items: "list[Any | _TensorlakeFutureWrapper[Future]] | _TensorlakeFutureWrapper[ListFuture]" = (items)
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
        if isinstance(_unwrap_future(self._items), list):
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
    items: (
        Iterable[Any | _TensorlakeFutureWrapper[Future]]
        | _TensorlakeFutureWrapper[ListFuture]
    ),
) -> ListFuture:
    items: list[Any | Future] | ListFuture = items
    if not isinstance(_unwrap_future(items), ListFuture):
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
        args: List[Any | _TensorlakeFutureWrapper[Future]],
        kwargs: Dict[str, Any | _TensorlakeFutureWrapper[Future]],
    ):
        super().__init__(id=id)
        self._function_name: str = function_name
        self._args: List[Any | _TensorlakeFutureWrapper[Future]] = args
        self._kwargs: Dict[str, Any | _TensorlakeFutureWrapper[Future]] = kwargs

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
        items: (
            list[Any | _TensorlakeFutureWrapper[Future]]
            | _TensorlakeFutureWrapper[ListFuture]
        ),
        initial: Any | _TensorlakeFutureWrapper[Future] | _InitialMissingType,
    ):
        super().__init__(id=id)
        self._function_name: str = function_name
        self._items: (
            list[Any | _TensorlakeFutureWrapper[Future]]
            | _TensorlakeFutureWrapper[ListFuture]
        ) = items
        self._initial: Any | _TensorlakeFutureWrapper[Future] | _InitialMissingType = (
            initial
        )

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
    items: (
        Iterable[Any | _TensorlakeFutureWrapper[Future]]
        | _TensorlakeFutureWrapper[ListFuture]
    ),
    initial: Any | _TensorlakeFutureWrapper[Future] | _InitialMissingType,
) -> ReduceOperationFuture:
    if not isinstance(_unwrap_future(items), ListFuture):
        items = list(items)
    return ReduceOperationFuture(
        id=_request_scoped_id(),
        function_name=function_name,
        items=items,
        initial=initial,
    )


def _wrap_future_into_coroutine(future: Future) -> Coroutine[Any, Any, Any]:
    """Wraps a Future into a coroutine that can be awaited.

    The returned coroutine will wait until the future is done and then return its result or raise its exception.
    """

    # Use a verbose name for the coroutine function to make it easier to understand stack traces and debugging
    # with async functions.
    async def tensorlake_async_function_coroutine() -> Any:
        # Blocks asyncio event loop until the Future is running.
        # This is absolutely important for strict ordering guarantees of function call starts
        # and thus determinism.
        future.run()
        # Unblocks asyncio event loop.
        return await future
        # Multiple Future.__await__() completions are not expected to be reordered because asyncio event
        # loop processes events in FIFO order. So this implementation is expected to be deterministic.

    coroutine: Coroutine[Any, Any, Any] = tensorlake_async_function_coroutine()
    future._coroutine = coroutine
    runtime_hook_register_coroutine(coroutine, future)
    return coroutine


def _unwrap_future(value: Any | _TensorlakeFutureWrapper[Future]) -> Any | Future:
    """Unwraps a Future from the given value if it's a future wrapper. Otherwise, returns the value itself."""
    if isinstance(value, Future):
        return value

    coroutine: Any | None = None
    if isinstance(value, asyncio.Task):
        coroutine = value.get_coro()
    elif isinstance(value, Coroutine):
        coroutine = value

    if coroutine is not None:
        future: Future | None = runtime_hook_coroutine_to_future(coroutine)
        if future is not None:
            return future

    # Something else, just return the value itself.
    return value
