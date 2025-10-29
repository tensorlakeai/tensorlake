from enum import Enum
from typing import Any, Dict, Iterable, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

from ..runtime_hooks import run_futures as runtime_hook_run_futures
from ..runtime_hooks import wait_futures as runtime_hook_wait_futures


def request_scoped_id() -> str:
    """Generates a unique ID scoped to a single request.

    This ID is used to identify function calls, futures and values (data payloads)
    within a single request.
    """
    # We need full sized nanoid here because we can run a request
    # for months and we don't want to ever collide these IDs between
    # function calls of the same request.
    return nanoid_generate()


class Awaitable:
    """A definition of a computation in a Tensorlake Application.

    Also defines data dependencies between different Awaitables
    by i.e. passing one Awaitable as an input to another Awaitable.
    """

    def __init__(self, id: str):
        self._id: str = id

    @property
    def id(self) -> str:
        return self._id

    def run(self) -> "Future":
        future: Future = self._create_future()
        runtime_hook_run_futures(futures=[future], start_delay=None)
        return future

    def run_later(self, start_delay: float) -> "Future":
        future: Future = self._create_future()
        runtime_hook_run_futures(futures=[future], start_delay=start_delay)
        return future

    def _create_future(self) -> "Future":
        raise NotImplementedError(
            "Awaitable subclasses must implement _create_future()"
        )

    def __await__(self):
        """Runs this Awaitable and returns its result."""
        result = yield from self.run().__await__()
        return result

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and used a Tensorlake Awaitable
        # embedded inside some other object like a list.
        raise TypeError(
            f"Attempt to pickle a Tensorlake Awaitable. "
            "A Tensorlake Awaitable cannot be stored inside an object "
            "which is a function argument or returned from a function."
            "If you want to pass a list of awaitables as a single argument to a Tensorlake Function, "
            "wrap them in a AwaitableList."
        )

    def __repr__(self) -> str:
        raise NotImplementedError("Awaitable subclasses must implement __repr__()")

    def __str__(self) -> str:
        return repr(self)


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
    """

    # Warning: a Future object cannot be copied by value because it'll result in
    # multiple copies of the same Future object that go out of sync. Also we key
    # all operations with Futures by their IDs and our internal data structures
    # might become inconsistent because of this.

    def __init__(self, awaitable: Awaitable):
        self._awaitable = awaitable
        # Up to date result and exception, kept updated by runtime.
        # Lock is not needed because we're not doing any blocking reads/writes here.
        self._result: Any | _FutureResultMissingType = _FutureResultMissing
        self._exception: BaseException | None = None

    @property
    def id(self) -> str:
        return self._awaitable.id

    @property
    def awaitable(self) -> Awaitable:
        """The Awaitable that created this Future."""
        return self._awaitable

    def set_result(self, result: Any):
        """Mark the Future as done and set its result."""
        self._result = result

    def set_exception(self, exception: BaseException):
        """Mark the Future as failed and set its exception."""
        self._exception = exception

    @property
    def exception(self) -> BaseException | None:
        """Returns the exception representing the failure of the future, or None if it is not completed yet or succeeded."""
        return self._exception

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
        if self._result is _FutureResultMissing and self._exception is None:
            self._wait(timeout=timeout)

        if self._exception is None:
            return self._result
        else:
            raise self._exception

    def _wait(self, timeout: float | None) -> None:
        # Default wait for future classes wait implemented by runtime.
        runtime_hook_wait_futures(
            [self],
            timeout=timeout,
            return_when=RETURN_WHEN.ALL_COMPLETED,
        )

    def done(self) -> bool:
        """Returns True if the future is done running (either successfully or with failure)."""
        # This logic relies on runtime setting these fields as soon as the future is done.
        return self._result is not _FutureResultMissing or self._exception is not None

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
        """
        return runtime_hook_wait_futures(
            futures=list(futures),
            timeout=timeout,
            return_when=return_when,
        )

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and used a Tensorlake Future
        # embedded inside some other object like a list.
        raise TypeError(
            f"Attempt to pickle a Tensorlake Future. "
            "A Tensorlake Future cannot be stored inside an object "
            "which is a function argument or returned from a function."
        )

    def __repr__(self) -> str:
        return (
            f"Tensorlake {type(self)}(\n"
            f"  awaitable={self.awaitable!r},\n"
            f"  done={self.done()!r}\n"
            f")"
        )

    def __str__(self) -> str:
        return repr(self)


class AwaitableList(Awaitable):
    """Combines a list of awaitables and user objects into a single awaitable.

    Allows to pass a list of awaitables as a single list argument to a Tensorlake Function.
    Cannot be returned from a Tensorlake Function.
    """

    def __init__(self, id: str, items: Iterable[Awaitable | Any]):
        super().__init__(id=id)
        self._items: List[Awaitable | Any] = list(items)

    @property
    def items(self) -> List[Awaitable | Any]:
        return self._items

    def _create_future(self) -> "ListFuture":
        return ListFuture(self)

    def __repr__(self) -> str:
        return (
            f"<Tensorlake AwaitableList(\n"
            f"  id={self.id!r},\n"
            f"  items=[\n    "
            + ",\n    ".join(repr(awaitable) for awaitable in self.items)
            + "\n  ]\n"
            f")>"
        )


def make_map_operation_awaitable(
    function_name: str, items: List[Any | Awaitable] | AwaitableList
) -> AwaitableList:
    if isinstance(items, AwaitableList):
        items = items.items
    return AwaitableList(
        id=request_scoped_id(),
        items=[
            FunctionCallAwaitable(
                id=request_scoped_id(),
                function_name=function_name,
                args=[item],
                kwargs={},
            )
            for item in items
        ],
    )


class ListFuture(Future):
    """A Future that represents a list of other Futures.

    Allows to wait for multiple Futures as a single Future.
    """

    def __init__(self, awaitable: AwaitableList):
        super().__init__(awaitable)

    @property
    def awaitable(self) -> AwaitableList:
        return self._awaitable


class FunctionCallAwaitable(Awaitable):
    """An Awaitable that defines a call of a Tensorlake Function.

    Arguments can be other Awaitables.
    """

    def __init__(
        self,
        id: str,
        function_name: str,
        args: List[Any | Awaitable],
        kwargs: Dict[str, Any | Awaitable],
    ):
        super().__init__(id=id)
        self._function_name: str = function_name
        self._args: List[Any | Awaitable] = args
        self._kwargs: Dict[str, Any | Awaitable] = kwargs

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def args(self) -> List[Any | Awaitable]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any | Awaitable]:
        return self._kwargs

    def _create_future(self) -> "FunctionCallFuture":
        return FunctionCallFuture(self)

    def __repr__(self) -> str:
        return (
            f"<Tensorlake FunctionCallAwaitable(\n"
            f"  id={self.id!r},\n"
            f"  function_name={self.function_name!r},\n"
            f"  args=[\n    "
            + ",\n    ".join(repr(arg) for arg in self.args)
            + "\n  ],\n"
            f"  kwargs={{\n    "
            + ",\n    ".join(f"{k!r}: {v!r}" for k, v in self.kwargs.items())
            + "\n  }}\n"
            f")>"
        )


class FunctionCallFuture(Future):
    """A Future that represents a call to a Tensorlake Function.

    Allows to track the result of a specific function call.
    """

    def __init__(
        self,
        awaitable: FunctionCallAwaitable,
    ):
        super().__init__(awaitable=awaitable)

    @property
    def awaitable(self) -> FunctionCallAwaitable:
        return self._awaitable


class ReduceOperationAwaitable(Awaitable):
    """Defines a reduce operation."""

    def __init__(
        self,
        id: str,
        function_name: str,
        inputs: List[Any | Awaitable],
    ):
        super().__init__(id=id)
        self._function_name: str = function_name
        # Contains at least one item due to prior inputs validation.
        self._inputs: List[Any | Awaitable] = inputs

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def inputs(self) -> List[Any | Awaitable]:
        return self._inputs

    def _create_future(self) -> "ReduceOperationFuture":
        return ReduceOperationFuture(self)

    def __repr__(self) -> str:
        return (
            f"<Tensorlake ReduceOperationAwaitable(\n"
            f"  id={self.id!r},\n"
            f"  function_name={self.function_name!r},\n"
            f"  inputs={self.inputs!r},\n"
            f")>"
        )


class _InitialMissingType:
    pass


_InitialMissing = _InitialMissingType()


def make_reduce_operation_awaitable(
    function_name: str,
    items: List[Any | Awaitable] | AwaitableList,
    initial: Any | Awaitable | _InitialMissingType,
) -> ReduceOperationAwaitable | Awaitable | Any:
    inputs: List[Any | Awaitable] = None
    if isinstance(items, AwaitableList):
        inputs = list(items.items)
    else:
        inputs = list(items)

    if len(inputs) == 0 and initial is _InitialMissing:
        raise TypeError("reduce of empty iterable with no initial value")

    if initial is not _InitialMissing:
        inputs.insert(0, initial)

    # Squash reduce operation into a single Awaitable if it's only one thing
    # in collection. Server requires at least two items to perform reduce.
    if len(inputs) == 1:
        return inputs[0]
    else:
        return ReduceOperationAwaitable(
            id=request_scoped_id(),
            function_name=function_name,
            inputs=inputs,
        )


class ReduceOperationFuture(Future):
    """A Future that represents a reduce operation over a collection.

    Allows to track the result of a reduce operation.
    """

    def __init__(self, awaitable: ReduceOperationAwaitable):
        super().__init__(awaitable)

    @property
    def awaitable(self) -> ReduceOperationAwaitable:
        return self._awaitable


# ListFuture is not yet supported by runtime (and server).
RuntimeFutureTypes = FunctionCallFuture | ReduceOperationFuture
# AwaitableList is supported by the runtime because it can be a function argument but
# can't be returned from a function call.
RuntimeAwaitableTypes = FunctionCallAwaitable | ReduceOperationAwaitable | AwaitableList
