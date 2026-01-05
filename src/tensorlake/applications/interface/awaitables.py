from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

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


class Awaitable:
    """A definition of a computation in a Tensorlake Application.

    Also defines data dependencies between different Awaitables
    by i.e. passing one Awaitable as an input to another Awaitable.
    """

    def __init__(self, id: str):
        self._id: str = id

    @property
    def id(self) -> str:
        """ID of this Awaitable object.

        Uniqueness guarantees are up to the caller that creates the Awaitable.
        """
        return self._id

    def run(self) -> "Future":
        """Runs this Awaitable as soon as possible and returns its Future.

        Raises TensorlakeError on error.
        """
        future: Future = self._create_future()
        runtime_hook_run_futures(futures=[future], start_delay=None)
        return future

    def run_later(self, start_delay: float) -> "Future":
        """Runs this Awaitable after the given delay in seconds and returns its Future.

        Raises TensorlakeError on error.
        """
        future: Future = self._create_future()
        runtime_hook_run_futures(futures=[future], start_delay=start_delay)
        return future

    def _create_future(self) -> "Future":
        raise InternalError("Awaitable subclasses must implement _create_future()")

    def __await__(self) -> Any:
        """Runs the Awaitable and returns its result.

        Raises RequestError if a function call represented by the Awaitable raised RequestError.
        Raises FunctionError if a function call represented by the Awaitable failed.
        Raises TensorlakeError on other errors.
        """
        result: Any = yield from self.run().__await__()
        return result

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and used a Tensorlake Awaitable
        # embedded inside some other object like a list.
        # Note: this exception will be converted into SerializationError when pickling is attempted.
        #
        # TODO: Provide a workaround to users by making it possible to manually create an AwaitableList
        # using something like Awaitable.gather() static method.
        raise SDKUsageError(
            f"Attempt to pickle {self}. It cannot be stored inside an object "
            "which is a function argument or returned from a function."
        )

    def __repr__(self) -> str:
        """Returns a prices string representation of the Awaitable.

        Used for debugging.
        """
        raise InternalError("Tensorlake Awaitable subclasses must implement __repr__()")

    def __eq__(self, other: object) -> bool:
        raise InternalError("Tensorlake Awaitable subclasses must implement __eq__()")

    def __str__(self) -> str:
        """Returns a pretty printed human readable string representation of the Awaitable.

        Used to show the structure of Awaitables in a concise way in error messages.
        """
        # Use local import to break circular dependency.
        from ._pretty_print import pretty_print

        return pretty_print(self)


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

    Each Awaitable object can only have one Future associated with it.
    This is validated by the runtime when creating Futures for Awaitables.
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
        self._exception: TensorlakeError | None = None

    @property
    def awaitable(self) -> Awaitable:
        """The Awaitable of this Future."""
        return self._awaitable

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

    def __await__(self):
        """Returns the result of the future when awaited, blocking until it is available.

        Raises FunctionError if the function call represented by the Future failed.
        Raises TensorlakeError on other errors.
        """
        raise SDKUsageError(
            "Future.__await__ is not implemented yet. Use Future.result() instead."
        )

    def result(self, timeout: float | None = None) -> Any:
        """
        Returns the result of the future, blocking until it is available.

        Raises RequestError if a function call represented by the Future raised RequestError.
        Raises FunctionError if a function call represented by the Future failed.
        Raises TimeoutError if the timeout is not None and is expired.
        Raises TensorlakeError on other errors.
        """
        if self._result is _FutureResultMissing and self._exception is None:
            runtime_hook_wait_futures(
                [self],
                timeout=timeout,
                return_when=RETURN_WHEN.ALL_COMPLETED,
            )

        if self._exception is None:
            return self._result
        else:
            raise self._exception

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
            f"{type(self)}(\n"
            f"  awaitable={self.awaitable!r},\n"
            f"  done={self.done()!r}\n"
            f")"
        )

    def __str__(self) -> str:
        """Returns a pretty printed human readable string representation of the Future.

        Used to show the structure of Futures in a concise way in error messages.
        """
        return f"Tensorlake Future of {self.awaitable}"


class _AwaitableListKind(Enum):
    MAP_OPERATION = 0


@dataclass
class _AwaitableListMetadata:
    kind: _AwaitableListKind
    # Not None for MAP_OPERATION kind.
    function_name: str | None

    @property
    def durability_key(self) -> str:
        if self.kind == _AwaitableListKind.MAP_OPERATION:
            return f"MAP_OPERATION:{self.function_name}"
        else:
            return f"UNKNOWN_KIND:{self.kind}"


class AwaitableList(Awaitable):
    """Combines a list of awaitables and user objects into a single awaitable.

    Allows to pass a list of awaitables as a single list argument to a Tensorlake Function.
    Cannot be returned from a Tensorlake Function.
    """

    def __init__(
        self,
        id: str,
        items: Iterable[Awaitable | Any],
        metadata: _AwaitableListMetadata,
    ):
        super().__init__(id=id)
        self._items: List[Awaitable | Any] = list(items)
        self._metadata: _AwaitableListMetadata = metadata

    @property
    def items(self) -> List[Awaitable | Any]:
        return self._items

    @property
    def kind_str(self) -> str:
        """Returns a human readable representation of the AwaitableList kind."""
        if self._metadata.kind == _AwaitableListKind.MAP_OPERATION:
            return "Tensorlake Map Operation"
        else:
            return "Tensorlake AwaitableList"

    @property
    def metadata(self) -> _AwaitableListMetadata:
        return self._metadata

    def _create_future(self) -> "ListFuture":
        return ListFuture(self)

    def __repr__(self) -> str:
        # Shows exact structure of the Awaitable. Used for debug logging.
        return (
            f"<{type(self)}(\n"
            f"  id={self.id!r},\n"
            f"  items=[\n    "
            + ",\n    ".join(repr(awaitable) for awaitable in self.items)
            + "\n  ]\n"
            f")>"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AwaitableList):
            return False
        return (
            self.id == other.id
            and self.items == other.items
            and self.metadata == other.metadata
        )


def make_map_operation_awaitable(
    function_name: str, items: List[Any | Awaitable] | AwaitableList
) -> AwaitableList:
    if isinstance(items, AwaitableList):
        items = items.items
    return AwaitableList(
        id=_request_scoped_id(),
        items=[
            FunctionCallAwaitable(
                id=_request_scoped_id(),
                function_name=function_name,
                args=[item],
                kwargs={},
            )
            for item in items
        ],
        metadata=_AwaitableListMetadata(
            kind=_AwaitableListKind.MAP_OPERATION,
            function_name=function_name,
        ),
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
        # Shows exact structure of the Awaitable. Used for debug logging.
        return (
            f"<{type(self)}(\n"
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FunctionCallAwaitable):
            return False
        return (
            self.id == other.id
            and self.function_name == other.function_name
            and self.args == other.args
            and self.kwargs == other.kwargs
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
        # Contains at least two items due to prior validations.
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
        # Shows exact structure of the Awaitable. Used for debug logging.
        return (
            f"<{type(self)}(\n"
            f"  id={self.id!r},\n"
            f"  function_name={self.function_name!r},\n"
            f"  inputs={self.inputs!r},\n"
            f")>"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ReduceOperationAwaitable):
            return False
        return (
            self.id == other.id
            and self.function_name == other.function_name
            and self.inputs == other.inputs
        )

    def _validate_inputs(self) -> None:
        """Performs reduce operation specific validation of its inputs.

        Raises SDKUsageError on error.
        """
        for input_item in self.inputs:
            # We don't support this right now because ReduceOp proto doesn't have have a field
            # where we can embed awaitable list items as data dependencies without calling the
            # reduce function on them.
            if isinstance(input_item, AwaitableList):
                error_message: str = (
                    f"A {input_item.kind_str} cannot be used as an input item for {self}. "
                )
                if input_item.metadata.kind == _AwaitableListKind.MAP_OPERATION:
                    error_message += (
                        f"You can work this around by creating function call awaitables using `{input_item.metadata.function_name}.awaitable(...)` and then passing "
                        f"them into `{self.function_name}.reduce(...)`."
                    )
                raise SDKUsageError(error_message)


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

    if initial is not _InitialMissing:
        inputs.insert(0, initial)

    if len(inputs) == 0:
        raise SDKUsageError(
            "reduce operation of an empty iterable with no initial value is not supported, "
            "please pass an initial value or an iterable with at least one item."
        )

    reduce_op = ReduceOperationAwaitable(
        id=_request_scoped_id(),
        function_name=function_name,
        inputs=inputs,
    )
    reduce_op._validate_inputs()

    # Squash reduce operation into a single Awaitable if it's only one thing
    # in collection. Server and Local Runner require at least two items to perform reduce.
    if len(inputs) == 1:
        return inputs[0]
    else:
        return reduce_op


class ReduceOperationFuture(Future):
    """A Future that represents a reduce operation over a collection.

    Allows to track the result of a reduce operation.
    """

    def __init__(self, awaitable: ReduceOperationAwaitable):
        super().__init__(awaitable)

    @property
    def awaitable(self) -> ReduceOperationAwaitable:
        return self._awaitable
