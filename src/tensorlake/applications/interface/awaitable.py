from collections.abc import Iterable
from typing import Any, Dict, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

from .future import FunctionCallFuture, Future, ListFuture, ReduceOperationFuture


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

    def run(self) -> Future:
        future: Future = self._create_future()
        # TODO: call runtime hook to actually start running the awaitable
        return future

    def run_later(self, start_delay: float) -> Future:
        future: Future = self._create_future()
        return future

    def _create_future(self) -> Future:
        raise NotImplementedError(
            "Awaitable subclasses must implement _run_subclass_override()"
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


class AwaitableList(Awaitable):
    """Combines a list of awaitables into a single object.

    Allows to pass a list of awaitables as a single list argument to a Tensorlake Function.
    Cannot be returned from a Tensorlake Function.
    """

    def __init__(self, id: str, awaitables: Iterable[Awaitable]):
        super().__init__(id=id)
        self._awaitables: List[Awaitable] = list(awaitables)

    @property
    def awaitables(self) -> List[Awaitable]:
        return self._awaitables

    def _create_future(self) -> Future:
        return ListFuture(
            id=self.id,
            futures=[awaitable.run() for awaitable in self.awaitables],
        )

    def __repr__(self) -> str:
        return (
            f"<Tensorlake AwaitableList(\n"
            f"  id={self.id!r},\n"
            f"  awaitables=[\n    "
            + ",\n    ".join(repr(awaitable) for awaitable in self.awaitables)
            + "\n  ]\n"
            f")>"
        )


def make_map_operation_awaitable(
    function_name: str, iterable: List[Any | Awaitable]
) -> AwaitableList:
    return AwaitableList(
        id=request_scoped_id(),
        awaitables=[
            FunctionCallAwaitable(
                id=request_scoped_id(),
                function_name=function_name,
                args=[item],
                kwargs={},
            )
            for item in iterable
        ],
    )


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

    def _create_future(self) -> Future:
        # TODO.
        return FunctionCallFuture()

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


class ReduceOperationAwaitable(Awaitable):
    """Defines a reduce operation."""

    def __init__(
        self,
        id: str,
        function_name: str,
        inputs: List[Any | Future],
    ):
        super().__init__(id=id)
        self._function_name: str = function_name
        # Contains at least one item due to prior inputs validation.
        self._inputs: List[Any | Future] = inputs

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def inputs(self) -> List[Any | Future]:
        return self._inputs

    def _create_future(self, start_delay: float | None) -> Future:
        # TODO
        pass

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
    iterable: List[Any | Awaitable],
    initial: Any | _InitialMissingType,
) -> ReduceOperationAwaitable:
    inputs: List[Any] = list(iterable)
    if len(inputs) == 0 and initial is _InitialMissing:
        raise TypeError("reduce of empty iterable with no initial value")

    if initial is not _InitialMissing:
        inputs.insert(0, initial)

    return ReduceOperationAwaitable(
        id=request_scoped_id(),
        function_name=function_name,
        inputs=inputs,
    )
