from collections.abc import Iterable
from typing import Any, Dict, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

from ..runtime_hooks import wait_futures


class _FutureResultMissingType:
    pass


_FutureResultMissing = _FutureResultMissingType()


def request_scoped_id() -> str:
    """Generates a unique ID scoped to a single request.

    This ID is used to identify function calls, futures and values (data payloads)
    within a single request.
    """
    # We need full sized nanoid here because we can run a request
    # for months and we don't want to ever collide these IDs between
    # function calls of the same request.
    return nanoid_generate()


class Future:
    """An object representing an ongoing computation in a Tensorlake Application.

    A Future tracks an asynchronous computation in Tensorlake Application
    and provides access to its result. It also defines data dependencies
    between Tensorlake Functions because it's used as function arguments
    and return values.
    """

    def __init__(self, id: str, start_delay: float | None):
        self._id: str = id
        self._start_delay: float | None = start_delay
        # Local cache in case user calls result() multiple times.
        self._result: Any | _FutureResultMissingType = _FutureResultMissing
        self._exception: BaseException | None = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def start_delay(self) -> float | None:
        return self._start_delay

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
            try:
                self._result = wait_futures([self], is_async=False, timeout=timeout)[0]
            except BaseException as ex:
                self._exception = ex

        if self._exception is None:
            return self._result
        else:
            raise self._exception

    @classmethod
    def gather(cls, items: Iterable[Any | "Future"]) -> "Future":
        """Returns a Future that resolves into a list of values made out of supplied items.

        If an item is not a Future then it's treated as a ready value.
        """
        # The futures in the items collection should be started already.
        # So we don't have to start them here.
        return CollectionFuture(id=request_scoped_id(), items=list(items))

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and used a Tensorlake Future
        # embedded inside some other object like a list.
        raise TypeError(
            f"Attempt to pickle a Tensorlake Future. "
            "A Tensorlake Future cannot be stored inside an object "
            "which is a function argument or returned from a function."
        )


class FunctionCallFutureBase(Future):
    """Base class for all Tensorlake Function call related futures."""

    def __init__(
        self,
        id: str,
        start_delay: float | None,
        function_name: str,
        output_serializer_name_override: str | None,
    ):
        super().__init__(id=id, start_delay=start_delay)
        self._function_name: str = function_name
        # If set, overrides the output serializer of this future's Tensorlake Function.
        # This is used when the output of this future is consumed by another Tensorlake Function
        # with a different output serializer. The serializer override is inherited from the very
        # first future in the chain of futures.
        self._output_serializer_name_override: str | None = (
            output_serializer_name_override
        )

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def output_serializer_name_override(self) -> str | None:
        return self._output_serializer_name_override


class FunctionCallFuture(FunctionCallFutureBase):
    """Represents a regular call of a Tensorlake Function."""

    def __init__(
        self,
        id: str,
        function_name: str,
        start_delay: float | None,
        output_serializer_name_override: str | None,
        args: List[Any],
        kwargs: Dict[str, Any],
    ):
        super().__init__(
            id=id,
            start_delay=start_delay,
            function_name=function_name,
            output_serializer_name_override=output_serializer_name_override,
        )

        self._args: List[Any] = args
        self._kwargs: Dict[str, Any] = kwargs

    @property
    def args(self) -> List[Any]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def __repr__(self) -> str:
        return (
            f"<Tensorlake FunctionCallFuture(\n"
            f"  id={self.id!r},\n"
            f"  function_name={self.function_name!r},\n"
            f"  start_delay={self.start_delay!r},\n"
            f"  output_serializer_name_override={self.output_serializer_name_override!r},\n"
            f"  args=[\n    "
            + ",\n    ".join(repr(arg) for arg in self.args)
            + "\n  ],\n"
            f"  kwargs={{\n    "
            + ",\n    ".join(f"{k!r}: {v!r}" for k, v in self.kwargs.items())
            + "\n  }}\n"
            f")>"
        )


class ReduceOperationFuture(FunctionCallFutureBase):
    """Represents a reduce operation."""

    def __init__(
        self,
        id: str,
        function_name: str,
        start_delay: float | None,
        output_serializer_name_override: str | None,
        inputs: List[Any | Future],
    ):
        super().__init__(
            id=id,
            start_delay=start_delay,
            function_name=function_name,
            output_serializer_name_override=output_serializer_name_override,
        )
        # Contains at least one item due to initial + SDK validation.
        self._inputs: List[Any | Future] = inputs

    @property
    def inputs(self) -> List[Any | Future]:
        return self._inputs

    def __repr__(self) -> str:
        return (
            f"<Tensorlake ReduceOperationFuture(\n"
            f"  id={self.id!r},\n"
            f"  function_name={self.function_name!r},\n"
            f"  start_delay={self.start_delay!r},\n"
            f"  output_serializer_name_override={self.output_serializer_name_override!r},\n"
            f"  inputs={self.inputs!r},\n"
            f")>"
        )


class CollectionFuture(Future):
    """A list of futures and values that resolves into a list of values.

    If an item is not a future then it's treated as a ready value.
    This class is used to signal SDK that a value is not a regular value but
    a list of values and futures that user wants to be resolved altogether.

    This class doesn't have a representation on the Server side yet but we'd
    add it in the future to avoid waiting for each item in the collection.
    """

    def __init__(self, id: str, items: List[Any | Future]):
        super().__init__(id=id, start_delay=None)
        self._items: List[Any | Future] = items

    @property
    def items(self) -> List[Any | Future]:
        return self._items

    def result(self, timeout: float | None = None) -> List[Any]:
        """Resolves the collection into a list of values."""
        # This is a special implementation of Future.result() for Collection.
        # It is required because server/runtime doesn't understand Collection yet
        # and so we have to resolve each future in the collection one by one.
        futures: List[tuple[int, Future]] = []
        for ix, item in enumerate(self._items):
            if isinstance(item, Future):
                futures.append((ix, item))

        future_values: List[Any] = wait_futures(
            [fut for _, fut in futures],
            is_async=False,
            timeout=timeout,
        )

        for (ix, _), value in zip(futures, future_values):
            self._items[ix] = value

        return list(self._items)

    def __repr__(self) -> str:
        return (
            f"Tensorlake CollectionFuture(\n"
            f"  id={self.id!r},\n"
            f"  start_delay={self.start_delay!r},\n"
            f"  items={self._items!r}\n"
            f")"
        )

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake Collection
        # object from a function. Collection object can only be used in function arguments.
        raise TypeError(
            f"Attempt to pickle a Tensorlake Collection object. "
            "A Tensorlake Collection object cannot be returned from a Tensorlake Function. "
            "It can only be used as a Tensorlake Function argument. "
            "A Tensorlake Collection object gets created by calling `gather` or `map_future`."
        )
