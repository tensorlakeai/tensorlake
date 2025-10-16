from enum import Enum
from typing import Any, Dict, Iterable, List

from ..runtime_hooks import wait_futures as runtime_hook_wait_futures
from .exceptions import RequestFailureException


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

    def __init__(self, id: str):
        self._id = id
        # Up to date result and exception, kept updated by runtime.
        # Lock is not needed because we're not doing any blocking reads/writes here.
        self._result: Any | _FutureResultMissingType = _FutureResultMissing
        self._exception: BaseException | None = None

    @property
    def id(self) -> str:
        return self._id

    def set_result(self, result: Any):
        """Mark the Future as done and set its result."""
        self._result = result

    def set_exception(self, exception: BaseException):
        """Mark the Future as failed and set its exception."""
        self._exception = exception

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
        # Default implementation for future classes implemented by runtime.
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
        """Returns when all the supplied futures are done running.

        timeout can be used to control the maximum number of seconds to wait before returning.
        Returns a tuple of two lists: (done, not_done) depending on return_when.
        """
        return runtime_hook_wait_futures(
            futures=list(futures),
            is_async=False,
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
            f"Tensorlake Future(\n"
            f"  id={self.id!r},\n"
            f"  done={self.done()!r}\n"
            f")"
        )

    def __str__(self) -> str:
        return repr(self)


class ListFuture(Future):
    """A Future that represents a list of other Futures.

    Allows to wait for multiple Futures as a single Future.
    """

    def __init__(self, id: str, futures: List[Future]):
        super().__init__(id=id)
        self._futures: List[Future] = futures

    def _wait(self, timeout: float | None) -> None:
        _, not_done = Future.wait(
            futures=self._futures,
            timeout=timeout,
            return_when=RETURN_WHEN.ALL_COMPLETED,
        )
        if len(not_done) == 0:
            # All futures completed successfully.
            self.set_result([future.result() for future in self._futures])
        else:
            self.set_exception(
                RequestFailureException(
                    "Some futures did not complete: "
                    + ", ".join(str(f.result()) for f in not_done)
                )
            )

    def __repr__(self) -> str:
        return (
            f"Tensorlake ListFuture(\n"
            f"  id={self.id!r},\n"
            f"  futures={self._futures!r}\n"
            f")"
        )


class FunctionCallFuture(Future):
    """A Future that represents a call to a Tensorlake Function.

    Allows to track the result of a specific function call.
    """

    def __init__(
        self,
        id: str,
        function_name: str,
        args: List[Any | Awaitable],
        kwargs: Dict[str, Any | Awaitable],
    ):
        super().__init__(id=id)
        self._function_name = function_name
        self._args: List[Any | Awaitable] = args
        self._kwargs: Dict[str, Any | Awaitable] = kwargs

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def args(self) -> List[Any | Awaitable]:
        return self._args

    @property
    def kwargs(self) -> dict[str, Any | Awaitable]:
        return self._kwargs

    def __repr__(self) -> str:
        return (
            f"Tensorlake FunctionCallFuture(\n"
            f"  id={self.id!r},\n"
            f"  function_name={self.function_name!r},\n"
            f"  args={self.args!r},\n"
            f"  kwargs={self.kwargs!r}\n"
            f")"
        )


class ReduceOperationFuture(Future):
    """A Future that represents a reduce operation over a collection.

    Allows to track the result of a reduce operation.
    """

    def __init__(self, id: str, function_name: str, inputs: List[Any | Awaitable]):
        super().__init__(id=id)
        self._function_name = function_name
        self._inputs: List[Any | Awaitable] = inputs

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def inputs(self) -> List[Any | Awaitable]:
        return self._inputs

    def __repr__(self) -> str:
        return (
            f"Tensorlake ReduceOperationFuture(\n"
            f"  id={self.id!r},\n"
            f"  function_name={self.function_name!r},\n"
            f"  inputs={self.inputs!r}\n"
            f")>"
        )
