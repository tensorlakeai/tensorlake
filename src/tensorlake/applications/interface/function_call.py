from typing import Any, Dict, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

from .future import Future, FutureType

# Classes in this module are not part of SDK interface but we keep them here
# to avoid circular imports.


class FunctionCall:
    """Abstract base class for function calls."""

    def __init__(self, function_name: str, start_delay: float | None):
        # We need full sized nanoid here because we can run a request
        # for months and we don't want to ever collide these IDs between
        # function calls of the same request.
        self._id: str = nanoid_generate()
        self._function_name: str = function_name
        self._start_delay: float | None = start_delay

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake Function
        # call embedded inside some other object like a list.
        raise TypeError(
            f"Attempt to pickle a Tensorlake Function Call. "
            "Please return a single Tensorlake Function Call from your Tensorlake Function. "
            "A Tensorlake Function Call cannot be a part of another returned object, i.e. a list."
        )

    def to_future(self) -> Future:
        """Returns a Future that resolves into the result of this function call."""
        return Future(self._id, FutureType.FUNCTION_CALL)


class RegularFunctionCall(FunctionCall):
    """Represents a regular call of a Tensorlake Function."""

    def __init__(
        self,
        function_name: str,
        args: List[Any],
        kwargs: Dict[str, Any],
        start_delay: float | None,
    ):
        super().__init__(function_name=function_name, start_delay=start_delay)

        self._args: List[Any] = args
        self._kwargs: Dict[str, Any] = kwargs

    def __repr__(self) -> str:
        return (
            f"<Tensorlake RegularFunctionCall(\n"
            f"  id={self._id!r},\n"
            f"  function_name={self._function_name!r},\n"
            f"  start_delay={self._start_delay!r},\n"
            f"  args=[\n    "
            + ",\n    ".join(repr(arg) for arg in self._args)
            + "\n  ],\n"
            f"  kwargs={{\n    "
            + ",\n    ".join(f"{k!r}: {v!r}" for k, v in self._kwargs.items())
            + "\n  }}\n"
            f")>"
        )


class ReducerFunctionCall(FunctionCall):
    def __init__(
        self,
        reducer_function_name: str,
        inputs: List[Any | Future],
        start_delay: float | None,
    ):
        super().__init__(function_name=reducer_function_name, start_delay=start_delay)
        # Contains at least one item due to initial + SDK validation.
        self._inputs: List[Any | Future] = inputs

    def __repr__(self) -> str:
        return (
            f"<Tensorlake ReducerFunctionCall(\n"
            f"  id={self._id!r},\n"
            f"  function_name={self._function_name!r},\n"
            f"  start_delay={self._start_delay!r},\n"
            f"  inputs={self._inputs!r},\n"
            f")>"
        )
