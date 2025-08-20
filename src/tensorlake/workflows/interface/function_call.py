from typing import Any, Dict, List

from .future import Future


class FunctionCall(Future):
    """Abstract base class for function calls in a workflow."""

    def __init__(self, function_name: str):
        self._function_name: str = function_name

    @property
    def function_name(self) -> str:
        return self._function_name

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake Function
        # call embedded inside some other object like a list.
        raise TypeError(
            f"Attempt to pickle a Tensorlake Function Call. "
            "Please return a single Tensorlake Function Call from your Tensorlake Function. "
            "A Tensorlake Function Call cannot be a part of another returned object, i.e. a list."
        )


class RegularFunctionCall(FunctionCall):
    """Represents a regular call of a Tensorlake Function."""

    def __init__(
        self,
        function_name: str,
        args: List[Any],
        kwargs: Dict[str, Any],
    ):
        super().__init__(function_name)

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
            f"<Tensorlake RegularFunctionCall(\n"
            f"  function_name={self.function_name!r},\n"
            f"  args=[\n    "
            + ",\n    ".join(repr(arg) for arg in self.args)
            + "\n  ],\n"
            f"  kwargs={{\n    "
            + ",\n    ".join(f"{k!r}: {v!r}" for k, v in self.kwargs.items())
            + "\n  }}\n"
            f")>"
        )
