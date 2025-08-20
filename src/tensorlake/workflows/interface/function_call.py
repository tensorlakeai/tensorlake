from typing import Any, Dict, List


class FunctionCall:
    """Represents a function call in the workflow."""

    def __init__(
        self,
        class_name: str | None,
        function_name: str,
        args: List[Any],
        kwargs: Dict[str, Any],
    ):
        # Class name is None if this is not a method call.
        self._class_name: str | None = class_name
        self._function_name: str = function_name
        self._args: List[Any] = args
        self._kwargs: Dict[str, Any] = kwargs

    @property
    def class_name(self) -> str | None:
        return self._class_name

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def args(self) -> List[Any]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def __eq__(self, value: Any) -> bool:
        """Equality operator mainly used in unit tests."""
        if not isinstance(value, FunctionCall):
            return False

        return (
            self._class_name == value._class_name
            and self._function_name == value._function_name
            and self._args == value._args
            and self._kwargs == value._kwargs
        )

    def __repr__(self) -> str:
        return (
            f"<Tensorlake FunctionCall(\n"
            f"  class_name={self._class_name!r},\n"
            f"  function_name={self._function_name!r},\n"
            f"  args=[\n    "
            + ",\n    ".join(repr(arg) for arg in self._args)
            + "\n  ],\n"
            f"  kwargs={{\n    "
            + ",\n    ".join(f"{k!r}: {v!r}" for k, v in self._kwargs.items())
            + "\n  }}\n"
            f")>"
        )
