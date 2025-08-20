from typing import Any


class Request:
    def __init__(self, id: str):
        self._id: str = id

    @property
    def id(self) -> str:
        return self._id

    def function_output(self, function: Any, call_index: int = 0) -> Any:
        """Retrieve the output of a function call.

        function is either function name or Function instance.
        Raises Exception on error.
        """
        raise NotImplementedError("function_output is implemented in subclasses.")
