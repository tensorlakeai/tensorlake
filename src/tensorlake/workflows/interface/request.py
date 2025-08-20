from typing import Any, List

from .function import Function


class Request:
    def __init__(self, id: str):
        self._id: str = id

    @property
    def id(self) -> str:
        return self._id

    def function_outputs(self, function: str | Function) -> List[Any]:
        """Retrieve all outputs of a function in the request.

        function is either function name or Function instance.
        Raises Exception on error.
        """
        raise NotImplementedError("function_output is implemented in subclasses.")
