from typing import Any


class Request:
    def __init__(self, id: str):
        self._id: str = id

    @property
    def id(self) -> str:
        return self._id

    def output(self) -> Any:
        """Returns output of the request API function.

        API function output is what was returned from it.
        Raises Exception on error.
        """
        raise NotImplementedError("output is implemented in subclasses.")
