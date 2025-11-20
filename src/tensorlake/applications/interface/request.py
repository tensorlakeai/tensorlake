from typing import Any

from .exceptions import InternalError


class Request:
    def __init__(self, id: str):
        self._id: str = id

    @property
    def id(self) -> str:
        return self._id

    def output(self) -> Any:
        """Returns output of the request API function.

        API function output is what was returned from it.
        Raises RequestFailed if the request failed.
        Raises RequestError if the request failed due to unhandled RequestError raised in a function.
        Raises TensorlakeError on other errors.
        """
        raise InternalError("Request subclasses must implement output method.")

    def __repr__(self) -> str:
        # Shows a exact structure of the Request. Used for debug logging.
        return f"{type(self)}: (id={self._id})"

    def __str__(self) -> str:
        # Shows a simple human readable representation of the Request. Used in error messages.
        return f"Tensorlake Request(id={self._id})"
