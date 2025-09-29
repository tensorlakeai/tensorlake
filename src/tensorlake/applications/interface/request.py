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
        Raises RequestFailureException on error during the request execution.
        Raises RequestNotFinished if the request is not yet completed.
        Raises RemoteAPIError on error communicating with the remote API.
        """
        raise NotImplementedError("output is implemented in subclasses.")
