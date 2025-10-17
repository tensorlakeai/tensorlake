from typing import Any

from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.request import Request


class LocalRequest(Request):
    def __init__(
        self,
        id: str,
        output: Any | None,
        exception: RequestFailureException | RequestError | None,
    ):
        """A local request that has completed running.

        Either `output` or `exception` must be set, but not both.
        """
        super().__init__(id)
        self._output: Any | None = output
        self._exception: RequestFailureException | RequestError | None = exception

    def output(self) -> Any:
        if self._exception is not None:
            raise self._exception
        else:
            return self._output
