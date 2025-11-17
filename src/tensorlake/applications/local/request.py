from typing import Any

from ..interface.exceptions import RequestFailed
from ..interface.request import Request


class LocalRequest(Request):
    def __init__(
        self,
        id: str,
        output: Any | None,
        error: RequestFailed | None,
    ):
        """A local request that has completed running.

        Either `output` or `error` must be set, but not both.
        """
        super().__init__(id)
        self._output: Any | None = output
        self._error: RequestFailed | None = error

    def output(self) -> Any:
        if self._error is not None:
            raise self._error
        else:
            return self._output
