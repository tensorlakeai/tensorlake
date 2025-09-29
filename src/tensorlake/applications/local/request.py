from typing import Any

from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.request import Request


class LocalRequest(Request):
    def __init__(self, id: str, output: Any | None, exception: BaseException | None):
        super().__init__(id)
        self._output: Any | None = output
        self._exception: BaseException | None = exception

    def output(self) -> Any:
        if isinstance(self._exception, RequestError):
            raise self._exception

        if self._exception is not None:
            raise RequestFailureException(
                "Request failed due to exception"
            ) from self._exception

        return self._output
