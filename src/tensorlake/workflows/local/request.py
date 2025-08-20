from typing import Any

from ..interface.request import Request


class LocalRequest(Request):
    def __init__(self, id: str, output: Any | None, exception: BaseException | None):
        super().__init__(id)
        self._output: Any | None = output
        self._exception: BaseException | None = exception

    def output(self) -> Any:
        if self._exception is not None:
            raise RuntimeError("Request failed due to exception") from self._exception

        return self._output
