from typing import Any

from ..interface.exceptions import TensorlakeError
from ..interface.request import Request


class LocalRequest(Request):
    def __init__(
        self,
        id: str,
        output: Any | None,
        error: TensorlakeError | None,
    ):
        """A local request that has completed running.

        Either `output` or `error` must be set, but not both.
        """
        super().__init__(id)
        self._output: Any | None = output
        self._error: TensorlakeError | None = error

    def output(self) -> Any:
        if self._error is not None:
            raise self._error
        else:
            return self._output
