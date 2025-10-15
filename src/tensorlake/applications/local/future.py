import time
from typing import Union

from ..interface.futures import FunctionCallFuture, ReduceOperationFuture

# CollectionFuture is not yet supported by runtime and Server.
FutureType = Union[FunctionCallFuture, ReduceOperationFuture]


class LocalFuture:
    """Represents a Future with additional metadata used by LocalRunner."""

    def __init__(self, future: FutureType):
        self._future: FutureType = future
        self._start_time: float | None = time.time() + future.start_delay
        # ID of the future which output is the same as this future output.
        # This is the future whos Tensorlake Function returned this future.
        self._output_consumer_future_id: str | None = None

    @property
    def id(self) -> str:
        return self._future.id

    @property
    def future(self) -> FutureType:
        return self._future

    @property
    def start_time_elapsed(self) -> bool:
        if self._start_time is None:
            return True
        return time.time() >= self._start_time

    @property
    def output_consumer_future_id(self) -> str | None:
        return self._output_consumer_future_id

    @output_consumer_future_id.setter
    def output_consumer_future_id(self, value: str) -> None:
        self._output_consumer_future_id = value
