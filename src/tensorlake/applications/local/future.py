import time
from typing import Union

from ..interface.futures import (
    FunctionCallFuture,
    ReduceOperationFuture,
)

# CollectionFuture is not yet supported by server and runtime.
UserFutureType = Union[FunctionCallFuture, ReduceOperationFuture]


class LocalFuture:
    """Represents a user generated Future with additional metadata used by LocalRunner."""

    def __init__(self, user_future: UserFutureType):
        self._user_future: UserFutureType = user_future
        self._start_time: float | None = time.time() + user_future.start_delay
        # ID of the future which output is the same as this future output.
        self._output_consumer_future_id: str | None = None

    @property
    def user_future(self) -> UserFutureType:
        return self._user_future

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
