import time

from ..interface.awaitables import Future
from ..metadata import FunctionCallMetadata, ReduceOperationMetadata

UserFutureMetadataType = FunctionCallMetadata | ReduceOperationMetadata


class LocalFuture:
    """Represents an SDK Future with additional metadata used by LocalRunner."""

    def __init__(
        self,
        user_future: Future,
        user_future_metadata: UserFutureMetadataType,
        start_delay: float | None,
        output_consumer_future_id: str | None,
    ) -> None:
        self._user_future: Future = user_future
        self._user_future_metadata: UserFutureMetadataType = user_future_metadata
        self._start_time: float | None = (
            None if start_delay is None else (time.time() + start_delay)
        )
        # ID of the future which output is the same as this future output.
        # This is the future whos Tensorlake Function returned this future.
        self._output_consumer_future_id: str | None = output_consumer_future_id

    @property
    def id(self) -> str:
        return self._user_future.id

    @property
    def user_future(self) -> Future:
        return self._user_future

    @property
    def user_future_metadata(self) -> UserFutureMetadataType:
        return self._user_future_metadata

    @property
    def start_time_elapsed(self) -> bool:
        if self._start_time is None:
            return True
        return time.time() >= self._start_time

    @property
    def output_consumer_future_id(self) -> str | None:
        return self._output_consumer_future_id
