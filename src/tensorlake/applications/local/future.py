import time

from ..interface.futures import Future
from ..metadata import FunctionCallMetadata, ReduceOperationMetadata

UserFutureMetadataType = FunctionCallMetadata | ReduceOperationMetadata | None


class LocalFuture:
    """Represents an SDK Future with additional metadata used by LocalRunner."""

    def __init__(
        self,
        future: Future,
        future_metadata: UserFutureMetadataType,
        start_delay: float | None,
    ) -> None:
        self._future: Future = future
        self._future_metadata: UserFutureMetadataType = future_metadata
        self._start_time: float | None = (
            None if start_delay is None else (time.time() + start_delay)
        )
        # IDs of the futures which output is the same as this future output.
        # This is the futures returned as tail calls and futures refernced by
        # Futures.
        self._output_consumer_future_ids: list[str] = []

    @property
    def future(self) -> Future:
        return self._future

    @property
    def future_metadata(self) -> UserFutureMetadataType:
        return self._future_metadata

    @property
    def start_time_elapsed(self) -> bool:
        if self._start_time is None:
            return True
        return time.time() >= self._start_time

    @property
    def output_consumer_future_ids(self) -> list[str]:
        return self._output_consumer_future_ids

    def add_output_consumer_future_id(self, consumer_future_id: str) -> None:
        self._output_consumer_future_ids.append(consumer_future_id)
