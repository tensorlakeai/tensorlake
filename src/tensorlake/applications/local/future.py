import time

from ..interface.awaitables import Future


class LocalFuture:
    """Represents an SDK Future with additional metadata used by LocalRunner."""

    def __init__(
        self,
        user_future: Future,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        self._user_future: Future = user_future
        self._start_time: float | None = (
            None if start_delay is None else (time.time() + start_delay)
        )
        # ID of the future which output is the same as this future output.
        # This is the future whos Tensorlake Function returned this future.
        self._output_consumer_future_id: str | None = output_consumer_future_id
        # If set, overrides the output serializer of this future's Tensorlake Function.
        # This is used when the output of this future is consumed by another Tensorlake Function
        # with a different output serializer. The serializer override is inherited from the very
        # first future in the chain of futures.
        self._output_serializer_name_override: str | None = (
            output_serializer_name_override
        )

    @property
    def id(self) -> str:
        return self._user_future.id

    @property
    def user_future(self) -> Future:
        return self._user_future

    @property
    def start_time_elapsed(self) -> bool:
        if self._start_time is None:
            return True
        return time.time() >= self._start_time

    @property
    def output_consumer_future_id(self) -> str | None:
        return self._output_consumer_future_id

    @property
    def output_serializer_name_override(self) -> str | None:
        return self._output_serializer_name_override
