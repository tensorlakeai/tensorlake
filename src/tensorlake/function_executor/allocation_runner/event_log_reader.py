import threading
from collections.abc import Generator

from ..proto.function_executor_pb2 import (
    AllocationEvent,
    ReadAllocationEventLogRequest,
    ReadAllocationEventLogResponse,
)

_MAX_SAFE_EVENT_LOG_CLOCK: int = (1 << 53) - 1
_RECOGNIZED_EVENT_PAYLOADS: frozenset[str] = frozenset(
    {
        "function_call_created",
        "function_call_watcher_created",
        "function_call_watcher_result",
    }
)


class EventLogReaderStopped(Exception):
    """Raised by read() when the reader has been stopped.

    This exception has to be used by event log consumer to stop reading and exit.
    """

    pass


class InvalidEventLogResponse(ValueError):
    """Raised when an event-log page violates the shared executor protocol."""


def validate_event_log_response(
    response: ReadAllocationEventLogResponse,
    requested_after_clock: int,
) -> int:
    """Validates an event-log page and returns its effective page clock.

    Event-log clocks cross the shared Python/TypeScript protocol boundary.
    Restricting them to JavaScript's safe integer range ensures both runtimes
    order and compare the same history.
    """
    response_clock = (
        response.last_clock
        if response.HasField("last_clock")
        else requested_after_clock
    )
    if response_clock < 0 or response_clock > _MAX_SAFE_EVENT_LOG_CLOCK:
        raise InvalidEventLogResponse(
            f"Allocation event log returned invalid clock {response_clock}"
        )
    if response_clock < requested_after_clock:
        raise InvalidEventLogResponse(
            "Allocation event log moved backwards from clock "
            f"{requested_after_clock} to {response_clock}"
        )
    if response.has_more and response_clock == requested_after_clock:
        raise InvalidEventLogResponse(
            "Allocation event log reported more entries without advancing clock "
            f"{requested_after_clock}"
        )
    if response.entries and response_clock == requested_after_clock:
        raise InvalidEventLogResponse(
            "Allocation event log returned entries without advancing clock "
            f"{requested_after_clock}"
        )

    previous_entry_clock = requested_after_clock
    for index, entry in enumerate(response.entries):
        _validate_event_log_entry(
            entry=entry,
            index=index,
            previous_entry_clock=previous_entry_clock,
            response_clock=response_clock,
        )
        previous_entry_clock = entry.clock

    return response_clock


def _validate_event_log_entry(
    entry: AllocationEvent,
    index: int,
    previous_entry_clock: int,
    response_clock: int,
) -> None:
    payload = entry.WhichOneof("event")
    if payload not in _RECOGNIZED_EVENT_PAYLOADS:
        raise InvalidEventLogResponse(
            f"Allocation event log entry {index} has no recognized payload"
        )
    if not entry.HasField("clock"):
        raise InvalidEventLogResponse(
            f"Allocation event log entry {index} is missing its clock"
        )
    if (
        entry.clock > _MAX_SAFE_EVENT_LOG_CLOCK
        or entry.clock <= previous_entry_clock
        or entry.clock > response_clock
    ):
        raise InvalidEventLogResponse(
            f"Allocation event log entry {index} has invalid clock {entry.clock} "
            f"after {previous_entry_clock} with page clock {response_clock}"
        )


OPTIMAL_EVENT_LOG_READ_BATCH_SIZE: int = 100


class EventLogReader:
    """Proto-only RPC transport layer for the allocation event log protocol.

    AllocationRunner's input event thread calls read() sequentially.
    RPC handlers iterate watch_read_requests() and call deliver_read_response().
    A newly connected watcher receives the current pending request, preventing
    a disconnected stream from stranding an allocation.
    """

    def __init__(self, allocation_id: str):
        self._allocation_id: str = allocation_id
        self._request_condition: threading.Condition = threading.Condition()
        self._request_generation: int = 0
        self._current_request: ReadAllocationEventLogRequest | None = None
        self._pending_response: threading.Event = threading.Event()
        self._response: ReadAllocationEventLogResponse | None = None
        self._stopped: bool = False

    def read(
        self, after_clock: int, max_entries: int = OPTIMAL_EVENT_LOG_READ_BATCH_SIZE
    ) -> ReadAllocationEventLogResponse:
        """Sends a read request and blocks until the response arrives.

        Called sequentially by AllocationRunner's input event thread.
        Raises EventLogReaderStopped if stopped.
        """
        request = ReadAllocationEventLogRequest(
            allocation_id=self._allocation_id,
            after_clock=after_clock,
            max_entries=max_entries,
        )
        with self._request_condition:
            if self._stopped:
                raise EventLogReaderStopped()
            if self._current_request is not None:
                raise RuntimeError("Allocation event log already has a pending read")
            self._pending_response.clear()
            self._response = None
            self._current_request = request
            self._request_generation += 1
            self._request_condition.notify_all()

        self._pending_response.wait()

        with self._request_condition:
            if self._stopped:
                raise EventLogReaderStopped()
            response = self._response
            self._response = None
        if response is None:
            raise RuntimeError("Allocation event log read completed without a response")
        return response

    def watch_read_requests(
        self,
    ) -> Generator[ReadAllocationEventLogRequest, None, None]:
        """Yields pending read requests until the reader stops.

        Every watcher receives requests created after it connects. A watcher
        that connects while a read is pending receives that request
        immediately, allowing the server to reconnect without losing work.
        """
        with self._request_condition:
            observed_generation = (
                self._request_generation - 1
                if self._current_request is not None
                else self._request_generation
            )

        while True:
            with self._request_condition:
                while (
                    not self._stopped
                    and self._request_generation <= observed_generation
                ):
                    self._request_condition.wait()
                if self._stopped:
                    return
                observed_generation = self._request_generation
                request = self._current_request
            if request is not None:
                yield request

    def deliver_read_response(self, response: ReadAllocationEventLogResponse) -> bool:
        """Delivers the response for the pending read() call.

        Returns False for a late or duplicate response. Such responses are safe
        to ignore because no read is waiting for them.
        """
        with self._request_condition:
            if (
                self._stopped
                or self._current_request is None
                or self._response is not None
            ):
                return False
            self._response = response
            self._current_request = None
            self._pending_response.set()
            return True

    def stop(self) -> None:
        """Unblocks any pending read() call and the watch stream."""
        with self._request_condition:
            self._stopped = True
            self._current_request = None
            self._request_condition.notify_all()
            self._pending_response.set()
