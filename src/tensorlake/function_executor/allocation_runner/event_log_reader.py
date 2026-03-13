import queue
import threading

from ..proto.function_executor_pb2 import (
    ReadAllocationEventLogRequest,
    ReadAllocationEventLogResponse,
)


class EventLogReaderStopped(Exception):
    """Raised by read() when the reader has been stopped.

    This exception has to be used by event log consumer to stop reading and exit.
    """

    pass


OPTIMAL_EVENT_LOG_READ_BATCH_SIZE: int = 100


class EventLogReader:
    """Proto-only RPC transport layer for the allocation event log protocol.

    AllocationRunner's input event thread calls read() sequentially.
    RPC handlers call get_next_read_request() and deliver_read_response().
    """

    def __init__(self, allocation_id: str):
        self._allocation_id: str = allocation_id
        self._read_request_queue: queue.Queue[ReadAllocationEventLogRequest | None] = (
            queue.Queue()
        )
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
        if self._stopped:
            raise EventLogReaderStopped()

        request = ReadAllocationEventLogRequest(
            allocation_id=self._allocation_id,
            after_clock=after_clock,
            max_entries=max_entries,
        )

        self._pending_response.clear()
        self._response = None
        self._read_request_queue.put(request)

        self._pending_response.wait()

        if self._stopped:
            raise EventLogReaderStopped()

        return self._response

    def get_next_read_request(self) -> ReadAllocationEventLogRequest | None:
        """Blocks until a read request is available. Returns None when stopped.

        Called by watch_allocation_event_log_reads RPC handler.
        stop() enqueues a None sentinel to unblock this call.
        """
        return self._read_request_queue.get()

    def deliver_read_response(self, response: ReadAllocationEventLogResponse) -> None:
        """Delivers the response for the pending read() call.

        Called by send_allocation_event_log_read_response RPC handler.
        """
        self._response = response
        self._pending_response.set()

    def stop(self) -> None:
        """Unblocks any pending read() call and the watch stream."""
        self._stopped = True
        self._read_request_queue.put(None)
        self._pending_response.set()
