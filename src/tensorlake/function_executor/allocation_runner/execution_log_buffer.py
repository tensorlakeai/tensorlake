import threading
from collections import deque

from ..proto.function_executor_pb2 import AllocationExecutionEvent


class ExecutionLogBuffer:
    """Thread-safe buffer between AllocationRunner (producer) and execution log RPCs (consumer).

    Stores batches of AllocationExecutionEvent protos. The producer adds batches,
    and the consumer retrieves and advances through them one at a time.
    """

    def __init__(self):
        self._condition: threading.Condition = threading.Condition()
        self._batches: deque[list[AllocationExecutionEvent]] = deque()
        self._stopped: bool = False
        self._terminal_added: bool = False
        self._terminal_observed: bool = False

    def add_batch(self, events: list[AllocationExecutionEvent]) -> None:
        """Called by AllocationRunner after converting output events to protos."""
        with self._condition:
            # Shutdown and normal completion can race. Once a terminal event is
            # queued, no later producer may append another batch.
            if self._terminal_added:
                return
            self._batches.append(events)
            self._terminal_added = any(
                event.HasField("finish_allocation") for event in events
            )
            self._condition.notify_all()

    def get_current_batch(self) -> list[AllocationExecutionEvent] | None:
        """Blocks until a batch is available or stopped. Returns None if stopped with no data."""
        with self._condition:
            while len(self._batches) == 0:
                if self._stopped:
                    return None
                self._condition.wait()
            batch = self._batches[0]
            if any(event.HasField("finish_allocation") for event in batch):
                self._terminal_observed = True
                self._condition.notify_all()
            return batch

    def advance(self) -> None:
        """Pops the front batch. Called by RPC handler after processing."""
        with self._condition:
            if len(self._batches) > 0:
                self._batches.popleft()

    def stop(self) -> None:
        """Unblocks get_current_batch when allocation is shutting down."""
        with self._condition:
            self._stopped = True
            self._condition.notify_all()

    def wait_for_terminal_observed(self, timeout: float) -> bool:
        """Waits until a consumer has received the terminal execution batch."""
        with self._condition:
            return self._condition.wait_for(
                lambda: self._terminal_observed,
                timeout=max(timeout, 0),
            )
