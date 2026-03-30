from collections import deque
from dataclasses import dataclass

from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.internal_logger import InternalLogger

from ..proto.function_executor_pb2 import (
    AllocationEvent,
    AllocationEventFunctionCallCreated,
    AllocationEventFunctionCallWatcherCreated,
    AllocationEventFunctionCallWatcherResult,
    AllocationExecutionEventFinishAllocation,
    ReadAllocationEventLogResponse,
)
from .allocation_event import (
    process_function_call_created,
    process_function_call_watcher_created,
    process_function_call_watcher_result,
)
from .event_log_reader import EventLogReader, EventLogReaderStopped
from .event_loop import (
    AllocationEventLoop,
    InputEventEmergencyShutdown,
    OutputEventBatch,
    OutputEventCreateFunctionCall,
    OutputEventCreateFunctionCallWatcher,
    OutputEventFinishAllocation,
    OutputEventType,
)
from .execution_log_buffer import ExecutionLogBuffer
from .finish_event_helper import FinishEventHelper


@dataclass
class StrictReplayResult:
    """Result of a strict mode replay.

    last_clock: the clock value to resume live execution from.
    pending_event_loop_output_events: output events emitted by the event loop
        that have to be processed in normal live execution mode.
    finish_event: if set, the replay ended with a mismatch and the allocation should
        finish with this event. pending_event_loop_output_events is empty when this is set.
    """

    last_clock: int
    pending_event_loop_output_events: list[OutputEventType]
    finish_event: AllocationExecutionEventFinishAllocation | None = None


class _AllocationEventReplayBuffer:
    """On-demand reader of allocation events during replay.

    Provides one-at-a-time reads via next().
    """

    def __init__(self, event_log_reader: EventLogReader) -> None:
        self._event_log_reader: EventLogReader = event_log_reader
        self._after_clock_cursor: int = 0
        # Did we reach end of alloc event log. Same as EOF.
        self._end_of_log: bool = False
        self._entries: deque[AllocationEvent] = deque()

    @property
    def empty(self) -> bool:
        """Returns True if the log is fully read and buffer is empty."""
        return self._end_of_log and len(self._entries) == 0

    @property
    def last_read_clock(self) -> int:
        """Returns the clock of the last read batch from the event log."""
        return self._after_clock_cursor

    def _ensure_entries(self) -> None:
        """Fetches a batch of events from the log if the internal deque is empty."""
        if len(self._entries) > 0 or self._end_of_log:
            return

        try:
            response: ReadAllocationEventLogResponse = self._event_log_reader.read(
                self._after_clock_cursor
            )
        except EventLogReaderStopped:
            self._end_of_log = True
            return

        for entry in response.entries:
            self._entries.append(entry)

        if response.HasField("last_clock"):
            self._after_clock_cursor = response.last_clock

        if not response.has_more:
            self._end_of_log = True

    def next(self) -> AllocationEvent | None:
        """Returns the next allocation event, or None if the log is exhausted."""
        self._ensure_entries()
        if len(self._entries) > 0:
            return self._entries.popleft()
        return None

    def peek(self) -> AllocationEvent | None:
        """Returns the next allocation event without removing it from the buffer, or None if the log is exhausted."""
        self._ensure_entries()
        if len(self._entries) > 0:
            return self._entries[0]
        return None


@dataclass
class _PendingEventLoopWatcher:
    output_event: OutputEventCreateFunctionCallWatcher
    creation_clock: int


class AllocationStrictModeReplayer:
    def __init__(
        self,
        event_log_reader: EventLogReader,
        event_loop: AllocationEventLoop,
        finish_event_helper: FinishEventHelper,
        blob_store: BLOBStore,
        logger: InternalLogger,
    ) -> None:
        self._event_loop: AllocationEventLoop = event_loop
        self._finish_event_helper: FinishEventHelper = finish_event_helper
        self._blob_store: BLOBStore = blob_store
        self._logger: InternalLogger = logger

        self._alloc_buffer: _AllocationEventReplayBuffer = _AllocationEventReplayBuffer(
            event_log_reader
        )
        # Watchers that are currently pending in the event loop.
        self._event_loop_pending_watchers: dict[str, _PendingEventLoopWatcher] = {}

    def _replay_mismatch(self) -> StrictReplayResult:
        """Sends emergency shutdown to event loop and returns a replay mismatch result."""
        self._event_loop.add_input_event(InputEventEmergencyShutdown())
        return StrictReplayResult(
            last_clock=self._alloc_buffer.last_read_clock,
            pending_event_loop_output_events=[],
            finish_event=self._finish_event_helper.from_replay_mismatch(),
        )

    def _finalize_replay(
        self, not_replayed_event_loop_output_events: list[OutputEventType]
    ) -> StrictReplayResult:
        """Handles the replay-to-live transition.

        Gathers all the event loop output events that were emitted during replay but
        were not actually replayed because replay reached end of allocation event log
        without any replay mismatch error.
        """
        return StrictReplayResult(
            last_clock=self._alloc_buffer.last_read_clock,
            pending_event_loop_output_events=not_replayed_event_loop_output_events,
        )

    def run(self) -> StrictReplayResult:
        """Runs the STRICT mode replay.

        Drives the event loop by consuming output events and feeding replayed
        allocation events back as input events. Returns a StrictReplayResult
        with either events to forward (success) or a finish event (mismatch).

        Doesn't raise any exceptions.
        """
        # Key algorithm assumptions and design choices:
        # -  AllocationEventFunctionCallCreated, AllocationEventFunctionCallWatcherCreated events appear in the
        #    allocation log in the same order as OutputEventCreateFunctionCall, OutputEventCreateFunctionCallWatcher
        #    events are emitted by the event loop. This is ensured by Server.
        # -  AllocationEventFunctionCallWatcherResult events appear in the allocation log in any order after
        #    their corresponding AllocationEventFunctionCallWatcherCreated events.
        #    This is because each function call takes an arbitrary amount of time to complete.
        # -  If an OutputEventFinishAllocation event is emitted during replay, it indicates replay mismatch because user code
        #    didn't finish in the original execution.
        # -  Replayed allocation event log can get exhausted (aka replay finished) in the middle of the current event loop output
        #    event batch processing. All the not matching events from that last batch needs to be converted to event loop execution
        #    events and forwarded to execution log buffer as during normal execution. This is because all these event loop output
        #    events are past the replay point and need to be executed normally.
        # -  Server re-creates all the unfinished watchers that didn't add their AllocationEventFunctionCallWatcherResult to allocation
        #    event log before starting the replay. Otherwise, user code will never get AllocationEventFunctionCallWatcherResult and will
        #    deadlock during replay. We can't recreate watchers in the replayer because this will result in extra allocation events logged
        #    during replay that don't match user code behavior, so the next replay will fail with a mismatch.
        # -  If Server deletes AllocationEventFunctionCallWatcherResult during pre-replay history edit then it has to re-create the watcher.
        #    Otherwise, user code will never get AllocationEventFunctionCallWatcherResult and deadlock during replay.

        result: StrictReplayResult | None = None
        while True:
            # Consume any pending unordered alloc events like watcher results before waiting for the next
            # output event batch from event loop. This is required because the event loop might be blocked
            # on watcher results and etc.
            result = self._consume_pending_unordered_alloc_events()
            if result is not None:
                return result

            if self._alloc_buffer.empty:
                # If log is fully read, we can finalize the replay and transition to live execution.
                return self._finalize_replay([])

            batch: OutputEventBatch = self._event_loop.wait_for_output_event_batch()
            for i, output_event in enumerate(batch.events):
                if isinstance(output_event, OutputEventFinishAllocation):
                    self._logger.info(
                        "Finish allocation event observed during replay, "
                        "indicating a replay mismatch."
                    )
                    return self._replay_mismatch()

                # Branch for all output events that require strict ordering for their corresponding alloc events.
                elif isinstance(
                    output_event,
                    (
                        OutputEventCreateFunctionCall,
                        OutputEventCreateFunctionCallWatcher,
                    ),
                ):
                    # For all output events that require their corresponding alloc events to come in order,
                    # scan the alloc log for the next strictly ordered event. Process all unordered alloc events
                    # at the same time. We only advance the alloc log while doing scans for ordered output events.
                    result = self._consume_pending_unordered_alloc_events()
                    if result is not None:
                        return result

                    alloc_event: AllocationEvent | None = self._alloc_buffer.next()
                    if alloc_event is None:
                        return self._finalize_replay(batch.events[i:])

                    if alloc_event.HasField("function_call_created"):
                        if not isinstance(output_event, OutputEventCreateFunctionCall):
                            self._logger.info(
                                "Replay mismatch: expected OutputEventCreateFunctionCall, got different output event type.",
                                expected_type="OutputEventCreateFunctionCall",
                                got_type=type(output_event).__name__,
                            )
                            return self._replay_mismatch()

                        fcc: AllocationEventFunctionCallCreated = (
                            alloc_event.function_call_created
                        )
                        if fcc.function_call_id != output_event.durable_id:
                            self._logger.info(
                                "Replay mismatch: positional AllocationEventFunctionCallCreated durable_id mismatch",
                                expected=output_event.durable_id,
                                got=fcc.function_call_id,
                            )
                            return self._replay_mismatch()
                        process_function_call_created(
                            event=fcc,
                            event_loop=self._event_loop,
                            logger=self._logger,
                        )
                    elif alloc_event.HasField("function_call_watcher_created"):
                        if not isinstance(
                            output_event, OutputEventCreateFunctionCallWatcher
                        ):
                            self._logger.info(
                                "Replay mismatch: expected OutputEventCreateFunctionCallWatcher, got different output event type.",
                                expected_type="OutputEventCreateFunctionCallWatcher",
                                type=type(output_event),
                            )
                            return self._replay_mismatch()

                        fwcc: AllocationEventFunctionCallWatcherCreated = (
                            alloc_event.function_call_watcher_created
                        )
                        if (
                            fwcc.function_call_id
                            != output_event.function_call_durable_id
                        ):
                            self._logger.info(
                                "Replay mismatch: positional AllocationEventFunctionCallWatcherCreated durable_id mismatch",
                                expected=output_event.function_call_durable_id,
                                got=fwcc.function_call_id,
                            )
                            return self._replay_mismatch()
                        process_function_call_watcher_created(
                            event=fwcc,
                            event_loop=self._event_loop,
                            logger=self._logger,
                        )
                    else:
                        self._logger.info(
                            "Replay mismatch: unknown allocation event type.",
                            type=alloc_event.WhichOneof("event"),
                        )
                        return self._replay_mismatch()

                else:
                    self._logger.info(
                        "Replay mismatch: unknown event loop output event type.",
                        type=type(output_event),
                    )
                    return self._replay_mismatch()

    def _consume_pending_unordered_alloc_events(self) -> StrictReplayResult | None:
        """Consumes all pending unordered alloc events from the alloc buffer.

        Unordered alloc events are those that don't have to match in order with event loop output events.
        Returns StrictReplayResult if a replay mismatch is detected during the processing of unordered alloc events,
        or None if all good.
        """
        while True:
            alloc_event: AllocationEvent | None = self._alloc_buffer.peek()
            if alloc_event is None:
                return None

            # Process all unordered alloc events here case by case.
            if alloc_event.HasField("function_call_watcher_result"):
                # Unordered alloc events are never checked for matching with event loop output events.
                fcwr: AllocationEventFunctionCallWatcherResult = (
                    alloc_event.function_call_watcher_result
                )
                self._alloc_buffer.next()
                process_function_call_watcher_result(
                    event=fcwr,
                    event_loop=self._event_loop,
                    blob_store=self._blob_store,
                    logger=self._logger,
                )
            else:
                # Strictly ordered alloc event. Stop consuming events here.
                return None
