import threading
from typing import Any

from tensorlake.applications import (
    DeserializationError,
    Function,
    InternalError,
    RequestContext,
)
from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.function.type_hints import (
    function_signature,
    return_type_hint,
)
from tensorlake.applications.function.user_data_serializer import (
    function_output_serializer,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.metadata import (
    FunctionCallMetadata,
)

from ..proto.function_executor_pb2 import (
    REPLAY_MODE_STRICT,
    Allocation,
    AllocationEvent,
    AllocationExecutionEvent,
    AllocationExecutionEventFinishAllocation,
    FunctionRef,
    ReadAllocationEventLogResponse,
)
from ..user_events import (
    AllocationEventDetails,
    log_user_event_allocations_finished,
    log_user_event_allocations_started,
)
from .allocation_event import (
    process_function_call_created,
    process_function_call_watcher_created,
    process_function_call_watcher_result,
)
from .allocation_state_wrapper import AllocationStateWrapper
from .blob_manager import AllocationBLOBManager
from .download import download_function_arguments
from .event_log_reader import EventLogReader, EventLogReaderStopped
from .event_loop import (
    AllocationEventLoop,
    InputEventEmergencyShutdown,
    OutputEventBatch,
    OutputEventFinishAllocation,
    OutputEventType,
    SpecialFunctionCallSettings,
)
from .execution_event import EventLoopOutputEventConverter
from .execution_log_buffer import ExecutionLogBuffer
from .finish_event_helper import FinishEventHelper
from .request_context.progress import AllocationProgress
from .request_context.request_state import AllocationRequestState
from .sdk_algorithms import (
    deserialize_application_function_call_args,
    deserialize_sdk_function_call_args,
    reconstruct_sdk_function_call_args,
    validate_and_deserialize_function_call_metadata,
)
from .strict_mode_replayer import AllocationStrictModeReplayer, StrictReplayResult
from .value import SerializedValue, Value

# TODO: Implement cause-effect exception -> alloc log event ID tracking
# via TensorlakeError._event_details field. See
# https://www.notion.so/tensorlake/UX-of-durable-execution-error-handling-and-replay-31ba5404295580b684f4f8be38eb4feb


class AllocationRunner:
    """Runs a single allocation in a separate thread, allowing to track its state.

    Adds finish event to execution log buffer when finished.
    """

    def __init__(
        self,
        allocation: Allocation,
        function_ref: FunctionRef,
        function: Function,
        function_instance_arg: Any | None,
        blob_store: BLOBStore,
        request_context: RequestContext,
        logger: InternalLogger,
    ):
        self._allocation: Allocation = allocation
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._function_instance_arg: Any | None = function_instance_arg
        self._blob_store: BLOBStore = blob_store
        self._request_context: RequestContext = request_context
        self._logger: InternalLogger = logger.bind(module=__name__)

        self._allocation_event_details: AllocationEventDetails = AllocationEventDetails(
            namespace=self._function_ref.namespace,
            application_name=self._function_ref.application_name,
            application_version=self._function_ref.application_version,
            function_name=self._function_ref.function_name,
            request_id=self._allocation.request_id,
            function_call_id=self._allocation.function_call_id,
            allocation_id=self._allocation.allocation_id,
        )

        self._allocation_state: AllocationStateWrapper = AllocationStateWrapper()
        self._request_state: AllocationRequestState = AllocationRequestState(
            allocation_state=self._allocation_state,
            logger=logger,
        )
        self._allocation_progress: AllocationProgress = AllocationProgress(
            allocation_state=self._allocation_state,
            logger=logger,
        )
        self._finish_event_helper: FinishEventHelper = FinishEventHelper(
            function_ref=function_ref,
            function=function,
            allocation_event_details=self._allocation_event_details,
            logger=self._logger,
        )
        self._blob_manager: AllocationBLOBManager = AllocationBLOBManager(
            allocation_state=self._allocation_state,
            logger=logger,
        )
        self._execution_log_buffer: ExecutionLogBuffer = ExecutionLogBuffer()
        self._output_event_converter: EventLoopOutputEventConverter = (
            EventLoopOutputEventConverter(
                finish_event_helper=self._finish_event_helper,
                request_error_blob=allocation.inputs.request_error_blob,
                blob_store=blob_store,
                function=function,
                function_ref=function_ref,
                blob_manager=self._blob_manager,
                logger=self._logger,
            )
        )
        self._event_log_reader: EventLogReader = EventLogReader(
            allocation_id=allocation.allocation_id,
        )
        self._run_allocation_thread: threading.Thread = threading.Thread(
            target=self._run_allocation,
            daemon=True,
        )
        # Processes allocation events after strict replay phase finished.
        self._process_allocation_events_thread: threading.Thread | None = None
        # Allocation function output related info.
        self._allocation_function_args: list[Any] | None = None
        self._allocation_function_kwargs: dict[str, Any] | None = None
        self._allocation_function_call_settings: SpecialFunctionCallSettings | None = (
            None
        )

        # Event loop for running user code and managing Futures.
        self._event_loop: AllocationEventLoop = AllocationEventLoop(
            function=function,
            function_call_id=allocation.function_call_id,
            allocation_id=allocation.allocation_id,
            request_context=request_context,
            logger=logger,
        )
        self._strict_mode_replayer: AllocationStrictModeReplayer = (
            AllocationStrictModeReplayer(
                event_log_reader=self._event_log_reader,
                event_loop=self._event_loop,
                finish_event_helper=self._finish_event_helper,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        )

    @property
    def allocation_state(self) -> AllocationStateWrapper:
        return self._allocation_state

    @property
    def event_loop(self) -> AllocationEventLoop:
        return self._event_loop

    @property
    def execution_log_buffer(self) -> ExecutionLogBuffer:
        return self._execution_log_buffer

    @property
    def event_log_reader(self) -> EventLogReader:
        return self._event_log_reader

    @property
    def request_state(self) -> AllocationRequestState:
        return self._request_state

    @property
    def allocation_progress(self) -> AllocationProgress:
        return self._allocation_progress

    @property
    def blob_manager(self) -> AllocationBLOBManager:
        return self._blob_manager

    def run(self) -> None:
        """Runs the allocation in a separate thread."""
        self._run_allocation_thread.start()

    def _process_allocation_events(self, after_clock: int) -> None:
        """Reads AllocationEvent protos via EventLogReader, converts to EventLoop InputEvents.

        Doesn't raise any exceptions.
        """
        while True:
            try:
                response: ReadAllocationEventLogResponse = self._event_log_reader.read(
                    after_clock
                )
            except EventLogReaderStopped:
                break
            try:
                for entry in response.entries:
                    self._process_allocation_event(entry)
                if response.HasField("last_clock"):
                    after_clock = response.last_clock
            except BaseException as e:
                # NB: If an exception is raised in an allocation event handler, we should not report it
                # to event loop as an "internal error" input event because the original event
                # that we failed to process might be a "success" event. If we report "internal error"
                # here then we'll diverge user code execution from the event history which is
                # a durable execution bug. Instead we just stop the allocation execution immediately.
                # This will allow us to fix the root cause of this failure an replay the function call
                # with its event history intact.
                self._logger.error(
                    "Error processing allocation event, sending emergency shutdown to event loop",
                    exc_info=e,
                )
                self._event_loop.add_input_event(InputEventEmergencyShutdown())

        self._logger.info("stopping allocation event processing thread")

    def _process_allocation_event(self, entry: AllocationEvent) -> None:
        """Processes a single AllocationEvent entry from the event log.

        Raises Exception on internal error.
        """
        if entry.HasField("function_call_created"):
            process_function_call_created(
                entry.function_call_created,
                event_loop=self._event_loop,
                logger=self._logger,
            )
        elif entry.HasField("function_call_watcher_created"):
            process_function_call_watcher_created(
                entry.function_call_watcher_created,
                event_loop=self._event_loop,
                logger=self._logger,
            )
        elif entry.HasField("function_call_watcher_result"):
            process_function_call_watcher_result(
                entry.function_call_watcher_result,
                event_loop=self._event_loop,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        else:
            raise InternalError(f"Unknown allocation event type: {type(entry)}")

    def _run_allocation(self) -> None:
        early_finish_event: AllocationExecutionEventFinishAllocation | None = (
            self._finish_event_helper.from_internal_error()
        )
        try:
            log_user_event_allocations_started([self._allocation_event_details])
            early_finish_event = self.__run_allocation()
        finally:
            if early_finish_event is not None:
                self._execution_log_buffer.add_batch(
                    [AllocationExecutionEvent(finish_allocation=early_finish_event)]
                )

            # Stops RPCs waiting for new execution events.
            self._execution_log_buffer.stop()
            # Stops RPCs waiting for new allocation event log read requests.
            # Makes _process_allocation_events_thread exit.
            self._event_log_reader.stop()
            if (
                self._process_allocation_events_thread is not None
                and self._process_allocation_events_thread.is_alive()
            ):
                try:
                    self._logger.info(
                        "waiting for allocation event processing thread to finish"
                    )
                    self._process_allocation_events_thread.join()
                except RuntimeError as e:
                    self._logger.error(
                        "error while waiting for allocation event processing thread to finish",
                        exc_info=e,
                    )
            self._logger.info("waiting for event loop to finish")
            self._event_loop.join()

            # This must be the last thing we do. Immediately after this the allocation can be deleted.
            self._allocation_state.set_finished()
            log_user_event_allocations_finished([self._allocation_event_details])

    def __run_allocation(self) -> AllocationExecutionEventFinishAllocation | None:
        """Runs the allocation to completion.

        Returns a finish event if the allocation finished early (e.g. error during preparation).
        Returns None if the allocation ran normally (finish event already in execution log buffer).
        Doesn't raise any exceptions.
        """
        finish_event: AllocationExecutionEventFinishAllocation | None = (
            self._prepare_allocation_run()
        )
        if finish_event is not None:
            return finish_event

        self._event_loop.start(
            self._allocation_function_args,
            self._allocation_function_kwargs,
            self._allocation_function_call_settings,
        )
        live_execution_after_clock: int = 0
        live_execution_initial_event_loop_output_events: list[OutputEventType] = []

        if self._allocation.replay_mode == REPLAY_MODE_STRICT:
            replay_result: StrictReplayResult = self._strict_mode_replayer.run()

            if replay_result.finish_event is not None:
                return replay_result.finish_event

            live_execution_after_clock = replay_result.last_clock
            live_execution_initial_event_loop_output_events = (
                replay_result.pending_event_loop_output_events
            )

        self._process_allocation_events_thread = threading.Thread(
            target=self._process_allocation_events,
            args=(live_execution_after_clock,),
            daemon=True,
        )
        self._process_allocation_events_thread.start()
        # Blocks until user function code finishes.
        self._process_event_loop_output_events(
            live_execution_initial_event_loop_output_events
        )
        return None

    def _prepare_allocation_run(
        self,
    ) -> AllocationExecutionEventFinishAllocation | None:
        """Prepares allocation for running.

        Sets up allocation state, parses function call metadata and arguments, and sets up output overrides.
        Doesn't raise any exceptions.
        """
        # We need to be very careful who's code we're running here. Exceptions raised in customer
        # code should be caught here and converted into proper finish event indicating customer code failure.
        # Exceptions in our internal FE code are just raised here and handled by caller.

        # This is internal FE code.
        try:
            serialized_args: list[SerializedValue] = download_function_arguments(
                self._allocation, self._blob_store, self._logger
            )
            function_call_metadata: FunctionCallMetadata | None = (
                validate_and_deserialize_function_call_metadata(
                    serialized_function_call_metadata=self._allocation.inputs.function_call_metadata,
                    serialized_args=serialized_args,
                    function=self._function,
                    logger=self._logger,
                )
            )

            self._read_function_call_metadata(function_call_metadata)
            return self._parse_function_call_args(
                function_call_metadata, serialized_args
            )
        except BaseException as e:
            self._logger.error(
                "error while preparing allocation run",
                exc_info=e,
            )
            return self._finish_event_helper.from_internal_error()

    def _process_event_loop_output_events(
        self, initial_events: list[OutputEventType] | None
    ) -> None:
        """Processes output events from the event loop until allocation completes.

        Doesn't raise any exceptions.
        """
        while True:
            if initial_events is None:
                event_loop_output_events: list[OutputEventType] = (
                    self._event_loop.wait_for_output_event_batch().events
                )
            else:
                event_loop_output_events: list[OutputEventType] = initial_events
                initial_events = None

            if self._process_event_loop_output_event_list(event_loop_output_events):
                break  # Alloc finished.

    def _process_event_loop_output_event_list(
        self, output_events: list[OutputEventType]
    ) -> bool:
        """Processes a batch of event loop output events. Returns True if allocation finished, False otherwise.

        Doesn't raise any exceptions.
        """
        event_loop_execution_events: list[AllocationExecutionEvent] = []
        alloc_finished: bool = False

        for output_event in output_events:
            try:
                event_loop_execution_events.append(
                    self._output_event_converter.to_execution_event(output_event)
                )
                if isinstance(output_event, OutputEventFinishAllocation):
                    alloc_finished = True
            except BaseException as e:
                self._logger.error(
                    "Error while processing event loop output event, sending emergency shutdown to event loop",
                    event=str(output_event),
                    exc_info=e,
                )
                self._event_loop.add_input_event(InputEventEmergencyShutdown())
                break

        if len(event_loop_execution_events) > 0:
            self._execution_log_buffer.add_batch(event_loop_execution_events)

        return alloc_finished

    def _read_function_call_metadata(
        self, function_call_metadata: FunctionCallMetadata | None
    ) -> None:
        """Reads output serializer/type hint overrides and function call settings from function call metadata.

        Raises exception on internal error.
        """
        if function_call_metadata is None:
            # Application function call created by Server.
            # Application function call doesn't have a parent call that can override output serializer.
            # Application function overrides output type hint and serializer for all its child tail call futures.
            self._output_event_converter.set_output_overrides(
                serializer_name=function_output_serializer(
                    function=self._function,
                    output_serializer_override=None,
                ).name,
                has_type_hint=True,
                type_hint=return_type_hint(
                    function_signature(self._function).return_annotation
                ),
            )
        else:
            # Regular function call created by SDK. Uses function call metadata.
            self._output_event_converter.set_output_overrides(
                serializer_name=function_call_metadata.output_serializer_name_override,
                has_type_hint=function_call_metadata.has_output_type_hint_override,
                type_hint=(
                    function_call_metadata.output_type_hint_override
                    if function_call_metadata.has_output_type_hint_override
                    else None
                ),
            )
            if (
                function_call_metadata.is_map_splitter
                or function_call_metadata.is_map_concat
                or function_call_metadata.is_reduce_splitter
            ):
                self._allocation_function_call_settings = SpecialFunctionCallSettings(
                    is_map_splitter=function_call_metadata.is_map_splitter,
                    is_reduce_splitter=function_call_metadata.is_reduce_splitter,
                    splitter_function_name=function_call_metadata.splitter_function_name,
                    splitter_input_mode=function_call_metadata.splitter_input_mode,
                    is_map_concat=function_call_metadata.is_map_concat,
                )

    def _parse_function_call_args(
        self,
        function_call_metadata: FunctionCallMetadata | None,
        serialized_args: list[SerializedValue],
    ) -> AllocationExecutionEventFinishAllocation | None:
        """Parses allocation function call arguments.

        Raises exception on internal error.
        Returns finish event on user code error.
        """
        args: list[Any]
        kwargs: dict[str, Any]
        if function_call_metadata is None:
            # Application function call created by Server.
            if len(serialized_args) == 0:
                raise InternalError(
                    f"Application function call must have at least one argument, "
                    f"got {len(serialized_args)}."
                )
            # This is user code.
            try:
                args, kwargs = deserialize_application_function_call_args(
                    function=self._function,
                    payload=serialized_args[0],
                    function_instance_arg=self._function_instance_arg,
                )
            except DeserializationError as e:
                return self._finish_event_helper.from_user_exception(e)
        else:
            # Regular function call created by SDK.
            # This is user code.
            try:
                arg_values: dict[str, Value] = deserialize_sdk_function_call_args(
                    serialized_args
                )
            except BaseException as e:
                return self._finish_event_helper.from_user_exception(e)

            # This is internal FE code.
            args, kwargs = reconstruct_sdk_function_call_args(
                function_call_metadata=function_call_metadata,
                arg_values=arg_values,
                function_instance_arg=self._function_instance_arg,
            )

        self._allocation_function_args = args
        self._allocation_function_kwargs = kwargs
        return None
