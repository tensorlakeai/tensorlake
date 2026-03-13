import datetime
import pickle
import threading
import time
from dataclasses import dataclass
from typing import Any

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from tensorlake.applications import (
    DeserializationError,
    Function,
    FunctionError,
    InternalError,
    RequestContext,
    RequestError,
    SerializationError,
    TensorlakeError,
    TimeoutError,
)
from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.function.type_hints import (
    function_signature,
    return_type_hint,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value_with_metadata,
    function_output_serializer,
)
from tensorlake.applications.interface.futures import (
    _request_scoped_id,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.metadata import (
    FunctionCallMetadata,
)
from tensorlake.applications.registry import get_function
from tensorlake.applications.user_data_serializer import (
    UserDataSerializer,
)

from ..proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationEventFunctionCallCreated,
    AllocationEventFunctionCallWatcherResult,
    AllocationExecutionEvent,
    AllocationExecutionEventCreateFunctionCall,
    AllocationExecutionEventCreateFunctionCallWatcher,
    AllocationExecutionEventFinishAllocation,
    AllocationExecutionEventFunctionCallCreationFailed,
    AllocationOutcomeCode,
    AllocationOutputBLOB,
    AllocationResult,
    AllocationState,
    AllocationUpdate,
    ExecutionPlanUpdates,
    FunctionCallWatcherStatus,
    FunctionRef,
    SerializedObjectInsideBLOB,
)
from ..user_events import (
    AllocationEventDetails,
    log_user_event_allocations_finished,
    log_user_event_allocations_started,
)
from .allocation_state_wrapper import AllocationStateWrapper
from .download import download_function_arguments, download_serialized_objects
from .event_log_reader import EventLogReader, EventLogReaderStopped
from .event_loop import (
    AllocationEventLoop,
    InputEventEmergencyShutdown,
    InputEventFunctionCallCreated,
    InputEventFunctionCallWatcherResult,
    OutputEventBatch,
    OutputEventCreateFunctionCall,
    OutputEventCreateFunctionCallWatcher,
    OutputEventFinishAllocation,
    OutputEventType,
    SpecialFunctionCallSettings,
)
from .execution_log_buffer import ExecutionLogBuffer
from .request_context.progress import AllocationProgress
from .request_context.request_state import AllocationRequestState
from .result_helper import ResultHelper
from .sdk_algorithms import (
    deserialize_application_function_call_args,
    deserialize_sdk_function_call_args,
    output_event_to_execution_plan_updates,
    reconstruct_sdk_function_call_args,
    serialize_output_event_args,
    serialize_user_value,
    validate_and_deserialize_function_call_metadata,
)
from .upload import (
    serialized_values_to_serialized_objects,
    upload_request_error,
    upload_serialized_objects_to_blob,
)
from .value import SerializedValue, Value


@dataclass
class _OutputBLOBRequestInfo:
    # Not None once the BLOB is ready to be used.
    blob: AllocationOutputBLOB | None
    # Set only once after the BLOB is set.
    blob_available: threading.Event


class AllocationRunner:
    """Runs a single allocation in a separate thread, allowing to track its state.

    Sets allocation.result when finished.
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
        self._result_helper: ResultHelper = ResultHelper(
            function_ref=function_ref,
            function=function,
            logger=self._logger,
        )
        self._execution_log_buffer: ExecutionLogBuffer = ExecutionLogBuffer()
        self._event_log_reader: EventLogReader = EventLogReader(
            allocation_id=allocation.allocation_id,
        )
        self._run_allocation_thread: threading.Thread = threading.Thread(
            target=self._run_allocation,
            daemon=True,
        )
        self._process_server_events_thread: threading.Thread = threading.Thread(
            target=self._process_server_events, daemon=True
        )
        # Allocation function output related info.
        self._output_value_serializer_name_override: str | None = None
        self._has_output_value_type_hint_override: bool = False
        self._output_value_type_hint_override: Any = None
        self._allocation_function_args: list[Any] | None = None
        self._allocation_function_kwargs: dict[str, Any] | None = None
        self._allocation_function_call_settings: SpecialFunctionCallSettings | None = (
            None
        )

        # BLOB ID -> _OutputBLOBRequestInfo.
        self._output_blob_requests: dict[str, _OutputBLOBRequestInfo] = {}

        # Event loop for running user code and managing Futures.
        self._event_loop: AllocationEventLoop = AllocationEventLoop(
            function=function,
            function_call_id=allocation.function_call_id,
            allocation_id=allocation.allocation_id,
            request_context=request_context,
            logger=logger,
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

    def run(self) -> None:
        """Runs the allocation in a separate thread.

        When the allocation is finished, sets it .result field.
        """
        self._run_allocation_thread.start()

    def deliver_allocation_update(self, update: AllocationUpdate) -> None:
        # No need for any locks because we never block here so we hold GIL non stop.
        if update.HasField("output_blob"):
            blob: AllocationOutputBLOB = update.output_blob
            blob_id: str = blob.blob.id

            if blob_id not in self._output_blob_requests:
                self._logger.error(
                    "received output blob update for unknown blob request",
                    blob_id=blob_id,
                )
                return

            blob_request_info: _OutputBLOBRequestInfo = self._output_blob_requests[
                blob_id
            ]
            blob_request_info.blob = blob
            blob_request_info.blob_available.set()
        else:
            self._logger.error(
                "received unexpected allocation update",
                update=str(update),
            )

    def _process_server_events(self) -> None:
        """Reads AllocationEvent protos via EventLogReader, converts to InputEvents.

        Doesn't raise any exceptions.
        """
        after_clock: int = 0
        while True:
            try:
                response = self._event_log_reader.read(
                    after_clock=after_clock, max_entries=None
                )
            except EventLogReaderStopped:
                break
            try:
                for entry in response.entries:
                    self._process_allocation_event(entry)
                if response.HasField("last_clock"):
                    after_clock = response.last_clock
            except BaseException as e:
                # NB: If an exception is raised in an event handler, we should not report it
                # to event loop as an "internal error" input event because the original event
                # that we failed to process might be a "success" event. If we report "internal error"
                # here then we'll diverge user code execution from the event history which is
                # a durable execution bug. Instead we just stop it immediately.
                self._logger.error(
                    "Error processing allocation event, sending emergency shutdown to event loop",
                    exc_info=e,
                )
                self._event_loop.add_input_event(InputEventEmergencyShutdown())

        self._logger.info("stopping server event processing thread")

    def _process_allocation_event(self, entry) -> None:
        """Processes a single AllocationEvent entry from the event log.

        Raises Exception on internal error.
        """
        if entry.HasField("function_call_created"):
            self._process_event_function_call_created(entry.function_call_created)
        elif entry.HasField("function_call_watcher_result"):
            self._process_event_function_call_watcher_result(
                entry.function_call_watcher_result
            )
        else:
            self._logger.error(
                "received unknown allocation event type",
                event=str(entry),
            )

    def _process_event_function_call_created(
        self, event: AllocationEventFunctionCallCreated
    ) -> None:
        """Processes function call created event from the event log.

        Raises Exception on internal error.
        """
        exception: TensorlakeError | None = None
        if (
            event.HasField("status")
            and event.status.code != grpc.StatusCode.OK.value[0]
        ):
            # Check if there's pickled error metadata from a creation failure.
            if event.HasField("metadata") and len(event.metadata) > 0:
                try:
                    exception = pickle.loads(event.metadata)
                except BaseException:
                    exception = InternalError("Failed to start function call")
            else:
                exception = InternalError("Failed to start function call")
            self._logger.error(
                "child future function call creation failed",
                future_fn_call_id=event.function_call_id,
                status=event.status,
            )
        else:
            self._logger.info(
                "started child function call future",
                future_fn_call_id=event.function_call_id,
            )

        self._event_loop.add_input_event(
            InputEventFunctionCallCreated(
                durable_id=event.function_call_id,
                exception=exception,
            )
        )

    def _process_event_function_call_watcher_result(
        self, event: AllocationEventFunctionCallWatcherResult
    ) -> None:
        """Processes function call watcher result event from the event log.

        Raises Exception on internal error.
        """
        output: Any = None
        exception: TensorlakeError | None = None

        if (
            event.HasField("watcher_status")
            and event.watcher_status
            == FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_TIMEDOUT
        ):
            exception = TimeoutError()
        elif (
            event.outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS
        ):
            serialized_output: SerializedValue = download_serialized_objects(
                serialized_objects=[event.value_output],
                serialized_object_blobs=[event.value_blob],
                blob_store=self._blob_store,
                logger=self._logger,
            )[0]
            output = deserialize_value_with_metadata(
                serialized_output.data, serialized_output.metadata
            )
        elif (
            event.outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
        ):
            if event.HasField("request_error_output"):
                serialized_request_error: SerializedValue = download_serialized_objects(
                    serialized_objects=[event.request_error_output],
                    serialized_object_blobs=[event.request_error_blob],
                    blob_store=self._blob_store,
                    logger=self._logger,
                )[0]
                exception = RequestError(
                    message=serialized_request_error.data.decode("utf-8")
                )
            else:
                exception = FunctionError("Function call failed")
        else:
            self._logger.error(
                f"Unexpected outcome code in function call watcher result: {event.outcome_code}"
            )
            raise InternalError(
                f"Unexpected outcome code in function call watcher result: "
                f"{event.outcome_code}"
            )

        self._logger.info(
            "child future completed",
            future_fn_call_id=event.function_call_id,
            success=exception is None,
        )

        self._event_loop.add_input_event(
            InputEventFunctionCallWatcherResult(
                function_call_durable_id=event.function_call_id,
                output=output,
                exception=exception,
            )
        )

    def _get_new_output_blob(self, size: int) -> BLOB:
        """Returns new BLOB to upload function outputs to.

        Raises exception on error.
        """
        blob_id: str = _request_scoped_id()
        blob_request_info: _OutputBLOBRequestInfo = _OutputBLOBRequestInfo(
            blob=None,
            blob_available=threading.Event(),
        )
        self._output_blob_requests[blob_id] = blob_request_info
        self._allocation_state.add_output_blob_request(id=blob_id, size=size)

        blob_request_info.blob_available.wait()

        self._allocation_state.remove_output_blob_request(id=blob_id)
        del self._output_blob_requests[blob_id]

        if isinstance(blob_request_info.blob, AllocationOutputBLOB):
            if blob_request_info.blob.status.code != grpc.StatusCode.OK.value[0]:
                self._logger.error(
                    "received output blob with error status",
                    blob_id=blob_request_info.blob.blob.id,
                    status=blob_request_info.blob.status,
                )
                raise RuntimeError(
                    f"Failed to create output BLOB: {blob_request_info.blob.status}"
                )
            return blob_request_info.blob.blob
        else:
            return blob_request_info.blob

    def _run_allocation(self) -> None:
        alloc_result: AllocationResult = self._result_helper.internal_error()
        try:
            log_user_event_allocations_started([self._allocation_event_details])
            alloc_result = self.__run_allocation()
        except BaseException as e:
            # This leaks event loop resources because we don't properly cleanup event loop in this case.
            self._logger.error(
                "Unexpected exception in function executor code while running allocation",
                exc_info=e,
            )
        finally:
            self._allocation.result.CopyFrom(alloc_result)
            # This must be the last thing we do. Immeditately after this the allocation can be deleted.
            self._allocation_state.set_result(alloc_result)
            log_user_event_allocations_finished([self._allocation_event_details])

    def __run_allocation(self) -> AllocationResult:
        """Runs the allocation to completion.

        Doesn't raise any exceptions.
        """
        alloc_result: AllocationResult | None = self._prepare_allocation_run()
        if alloc_result is not None:
            return alloc_result

        self._event_loop.start(
            self._allocation_function_args,
            self._allocation_function_kwargs,
            self._allocation_function_call_settings,
        )
        self._process_server_events_thread.start()
        # Blocks here until event loop finishes and cleans up its resources.
        alloc_result = self._process_event_loop_output_events()

        self._event_log_reader.stop()
        self._execution_log_buffer.stop()
        if self._process_server_events_thread.is_alive():
            try:
                self._logger.info(
                    "waiting for server event processing thread to finish"
                )
                self._process_server_events_thread.join()
            except RuntimeError as e:
                self._logger.error(
                    "error while waiting for server event processing thread to finish",
                    exc_info=e,
                )
        self._logger.info("waiting for event loop to finish")
        self._event_loop.join()

        return alloc_result

    def _prepare_allocation_run(self) -> AllocationResult | None:
        """Prepares allocation for running.

        Sets up allocation state, parses function call metadata and arguments, and sets up output overrides.
        Doesn't raise any exceptions.
        """
        # We need to be very careful who's code we're running here. Exceptions raised in customer
        # code should be caught here and converted into proper AllocationResult indicating customer code failure.
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
            return self._result_helper.internal_error()

    def _process_event_loop_output_events(self) -> AllocationResult:
        """Processes output events from the event loop until allocation completes.

        Doesn't raise any exceptions.
        """
        while True:
            batch: OutputEventBatch = self._event_loop.wait_for_output_event_batch()
            execution_events: list[AllocationExecutionEvent] = []
            alloc_result: AllocationResult | None = None

            for output_event in batch.events:
                try:
                    exec_event, result = self._convert_output_event_to_execution_event(
                        output_event
                    )
                    if exec_event is not None:
                        execution_events.append(exec_event)
                    if result is not None:
                        alloc_result = result
                except BaseException as e:
                    self._logger.error(
                        "Error while processing event loop output event, sending emergency shutdown to event loop",
                        event=str(output_event),
                        exc_info=e,
                    )
                    self._event_loop.add_input_event(InputEventEmergencyShutdown())
                    break

            if execution_events:
                self._execution_log_buffer.add_batch(execution_events)

            if alloc_result is not None:
                return alloc_result

    def _convert_output_event_to_execution_event(
        self, output_event: OutputEventType
    ) -> tuple[AllocationExecutionEvent | None, AllocationResult | None]:
        """Converts a single event loop output event to an execution event proto.

        Returns (execution_event, alloc_result). execution_event is None if no event should be added.
        alloc_result is not None only for FinishAllocation events.
        Raises Exception on internal error.
        """
        if isinstance(output_event, OutputEventFinishAllocation):
            exec_event, alloc_result = (
                self._convert_finish_allocation_to_execution_event(output_event)
            )
            return exec_event, alloc_result
        elif isinstance(output_event, OutputEventCreateFunctionCall):
            return self._convert_call_function_to_execution_event(output_event), None
        elif isinstance(output_event, OutputEventCreateFunctionCallWatcher):
            return self._convert_add_watcher_to_execution_event(output_event), None
        else:
            self._logger.error(
                "received unknown output event from event loop",
                event_type=str(type(output_event)),
                event=str(output_event),
            )
            return None, None

    def _convert_finish_allocation_to_execution_event(
        self, output_event: OutputEventFinishAllocation
    ) -> tuple[AllocationExecutionEvent | None, AllocationResult]:
        """Converts OutputEventFinishAllocation to an execution event and AllocationResult.

        Returns (execution_event, alloc_result). execution_event may be None for internal errors.
        Raises Exception on internal error while processing the event.
        """
        if output_event.internal_exception is not None:
            self._logger.error(
                "allocation finished with internal error",
                exc_info=output_event.internal_exception,
            )
            finish_event = self._result_helper.to_finish_event_internal_error()
            alloc_result = self._result_helper.internal_error()
            return (
                AllocationExecutionEvent(finish_allocation=finish_event),
                alloc_result,
            )

        if output_event.user_exception is not None:
            if isinstance(output_event.user_exception, RequestError):
                # This is user code.
                try:
                    utf8_message: bytes = output_event.user_exception.message.encode(
                        "utf-8"
                    )
                except BaseException:
                    finish_event = (
                        self._result_helper.to_finish_event_from_user_exception()
                    )
                    alloc_result = self._result_helper.from_user_exception(
                        self._allocation_event_details, output_event.user_exception
                    )
                    return (
                        AllocationExecutionEvent(finish_allocation=finish_event),
                        alloc_result,
                    )

                # This is internal FE code.
                request_error_so, uploaded_output_blob = upload_request_error(
                    utf8_message=utf8_message,
                    destination_blob=self._allocation.inputs.request_error_blob,
                    blob_store=self._blob_store,
                    logger=self._logger,
                )
                finish_event = self._result_helper.to_finish_event_from_request_error(
                    request_error_output=request_error_so,
                    uploaded_request_error_blob=uploaded_output_blob,
                )
                alloc_result = self._result_helper.from_request_error(
                    details=self._allocation_event_details,
                    request_error=output_event.user_exception,
                    request_error_output=request_error_so,
                    uploaded_request_error_blob=uploaded_output_blob,
                )
                return (
                    AllocationExecutionEvent(finish_allocation=finish_event),
                    alloc_result,
                )
            else:
                finish_event = self._result_helper.to_finish_event_from_user_exception()
                alloc_result = self._result_helper.from_user_exception(
                    self._allocation_event_details, output_event.user_exception
                )
                return (
                    AllocationExecutionEvent(finish_allocation=finish_event),
                    alloc_result,
                )

        if output_event.tail_call is not None:
            finish_event = self._result_helper.to_finish_event_from_function_output(
                output=output_event.tail_call.durable_id, uploaded_outputs_blob=None
            )
            alloc_result = self._result_helper.from_function_output(
                output=output_event.tail_call.durable_id, uploaded_outputs_blob=None
            )
            return (
                AllocationExecutionEvent(finish_allocation=finish_event),
                alloc_result,
            )

        # Regular value output. This is user code (serialization).
        output_value_serializer: UserDataSerializer = function_output_serializer(
            function=self._function,
            output_serializer_override=self._output_value_serializer_name_override,
        )
        try:
            serialized_output_value: SerializedValue = serialize_user_value(
                value=output_event.value,
                serializer=output_value_serializer,
                type_hint=(
                    self._output_value_type_hint_override
                    if self._has_output_value_type_hint_override
                    else type(output_event.value)
                ),
            )
        except BaseException as e:
            finish_event = self._result_helper.to_finish_event_from_user_exception()
            alloc_result = self._result_helper.from_user_exception(
                self._allocation_event_details, e
            )
            return (
                AllocationExecutionEvent(finish_allocation=finish_event),
                alloc_result,
            )

        # This is internal FE code.
        serialized_objects, blob_data = serialized_values_to_serialized_objects(
            serialized_values={
                serialized_output_value.metadata.id: serialized_output_value
            }
        )
        serialized_output = serialized_objects[serialized_output_value.metadata.id]
        outputs_blob: BLOB = self._get_new_output_blob(
            size=sum(len(data) for data in blob_data)
        )
        uploaded_output_blob = upload_serialized_objects_to_blob(
            serialized_objects=serialized_objects,
            blob_data=blob_data,
            destination_blob=outputs_blob,
            blob_store=self._blob_store,
            logger=self._logger,
        )

        finish_event = self._result_helper.to_finish_event_from_function_output(
            output=serialized_output, uploaded_outputs_blob=uploaded_output_blob
        )
        alloc_result = self._result_helper.from_function_output(
            output=serialized_output, uploaded_outputs_blob=uploaded_output_blob
        )
        return (
            AllocationExecutionEvent(finish_allocation=finish_event),
            alloc_result,
        )

    def _convert_call_function_to_execution_event(
        self, output_event: OutputEventCreateFunctionCall
    ) -> AllocationExecutionEvent | None:
        """Converts OutputEventCreateFunctionCall to an execution event.

        Returns None if the event should not be added to the execution log (e.g. serialization error).
        Raises Exception on internal error.
        """
        # This is user code.
        try:
            serialized_args, serialized_kwargs, serialized_values = (
                serialize_output_event_args(
                    args=output_event.args,
                    kwargs=output_event.kwargs,
                    function_name=output_event.function_name,
                )
            )
        except SerializationError as e:
            # Send function_call_creation_failed event with pickled error so server can
            # add it to event log for deterministic replay.
            try:
                pickled_error: bytes = pickle.dumps(e)
            except BaseException:
                pickled_error = b""
            return AllocationExecutionEvent(
                function_call_creation_failed=AllocationExecutionEventFunctionCallCreationFailed(
                    function_call_id=output_event.durable_id,
                    metadata=pickled_error,
                )
            )

        # This is our code.
        serialized_objects: dict[str, SerializedObjectInsideBLOB] = {}
        uploaded_args_blob: BLOB | None = None
        if len(serialized_values) > 0:
            serialized_objects, blob_data = serialized_values_to_serialized_objects(
                serialized_values=serialized_values
            )
            args_blob: BLOB = self._get_new_output_blob(
                size=sum(len(data) for data in blob_data)
            )
            uploaded_args_blob = upload_serialized_objects_to_blob(
                serialized_objects=serialized_objects,
                blob_data=blob_data,
                destination_blob=args_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )

        output_serializer_name_override: str | None = None
        if output_event.is_tail_call:
            output_serializer_name_override = (
                self._output_value_serializer_name_override
            )
        elif (
            output_event.special_settings is not None
            and output_event.special_settings.splitter_function_name is not None
        ):
            splitter_function: Function = get_function(
                output_event.special_settings.splitter_function_name
            )
            output_serializer_name_override = function_output_serializer(
                splitter_function, None
            ).name

        execution_plan_pb: ExecutionPlanUpdates = (
            output_event_to_execution_plan_updates(
                output_event=output_event,
                serialized_args=serialized_args,
                serialized_kwargs=serialized_kwargs,
                uploaded_serialized_objects=serialized_objects,
                output_serializer_name_override=output_serializer_name_override,
                has_output_type_hint_override=(
                    self._has_output_value_type_hint_override
                    if output_event.is_tail_call
                    else False
                ),
                output_type_hint_override=(
                    self._output_value_type_hint_override
                    if output_event.is_tail_call
                    else None
                ),
                function_ref=self._function_ref,
                settings=output_event.special_settings,
            )
        )
        if output_event.start_delay is not None:
            start_at: Timestamp = Timestamp()
            start_at.FromDatetime(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=output_event.start_delay)
            )
            execution_plan_pb.start_at.CopyFrom(start_at)

        self._logger.info(
            "starting child future",
            future_fn_call_id=output_event.durable_id,
        )

        return AllocationExecutionEvent(
            create_function_call=AllocationExecutionEventCreateFunctionCall(
                updates=execution_plan_pb,
                args_blob=uploaded_args_blob,
            )
        )

    def _convert_add_watcher_to_execution_event(
        self, output_event: OutputEventCreateFunctionCallWatcher
    ) -> AllocationExecutionEvent:
        """Converts OutputEventCreateFunctionCallWatcher to an execution event.

        Raises Exception on internal error.
        """
        durable_id: str = output_event.function_call_durable_id

        deadline: Timestamp | None = None
        if output_event.deadline is not None:
            deadline = Timestamp()
            deadline.FromDatetime(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=output_event.deadline - time.monotonic())
            )

        self._logger.info(
            "waiting for child future completion",
            future_fn_call_id=durable_id,
        )

        watcher_event = AllocationExecutionEventCreateFunctionCallWatcher(
            function_call_id=durable_id,
        )
        if deadline is not None:
            watcher_event.deadline.CopyFrom(deadline)

        return AllocationExecutionEvent(
            create_function_call_watcher=watcher_event,
        )

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
            self._output_value_serializer_name_override = function_output_serializer(
                function=self._function,
                output_serializer_override=None,
            ).name
            self._output_value_type_hint_override = return_type_hint(
                function_signature(self._function).return_annotation
            )
            self._has_output_value_type_hint_override = True
        else:
            # Regular function call created by SDK. Uses function call metadata.
            self._output_value_serializer_name_override = (
                function_call_metadata.output_serializer_name_override
            )
            if function_call_metadata.has_output_type_hint_override:
                self._output_value_type_hint_override = (
                    function_call_metadata.output_type_hint_override
                )
                self._has_output_value_type_hint_override = True
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
    ) -> AllocationResult | None:
        """Parses allocation function call arguments.

        Raises exception on internal error.
        Returns AllocationResult on user code error.
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
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )
        else:
            # Regular function call created by SDK.
            # This is user code.
            try:
                arg_values: dict[str, Value] = deserialize_sdk_function_call_args(
                    serialized_args
                )
            except BaseException as e:
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )

            # This is internal FE code.
            args, kwargs = reconstruct_sdk_function_call_args(
                function_call_metadata=function_call_metadata,
                arg_values=arg_values,
                function_instance_arg=self._function_instance_arg,
            )

        self._allocation_function_args = args
        self._allocation_function_kwargs = kwargs
        return None
