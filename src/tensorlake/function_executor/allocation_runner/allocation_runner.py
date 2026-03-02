import datetime
import queue
import threading
from dataclasses import dataclass
from typing import Any

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from tensorlake.applications import (
    DeserializationError,
    Function,
    InternalError,
    RequestContext,
    RequestError,
    SerializationError,
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
from tensorlake.applications.request_context.http_server.handlers.progress_update import (
    FunctionProgressUpdateRequest,
    FunctionProgressUpdateResponse,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.commit_write import (
    CommitWriteRequest,
    CommitWriteResponse,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.prepare_read import (
    PrepareReadRequest,
    PrepareReadResponse,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.prepare_write import (
    PrepareWriteRequest,
    PrepareWriteResponse,
)
from tensorlake.applications.user_data_serializer import (
    UserDataSerializer,
)

from ..proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationFunctionCallCreationResult,
    AllocationFunctionCallResult,
    AllocationOutcomeCode,
    AllocationOutputBLOB,
    AllocationResult,
    AllocationState,
    AllocationUpdate,
    ExecutionPlanUpdates,
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
)
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
class _ServerEventFunctionCallCreationResult:
    result: AllocationFunctionCallCreationResult


@dataclass
class _ServerEventFunctionCallResult:
    result: AllocationFunctionCallResult


@dataclass
class _ServerEventStopProcessingThread:
    pass  # A placeholder event just to stop the thread


_ServerEvent = (
    _ServerEventFunctionCallCreationResult
    | _ServerEventFunctionCallResult
    | _ServerEventStopProcessingThread
)


@dataclass
class _OutputBLOBRequestInfo:
    # Not None once the BLOB is ready to be used.
    # BLOB type use here is deprecated.
    blob: BLOB | AllocationOutputBLOB | None
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
        self._request_context: RequestContext = request_context
        self._result_helper: ResultHelper = ResultHelper(
            function_ref=function_ref,
            function=function,
            logger=self._logger,
        )
        self._run_allocation_thread: threading.Thread = threading.Thread(
            target=self._run_allocation,
            daemon=True,
        )
        self._process_server_events_thread: threading.Thread = threading.Thread(
            target=self._process_server_events, daemon=True
        )
        # Allocation function output related overrides.
        self._output_value_serializer_name_override: str | None = None
        self._has_output_value_type_hint_override: bool = False
        self._output_value_type_hint_override: Any = None
        self._allocation_function_args: list[Any] | None = None
        self._allocation_function_kwargs: dict[str, Any] | None = None

        # BLOB ID -> _OutputBLOBRequestInfo.
        self._output_blob_requests: dict[str, _OutputBLOBRequestInfo] = {}
        # Queue for events coming from Server.
        self._server_event_queue: queue.Queue[_ServerEvent] = queue.Queue()

        # Event loop for running user code and managing Futures.
        self._event_loop: AllocationEventLoop = AllocationEventLoop(
            function=function,
            function_call_id=allocation.function_call_id,
            allocation_id=allocation.allocation_id,
            request_context=request_context,
            logger=logger,
        )

    @property
    def event_loop(self) -> AllocationEventLoop:
        return self._event_loop

    def wait_allocation_state_update(
        self, last_seen_hash: str | None
    ) -> AllocationState:
        """Returns copy of the current allocation state when it's updated."""
        return self._allocation_state.wait_for_update(last_seen_hash)

    def run(self) -> None:
        """Runs the allocation in a separate thread.

        When the allocation is finished, sets it .result field.
        """
        self._run_allocation_thread.start()

    # finished() and is_terminal_state() need to be consistent with each other.
    # So once we return a terminal state to client and it calls delete_allocation,
    # finished() must return True.
    @property
    def finished(self) -> bool:
        return self._allocation_state.has_result()

    @classmethod
    def is_terminal_state(cls, state: AllocationState) -> bool:
        return state.HasField("result")

    def deliver_allocation_update(self, update: AllocationUpdate) -> None:
        # No need for any locks because we never block here so we hold GIL non stop.
        if update.HasField("function_call_creation_result"):
            self._server_event_queue.put(
                _ServerEventFunctionCallCreationResult(
                    result=update.function_call_creation_result,
                )
            )
        elif update.HasField("function_call_result"):
            self._server_event_queue.put(
                _ServerEventFunctionCallResult(
                    result=update.function_call_result,
                )
            )
        elif update.HasField("output_blob_deprecated") or update.HasField(
            "output_blob"
        ):
            blob: BLOB | AllocationOutputBLOB | None = None
            blob_id: str | None = None
            if update.HasField("output_blob_deprecated"):
                blob = update.output_blob_deprecated
                blob_id = blob.id
            else:
                blob = update.output_blob
                blob_id = blob.blob.id

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
        elif update.HasField("request_state_operation_result"):
            self._request_state.deliver_operation_result(
                update.request_state_operation_result
            )
        else:
            self._logger.error(
                "received unexpected allocation update",
                update=str(update),
            )

    def run_request_context_operation(
        self,
        operation: (
            PrepareWriteRequest
            | PrepareReadRequest
            | CommitWriteRequest
            | FunctionProgressUpdateRequest
        ),
    ) -> (
        PrepareWriteResponse
        | PrepareReadResponse
        | CommitWriteResponse
        | FunctionProgressUpdateResponse
    ):
        """Runs the given request context operation and returns its result.

        Blocks until the operation completes.
        Raises exception on error.
        """
        if isinstance(operation, PrepareReadRequest):
            return self._request_state.prepare_read(operation)
        elif isinstance(operation, PrepareWriteRequest):
            return self._request_state.prepare_write(operation)
        elif isinstance(operation, CommitWriteRequest):
            return self._request_state.commit_write(operation)
        elif isinstance(operation, FunctionProgressUpdateRequest):
            return self._allocation_progress.update(operation)
        else:
            raise RuntimeError(
                f"Unknown request context operation type: {type(operation)}"
            )

    def _process_event_loop_output_event_call_function(
        self, output_event: OutputEventCreateFunctionCall
    ) -> None:
        """Processes an OutputEventCreateFunctionCall: serialize, upload, create on server.

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
            # FIXME: This is not going via Server event log, so not replayable.
            # Fix this by serializing inside event loop so the exception is raised there immediately.
            self._event_loop.add_input_event(
                InputEventFunctionCallCreated(
                    durable_id=output_event.durable_id,
                    exception=e,
                )
            )
            return

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

        execution_plan_pb: ExecutionPlanUpdates = (
            output_event_to_execution_plan_updates(
                output_event=output_event,
                serialized_args=serialized_args,
                serialized_kwargs=serialized_kwargs,
                uploaded_serialized_objects=serialized_objects,
                output_serializer_name_override=(
                    self._output_value_serializer_name_override
                    if output_event.is_tail_call
                    else None
                ),
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
            )
        )
        if output_event.start_delay is not None:
            start_at: Timestamp = Timestamp()
            start_at.FromDatetime(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=output_event.start_delay)
            )
            execution_plan_pb.start_at.CopyFrom(start_at)

        alloc_function_call_id: str = _request_scoped_id()
        self._logger.info(
            "starting child future",
            future_fn_call_id=output_event.durable_id,
            future_alloc_fn_call_id=alloc_function_call_id,
        )
        self._allocation_state.add_function_call(
            id=alloc_function_call_id,
            execution_plan_updates=execution_plan_pb,
            args_blob=uploaded_args_blob,
        )

    def _process_event_loop_output_event_add_watcher(
        self, output_event: OutputEventCreateFunctionCallWatcher
    ) -> None:
        """Processes an OutputEventCreateFunctionCallWatcher: register watcher.

        Raises Exception on internal error.
        """
        function_call_watcher_id: str = _request_scoped_id()
        self._allocation_state.add_function_call_watcher(
            id=function_call_watcher_id,
            root_function_call_id=output_event.function_call_durable_id,
        )
        self._logger.info(
            "waiting for child future completion",
            future_fn_call_id=output_event.function_call_durable_id,
            future_watcher_id=function_call_watcher_id,
        )

    def _process_server_events(self) -> None:
        """Processes Server events from server event queue.

        Doesn't raise any exceptions.
        """
        while True:
            event: _ServerEvent = self._server_event_queue.get()
            try:
                if self._process_server_event(event):
                    break
            except BaseException as e:
                # NB: If an exception is raised in an event handler then we should not report
                # it back to event loop as an input event because the exception is an internal error
                # which is not replayable because it's not in the input event log. Instead, we must
                # stop the allocation immediately and set the allocation result to internal error.
                # This ensures that the allocation is not progressint past the point of internal failure
                # so when we replay it with input events it starts where it left.
                self._logger.error(
                    "Error while processing server event, sending emergency shutdown to event loop",
                    event=str(event),
                    exc_info=e,
                )
                self._event_loop.add_input_event(
                    InputEventEmergencyShutdown(internal_exception=e)
                )

        self._logger.info("stopping server event processing thread")

    def _process_server_event(self, event: _ServerEvent) -> bool:
        if isinstance(event, _ServerEventFunctionCallCreationResult):
            self._process_server_event_function_call_creation_result(event)
        elif isinstance(event, _ServerEventFunctionCallResult):
            self._process_server_event_function_call_result(event)
        elif isinstance(event, _ServerEventStopProcessingThread):
            return True
        else:
            self._logger.error(
                "received unknown server event type",
                event_type=str(type(event)),
                event=str(event),
            )
        return False

    def _process_server_event_function_call_creation_result(
        self, event: _ServerEventFunctionCallCreationResult
    ) -> None:
        """Processes function call creation result event from Server.

        Raises Exception on internal error while processing the event.
        """
        fc_creation_result: AllocationFunctionCallCreationResult = event.result
        self._allocation_state.delete_function_call(
            id=fc_creation_result.allocation_function_call_id
        )

        exception: InternalError | None = None
        if fc_creation_result.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "child future function call creation failed",
                future_fn_call_id=fc_creation_result.function_call_id,
                future_alloc_fn_call_id=fc_creation_result.allocation_function_call_id,
                status=fc_creation_result.status,
            )
            exception = InternalError("Failed to start function call")
        else:
            self._logger.info(
                "started child function call future",
                future_fn_call_id=fc_creation_result.function_call_id,
                future_alloc_fn_call_id=fc_creation_result.allocation_function_call_id,
            )

        self._event_loop.add_input_event(
            InputEventFunctionCallCreated(
                durable_id=fc_creation_result.function_call_id,
                exception=exception,
            )
        )

    def _process_server_event_function_call_result(
        self, event: _ServerEventFunctionCallResult
    ) -> None:
        """Processes function call result event from Server.

        Raises Exception on internal error while processing the event.
        """
        self._allocation_state.delete_function_call_watcher(id=event.result.watcher_id)

        output: Any = None
        exception = None
        fc_result: AllocationFunctionCallResult = event.result
        if (
            fc_result.outcome_code
            == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS
        ):
            serialized_output: SerializedValue = download_serialized_objects(
                serialized_objects=[fc_result.value_output],
                serialized_object_blobs=[fc_result.value_blob],
                blob_store=self._blob_store,
                logger=self._logger,
            )[0]
            if serialized_output.metadata is None:
                raise InternalError(
                    "Function Call output SerializedValue is missing metadata."
                )
            else:
                output = deserialize_value_with_metadata(
                    serialized_output.data, serialized_output.metadata
                )
        elif (
            fc_result.outcome_code
            == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
        ):
            if fc_result.HasField("request_error_output"):
                serialized_request_error: SerializedValue = download_serialized_objects(
                    serialized_objects=[fc_result.request_error_output],
                    serialized_object_blobs=[fc_result.request_error_blob],
                    blob_store=self._blob_store,
                    logger=self._logger,
                )[0]
                exception = RequestError(
                    message=serialized_request_error.data.decode("utf-8")
                )
            else:
                exception = InternalError("Function call failed")
        else:
            self._logger.error(
                f"Unexpected outcome code in function call result: {fc_result.outcome_code}"
            )
            raise InternalError(
                f"Unexpected outcome code in function call result: "
                f"{fc_result.outcome_code}"
            )

        self._logger.info(
            "child future completed",
            future_fn_call_id=fc_result.function_call_id,
            future_watcher_id=fc_result.watcher_id,
            success=exception is None,
        )

        self._event_loop.add_input_event(
            InputEventFunctionCallWatcherResult(
                function_call_durable_id=fc_result.function_call_id,
                output=output,
                exception=exception,
            )
        )

    def _process_event_loop_output_event_finish_allocation(
        self, output_event: OutputEventFinishAllocation
    ) -> AllocationResult:
        if output_event.internal_exception is not None:
            raise output_event.internal_exception

        if output_event.user_exception is not None:
            if isinstance(output_event.user_exception, RequestError):
                # This is user code.
                try:
                    utf8_message: bytes = output_event.user_exception.message.encode(
                        "utf-8"
                    )
                except BaseException:
                    return self._result_helper.from_user_exception(
                        self._allocation_event_details, output_event.user_exception
                    )

                # This is internal FE code.
                request_error_so, uploaded_output_blob = upload_request_error(
                    utf8_message=utf8_message,
                    destination_blob=self._allocation.inputs.request_error_blob,
                    blob_store=self._blob_store,
                    logger=self._logger,
                )
                return self._result_helper.from_request_error(
                    details=self._allocation_event_details,
                    request_error=output_event.user_exception,
                    request_error_output=request_error_so,
                    uploaded_request_error_blob=uploaded_output_blob,
                )
            else:
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, output_event.user_exception
                )

        if output_event.tail_call is not None:
            output_pb: ExecutionPlanUpdates = ExecutionPlanUpdates(
                root_function_call_id=output_event.tail_call.durable_id,
            )
            return self._result_helper.from_function_output(
                output=output_pb, uploaded_outputs_blob=None
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
            return self._result_helper.from_user_exception(
                self._allocation_event_details, e
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

        return self._result_helper.from_function_output(
            output=serialized_output, uploaded_outputs_blob=uploaded_output_blob
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
            self._allocation_function_args, self._allocation_function_kwargs
        )
        self._process_server_events_thread.start()
        # Blocks here until event loop finishes and cleans up its resources.
        alloc_result = self._process_event_loop_output_events()

        self._server_event_queue.put(_ServerEventStopProcessingThread())
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

            self._parse_output_overrides(function_call_metadata)
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
            for output_event in batch.events:
                try:
                    alloc_result: AllocationResult | None = (
                        self._process_event_loop_output_event(output_event)
                    )
                except BaseException as e:
                    # NB: If an exception is raised in an event handler then we should not report
                    # it back to event loop as an input event because the exception is an internal error
                    # which is not replayable because it's not in the input event log. Instead, we must
                    # stop the allocation immediately and set the allocation result to internal error.
                    # This ensures that the allocation is not progressint past the point of internal failure
                    # so when we replay it with input events it starts where it left.
                    self._logger.error(
                        "Error while processing event loop output event, sending emergency shutdown to event loop",
                        event=str(output_event),
                        exc_info=e,
                    )
                    self._event_loop.add_input_event(
                        InputEventEmergencyShutdown(internal_exception=e)
                    )
                    alloc_result = self._result_helper.internal_error()

                if alloc_result is not None:
                    break

            self._event_loop.join()

    def _process_event_loop_output_event(
        self, output_event: OutputEventType
    ) -> AllocationResult | None:
        if isinstance(output_event, OutputEventFinishAllocation):
            return self._process_event_loop_output_event_finish_allocation(output_event)
        elif isinstance(output_event, OutputEventCreateFunctionCall):
            self._process_event_loop_output_event_call_function(output_event)
        elif isinstance(output_event, OutputEventCreateFunctionCallWatcher):
            self._process_event_loop_output_event_add_watcher(output_event)
        else:
            self._logger.error(
                "received unknown output event from event loop",
                event_type=str(type(output_event)),
                event=str(output_event),
            )

    def _parse_output_overrides(
        self, function_call_metadata: FunctionCallMetadata | None
    ) -> None:
        """Parses output serializer/type hint overrides from function call metadata.

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
