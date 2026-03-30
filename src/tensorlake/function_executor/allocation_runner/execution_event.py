import datetime
import time
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from tensorlake.applications import (
    Function,
    InternalError,
    RequestError,
    SerializationError,
)
from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.function.user_data_serializer import (
    function_output_serializer,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.registry import get_function
from tensorlake.applications.user_data_serializer import UserDataSerializer

from ..proto.function_executor_pb2 import (
    BLOB,
    AllocationExecutionEvent,
    AllocationExecutionEventCreateFunctionCall,
    AllocationExecutionEventCreateFunctionCallWatcher,
    AllocationExecutionEventFunctionCallCreationFailed,
    ExecutionPlanUpdates,
    FunctionRef,
    SerializedObjectInsideBLOB,
)
from .blob_manager import AllocationBLOBManager
from .event_loop import (
    OutputEventCreateFunctionCall,
    OutputEventCreateFunctionCallWatcher,
    OutputEventFinishAllocation,
    OutputEventType,
)
from .exception_helper import serialize_user_exception
from .finish_event_helper import FinishEventHelper
from .sdk_algorithms import (
    output_event_to_execution_plan_updates,
    serialize_output_event_args,
    serialize_user_value,
)
from .upload import (
    serialized_values_to_serialized_objects,
    upload_request_error,
    upload_serialized_objects_to_blob,
)
from .value import SerializedValue


class EventLoopOutputEventConverter:
    """Converts event loop output events to execution event protos.

    Holds all the dependencies needed for conversion so that the per-event
    call site is just ``to_execution_event(output_event)``.
    """

    def __init__(
        self,
        finish_event_helper: FinishEventHelper,
        request_error_blob: BLOB,
        blob_store: BLOBStore,
        function: Function,
        function_ref: FunctionRef,
        blob_manager: AllocationBLOBManager,
        logger: InternalLogger,
    ) -> None:
        self._finish_event_helper: FinishEventHelper = finish_event_helper
        self._request_error_blob: BLOB = request_error_blob
        self._blob_store: BLOBStore = blob_store
        self._function: Function = function
        self._function_ref: FunctionRef = function_ref
        self._blob_manager: AllocationBLOBManager = blob_manager
        self._logger: InternalLogger = logger

        # Set later via set_output_overrides() after function call metadata is parsed.
        self._output_value_serializer_name_override: str | None = None
        self._has_output_value_type_hint_override: bool = False
        self._output_value_type_hint_override: Any = None

    def set_output_overrides(
        self,
        serializer_name: str | None,
        has_type_hint: bool,
        type_hint: Any,
    ) -> None:
        """Sets the output serializer/type hint overrides from function call metadata."""
        self._output_value_serializer_name_override = serializer_name
        self._has_output_value_type_hint_override = has_type_hint
        self._output_value_type_hint_override = type_hint

    def to_execution_event(
        self, output_event: OutputEventType
    ) -> AllocationExecutionEvent:
        """Converts a single event loop output event to an execution event proto.

        Raises Exception on internal error.
        """
        if isinstance(output_event, OutputEventFinishAllocation):
            return self._convert_finish_allocation(output_event)
        elif isinstance(output_event, OutputEventCreateFunctionCall):
            return self._convert_create_function_call(output_event)
        elif isinstance(output_event, OutputEventCreateFunctionCallWatcher):
            return self._convert_create_watcher(output_event)
        else:
            raise InternalError(f"Unknown output event type: {type(output_event)}")

    def _convert_finish_allocation(
        self, output_event: OutputEventFinishAllocation
    ) -> AllocationExecutionEvent:
        """Converts OutputEventFinishAllocation to an execution event.

        Raises Exception on internal error while processing the event.
        """
        if output_event.internal_exception is not None:
            self._logger.error(
                "allocation finished with internal error",
                exc_info=output_event.internal_exception,
            )
            return AllocationExecutionEvent(
                finish_allocation=self._finish_event_helper.from_internal_error()
            )

        if output_event.user_exception is not None:
            if isinstance(output_event.user_exception, RequestError):
                # This is user code.
                try:
                    utf8_message: bytes = output_event.user_exception.message.encode(
                        "utf-8"
                    )
                except BaseException:
                    return AllocationExecutionEvent(
                        finish_allocation=self._finish_event_helper.from_user_exception(
                            output_event.user_exception,
                        )
                    )

                # This is internal FE code.
                request_error_so, uploaded_output_blob = upload_request_error(
                    utf8_message=utf8_message,
                    destination_blob=self._request_error_blob,
                    blob_store=self._blob_store,
                    logger=self._logger,
                )
                return AllocationExecutionEvent(
                    finish_allocation=self._finish_event_helper.from_request_error(
                        request_error=output_event.user_exception,
                        request_error_output=request_error_so,
                        uploaded_request_error_blob=uploaded_output_blob,
                    )
                )
            else:
                return AllocationExecutionEvent(
                    finish_allocation=self._finish_event_helper.from_user_exception(
                        output_event.user_exception,
                    )
                )

        if output_event.tail_call is not None:
            return AllocationExecutionEvent(
                finish_allocation=self._finish_event_helper.from_tail_call(
                    tail_call_durable_id=output_event.tail_call.durable_id,
                )
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
            return AllocationExecutionEvent(
                finish_allocation=self._finish_event_helper.from_user_exception(e),
            )

        # This is internal FE code.
        serialized_objects, blob_data = serialized_values_to_serialized_objects(
            serialized_values={
                serialized_output_value.metadata.id: serialized_output_value
            }
        )
        serialized_output = serialized_objects[serialized_output_value.metadata.id]
        outputs_blob: BLOB = self._blob_manager.get_new_output_blob(
            size=sum(len(data) for data in blob_data)
        )
        uploaded_output_blob = upload_serialized_objects_to_blob(
            serialized_objects=serialized_objects,
            blob_data=blob_data,
            destination_blob=outputs_blob,
            blob_store=self._blob_store,
            logger=self._logger,
        )

        return AllocationExecutionEvent(
            finish_allocation=self._finish_event_helper.from_value_output(
                value=serialized_output,
                uploaded_outputs_blob=uploaded_output_blob,
            )
        )

    def _convert_create_function_call(
        self, output_event: OutputEventCreateFunctionCall
    ) -> AllocationExecutionEvent:
        """Converts OutputEventCreateFunctionCall to an execution event.

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
            return AllocationExecutionEvent(
                function_call_creation_failed=AllocationExecutionEventFunctionCallCreationFailed(
                    function_call_id=output_event.durable_id,
                    metadata=serialize_user_exception(e),
                )
            )

        # This is our code.
        serialized_objects: dict[str, SerializedObjectInsideBLOB] = {}
        uploaded_args_blob: BLOB | None = None
        if len(serialized_values) > 0:
            serialized_objects, blob_data = serialized_values_to_serialized_objects(
                serialized_values=serialized_values
            )
            args_blob: BLOB = self._blob_manager.get_new_output_blob(
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
            splitter_function = get_function(
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

    def _convert_create_watcher(
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
