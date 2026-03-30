from tensorlake.applications import Function
from tensorlake.applications.internal_logger import InternalLogger

from ..proto.function_executor_pb2 import (
    ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
)
from ..proto.function_executor_pb2 import BLOB as BLOBProto
from ..proto.function_executor_pb2 import (
    AllocationExecutionEventFinishAllocation,
    AllocationFailureReason,
    AllocationOutcomeCode,
    FunctionRef,
    SerializedObjectInsideBLOB,
)
from ..user_events import AllocationEventDetails, log_user_event_function_call_failed


class FinishEventHelper:
    def __init__(
        self,
        function_ref: FunctionRef,
        function: Function,
        allocation_event_details: AllocationEventDetails,
        logger: InternalLogger,
    ):
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._allocation_event_details: AllocationEventDetails = (
            allocation_event_details
        )
        self._logger: InternalLogger = logger.bind(module=__name__)

    def from_internal_error(
        self,
    ) -> AllocationExecutionEventFinishAllocation:
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
        )

    def from_replay_mismatch(
        self,
    ) -> AllocationExecutionEventFinishAllocation:
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
        )

    def from_user_exception(
        self,
        exception: BaseException,
    ) -> AllocationExecutionEventFinishAllocation:
        """Builds the execution event proto."""
        log_user_event_function_call_failed(self._allocation_event_details, exception)
        self._logger.info("function raised an exception")
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
        )

    def from_request_error(
        self,
        request_error: BaseException,
        request_error_output: SerializedObjectInsideBLOB,
        uploaded_request_error_blob: BLOBProto,
    ) -> AllocationExecutionEventFinishAllocation:
        """Builds the execution event proto."""
        log_user_event_function_call_failed(
            self._allocation_event_details, request_error
        )
        self._logger.info("function raised an exception")
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR,
            request_error_output=request_error_output,
            uploaded_request_error_blob=uploaded_request_error_blob,
        )

    def from_value_output(
        self,
        value: SerializedObjectInsideBLOB,
        uploaded_outputs_blob: BLOBProto,
    ) -> AllocationExecutionEventFinishAllocation:
        event = AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            uploaded_function_outputs_blob=uploaded_outputs_blob,
        )
        event.value.CopyFrom(value)
        return event

    def from_tail_call(
        self,
        tail_call_durable_id: str,
    ) -> AllocationExecutionEventFinishAllocation:
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            tail_call_durable_id=tail_call_durable_id,
        )
