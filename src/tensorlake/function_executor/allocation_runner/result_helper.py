from tensorlake.applications import Function
from tensorlake.applications.internal_logger import InternalLogger

from ..proto.function_executor_pb2 import BLOB as BLOBProto
from ..proto.function_executor_pb2 import (
    AllocationExecutionEventFinishAllocation,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    FunctionRef,
    SerializedObjectInsideBLOB,
)


class ResultHelper:
    def __init__(
        self,
        function_ref: FunctionRef,
        function: Function,
        logger: InternalLogger,
    ):
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._logger: InternalLogger = logger.bind(module=__name__)

    def internal_error(self) -> AllocationResult:
        """Creates an AllocationResult representing an internal error in Function Executor code."""
        # The error is logged outside of this method.
        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
        )

    def result_from_finish_event(
        self, event: AllocationExecutionEventFinishAllocation
    ) -> AllocationResult:
        """Converts an AllocationExecutionEventFinishAllocation to an AllocationResult."""
        result = AllocationResult(
            outcome_code=event.outcome_code,
        )
        if event.HasField("failure_reason"):
            result.failure_reason = event.failure_reason
        if event.HasField("value"):
            result.value.CopyFrom(event.value)
        if event.HasField("tail_call_durable_id"):
            result.tail_function_call_id = event.tail_call_durable_id
        if event.HasField("uploaded_function_outputs_blob"):
            result.uploaded_function_outputs_blob.CopyFrom(
                event.uploaded_function_outputs_blob
            )
        if event.HasField("request_error_output"):
            result.request_error_output.CopyFrom(event.request_error_output)
        if event.HasField("uploaded_request_error_blob"):
            result.uploaded_request_error_blob.CopyFrom(
                event.uploaded_request_error_blob
            )
        return result

    def to_finish_event_internal_error(
        self,
    ) -> AllocationExecutionEventFinishAllocation:
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
        )

    def to_finish_event_from_user_exception(
        self,
    ) -> AllocationExecutionEventFinishAllocation:
        """Builds the execution event proto. Does not log."""
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
        )

    def to_finish_event_from_request_error(
        self,
        request_error_output: SerializedObjectInsideBLOB,
        uploaded_request_error_blob: BLOBProto,
    ) -> AllocationExecutionEventFinishAllocation:
        """Builds the execution event proto. Does not log."""
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR,
            request_error_output=request_error_output,
            uploaded_request_error_blob=uploaded_request_error_blob,
        )

    def to_finish_event_from_function_output(
        self,
        output: SerializedObjectInsideBLOB | str,
        uploaded_outputs_blob: BLOBProto | None,
    ) -> AllocationExecutionEventFinishAllocation:
        event = AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            uploaded_function_outputs_blob=uploaded_outputs_blob,
        )
        if isinstance(output, SerializedObjectInsideBLOB):
            event.value.CopyFrom(output)
        else:
            event.tail_call_durable_id = output
        return event
