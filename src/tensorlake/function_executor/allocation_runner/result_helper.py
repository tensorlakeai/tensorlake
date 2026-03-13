from tensorlake.applications import Function, RequestError
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.function_executor.user_events import (
    AllocationEventDetails,
    log_user_event_function_call_failed,
)

from ..proto.function_executor_pb2 import BLOB as BLOBProto
from ..proto.function_executor_pb2 import (
    AllocationExecutionEventFinishAllocation,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    ExecutionPlanUpdates,
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

    def from_user_exception(
        self, details: AllocationEventDetails, exception: BaseException
    ) -> AllocationResult:
        """Creates an AllocationResult representing a user exception raised during function execution."""
        # Give the full traceback + alloc metadata to the user for debugging.
        log_user_event_function_call_failed(details, exception)

        # This is FE internal code.
        # Don't log the user exception as it might contain customer data.
        self._logger.info("function raised an exception")

        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
        )

    def from_request_error(
        self,
        details: AllocationEventDetails,
        request_error: RequestError,
        request_error_output: SerializedObjectInsideBLOB,
        uploaded_request_error_blob: BLOBProto,
    ) -> AllocationResult:
        """Creates an AllocationResult representing a request error."""
        # Give the full traceback + alloc metadata to the user for debugging.
        log_user_event_function_call_failed(details, request_error)

        # This is FE internal code.
        # Don't log the user exception as it might contain customer data.
        self._logger.info("function raised a request error")

        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR,
            request_error_output=request_error_output,
            uploaded_request_error_blob=uploaded_request_error_blob,
        )

    def from_function_output(
        self,
        output: SerializedObjectInsideBLOB | str,
        uploaded_outputs_blob: BLOBProto | None,
    ) -> AllocationResult:
        """Creates an AllocationResult representing a successful function execution with the given output.

        If output is SerializedObjectInsideBLOB, it's set in the value field of the result. If output is a string,
        it's set as the tail_function_call_id of the result.
        """
        result = AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            uploaded_function_outputs_blob=uploaded_outputs_blob,
        )

        if isinstance(output, SerializedObjectInsideBLOB):
            result.value.CopyFrom(output)
        else:
            result.tail_function_call_id = output
            # Deprecated
            result.updates.CopyFrom(
                ExecutionPlanUpdates(
                    root_function_call_id=output,
                )
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
        """Builds the execution event proto. Does not log — caller must also call from_user_exception."""
        return AllocationExecutionEventFinishAllocation(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
        )

    def to_finish_event_from_request_error(
        self,
        request_error_output: SerializedObjectInsideBLOB,
        uploaded_request_error_blob: BLOBProto,
    ) -> AllocationExecutionEventFinishAllocation:
        """Builds the execution event proto. Does not log — caller must also call from_request_error."""
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
