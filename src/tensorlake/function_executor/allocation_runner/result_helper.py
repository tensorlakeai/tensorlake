from tensorlake.applications import Function, RequestError
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.function_executor.user_events import (
    AllocationEventDetails,
    log_user_event_function_call_failed,
)

from ..proto.function_executor_pb2 import BLOB as BLOBProto
from ..proto.function_executor_pb2 import (
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
        output: SerializedObjectInsideBLOB | ExecutionPlanUpdates,
        uploaded_outputs_blob: BLOBProto | None,
    ) -> AllocationResult:
        result = AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            uploaded_function_outputs_blob=uploaded_outputs_blob,
        )

        if isinstance(output, SerializedObjectInsideBLOB):
            result.value.CopyFrom(output)
        else:
            result.updates.CopyFrom(output)

        return result
