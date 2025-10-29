import traceback

from tensorlake.applications import Function, RequestError
from tensorlake.applications.request_context.request_metrics_recorder import (
    RequestMetricsRecorder,
)

from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import (
    BLOB,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    ExecutionPlanUpdates,
    FunctionRef,
    Metrics,
    SerializedObjectInsideBLOB,
)


class ResultHelper:
    def __init__(
        self,
        function_ref: FunctionRef,
        function: Function,
        metrics: RequestMetricsRecorder,
        logger: FunctionExecutorLogger,
    ):
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._request_metrics = metrics
        self._logger: FunctionExecutorLogger = logger.bind(module=__name__)

    def internal_error(self) -> AllocationResult:
        """Creates an AllocationResult representing an internal error in Function Executor code."""
        # The error is logged outside of this method.
        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
            metrics=self._generate_metrics_proto(),
        )

    def from_user_exception(self, exception: BaseException) -> AllocationResult:
        """Creates an AllocationResult representing a user exception raised during function execution."""
        try:
            # This is user code.
            # Give the full traceback to the user for debugging. Flush to make sure user sees it.
            print("".join(traceback.format_exception(exception)), flush=True)
        except BaseException as e:
            print("Failed to print exception traceback: ", str(e), flush=True)
            print("Original exception: ", str(exception), flush=True)

        # This is FE internal code.
        # Don't log the user exception as it might contain customer data.
        self._logger.info("function raised an exception")

        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
            metrics=self._generate_metrics_proto(),
        )

    def from_request_error(
        self,
        request_error: RequestError,
        request_error_output: SerializedObjectInsideBLOB,
        uploaded_request_error_blob: BLOB,
    ) -> AllocationResult:
        """Creates an AllocationResult representing a request error."""
        try:
            # This is user code.
            # Give the full traceback to the user for debugging. Flush to make sure user sees it.
            print("".join(traceback.format_exception(request_error)), flush=True)
        except BaseException as e:
            print("Failed to print request error traceback: ", str(e))
            print("Original request error: ", str(request_error))

        # This is FE internal code.
        # Don't log the user exception as it might contain customer data.
        self._logger.info("function raised a request error")

        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR,
            request_error_output=request_error_output,
            uploaded_request_error_blob=uploaded_request_error_blob,
            metrics=self._generate_metrics_proto(),
        )

    def from_function_output(
        self,
        output: SerializedObjectInsideBLOB | ExecutionPlanUpdates,
        uploaded_outputs_blob: BLOB,
    ) -> AllocationResult:
        result = AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            uploaded_function_outputs_blob=uploaded_outputs_blob,
            metrics=self._generate_metrics_proto(),
        )

        if isinstance(output, SerializedObjectInsideBLOB):
            result.value.CopyFrom(output)
        else:
            result.updates.CopyFrom(output)

        return result

    def _generate_metrics_proto(self) -> Metrics:
        return Metrics(
            timers=self._request_metrics.timers,
            counters=self._request_metrics.counters,
        )
