from tensorlake.applications.request_context.request_metrics_recorder import (
    RequestMetricsRecorder,
)

from ..proto.function_executor_pb2 import (
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    Metrics,
)


class ResultHelper:
    def __init__(self, metrics: RequestMetricsRecorder):
        self._request_metrics = metrics

    def internal_error_result(self) -> AllocationResult:
        """Creates an AllocationResult representing an internal error in Function Executor code."""
        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
            metrics=self._generate_metrics_proto(),
        )

    def _generate_metrics_proto(self) -> Metrics:
        return Metrics(
            timers=self._request_metrics.timers,
            counters=self._request_metrics.counters,
        )
