import traceback
from typing import Any, List, Optional

from tensorlake.functions_sdk.data_objects import Metrics, TensorlakeData
from tensorlake.functions_sdk.function_errors import InvocationError
from tensorlake.functions_sdk.functions import FunctionCallResult
from tensorlake.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    JsonSerializer,
)

from ...proto.function_executor_pb2 import Metrics as MetricsProto
from ...proto.function_executor_pb2 import (
    RunTaskResponse,
    SerializedObject,
    SerializedObjectEncoding,
    TaskFailureReason,
    TaskOutcomeCode,
)


class ResponseHelper:
    """Helper class for generating RunFunctionResponse."""

    def __init__(self, task_id: str, logger: Any):
        self._task_id = task_id
        self._logger = logger.bind(module=__name__)

    def from_function_call(
        self,
        result: FunctionCallResult,
        is_reducer: bool,
        stdout: str,
        stderr: str,
    ) -> RunTaskResponse:
        if result.exception is not None:
            return self.from_function_exception(
                exception=result.exception,
                stdout=stdout,
                stderr=stderr,
                metrics=result.metrics,
            )

        return RunTaskResponse(
            task_id=self._task_id,
            function_outputs=self._to_function_outputs(result.ser_outputs),
            next_functions=[] if result.edges is None else result.edges,
            use_graph_routing=result.edges is None,
            stdout=stdout,
            stderr=stderr,
            is_reducer=is_reducer,
            metrics=self._to_metrics(result.metrics),
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
        )

    def from_function_exception(
        self, exception: Exception, stdout: str, stderr: str, metrics: Optional[Metrics]
    ) -> RunTaskResponse:
        if isinstance(exception, InvocationError):
            failure_reason: TaskFailureReason = (
                TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR
            )
            failure_message: str = exception.message
        else:
            failure_reason: TaskFailureReason = (
                TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR
            )
            failure_message: str = "".join(traceback.format_exception(exception))

        # Add the formatted exception message to stderr so customer can see it there too.
        stderr = "\n".join([stderr, failure_message])
        return RunTaskResponse(
            task_id=self._task_id,
            stdout=stdout,
            stderr=stderr,
            is_reducer=False,
            use_graph_routing=True,
            metrics=self._to_metrics(metrics),
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=failure_reason,
            failure_message=failure_message,
        )

    def _to_function_outputs(
        self, tl_datas: List[TensorlakeData]
    ) -> List[SerializedObject]:
        outputs: List[SerializedObject] = []
        for tl_data in tl_datas:
            data: bytes = None
            encoding: SerializedObjectEncoding = None
            encoding_version: int = 1
            if tl_data.encoder == JsonSerializer.encoding_type:
                data = tl_data.payload.encode("utf-8")
                encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
            elif tl_data.encoder == CloudPickleSerializer.encoding_type:
                data = tl_data.payload
                encoding = (
                    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
                )
            else:
                self._logger.warning(
                    "Unsupported encoder type",
                    encoder=tl_data.encoder,
                    payload_type=type(tl_data.payload),
                )
                continue

            outputs.append(
                SerializedObject(
                    data=data,
                    encoding=encoding,
                    encoding_version=encoding_version,
                )
            )
        return outputs

    def _to_metrics(self, metrics: Optional[Metrics]) -> Optional[MetricsProto]:
        if metrics is None:
            return None
        return MetricsProto(
            timers=metrics.timers,
            counters=metrics.counters,
        )
