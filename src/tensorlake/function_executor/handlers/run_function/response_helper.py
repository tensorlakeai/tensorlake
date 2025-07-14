import time
import traceback
from typing import Any, List, Optional

from tensorlake.functions_sdk.data_objects import Metrics, TensorlakeData
from tensorlake.functions_sdk.function_errors import InvocationError
from tensorlake.functions_sdk.functions import FunctionCallResult
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    JsonSerializer,
)

from ...blob_store.blob_store import BLOBStore
from ...proto.function_executor_pb2 import Metrics as MetricsProto
from ...proto.function_executor_pb2 import (
    RunTaskRequest,
    RunTaskResponse,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
    TaskFailureReason,
    TaskOutcomeCode,
)


class ResponseHelper:
    """Helper class for uploading function outputs and generating RunFunctionResponse."""

    def __init__(
        self,
        request: RunTaskRequest,
        graph_metadata: ComputeGraphMetadata,
        blob_store: BLOBStore,
        logger: Any,
    ):
        self._request: RunTaskRequest = request
        self._graph_metadata: ComputeGraphMetadata = graph_metadata
        self._blob_store: BLOBStore = blob_store
        self._logger: Any = logger.bind(module=__name__)

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

        if result.edges is None:
            # Fallback to the graph edges if not provided by the function.
            # Some functions don't have any outer edges.
            next_functions = self._graph_metadata.edges.get(
                self._request.function_name, []
            )
        else:
            next_functions = result.edges

        self._upload_function_stdout(stdout)
        self._upload_function_stderr(stderr)

        return RunTaskResponse(
            task_id=self._request.task_id,
            function_outputs=self._upload_function_outputs(result.ser_outputs),
            next_functions=next_functions,
            is_reducer=is_reducer,
            metrics=self._to_metrics(result.metrics),
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
        )

    def from_function_exception(
        self, exception: Exception, stdout: str, stderr: str, metrics: Optional[Metrics]
    ) -> RunTaskResponse:
        invocation_error_output: Optional[SerializedObjectInsideBLOB] = None
        if isinstance(exception, InvocationError):
            failure_reason: TaskFailureReason = (
                TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR
            )
            invocation_error_output = self._upload_invocation_error_output(
                exception.message
            )
        else:
            failure_reason: TaskFailureReason = (
                TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR
            )
            # Add the formatted exception message to stderr so customer can see it there.
            formatted_exception: str = "".join(traceback.format_exception(exception))
            stderr = "\n".join([stderr, formatted_exception])

        self._upload_function_stdout(stdout)
        self._upload_function_stderr(stderr)

        return RunTaskResponse(
            task_id=self._request.task_id,
            is_reducer=False,
            next_functions=[],
            metrics=self._to_metrics(metrics),
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=failure_reason,
            invocation_error_output=invocation_error_output,
        )

    def _upload_function_outputs(
        self, tl_datas: List[TensorlakeData]
    ) -> List[SerializedObjectInsideBLOB]:
        blob_offset: int = 0
        outputs: List[SerializedObjectInsideBLOB] = []

        for tl_data in tl_datas:
            data: bytes = None
            encoding: SerializedObjectEncoding = None
            encoding_version: int = 0
            if tl_data.encoder == JsonSerializer.encoding_type:
                data = tl_data.payload.encode("utf-8")
                encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
            elif tl_data.encoder == CloudPickleSerializer.encoding_type:
                data = tl_data.payload
                encoding = (
                    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
                )
            else:
                self._logger.error(
                    "Unsupported encoder type",
                    encoder=tl_data.encoder,
                    payload_type=type(tl_data.payload),
                )
                continue

            start_time = time.monotonic()
            self._logger.info(
                "uploading function output",
                offset=blob_offset,
                size=len(data),
            )
            self._blob_store.put(
                uri=self._request.function_outputs.uri,
                offset=blob_offset,
                data=data,
                logger=self._logger,
            )
            self._logger.info(
                "function output uploaded",
                offset=blob_offset,
                size=len(data),
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )

            outputs.append(
                SerializedObjectInsideBLOB(
                    manifest=SerializedObjectManifest(
                        encoding=encoding,
                        encoding_version=encoding_version,
                        size=len(data),
                    ),
                    blob=self._request.function_outputs,
                    offset=blob_offset,
                )
            )

            blob_offset += len(data)

        return outputs

    def _upload_function_stdout(self, stdout: str) -> None:
        data: bytes = stdout.encode("utf-8")
        start_time = time.monotonic()
        self._logger.info(
            "uploading function stdout",
            size=len(data),
        )
        self._blob_store.put(
            uri=self._request.stdout.uri,
            offset=0,
            data=data,
            logger=self._logger,
        )
        self._logger.info(
            "function stdout uploaded",
            size=len(data),
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )

    def _upload_function_stderr(self, stderr: str) -> None:
        data: bytes = stderr.encode("utf-8")
        start_time = time.monotonic()
        self._logger.info(
            "uploading function stderr",
            size=len(data),
        )
        self._blob_store.put(
            uri=self._request.stderr.uri,
            offset=0,
            data=data,
            logger=self._logger,
        )
        self._logger.info(
            "function stderr uploaded",
            size=len(data),
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )

    def _upload_invocation_error_output(
        self, message: str
    ) -> SerializedObjectInsideBLOB:
        data: bytes = message.encode("utf-8")
        start_time = time.monotonic()
        self._logger.info(
            "uploading invocation error output",
            size=len(data),
        )
        # There are no function outputs for invocation errors so we can just write at 0 offset.
        self._blob_store.put(
            uri=self._request.function_outputs.uri,
            offset=0,
            data=data,
            logger=self._logger,
        )
        self._logger.info(
            "invocation error output uploaded",
            size=len(data),
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )

        return SerializedObjectInsideBLOB(
            manifest=SerializedObjectManifest(
                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT,
                encoding_version=0,
                size=len(data),
            ),
            blob=self._request.function_outputs,
            offset=0,
        )

    def _to_metrics(self, metrics: Optional[Metrics]) -> Optional[MetricsProto]:
        if metrics is None:
            return None
        return MetricsProto(
            timers=metrics.timers,
            counters=metrics.counters,
        )
