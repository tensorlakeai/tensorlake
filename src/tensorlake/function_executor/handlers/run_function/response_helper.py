import hashlib
import time
import traceback
from typing import List, Optional, Tuple

from tensorlake.functions_sdk.data_objects import Metrics, TensorlakeData
from tensorlake.functions_sdk.exceptions import RequestException
from tensorlake.functions_sdk.functions import FunctionCallResult
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    JsonSerializer,
)

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import (
    BLOB,
    FunctionInputs,
)
from ...proto.function_executor_pb2 import Metrics as MetricsProto
from ...proto.function_executor_pb2 import (
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
    TaskDiagnostics,
    TaskFailureReason,
    TaskOutcomeCode,
    TaskResult,
)


class ResponseHelper:
    """Helper class for generating TaskResult."""

    def __init__(
        self,
        function_name: str,
        inputs: FunctionInputs,
        graph_metadata: ComputeGraphMetadata,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._function_name: str = function_name
        self._inputs: FunctionInputs = inputs
        self._graph_metadata: ComputeGraphMetadata = graph_metadata
        self._blob_store: BLOBStore = blob_store
        self._logger: FunctionExecutorLogger = logger.bind(module=__name__)

    def from_function_call(
        self,
        result: FunctionCallResult,
        fe_log_start: int,
    ) -> TaskResult:
        if result.exception is not None:
            return self.from_function_exception(
                exception=result.exception,
                metrics=result.metrics,
                fe_log_start=fe_log_start,
            )

        if result.edges is None:
            # Fallback to the graph edges if not provided by the function.
            # Some functions don't have any outer edges.
            next_functions = self._graph_metadata.edges.get(self._function_name, [])
        else:
            next_functions = result.edges

        function_outputs, uploaded_function_outputs_blob = (
            self._upload_function_outputs(result.ser_outputs)
        )

        return TaskResult(
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
            function_outputs=function_outputs,
            uploaded_function_outputs_blob=uploaded_function_outputs_blob,
            next_functions=next_functions,
            metrics=_to_metrics(result.metrics),
            diagnostics=TaskDiagnostics(
                function_executor_log=self._logger.read_till_the_end(
                    start=fe_log_start
                ),
            ),
        )

    def from_function_exception(
        self, exception: Exception, fe_log_start: int, metrics: Optional[Metrics]
    ) -> TaskResult:
        # Print the exception to stderr so customer can see it there.
        traceback.print_exception(exception)

        invocation_error_output: Optional[SerializedObjectInsideBLOB] = None
        uploaded_invocation_error_blob: Optional[BLOB] = None
        if isinstance(exception, RequestException):
            failure_reason: TaskFailureReason = (
                TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR
            )
            invocation_error_output, uploaded_invocation_error_blob = (
                self._upload_invocation_error_output(exception.message)
            )
        else:
            failure_reason: TaskFailureReason = (
                TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR
            )

        return TaskResult(
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=failure_reason,
            invocation_error_output=invocation_error_output,
            uploaded_invocation_error_blob=uploaded_invocation_error_blob,
            next_functions=[],
            metrics=_to_metrics(metrics),
            diagnostics=TaskDiagnostics(
                function_executor_log=self._logger.read_till_the_end(
                    start=fe_log_start
                ),
            ),
        )

    def _upload_function_outputs(
        self, tl_datas: List[TensorlakeData]
    ) -> Tuple[List[SerializedObjectInsideBLOB], BLOB]:
        serialized_objects: List[SerializedObjectInsideBLOB] = []
        serialized_datas: List[bytes] = []

        blob_offset: int = 0
        for tl_data in tl_datas:
            serialized_data: bytes = None
            encoding: SerializedObjectEncoding = None
            encoding_version: int = 0
            if tl_data.encoder == JsonSerializer.encoding_type:
                serialized_data = tl_data.payload.encode("utf-8")
                encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
            elif tl_data.encoder == CloudPickleSerializer.encoding_type:
                serialized_data = tl_data.payload
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

            serialized_objects.append(
                SerializedObjectInsideBLOB(
                    manifest=SerializedObjectManifest(
                        encoding=encoding,
                        encoding_version=encoding_version,
                        size=len(serialized_data),
                        sha256_hash=_sha256_hexdigest(serialized_data),
                    ),
                    offset=blob_offset,
                )
            )
            serialized_datas.append(serialized_data)
            blob_offset += len(serialized_data)

        serialized_datas_size: int = sum(
            len(serialized_data) for serialized_data in serialized_datas
        )
        start_time = time.monotonic()
        self._logger.info(
            "uploading function output",
            outputs_count=len(serialized_datas),
            total_size=serialized_datas_size,
        )
        uploaded_blob: BLOB = _upload_outputs(
            serialized_datas,
            self._inputs.function_outputs_blob,
            self._blob_store,
            self._logger,
        )
        self._logger.info(
            "function output uploaded",
            outputs_count=len(serialized_datas),
            total_size=serialized_datas_size,
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )

        return (serialized_objects, uploaded_blob)

    def _upload_invocation_error_output(
        self, message: str
    ) -> Tuple[SerializedObjectInsideBLOB, BLOB]:
        data: bytes = message.encode("utf-8")
        start_time = time.monotonic()
        self._logger.info(
            "uploading invocation error output",
            size=len(data),
        )
        uploaded_blob: BLOB = _upload_outputs(
            [data],
            self._inputs.invocation_error_blob,
            self._blob_store,
            self._logger,
        )
        self._logger.info(
            "invocation error output uploaded",
            size=len(data),
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )

        return (
            SerializedObjectInsideBLOB(
                manifest=SerializedObjectManifest(
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT,
                    encoding_version=0,
                    size=len(data),
                    sha256_hash=_sha256_hexdigest(data),
                ),
                offset=0,
            ),
            uploaded_blob,
        )


def _upload_outputs(
    outputs: List[bytes],
    destination_blob: BLOB,
    blob_store: BLOBStore,
    logger: FunctionExecutorLogger,
) -> BLOB:
    """Uploads outputs to the blob and returns it with the updated chunks."""
    outputs_size: int = sum(len(output) for output in outputs)
    blob_size: int = sum(chunk.size for chunk in destination_blob.chunks)
    if outputs_size > blob_size:
        # Let customers know why the function failed while still treating it as internal error
        # because BLOB size is controlled by Executor.
        print(
            f"Function output size {outputs_size} exceeds the max size of {blob_size}.\n"
            "Please contact Tensorlake support to resolve this issue.",
            flush=True,
        )
        raise ValueError(
            f"Function output size {outputs_size} exceeds the total size of BLOB {blob_size}."
        )

    return blob_store.put(
        blob=destination_blob,
        data=outputs,
        logger=logger,
    )


def _to_metrics(metrics: Optional[Metrics]) -> Optional[MetricsProto]:
    if metrics is None:
        return None
    return MetricsProto(
        timers=metrics.timers,
        counters=metrics.counters,
    )


def _sha256_hexdigest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
