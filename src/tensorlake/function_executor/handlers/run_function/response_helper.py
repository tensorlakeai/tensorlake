import hashlib
import time
import traceback
from typing import Any, List, Tuple

from tensorlake.workflows.ast.ast import ASTNode, ast_from_user_object
from tensorlake.workflows.ast.value_node import ValueNode
from tensorlake.workflows.function.user_data_serializer import (
    function_output_serializer,
)
from tensorlake.workflows.interface.exceptions import RequestException
from tensorlake.workflows.interface.function import Function
from tensorlake.workflows.request_state_base import RequestStateBase

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import (
    BLOB,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    FunctionInputs,
)
from ...proto.function_executor_pb2 import Metrics as MetricsProto
from ...proto.function_executor_pb2 import (
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)


class ResponseHelper:
    """Helper class for generating AllocationResult."""

    def __init__(
        self,
        function: Function,
        inputs: FunctionInputs,
        request_state: RequestStateBase,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._function: Function = function
        self._inputs: FunctionInputs = inputs
        self._request_state: RequestStateBase = request_state
        self._blob_store: BLOBStore = blob_store
        self._logger: FunctionExecutorLogger = logger.bind(module=__name__)

    def from_function_output(
        self,
        output: Any,
    ) -> AllocationResult:
        output_ast: ASTNode = ast_from_user_object(
            output, function_output_serializer(self._function)
        )

        if isinstance(output_ast, ValueNode):
            self._upload_function_output_value(output_ast)
        else:
            pass
            # TODO: Walk the output_ast tree and for each ValueNode
            # upload it to BLOB store and then remember its serialized objects.
            #
            # Then flatten the tree and convert it into proto tree.

        # function_outputs, uploaded_function_outputs_blob = (
        #     self._upload_function_outputs(result.ser_outputs)
        # )

        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            # TODO: set either value or updates field.
            metrics=self._get_metrics(),
        )

    def from_function_exception(self, exception: Exception) -> AllocationResult:
        # Print the exception to stderr so customer can see it there.
        traceback.print_exception(exception)

        request_error_output: SerializedObjectInsideBLOB | None = None
        uploaded_request_error_blob: BLOB | None = None
        if isinstance(exception, RequestException):
            failure_reason: AllocationFailureReason = (
                AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR
            )
            request_error_output, uploaded_request_error_blob = (
                self._upload_request_error_output(exception.message)
            )
        else:
            failure_reason: AllocationFailureReason = (
                AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR
            )

        return AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=failure_reason,
            request_error_output=request_error_output,
            uploaded_request_error_blob=uploaded_request_error_blob,
            metrics=self._get_metrics(),
        )

    def _get_metrics(self) -> MetricsProto:
        return MetricsProto(
            timers=self._request_state.timers,
            counters=self._request_state.counters,
        )

    def _upload_function_output_value(
        self, value_node: ValueNode
    ) -> Tuple[List[SerializedObjectInsideBLOB], BLOB]:
        serialized_objects: List[SerializedObjectInsideBLOB] = []
        serialized_datas: List[bytes] = []

        # TODO: Use deserialized value node metadata to figure out encoding.
        # TODO: Store serialized metadata in front of the serialized object.
        blob_offset: int = 0
        encoding_version: int = 0
        serialized_data: bytes = value_node.val

        serialized_objects.append(
            SerializedObjectInsideBLOB(
                manifest=SerializedObjectManifest(
                    encoding=value_node.metadata.serializer,  # Convert to encoding.
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

    def _upload_request_error_output(
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
            self._inputs.request_error_blob,
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


def _sha256_hexdigest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
