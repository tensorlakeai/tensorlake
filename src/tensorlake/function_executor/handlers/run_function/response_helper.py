import hashlib
import time
import traceback
from typing import Any, Dict, List, Tuple

from tensorlake.applications.ast import (
    ASTNode,
    ValueMetadata,
    ast_from_user_object,
    flatten_ast,
    override_output_serializer_at_child_call_tree_root,
    traverse_ast,
)
from tensorlake.applications.ast.function_call_node import (
    RegularFunctionCallNode,
)
from tensorlake.applications.ast.reducer_call_node import ReducerFunctionCallNode
from tensorlake.applications.ast.value_node import ValueNode
from tensorlake.applications.function.user_data_serializer import (
    function_output_serializer,
)
from tensorlake.applications.interface.exceptions import RequestError
from tensorlake.applications.interface.function import Function
from tensorlake.applications.request_context.request_metrics_recorder import (
    RequestMetricsRecorder,
)
from tensorlake.applications.user_data_serializer import (
    UserDataSerializer,
    serializer_by_name,
)

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import (
    BLOB,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    ExecutionPlanUpdate,
    ExecutionPlanUpdates,
    FunctionArg,
    FunctionCall,
    FunctionInputs,
    FunctionRef,
)
from ...proto.function_executor_pb2 import Metrics as MetricsProto
from ...proto.function_executor_pb2 import (
    ReduceOp,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from .function_call_node_metadata import FunctionCallNodeMetadata, FunctionCallType
from .value_node_metadata import ValueNodeMetadata


class ResponseHelper:
    """Helper class for generating AllocationResult."""

    def __init__(
        self,
        function_ref: FunctionRef,
        function: Function,
        inputs: FunctionInputs,
        request_metrics: RequestMetricsRecorder,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._inputs: FunctionInputs = inputs
        self._request_metrics: RequestMetricsRecorder = request_metrics
        self._blob_store: BLOBStore = blob_store
        self._logger: FunctionExecutorLogger = logger.bind(module=__name__)

    def from_function_output(
        self,
        output: Any,
        output_serializer_override: str | None,
    ) -> AllocationResult:
        output_serializer: UserDataSerializer = function_output_serializer(
            self._function, output_serializer_override
        )
        output_ast: ASTNode = ast_from_user_object(output, output_serializer)

        value: SerializedObjectInsideBLOB | None = None
        updates: ExecutionPlanUpdates | None = None
        uploaded_function_outputs_blob: BLOB

        if isinstance(output_ast, ValueNode):
            uploaded_sos, uploaded_function_outputs_blob = (
                self._upload_function_output_values([output_ast])
            )
            value = uploaded_sos[output_ast.id]
            value.manifest.source_function_call_id = output_ast.id
        else:
            override_output_serializer_at_child_call_tree_root(
                function_output_serializer_name=output_serializer.name,
                function_output_ast=output_ast,
            )
            updates, uploaded_function_outputs_blob = self._upload_function_output_ast(
                output_ast
            )

        result = AllocationResult(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            uploaded_function_outputs_blob=uploaded_function_outputs_blob,
            metrics=self._get_metrics(),
        )

        if updates is None:
            result.value.CopyFrom(value)
        else:
            result.updates.CopyFrom(updates)

        return result

    def from_function_exception(self, exception: Exception) -> AllocationResult:
        # Print the exception to stderr so customer can see it there.
        traceback.print_exception(exception)

        request_error_output: SerializedObjectInsideBLOB | None = None
        uploaded_request_error_blob: BLOB | None = None
        if isinstance(exception, RequestError):
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
            timers=self._request_metrics.timers,
            counters=self._request_metrics.counters,
        )

    def _upload_function_output_ast(
        self, root: ASTNode
    ) -> Tuple[ExecutionPlanUpdates, BLOB]:
        flattened_ast: Dict[str, ASTNode] = flatten_ast(root)
        value_nodes: List[ValueNode] = [
            node for node in flattened_ast.values() if isinstance(node, ValueNode)
        ]
        uploaded_value_node_sos, uploaded_function_outputs_blob = (
            self._upload_function_output_values(value_nodes)
        )
        uploaded_value_node_sos: Dict[str, SerializedObjectInsideBLOB]
        uploaded_function_outputs_blob: BLOB
        updates: List[ExecutionPlanUpdate] = []

        for node in traverse_ast(root):
            if isinstance(node, ValueNode):
                continue

            node: RegularFunctionCallNode | ReducerFunctionCallNode
            update: ExecutionPlanUpdate
            data_dependencies: List[FunctionArg] = []
            for child in node.children.values():
                if isinstance(child, ValueNode):
                    data_dependencies.append(
                        FunctionArg(value=uploaded_value_node_sos[child.id])
                    )
                elif isinstance(child, RegularFunctionCallNode):
                    data_dependencies.append(
                        FunctionArg(
                            function_call_id=child.id,
                        )
                    )
                elif isinstance(child, ReducerFunctionCallNode):
                    data_dependencies.append(
                        FunctionArg(
                            function_call_id=child.id,
                        )
                    )

            if isinstance(node, RegularFunctionCallNode):
                update = ExecutionPlanUpdate(
                    function_call=FunctionCall(
                        id=node.id,
                        target=FunctionRef(
                            namespace=self._function_ref.namespace,
                            application_name=self._function_ref.application_name,
                            function_name=node.function_name,
                            application_version=self._function_ref.application_version,
                        ),
                        args=data_dependencies,
                        call_metadata=FunctionCallNodeMetadata(
                            nid=node.id,
                            type=FunctionCallType.REGULAR,
                            metadata=node.serialized_metadata,
                        ).serialize(),
                    )
                )
            elif isinstance(node, ReducerFunctionCallNode):
                update = ExecutionPlanUpdate(
                    reduce=ReduceOp(
                        id=node.id,
                        reducer=FunctionRef(
                            namespace=self._function_ref.namespace,
                            application_name=self._function_ref.application_name,
                            function_name=node.reducer_function_name,
                            application_version=self._function_ref.application_version,
                        ),
                        collection=data_dependencies,
                        call_metadata=FunctionCallNodeMetadata(
                            nid=node.id,
                            type=FunctionCallType.REDUCER,
                            metadata=node.serialized_metadata,
                        ).serialize(),
                    )
                )
            else:
                raise ValueError(f"Unknown AST node type: {type(node)}")

            updates.append(update)

        return (
            ExecutionPlanUpdates(
                updates=updates,
                root_function_call_id=root.id,
            ),
            uploaded_function_outputs_blob,
        )

    def _upload_function_output_values(
        self, value_nodes: List[ValueNode]
    ) -> Tuple[Dict[str, SerializedObjectInsideBLOB], BLOB]:
        serialized_objects: Dict[str, SerializedObjectInsideBLOB] = {}
        blob_datas: List[bytes] = []
        blob_offset: int = 0
        encoding_version: int = 0

        for value_node in value_nodes:
            value_node_serialized_metadata: bytes = ValueNodeMetadata(
                nid=value_node.id, metadata=value_node.serialized_metadata
            ).serialize()
            value_metadata: ValueMetadata = ValueMetadata.deserialize(
                value_node.serialized_metadata
            )
            serializer: UserDataSerializer = None

            if value_metadata.serializer_name is not None:
                serializer = serializer_by_name(value_metadata.serializer_name)

            value_node_so: SerializedObjectInsideBLOB = SerializedObjectInsideBLOB(
                manifest=SerializedObjectManifest(
                    encoding=(
                        SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW
                        if serializer is None
                        else serializer.serialized_object_encoding
                    ),
                    encoding_version=encoding_version,
                    size=len(value_node_serialized_metadata) + len(value_node.value),
                    metadata_size=len(value_node_serialized_metadata),
                    sha256_hash=_sha256_hexdigest(
                        value_node_serialized_metadata, value_node.value
                    ),
                    content_type=value_node.content_type,
                ),
                offset=blob_offset,
            )
            serialized_objects[value_node.id] = value_node_so
            blob_datas.append(value_node_serialized_metadata)
            blob_datas.append(value_node.value)
            blob_offset += value_node_so.manifest.size

        start_time = time.monotonic()
        self._logger.info(
            "uploading function output values",
            outputs_count=len(serialized_objects),
            total_size=blob_offset,
        )
        uploaded_blob: BLOB = _upload_outputs(
            blob_datas,
            self._inputs.function_outputs_blob,
            self._blob_store,
            self._logger,
        )
        self._logger.info(
            "function output values uploaded",
            outputs_count=len(serialized_objects),
            total_size=blob_offset,
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )

        return serialized_objects, uploaded_blob

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
                    metadata_size=0,
                    sha256_hash=_sha256_hexdigest(b"", data),
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


def _sha256_hexdigest(metadata: bytes, data: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(metadata)
    hasher.update(data)
    return hasher.hexdigest()
