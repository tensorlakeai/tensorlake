import hashlib
import unittest

from testing import (
    FunctionExecutorProcessContextManager,
    create_tmp_blob,
    deserialized_function_output,
    read_tmp_blob_bytes,
    rpc_channel,
    run_allocation,
)

from tensorlake import Graph, RequestException
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
    TaskFailureReason,
    TaskOutcomeCode,
    TaskResult,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.graph_serialization import (
    graph_code_dir_path,
    zip_graph_code,
)

GRAPH_CODE_DIR_PATH = graph_code_dir_path(__file__)


@tensorlake_function()
def raise_invocation_error(x: int) -> str:
    raise RequestException(f"The invocation can't succeed: {x}")


class TestInvocationError(unittest.TestCase):
    def test_invocation_error_response(self):
        graph = Graph(
            name="test", description="test", start_node=raise_invocation_error
        )
        graph_data: bytes = zip_graph_code(
            graph=graph,
            code_dir_path=GRAPH_CODE_DIR_PATH,
        )

        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)

                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="raise_invocation_error",
                        graph=SerializedObject(
                            manifest=SerializedObjectManifest(
                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                                encoding_version=0,
                                size=len(graph_data),
                                sha256_hash=hashlib.sha256(graph_data).hexdigest(),
                            ),
                            data=graph_data,
                        ),
                    )
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                function_outputs_blob = create_tmp_blob()
                invocation_error_blob = create_tmp_blob()
                task_result: TaskResult = run_allocation(
                    stub,
                    function_name="raise_invocation_error",
                    input=10,
                    function_outputs_blob=function_outputs_blob,
                    invocation_error_blob=invocation_error_blob,
                )

                self.assertEqual(
                    task_result.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    task_result.failure_reason,
                    TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR,
                )
                self.assertEqual(
                    task_result.invocation_error_output.manifest.encoding,
                    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT,
                )
                self.assertIn(
                    "The invocation can't succeed: 10",
                    read_tmp_blob_bytes(
                        invocation_error_blob,
                        task_result.invocation_error_output.offset,
                        task_result.invocation_error_output.manifest.size,
                    ).decode("utf-8"),
                )
                fn_outputs = deserialized_function_output(
                    self,
                    task_result.function_outputs,
                    function_outputs_blob=function_outputs_blob,
                )
                self.assertEqual(len(fn_outputs), 0)


if __name__ == "__main__":
    unittest.main()
