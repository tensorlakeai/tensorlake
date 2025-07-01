import unittest

from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    deserialized_function_output,
    rpc_channel,
    run_task,
)

from tensorlake import Graph, InvocationError
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
    SerializedObjectEncoding,
    TaskFailureReason,
    TaskOutcomeCode,
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
    raise InvocationError(f"The invocation can't succeed: {x}")


class TestInvocationError(unittest.TestCase):
    def test_invocation_error_response(self):
        graph = Graph(
            name="test", description="test", start_node=raise_invocation_error
        )
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="raise_invocation_error",
                        graph=SerializedObject(
                            data=zip_graph_code(
                                graph=graph,
                                code_dir_path=GRAPH_CODE_DIR_PATH,
                            ),
                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                            encoding_version=0,
                        ),
                    )
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
                )

                run_task_response: RunTaskResponse = run_task(
                    stub,
                    function_name="raise_invocation_error",
                    input=10,
                )

                self.assertEqual(
                    run_task_response.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    run_task_response.failure_reason,
                    TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR,
                )
                self.assertEqual(
                    run_task_response.invocation_error_output.encoding,
                    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT,
                )
                self.assertIn(
                    "The invocation can't succeed: 10",
                    run_task_response.invocation_error_output.data.decode("utf-8"),
                )
                self.assertFalse(run_task_response.is_reducer)
                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_outputs
                )
                self.assertEqual(len(fn_outputs), 0)


if __name__ == "__main__":
    unittest.main()
