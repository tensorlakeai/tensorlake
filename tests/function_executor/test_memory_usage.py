import math
import unittest

import psutil
from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    deserialized_function_output,
    rpc_channel,
    run_task,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
    SerializedObjectEncoding,
    TaskOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.graph_serialization import (
    ZIPPED_GRAPH_CODE_CONTENT_TYPE,
    graph_code_dir_path,
    zip_graph_code,
)

GRAPH_CODE_DIR_PATH = graph_code_dir_path(__file__)

# This test checks if the memory usage of a Function Executor process is below
# a known threshold. Customers rely on this threshold because if FE memory usage
# grows then customer functions can start failing with out of memory errors.
#
# Real max memory we saw in tests is 80 MB, add extra 5 MB to remove flakiness from
# the test.
_FUNCTION_EXECUTOR_MAX_MEMORY_MB = 85


@tensorlake_function()
def process_rss_mb(x: int) -> int:
    # rss is in bytes
    return math.ceil(psutil.Process().memory_info().rss / (1024 * 1024))


class TestMemoryUsage(unittest.TestCase):
    def test_memory_usage_is_below_max_threshold(self):
        graph = Graph(name="test", description="test", start_node=process_rss_mb)

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
                        function_name="process_rss_mb",
                        graph=SerializedObject(
                            data=zip_graph_code(
                                graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH
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
                    function_name="process_rss_mb",
                    input=0,
                )

                self.assertEqual(
                    run_task_response.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
                )

                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_outputs
                )
                self.assertEqual(len(fn_outputs), 1)
                fe_process_rss_mb = fn_outputs[0]
                print(
                    f"Function Executor process RSS memory usage: {fe_process_rss_mb} MB"
                )
                self.assertLessEqual(
                    fe_process_rss_mb, _FUNCTION_EXECUTOR_MAX_MEMORY_MB
                )


if __name__ == "__main__":
    unittest.main()
