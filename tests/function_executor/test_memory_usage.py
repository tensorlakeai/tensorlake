import hashlib
import math
import unittest

import psutil
from testing import (
    FunctionExecutorProcessContextManager,
    create_tmp_blob,
    deserialized_function_output,
    rpc_channel,
    run_task,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
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
                        function_name="process_rss_mb",
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

                function_outputs_blob: BLOB = create_tmp_blob()
                task_result: TaskResult = run_task(
                    stub,
                    function_name="process_rss_mb",
                    input=0,
                    function_outputs_blob=function_outputs_blob,
                    invocation_error_blob=create_tmp_blob(),
                )

                self.assertEqual(
                    task_result.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
                )

                fn_outputs = deserialized_function_output(
                    self, task_result.function_outputs, function_outputs_blob
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
