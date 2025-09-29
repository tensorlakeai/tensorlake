import math
import os
import unittest

import psutil
from testing import (
    FunctionExecutorProcessContextManager,
    api_function_inputs,
    download_and_deserialize_so,
    initialize,
    rpc_channel,
    run_allocation,
)

import tensorlake.applications.interface as tensorlake
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutcomeCode,
    AllocationResult,
    InitializationOutcomeCode,
    InitializeResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

app: tensorlake.Application = tensorlake.define_application(name=__file__)

# This test checks if the memory usage of a Function Executor process is below
# a known threshold. Customers rely on this threshold because if FE memory usage
# grows then customer functions can start failing with out of memory errors.
#
# Real max memory we saw in tests is 80 MB, add extra 5 MB to remove flakiness from
# the test.
_FUNCTION_EXECUTOR_MAX_MEMORY_MB = 85


@tensorlake.api()
@tensorlake.function()
def process_rss_mb(x: int) -> int:
    # rss is in bytes
    return math.ceil(psutil.Process().memory_info().rss / (1024 * 1024))


class TestMemoryUsage(unittest.TestCase):
    def test_memory_usage_is_below_max_threshold(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub=stub,
                    app=app,
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="process_rss_mb",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=api_function_inputs(0),
                )

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )

                fe_process_rss_mb: int = download_and_deserialize_so(
                    self,
                    alloc_result.value,
                    alloc_result.uploaded_function_outputs_blob,
                )
                print(
                    f"Function Executor process RSS memory usage: {fe_process_rss_mb} MB"
                )
                self.assertLessEqual(
                    fe_process_rss_mb, _FUNCTION_EXECUTOR_MAX_MEMORY_MB
                )


if __name__ == "__main__":
    unittest.main()
