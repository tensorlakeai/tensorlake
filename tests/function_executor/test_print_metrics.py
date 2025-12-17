import json
import os
import random
import unittest

from testing import (
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    initialize,
    rpc_channel,
    run_allocation_that_returns_output,
)

from tensorlake.applications import (
    RequestContext,
    application,
    function,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    Allocation,
    AllocationOutcomeCode,
    AllocationResult,
    CreateAllocationRequest,
    InitializationOutcomeCode,
    InitializeResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


@application()
@function()
def emit_metrics(total: int) -> int:
    ctx: RequestContext = RequestContext.get()
    for num in range(total):
        ctx.metrics.timer("test_timer", 2.5)
        ctx.metrics.counter("test_counter", num)
    return total


class TestPrintMetrics(unittest.TestCase):
    def test_function_prints_metrics(self):
        arg: int = random.randint(1, 10)

        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="emit_metrics",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="emit_metrics",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                allocation_id: str = "test-allocation-id"
                alloc_result: AllocationResult = run_allocation_that_returns_output(
                    self,
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(arg),
                        ),
                    ),
                )

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )

        fe_stdout = process.read_stdout()
        self.assertIn(
            '"type": "ai.tensorlake.metric.counter.inc", "source": "/tensorlake/applications/metrics"',
            fe_stdout,
        )

        for num in range(arg):
            data = {
                "request_id": "123",
                "function_name": "emit_metrics",
                "counter_name": "test_counter",
                "counter_inc": num,
            }

            self.assertIn(
                json.dumps(data),
                fe_stdout,
            )

        self.assertIn(
            '"type": "ai.tensorlake.metric.timer", "source": "/tensorlake/applications/metrics"',
            fe_stdout,
        )
        self.assertIn(
            json.dumps(
                {
                    "request_id": "123",
                    "function_name": "emit_metrics",
                    "timer_name": "test_timer",
                    "timer_value": 2.5,
                }
            ),
            fe_stdout,
        )


if __name__ == "__main__":
    unittest.main()
