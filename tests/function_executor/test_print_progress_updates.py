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
def prints_progress_updates(total: int) -> int:
    ctx: RequestContext = RequestContext.get()
    attributes = {"key": "value"}
    for num in range(total):
        ctx.progress.update(
            current=num,
            total=total,
            attributes=attributes,
        )
    return total


@application()
@function()
def prints_progress_updates_with_message(total: int) -> int:
    ctx: RequestContext = RequestContext.get()
    attributes = {"key": "value"}
    for num in range(total):
        ctx.progress.update(
            current=num,
            total=total,
            message=f"this is step {num} of {total} steps in this function",
            attributes=attributes,
        )
    return total


class TestPrintProgressUpdates(unittest.TestCase):
    def test_function_prints_progres_update(self):
        arg: int = random.randint(1, 10)

        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="prints_progress_updates",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="prints_progress_updates",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                alloc_result: AllocationResult = run_allocation_that_returns_output(
                    self,
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id="test-allocation-id",
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
            '"type": "ai.tensorlake.progress_update", "source": "/tensorlake/applications/progress"',
            fe_stdout,
        )

        for num in range(arg):
            data = {
                "request_id": "123",
                "function_name": "prints_progress_updates",
                "function_run_id": "test-function-call",
                "allocation_id": "test-allocation-id",
                "message": f"prints_progress_updates: executing step {num} of {arg}",
                "step": num,
                "total": arg,
                "attributes": {"key": "value"},
            }

            self.assertIn(
                # strip the closing braket because there might be other fields after attributes,
                # but we don't care about them.
                json.dumps(data).strip("}"),
                fe_stdout,
            )

    def test_function_prints_progres_update_with_message(self):
        arg: int = random.randint(1, 10)

        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="prints_progress_updates_with_message",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="prints_progress_updates_with_message",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                alloc_result: AllocationResult = run_allocation_that_returns_output(
                    self,
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id="test-allocation-id",
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
            '"type": "ai.tensorlake.progress_update", "source": "/tensorlake/applications/progress"',
            fe_stdout,
        )

        for num in range(arg):
            data = {
                "request_id": "123",
                "function_name": "prints_progress_updates_with_message",
                "function_run_id": "test-function-call",
                "allocation_id": "test-allocation-id",
                "message": f"this is step {num} of {arg} steps in this function",
                "step": num,
                "total": arg,
                "attributes": {"key": "value"},
            }

            self.assertIn(
                # strip the closing braket because there might be other fields after attributes,
                # but we don't care about them.
                json.dumps(data).strip("}"),
                fe_stdout,
            )


if __name__ == "__main__":
    unittest.main()
