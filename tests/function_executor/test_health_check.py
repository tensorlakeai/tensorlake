import os
import signal
import threading
import time
import unittest

from grpc import RpcError
from testing import (
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    initialize,
    rpc_channel,
    run_allocation_that_fails,
)

from tensorlake.applications import application, function
from tensorlake.function_executor.proto.function_executor_pb2 import (
    Allocation,
    AllocationOutcomeCode,
    AllocationResult,
    CreateAllocationRequest,
    HealthCheckRequest,
    HealthCheckResponse,
    InitializationOutcomeCode,
    InitializeResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

# Lower - faster tests but more CPU usage.
HEALTH_CHECK_POLL_PERIOD_SEC = 0.1
HEALTH_CHECK_TIMEOUT_SEC = 5


@application()
@function()
def action_function(action: str) -> str:
    if action == "crash_process":
        print("Crashing process...")
        os.kill(os.getpid(), signal.SIGKILL)
    elif action == "deadlock":
        import threading

        lock = threading.Lock()
        lock.acquire()
        lock.acquire()
    elif action == "raise_exception":
        raise Exception("Test exception")
    elif action == "close_connections":
        # 1000 is enough to close server socket.
        os.closerange(0, 1000)
    else:
        return "success"


def wait_health_check_failure(test_case: unittest.TestCase, stub: FunctionExecutorStub):
    print("Waiting for health check to fail...")
    HEALTH_CHECK_FAIL_WAIT_SEC = 5
    start_time = time.time()
    while time.time() - start_time < HEALTH_CHECK_FAIL_WAIT_SEC:
        try:
            response: HealthCheckResponse = stub.check_health(
                HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
            )
            test_case.assertTrue(response.healthy)
            time.sleep(HEALTH_CHECK_POLL_PERIOD_SEC)
        except RpcError:
            return

    test_case.fail(f"Health check didn't fail in {HEALTH_CHECK_FAIL_WAIT_SEC} secs.")


class TestHealthCheck(unittest.TestCase):
    def test_not_initialized_fails(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                try:
                    stub.check_health(
                        HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
                    )
                    self.fail("Health check should have failed for not initialized FE.")
                except RpcError as e:
                    self.assertIn(
                        "Function Executor is not initialized",
                        str(e),
                    )

    def test_function_deadlock_success(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: InitializeResponse = initialize(
                    stub,
                    app_name="action_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="action_function",
                )
                self.assertEqual(
                    response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                def run_task_in_thread():
                    try:
                        allocation_id: str = "test-allocation-id"
                        run_allocation_that_fails(
                            stub,
                            request=CreateAllocationRequest(
                                allocation=Allocation(
                                    request_id="123",
                                    function_call_id="test-function-call",
                                    allocation_id=allocation_id,
                                    inputs=application_function_inputs("deadlock", str),
                                ),
                            ),
                            timeout_sec=HEALTH_CHECK_TIMEOUT_SEC,
                        )

                        self.fail("Waiting for task result should have timed out.")
                    except RpcError:
                        pass

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                print("Waiting for run task thread to fail and unblock...")
                while task_thread.is_alive():
                    response: HealthCheckResponse = stub.check_health(
                        HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
                    )
                    self.assertTrue(response.healthy)
                    time.sleep(HEALTH_CHECK_POLL_PERIOD_SEC)
                task_thread.join()

    def test_function_raises_exception_success(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: InitializeResponse = initialize(
                    stub,
                    app_name="action_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="action_function",
                )
                self.assertEqual(
                    response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                def run_task_in_thread():
                    allocation_result: AllocationResult = run_allocation_that_fails(
                        stub,
                        request=CreateAllocationRequest(
                            allocation=Allocation(
                                request_id="123",
                                function_call_id="test-function-call",
                                allocation_id="test-allocation-id",
                                inputs=application_function_inputs(
                                    "raise_exception", str
                                ),
                            ),
                        ),
                    )

                    self.assertEqual(
                        allocation_result.outcome_code,
                        AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                    )

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                print("Waiting for run task thread to fail and unblock...")
                while task_thread.is_alive():
                    response: HealthCheckResponse = stub.check_health(
                        HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
                    )
                    self.assertTrue(response.healthy)
                    time.sleep(HEALTH_CHECK_POLL_PERIOD_SEC)
                task_thread.join()

    def test_process_crash_failure(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: InitializeResponse = initialize(
                    stub,
                    app_name="action_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="action_function",
                )
                self.assertEqual(
                    response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                def run_task_in_thread():
                    try:
                        run_allocation_that_fails(
                            stub,
                            request=CreateAllocationRequest(
                                allocation=Allocation(
                                    request_id="123",
                                    function_call_id="test-function-call",
                                    allocation_id="test-allocation-id",
                                    inputs=application_function_inputs(
                                        "crash_process", str
                                    ),
                                ),
                            ),
                        )
                        # Due to "tcp keep-alive" property of the health checks the allocation
                        # watch state iterator read should unblock with RpcError.

                        self.fail("Waiting for task result should have failed.")
                    except RpcError:
                        pass

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                wait_health_check_failure(self, stub)
                print("Waiting for run task thread to fail and unblock...")
                task_thread.join()

    def test_process_closes_server_socket_failure(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: InitializeResponse = initialize(
                    stub,
                    app_name="action_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="action_function",
                )
                self.assertEqual(
                    response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                def run_task_in_thread():
                    try:
                        run_allocation_that_fails(
                            stub,
                            request=CreateAllocationRequest(
                                allocation=Allocation(
                                    request_id="123",
                                    function_call_id="test-function-call",
                                    allocation_id="test-allocation-id",
                                    inputs=application_function_inputs(
                                        "close_connections",
                                        str,
                                    ),
                                ),
                            ),
                        )

                        # Due to "tcp keep-alive" property of the health checks the allocation
                        # watch state iterator read should unblock with RpcError.

                        self.fail("Waiting for task result should have failed.")
                    except RpcError:
                        pass

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                wait_health_check_failure(self, stub)
                print("Waiting for run task thread to fail and unblock...")
                task_thread.join()


if __name__ == "__main__":
    unittest.main()
