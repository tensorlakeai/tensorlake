import hashlib
import os
import signal
import threading
import time
import unittest

from grpc import RpcError
from testing import (
    FunctionExecutorProcessContextManager,
    create_tmp_blob,
    rpc_channel,
    run_allocation,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    HealthCheckRequest,
    HealthCheckResponse,
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

# Lower - faster tests but more CPU usage.
HEALTH_CHECK_POLL_PERIOD_SEC = 0.1
HEALTH_CHECK_TIMEOUT_SEC = 5


@tensorlake_function()
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


def initialize(test_case: unittest.TestCase, stub: FunctionExecutorStub):
    graph_data: bytes = zip_graph_code(
        graph=Graph(name="test", description="test", start_node=action_function),
        code_dir_path=GRAPH_CODE_DIR_PATH,
    )
    initialize_response: InitializeResponse = stub.initialize(
        InitializeRequest(
            namespace="test",
            graph_name="test",
            graph_version="1",
            function_name="action_function",
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
    test_case.assertEqual(
        initialize_response.outcome_code,
        InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
    )


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
                initialize(self, stub)
                function_outputs_blob: BLOB = create_tmp_blob()

                def run_task_in_thread():
                    try:
                        run_allocation(
                            stub,
                            function_name="action_function",
                            input="deadlock",
                            function_outputs_blob=function_outputs_blob,
                            invocation_error_blob=create_tmp_blob(),
                            timeout_sec=HEALTH_CHECK_TIMEOUT_SEC,
                        )
                        self.fail("Run task should have timed out.")
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
                initialize(self, stub)
                function_outputs_blob: BLOB = create_tmp_blob()

                def run_task_in_thread():
                    task_result: TaskResult = run_allocation(
                        stub,
                        function_name="action_function",
                        input="raise_exception",
                        function_outputs_blob=function_outputs_blob,
                        invocation_error_blob=create_tmp_blob(),
                    )
                    self.assertEqual(
                        task_result.outcome_code,
                        TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
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
                initialize(self, stub)
                function_outputs_blob: BLOB = create_tmp_blob()

                def run_task_in_thread():
                    try:
                        # Due to "tcp keep-alive" property of the health checks the task should unblock with RpcError.
                        run_allocation(
                            stub,
                            function_name="action_function",
                            input="crash_process",
                            function_outputs_blob=function_outputs_blob,
                            invocation_error_blob=create_tmp_blob(),
                        )
                        self.fail("Run task should have failed.")
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
                initialize(self, stub)
                function_outputs_blob: BLOB = create_tmp_blob()

                def run_task_in_thread():
                    try:
                        # Due to "tcp keep-alive" property of the health checks the task should unblock with RpcError.
                        run_allocation(
                            stub,
                            function_name="action_function",
                            input="close_connections",
                            function_outputs_blob=function_outputs_blob,
                            invocation_error_blob=create_tmp_blob(),
                        )
                        self.fail("Run task should have failed.")
                    except RpcError:
                        pass

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                wait_health_check_failure(self, stub)
                print("Waiting for run task thread to fail and unblock...")
                task_thread.join()


if __name__ == "__main__":
    unittest.main()
