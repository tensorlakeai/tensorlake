import logging
import sys
import unittest

import parameterized
from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    deserialized_function_output,
    read_local_blob_str,
    rpc_channel,
    run_task,
    tmp_local_file_blob,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
    TaskOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.functions_sdk.functions import TensorlakeCompute, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import (
    graph_code_dir_path,
    zip_graph_code,
)

GRAPH_CODE_DIR_PATH = graph_code_dir_path(__file__)

# Previously function outputs were missing time to time due to race conditions.
# Run many iterations of the tests to ensure that the race conditions are really fixed.
TEST_ITERATIONS = 100


class PrintFunction(TensorlakeCompute):
    name = "print_function"

    def __init__(self):
        super().__init__()
        print(f"{self.name} initialized")

    def run(self, content: str) -> str:
        print(content)
        return "success"


def validate_print_function_output(
    test_case: unittest.TestCase,
    content: str,
    initialize_request: InitializeRequest,
    stdout_blob: BLOB,
    stderr_blob: BLOB,
):
    test_case.assertEqual(
        "print_function initialized\n", read_local_blob_str(initialize_request.stdout)
    )
    test_case.assertEqual("", read_local_blob_str(initialize_request.stderr))
    test_case.assertEqual(content + "\n", read_local_blob_str(stdout_blob))
    test_case.assertEqual("", read_local_blob_str(stderr_blob))


class StdoutFunction(TensorlakeCompute):
    name = "stdout_function"

    def __init__(self):
        super().__init__()
        sys.stdout.write(f"{self.name} initialized")

    def run(self, content: str) -> str:
        sys.stdout.write(content)
        return "success"


def validate_stdout_function_output(
    test_case: unittest.TestCase,
    content: str,
    initialize_request: InitializeRequest,
    stdout_blob: BLOB,
    stderr_blob: BLOB,
):
    test_case.assertEqual(
        "stdout_function initialized", read_local_blob_str(initialize_request.stdout)
    )
    test_case.assertEqual("", read_local_blob_str(initialize_request.stderr))
    test_case.assertEqual(content, read_local_blob_str(stdout_blob))
    test_case.assertEqual("", read_local_blob_str(stderr_blob))


class StderrFunction(TensorlakeCompute):
    name = "stderr_function"

    def __init__(self):
        super().__init__()
        sys.stderr.write(f"{self.name} initialized")

    def run(self, content: str) -> str:
        sys.stderr.write(content)
        return "success"


def validate_stderr_function_output(
    test_case: unittest.TestCase,
    content: str,
    initialize_request: InitializeRequest,
    stdout_blob: BLOB,
    stderr_blob: BLOB,
):
    test_case.assertEqual("", read_local_blob_str(initialize_request.stdout))
    test_case.assertEqual(
        "stderr_function initialized", read_local_blob_str(initialize_request.stderr)
    )
    test_case.assertEqual("", read_local_blob_str(stdout_blob))
    test_case.assertEqual(content, read_local_blob_str(stderr_blob))


class StdlogFunction(TensorlakeCompute):
    name = "stdlog_function"

    def __init__(self):
        super().__init__()
        logging.error(f"{self.name} initialized")

    def run(self, content: str) -> str:
        logging.error(content)
        return "success"


def validate_stdlog_function_output(
    test_case: unittest.TestCase,
    content: str,
    initialize_request: InitializeRequest,
    stdout_blob: BLOB,
    stderr_blob: BLOB,
):
    # FIXME: This test is validating empty stderr, stdout because
    # currently standard logging doesn't work in functions because root logger of std logging module
    # is created on its first use and it uses the sys.stderr file handle that existed at that moment.
    # This can only be fixed if we split customer code into a separate process.
    test_case.assertEqual(read_local_blob_str(initialize_request.stdout), "")
    test_case.assertEqual(read_local_blob_str(initialize_request.stderr), "")
    test_case.assertEqual(read_local_blob_str(stdout_blob), "")
    test_case.assertEqual(read_local_blob_str(stderr_blob), "")


class TestRunTask(unittest.TestCase):
    @parameterized.parameterized.expand(
        [
            (
                "print function call",
                PrintFunction,
                PrintFunction.name,
                validate_print_function_output,
            ),
            (
                "stdout file descriptor write",
                StdoutFunction,
                StdoutFunction.name,
                validate_stdout_function_output,
            ),
            (
                "stderr file descriptor write",
                StderrFunction,
                StderrFunction.name,
                validate_stderr_function_output,
            ),
            (
                "standard logging library call",
                StdlogFunction,
                StdlogFunction.name,
                validate_stdlog_function_output,
            ),
        ]
    )
    def test_expected_run_task_response_stdout_stderr(
        self, test_case_name, function, function_name, validation_function
    ):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as process:
            with rpc_channel(process) as channel:
                graph = Graph(name="test", description="test", start_node=function)
                graph_data: bytes = zip_graph_code(
                    graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH
                )
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_request: InitializeRequest = InitializeRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version="1",
                    function_name=function_name,
                    graph=SerializedObject(
                        manifest=SerializedObjectManifest(
                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                            encoding_version=0,
                            size=len(graph_data),
                        ),
                        data=graph_data,
                    ),
                    stdout=tmp_local_file_blob(),
                    stderr=tmp_local_file_blob(),
                )
                initialize_response: InitializeResponse = stub.initialize(
                    initialize_request
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
                )

                for test_iteration in range(TEST_ITERATIONS):
                    content = f"test content, test case: {test_case_name}, test iteration: {test_iteration}"
                    stdout_blob = tmp_local_file_blob()
                    stderr_blob = tmp_local_file_blob()
                    run_task_response: RunTaskResponse = run_task(
                        stub,
                        function_name=function_name,
                        input=content,
                        stdout_blob=stdout_blob,
                        stderr_blob=stderr_blob,
                    )

                    self.assertEqual(
                        run_task_response.outcome_code,
                        TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
                    )
                    fn_outputs = deserialized_function_output(
                        self, run_task_response.function_outputs
                    )
                    self.assertEqual(len(fn_outputs), 1)
                    self.assertEqual("success", fn_outputs[0])
                    validation_function(
                        self, content, initialize_request, stdout_blob, stderr_blob
                    )


if __name__ == "__main__":
    unittest.main()
