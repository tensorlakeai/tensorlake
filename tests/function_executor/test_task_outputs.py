import logging
import sys
import unittest

import parameterized
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

# Previously function outputs were missing time to time due to race conditions.
# Run many iterations of the tests to ensure that the race conditions are really fixed.
TEST_ITERATIONS = 100


@tensorlake_function()
def print_function(content: str) -> str:
    print(content)
    return "success"


def validate_print_function_output(
    test_case: unittest.TestCase, content: str, run_task_response: RunTaskResponse
):
    test_case.assertEqual(content + "\n", run_task_response.stdout)


@tensorlake_function()
def stdout_function(content: str) -> str:
    sys.stdout.write(content)
    return "success"


def validate_stdout_function_output(
    test_case: unittest.TestCase, content: str, run_task_response: RunTaskResponse
):
    test_case.assertEqual(content, run_task_response.stdout)


@tensorlake_function()
def stderr_function(content: str) -> str:
    sys.stderr.write(content)
    return "success"


def validate_stderr_function_output(
    test_case: unittest.TestCase, content: str, run_task_response: RunTaskResponse
):
    test_case.assertEqual(content, run_task_response.stderr)


@tensorlake_function()
def stdlog_function(content: str) -> str:
    logging.error(content)
    return "success"


def validate_stdlog_function_output(
    test_case: unittest.TestCase, content: str, run_task_response: RunTaskResponse
):
    # FIXME: This test is validating empty stderr, stdout because
    # currently standard logging doesn't work in functions because root logger of std logging module
    # is created on its first use and it uses the sys.stderr file handle that existed at that moment.
    test_case.assertEqual(run_task_response.stdout, "")
    test_case.assertEqual(run_task_response.stderr, "")


class TestRunTask(unittest.TestCase):
    @parameterized.parameterized.expand(
        [
            (
                "print function call",
                print_function,
                "print_function",
                validate_print_function_output,
            ),
            (
                "stdout file descriptor write",
                stdout_function,
                "stdout_function",
                validate_stdout_function_output,
            ),
            (
                "stderr file descriptor write",
                stderr_function,
                "stderr_function",
                validate_stderr_function_output,
            ),
            (
                "standard logging library call",
                stdlog_function,
                "stdlog_function",
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
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name=function_name,
                        graph=SerializedObject(
                            data=zip_graph_code(
                                graph=graph,
                                code_dir_path=GRAPH_CODE_DIR_PATH,
                            ),
                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                            encoding_version=1,
                        ),
                    )
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
                )

                for test_iteration in range(TEST_ITERATIONS):
                    content = f"test content, test case: {test_case_name}, test iteration: {test_iteration}"
                    run_task_response: RunTaskResponse = run_task(
                        stub, function_name=function_name, input=content
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
                    validation_function(self, content, run_task_response)

    def test_expected_run_task_response_stdout_stderr_with_disabled_capture_env_var(
        self,
    ):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 1,
            extra_env={"INDEXIFY_FUNCTION_EXECUTOR_DISABLE_OUTPUT_CAPTURE": "1"},
        ) as process:
            with rpc_channel(process) as channel:
                graph = Graph(
                    name="test", description="test", start_node=print_function
                )
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="print_function",
                        graph=SerializedObject(
                            data=zip_graph_code(
                                graph=graph,
                                code_dir_path=GRAPH_CODE_DIR_PATH,
                            ),
                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                            encoding_version=1,
                        ),
                    )
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
                )

                run_task_response: RunTaskResponse = run_task(
                    stub,
                    function_name="print_function",
                    input="print function argument to print",
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
                self.assertEqual(
                    "Function output capture is disabled using INDEXIFY_FUNCTION_EXECUTOR_DISABLE_OUTPUT_CAPTURE env var.\n",
                    run_task_response.stdout,
                )
                self.assertEqual("", run_task_response.stderr)


if __name__ == "__main__":
    unittest.main()
