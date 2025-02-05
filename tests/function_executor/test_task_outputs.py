import logging
import re
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
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer

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
                            bytes=CloudPickleSerializer.serialize(
                                graph.serialize(additional_modules=[])
                            ),
                            content_type=CloudPickleSerializer.content_type,
                        ),
                    )
                )
                self.assertTrue(initialize_response.success)

                for test_iteration in range(TEST_ITERATIONS):
                    content = f"test content, test case: {test_case_name}, test iteration: {test_iteration}"
                    run_task_response: RunTaskResponse = run_task(
                        stub, function_name=function_name, input=content
                    )

                    self.assertTrue(run_task_response.success)
                    fn_outputs = deserialized_function_output(
                        self, run_task_response.function_output
                    )
                    self.assertEqual(len(fn_outputs), 1)
                    self.assertEqual("success", fn_outputs[0])
                    validation_function(self, content, run_task_response)


if __name__ == "__main__":
    unittest.main()
