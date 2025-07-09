import unittest
from typing import Generator, Iterator

import grpc
from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    rpc_channel,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    OpenSessionRequest,
    OpenSessionResponse,
    RunTaskAllocationsSessionClientMessage,
    RunTaskAllocationsSessionServerMessage,
    SerializedObject,
    SerializedObjectEncoding,
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


@tensorlake_function()
def success_function(_: str) -> str:
    return "success"


def initialize(test_case: unittest.TestCase, stub: FunctionExecutorStub):
    initialize_response: InitializeResponse = stub.initialize(
        InitializeRequest(
            namespace="test",
            graph_name="test",
            graph_version="1",
            function_name="success_function",
            graph=SerializedObject(
                data=zip_graph_code(
                    graph=Graph(
                        name="test", description="test", start_node=success_function
                    ),
                    code_dir_path=GRAPH_CODE_DIR_PATH,
                ),
                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                encoding_version=0,
            ),
        )
    )
    test_case.assertEqual(
        initialize_response.outcome_code,
        InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
    )


class TestOpenSession(unittest.TestCase):
    def test_open_new_session(self):
        def client_messages() -> (
            Generator[RunTaskAllocationsSessionClientMessage, None, None]
        ):
            yield RunTaskAllocationsSessionClientMessage(
                open_session_request=OpenSessionRequest(
                    session_id="new_session",
                )
            )

        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(self, stub)
                server_messages: Generator[RunTaskAllocationsSessionServerMessage] = (
                    stub.run_task_allocations_session(client_messages())
                )
                response: RunTaskAllocationsSessionServerMessage = next(server_messages)
                self.assertTrue(response.HasField("open_session_response"))
                open_session_response: OpenSessionResponse = (
                    response.open_session_response
                )
                self.assertEqual(
                    open_session_response.status.code, grpc.StatusCode.OK.value[0]
                )
                self.assertTrue(open_session_response.is_new)


if __name__ == "__main__":
    unittest.main()
