import hashlib
import unittest
from typing import Generator, Iterator

import grpc
from testing import (
    FunctionExecutorProcessContextManager,
    rpc_channel,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    LeaveSessionRequest,
    LeaveSessionResponse,
    OpenSessionRequest,
    OpenSessionResponse,
    RunTaskAllocationsSessionClientMessage,
    RunTaskAllocationsSessionServerMessage,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectID,
    SerializedObjectManifest,
    UploadSerializedObjectRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.google.rpc.code_pb2 import Code
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


def sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class TestOpenSession(unittest.TestCase):
    def test_create_session(self):
        def client_messages() -> (
            Generator[RunTaskAllocationsSessionClientMessage, None, None]
        ):
            yield RunTaskAllocationsSessionClientMessage(
                open_session_request=OpenSessionRequest(
                    session_id="test_session",
                )
            )

        with FunctionExecutorProcessContextManager() as process:
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
                self.assertTrue(open_session_response.HasField("status"))
                self.assertEqual(open_session_response.status.code, Code.OK)
                self.assertTrue(open_session_response.is_new)

    def test_rejoin_session(self):
        def client_messages() -> (
            Generator[RunTaskAllocationsSessionClientMessage, None, None]
        ):
            yield RunTaskAllocationsSessionClientMessage(
                open_session_request=OpenSessionRequest(
                    session_id="test_session",
                )
            )
            yield RunTaskAllocationsSessionClientMessage(
                leave_session_request=LeaveSessionRequest(
                    close=False,
                )
            )

        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(self, stub)

                for is_new_session in (True, False):
                    server_messages: Generator[
                        RunTaskAllocationsSessionServerMessage
                    ] = stub.run_task_allocations_session(client_messages())

                    response: RunTaskAllocationsSessionServerMessage = next(
                        server_messages
                    )
                    self.assertTrue(response.HasField("open_session_response"))
                    open_session_response: OpenSessionResponse = (
                        response.open_session_response
                    )
                    self.assertTrue(open_session_response.HasField("status"))
                    self.assertEqual(open_session_response.status.code, Code.OK)
                    self.assertEqual(open_session_response.is_new, is_new_session)

                    response: RunTaskAllocationsSessionServerMessage = next(
                        server_messages
                    )
                    self.assertTrue(response.HasField("leave_session_response"))
                    leave_session_response: LeaveSessionResponse = (
                        response.leave_session_response
                    )
                    self.assertTrue(leave_session_response.HasField("status"))
                    self.assertEqual(leave_session_response.status.code, Code.OK)

    def test_failure_when_first_message_not_open_session(self):
        def client_messages() -> (
            Generator[RunTaskAllocationsSessionClientMessage, None, None]
        ):
            yield RunTaskAllocationsSessionClientMessage(
                upload_serialized_object_request=UploadSerializedObjectRequest(
                    manifest=SerializedObjectManifest(
                        id=SerializedObjectID(value="test_object_id"),
                        encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT,
                        encoding_version=0,
                        size=9,
                        sha256_hash="test_hash",
                    ),
                )
            )

        with FunctionExecutorProcessContextManager() as process:
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
                    open_session_response.status.code,
                    Code.INVALID_ARGUMENT,
                )

    def test_failure_when_joining_same_session_second_time_without_disconnect(self):
        def client_messages() -> (
            Generator[RunTaskAllocationsSessionClientMessage, None, None]
        ):
            yield RunTaskAllocationsSessionClientMessage(
                open_session_request=OpenSessionRequest(
                    session_id="test_session",
                )
            )

        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(self, stub)

                server_messages_conn_a: Generator[
                    RunTaskAllocationsSessionServerMessage
                ] = stub.run_task_allocations_session(client_messages())
                response: RunTaskAllocationsSessionServerMessage = next(
                    server_messages_conn_a
                )
                self.assertTrue(response.HasField("open_session_response"))
                open_session_response: OpenSessionResponse = (
                    response.open_session_response
                )
                self.assertEqual(open_session_response.status.code, Code.OK)

                server_messages_conn_b: Generator[
                    RunTaskAllocationsSessionServerMessage
                ] = stub.run_task_allocations_session(client_messages())
                response: RunTaskAllocationsSessionServerMessage = next(
                    server_messages_conn_b
                )
                self.assertTrue(response.HasField("open_session_response"))
                open_session_response: OpenSessionResponse = (
                    response.open_session_response
                )
                self.assertEqual(
                    open_session_response.status.code, Code.FAILED_PRECONDITION
                )

    def test_failure_when_called_before_initialize(self):
        def client_messages() -> (
            Generator[RunTaskAllocationsSessionClientMessage, None, None]
        ):
            yield RunTaskAllocationsSessionClientMessage(
                open_session_request=OpenSessionRequest(
                    session_id="test_session",
                )
            )

        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                server_messages: Generator[RunTaskAllocationsSessionServerMessage] = (
                    stub.run_task_allocations_session(client_messages())
                )
                response: RunTaskAllocationsSessionServerMessage = next(server_messages)
                self.assertTrue(response.HasField("open_session_response"))
                open_session_response: OpenSessionResponse = (
                    response.open_session_response
                )
                self.assertEqual(
                    open_session_response.status.code,
                    Code.FAILED_PRECONDITION,
                )


class TestUploadLargeFileToServer(unittest.TestCase):
    pass  # TODO


class TestUploadLargeFileToClient(unittest.TestCase):
    pass  # TODO


if __name__ == "__main__":
    unittest.main()
