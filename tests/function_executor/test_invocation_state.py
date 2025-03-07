import threading
import time
import unittest
from typing import Any, Dict, Generator, Iterator, List

import grpc
from pydantic import BaseModel
from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    deserialized_function_output,
    rpc_channel,
    run_task,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    GetInvocationStateRequest,
    GetInvocationStateResponse,
    InitializeRequest,
    InitializeResponse,
    InvocationStateRequest,
    InvocationStateResponse,
    RunTaskResponse,
    SerializedObject,
    SetInvocationStateRequest,
    SetInvocationStateResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.functions_sdk.functions import (
    GraphInvocationContext,
    tensorlake_function,
)
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer


class StructuredField(BaseModel):
    list: List[int]
    dictionary: Dict[str, Any]


class StructuredState(BaseModel):
    string: str
    integer: int
    structured: StructuredField


def invocation_state_client_stub(
    test_case: unittest.TestCase,
    stub: FunctionExecutorStub,
    expected_requests: List[InvocationStateRequest],
    responses: List[InvocationStateResponse],
) -> threading.Thread:
    server_request_iterator = stub.initialize_invocation_state_server(iter(responses))

    def loop():
        for expected_request in expected_requests:
            request = next(server_request_iterator)
            request: InvocationStateRequest
            test_case.assertEqual(request.request_id, expected_request.request_id)
            test_case.assertEqual(request.task_id, expected_request.task_id)
            if request.HasField("set"):
                test_case.assertEqual(request.set.key, expected_request.set.key)
                # Two different serialized objects are not equal so we need to deserialize them.
                test_case.assertEqual(
                    CloudPickleSerializer.deserialize(request.set.value.bytes),
                    CloudPickleSerializer.deserialize(expected_request.set.value.bytes),
                )
            else:
                test_case.assertEqual(request.get.key, expected_request.get.key)

    invocation_state_client_thread = threading.Thread(target=loop)
    invocation_state_client_thread.start()
    return invocation_state_client_thread


class TestSetInvocationState(unittest.TestCase):
    def _create_graph(self):
        @tensorlake_function(inject_ctx=True)
        def set_invocation_state(ctx: GraphInvocationContext, x: int) -> str:
            ctx.invocation_state.set(
                "test_state_key",
                StructuredState(
                    string="hello",
                    integer=x,
                    structured=StructuredField(
                        list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                    ),
                ),
            )
            return "success"

        return Graph(
            name="TestSetInvocationState",
            description="test",
            start_node=set_invocation_state,
        )

    def _initialize_function_executor(self, stub: FunctionExecutorStub):
        graph = self._create_graph()
        initialize_response: InitializeResponse = stub.initialize(
            InitializeRequest(
                namespace="test",
                graph_name="test",
                graph_version="1",
                function_name="set_invocation_state",
                graph=SerializedObject(
                    bytes=CloudPickleSerializer.serialize(
                        graph.serialize(additional_modules=[])
                    ),
                    content_type=CloudPickleSerializer.content_type,
                ),
            )
        )
        self.assertTrue(initialize_response.success)

    def test_success(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(stub)
                expected_requests = [
                    InvocationStateRequest(
                        request_id="0",
                        task_id="test-task",
                        set=SetInvocationStateRequest(
                            key="test_state_key",
                            value=SerializedObject(
                                content_type=CloudPickleSerializer.content_type,
                                bytes=CloudPickleSerializer.serialize(
                                    StructuredState(
                                        string="hello",
                                        integer=42,
                                        structured=StructuredField(
                                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                                        ),
                                    )
                                ),
                            ),
                        ),
                    ),
                ]
                responses = [
                    InvocationStateResponse(
                        request_id="0", success=True, set=SetInvocationStateResponse()
                    ),
                ]
                client_thread = invocation_state_client_stub(
                    self, stub, expected_requests, responses
                )
                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="set_invocation_state", input=42
                )
                self.assertTrue(run_task_response.success)
                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_output
                )
                self.assertEqual(len(fn_outputs), 1)
                self.assertEqual("success", fn_outputs[0])

                print(
                    "Joining invocation state client thread, it should exit immediately..."
                )
                client_thread.join()

    def test_client_failure(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 1
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(stub)
                expected_requests = [
                    InvocationStateRequest(
                        request_id="0",
                        task_id="test-task",
                        set=SetInvocationStateRequest(
                            key="test_state_key",
                            value=SerializedObject(
                                content_type=CloudPickleSerializer.content_type,
                                bytes=CloudPickleSerializer.serialize(
                                    StructuredState(
                                        string="hello",
                                        integer=42,
                                        structured=StructuredField(
                                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                                        ),
                                    )
                                ),
                            ),
                        ),
                    ),
                ]
                responses = [
                    InvocationStateResponse(
                        request_id="0", success=False, set=SetInvocationStateResponse()
                    ),
                ]
                client_thread = invocation_state_client_stub(
                    self, stub, expected_requests, responses
                )
                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="set_invocation_state", input=42
                )
                self.assertFalse(run_task_response.success)
                self.assertTrue(
                    'RuntimeError("failed to set the invocation state for key")'
                    in run_task_response.stderr
                )

                print(
                    "Joining invocation state client thread, it should exit immediately..."
                )
                client_thread.join()


class TestGetInvocationState(unittest.TestCase):
    def _create_graph_with_result_validation(self):
        @tensorlake_function(inject_ctx=True)
        def get_invocation_state(ctx: GraphInvocationContext, x: int) -> str:
            got_state: StructuredState = ctx.invocation_state.get("test_state_key")
            expected_state: StructuredState = StructuredState(
                string="hello",
                integer=x,
                structured=StructuredField(list=[1, 2, 3], dictionary={"a": 1, "b": 2}),
            )
            return "success" if got_state == expected_state else "failure"

        return Graph(
            name="TestGetInvocationState",
            description="test",
            start_node=get_invocation_state,
        )

    def _initialize_function_executor(self, graph: Graph, stub: FunctionExecutorStub):
        initialize_response: InitializeResponse = stub.initialize(
            InitializeRequest(
                namespace="test",
                graph_name="test",
                graph_version="1",
                function_name="get_invocation_state",
                graph=SerializedObject(
                    bytes=CloudPickleSerializer.serialize(
                        graph.serialize(additional_modules=[])
                    ),
                    content_type=CloudPickleSerializer.content_type,
                ),
            )
        )
        self.assertTrue(initialize_response.success)

    def test_success(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 2
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(
                    self._create_graph_with_result_validation(), stub
                )
                expected_requests = [
                    InvocationStateRequest(
                        request_id="0",
                        task_id="test-task",
                        get=GetInvocationStateRequest(
                            key="test_state_key",
                        ),
                    ),
                ]
                responses = [
                    InvocationStateResponse(
                        request_id="0",
                        success=True,
                        get=GetInvocationStateResponse(
                            key="test_state_key",
                            value=SerializedObject(
                                content_type=CloudPickleSerializer.content_type,
                                bytes=CloudPickleSerializer.serialize(
                                    StructuredState(
                                        string="hello",
                                        integer=33,
                                        structured=StructuredField(
                                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                                        ),
                                    )
                                ),
                            ),
                        ),
                    ),
                ]
                client_thread = invocation_state_client_stub(
                    self, stub, expected_requests, responses
                )
                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="get_invocation_state", input=33
                )
                self.assertTrue(run_task_response.success)
                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_output
                )
                self.assertEqual(len(fn_outputs), 1)
                self.assertEqual("success", fn_outputs[0])

                print(
                    "Joining invocation state client thread, it should exit immediately..."
                )
                client_thread.join()

    def test_success_none_value(self):
        @tensorlake_function(inject_ctx=True)
        def get_invocation_state(ctx: GraphInvocationContext, x: int) -> str:
            got_state: StructuredState = ctx.invocation_state.get("test_state_key")
            return "success" if got_state is None else "failure"

        graph = Graph(
            name="TestGetInvocationState",
            description="test",
            start_node=get_invocation_state,
        )

        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 3
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(graph, stub)
                expected_requests = [
                    InvocationStateRequest(
                        request_id="0",
                        task_id="test-task",
                        get=GetInvocationStateRequest(
                            key="test_state_key",
                        ),
                    ),
                ]
                responses = [
                    InvocationStateResponse(
                        request_id="0",
                        success=True,
                        get=GetInvocationStateResponse(
                            key="test_state_key",
                            value=None,
                        ),
                    ),
                ]
                client_thread = invocation_state_client_stub(
                    self, stub, expected_requests, responses
                )
                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="get_invocation_state", input=33
                )
                self.assertTrue(run_task_response.success)
                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_output
                )
                self.assertEqual(len(fn_outputs), 1)
                self.assertEqual("success", fn_outputs[0])

                print(
                    "Joining invocation state client thread, it should exit immediately..."
                )
                client_thread.join()

    def test_client_failure(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 4
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(
                    self._create_graph_with_result_validation(), stub
                )
                expected_requests = [
                    InvocationStateRequest(
                        request_id="0",
                        task_id="test-task",
                        get=GetInvocationStateRequest(
                            key="test_state_key",
                        ),
                    ),
                ]
                responses = [
                    InvocationStateResponse(
                        request_id="0",
                        success=False,
                        get=GetInvocationStateResponse(key="test_state_key"),
                    ),
                ]
                client_thread = invocation_state_client_stub(
                    self, stub, expected_requests, responses
                )
                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="get_invocation_state", input=14
                )
                self.assertFalse(run_task_response.success)
                self.assertTrue(
                    'RuntimeError("failed to get the invocation state for key")'
                    in run_task_response.stderr
                )

                print(
                    "Joining invocation state client thread, it should exit immediately..."
                )
                client_thread.join()


class TestInvocationStateServerReconnect(unittest.TestCase):
    def test_second_initialize_invocation_state_server_request_fails(self):
        def infinite_response_generator() -> (
            Generator[InvocationStateResponse, None, None]
        ):
            while True:
                yield InvocationStateResponse(
                    request_id="0", success=True, set=SetInvocationStateResponse()
                )

        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 5
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                first_request_iterator: Iterator[InvocationStateRequest] = (
                    stub.initialize_invocation_state_server(
                        infinite_response_generator()
                    )
                )
                # The fact that first request iterator works correctly is checked in other tests.

                # The second request should fail because there's already a proxy server running.
                second_request_iterator: Iterator[InvocationStateRequest] = (
                    stub.initialize_invocation_state_server(
                        infinite_response_generator()
                    )
                )
                try:
                    for request in second_request_iterator:
                        self.fail(
                            "Second request iterator should not return any requests but should raise an exception"
                        )
                except grpc.RpcError as e:
                    self.assertEqual(grpc.StatusCode.ALREADY_EXISTS, e.code())

    def test_second_initialize_invocation_state_server_request_succeeds_after_channel_close(
        self,
    ):
        def infinite_response_generator() -> (
            Generator[InvocationStateResponse, None, None]
        ):
            while True:
                yield InvocationStateResponse(
                    request_id="0", success=True, set=SetInvocationStateResponse()
                )

        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 5
        ) as fe:
            with rpc_channel(fe) as channel_1:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel_1)
                first_request_iterator: Iterator[InvocationStateRequest] = (
                    stub.initialize_invocation_state_server(
                        infinite_response_generator()
                    )
                )
                # On exit from this with block the channel is closed and proxy server should cleanely shutdown.

            time.sleep(5)  # Wait until the channel closes and proxy server shuts down.

            with rpc_channel(fe) as channel_2:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel_2)
                # The second request should succeed because the first channel was closed with results in clean proxy server shutdown.
                second_request_iterator: Iterator[InvocationStateRequest] = (
                    stub.initialize_invocation_state_server(
                        infinite_response_generator()
                    )
                )

                def thread_func():
                    try:
                        for request in second_request_iterator:
                            self.fail(
                                "Second request iterator should not return any requests"
                            )
                    except grpc.RpcError as e:
                        self.assertEqual(
                            grpc.StatusCode.CANCELLED, e.code()
                        )  # This happens when we close the channel
                    except Exception as e:
                        self.fail(
                            "Second request iterator should not raise any exceptions"
                        )

                thread = threading.Thread(target=thread_func)
                thread.start()
                time.sleep(
                    5
                )  # Wait for the thread to start and check that it doesn't raise any exceptions.
                self.assertTrue(
                    thread.is_alive()
                )  # Check that the thread is still blocked on the iterator without any Exceptions.

            # channel_2 is closed, the thread should return immediately.
            thread.join()


if __name__ == "__main__":
    unittest.main()
