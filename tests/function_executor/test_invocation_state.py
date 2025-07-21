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
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    InvocationStateRequest,
    InvocationStateResponse,
    RunTaskResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SetInvocationStateRequest,
    SetInvocationStateResponse,
    TaskOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.functions_sdk.functions import (
    GraphRequestContext,
    tensorlake_function,
)
from tensorlake.functions_sdk.graph_serialization import (
    ZIPPED_GRAPH_CODE_CONTENT_TYPE,
    graph_code_dir_path,
    zip_graph_code,
)
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer

GRAPH_CODE_DIR_PATH = graph_code_dir_path(__file__)


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
                # Two different serialized objects are not equal so we need to deserialize them and dump
                # into models that have corretly functioning equality operator.
                test_case.assertEqual(
                    CloudPickleSerializer.deserialize(
                        request.set.value.data
                    ).model_dump(),
                    CloudPickleSerializer.deserialize(
                        expected_request.set.value.data
                    ).model_dump(),
                )
            else:
                test_case.assertEqual(request.get.key, expected_request.get.key)

    invocation_state_client_thread = threading.Thread(target=loop)
    invocation_state_client_thread.start()
    return invocation_state_client_thread


@tensorlake_function(inject_ctx=True)
def set_invocation_state(ctx: GraphRequestContext, x: int) -> str:
    ctx.request_state.set(
        "test_state_key",
        StructuredState(
            string="hello",
            integer=x,
            structured=StructuredField(list=[1, 2, 3], dictionary={"a": 1, "b": 2}),
        ),
    )
    return "success"


class TestSetInvocationState(unittest.TestCase):
    def _initialize_function_executor(self, stub: FunctionExecutorStub):
        graph = Graph(
            name="TestSetInvocationState",
            description="test",
            start_node=set_invocation_state,
        )
        initialize_response: InitializeResponse = stub.initialize(
            InitializeRequest(
                namespace="test",
                graph_name="test",
                graph_version="1",
                function_name="set_invocation_state",
                graph=SerializedObject(
                    data=zip_graph_code(graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH),
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                    encoding_version=0,
                ),
            )
        )
        self.assertEqual(
            initialize_response.outcome_code,
            InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
        )

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
                                data=CloudPickleSerializer.serialize(
                                    StructuredState(
                                        string="hello",
                                        integer=42,
                                        structured=StructuredField(
                                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                                        ),
                                    )
                                ),
                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                encoding_version=0,
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
                self.assertEqual(
                    run_task_response.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
                )
                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_outputs
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
                                data=CloudPickleSerializer.serialize(
                                    StructuredState(
                                        string="hello",
                                        integer=42,
                                        structured=StructuredField(
                                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                                        ),
                                    )
                                ),
                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                encoding_version=0,
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
                self.assertEqual(
                    run_task_response.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
                )
                self.assertTrue(
                    'RuntimeError("failed to set the invocation state for key")'
                    in run_task_response.stderr
                )

                print(
                    "Joining invocation state client thread, it should exit immediately..."
                )
                client_thread.join()


@tensorlake_function(inject_ctx=True)
def check_invocation_state_is_expected(ctx: GraphRequestContext, x: int) -> str:
    got_state: StructuredState = ctx.request_state.get("test_state_key")
    expected_state: StructuredState = StructuredState(
        string="hello",
        integer=x,
        structured=StructuredField(list=[1, 2, 3], dictionary={"a": 1, "b": 2}),
    )
    return (
        "success"
        if got_state.model_dump() == expected_state.model_dump()
        else "failure"
    )


@tensorlake_function(inject_ctx=True)
def check_invocation_state_is_none(ctx: GraphRequestContext, x: int) -> str:
    got_state: StructuredState = ctx.request_state.get("test_state_key")
    return "success" if got_state is None else "failure"


class TestGetInvocationState(unittest.TestCase):
    def _create_graph_with_result_validation(self):
        return Graph(
            name="TestGetInvocationState",
            description="test",
            start_node=check_invocation_state_is_expected,
        )

    def _initialize_function_executor(
        self, graph: Graph, function_name: str, stub: FunctionExecutorStub
    ):
        initialize_response: InitializeResponse = stub.initialize(
            InitializeRequest(
                namespace="test",
                graph_name="test",
                graph_version="1",
                function_name=function_name,
                graph=SerializedObject(
                    data=zip_graph_code(graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH),
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                    encoding_version=0,
                ),
            )
        )
        self.assertEqual(
            initialize_response.outcome_code,
            InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
        )

    def test_success(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 2
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(
                    self._create_graph_with_result_validation(),
                    "check_invocation_state_is_expected",
                    stub,
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
                                data=CloudPickleSerializer.serialize(
                                    StructuredState(
                                        string="hello",
                                        integer=33,
                                        structured=StructuredField(
                                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                                        ),
                                    )
                                ),
                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                encoding_version=0,
                            ),
                        ),
                    ),
                ]
                client_thread = invocation_state_client_stub(
                    self, stub, expected_requests, responses
                )
                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="check_invocation_state_is_expected", input=33
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

                print(
                    "Joining invocation state client thread, it should exit immediately..."
                )
                client_thread.join()

    def test_success_none_value(self):
        graph = Graph(
            name="TestGetInvocationState",
            description="test",
            start_node=check_invocation_state_is_none,
        )

        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 3
        ) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(
                    graph, "check_invocation_state_is_none", stub
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
                            value=None,
                        ),
                    ),
                ]
                client_thread = invocation_state_client_stub(
                    self, stub, expected_requests, responses
                )
                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="check_invocation_state_is_none", input=33
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
                    self._create_graph_with_result_validation(),
                    "check_invocation_state_is_expected",
                    stub,
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
                    stub, function_name="check_invocation_state_is_expected", input=14
                )
                self.assertEqual(
                    run_task_response.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
                )
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
