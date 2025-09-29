import hashlib
import os
import threading
import time
import unittest
from typing import Generator, Iterator, List

import grpc
from models import StructuredField, StructuredState
from testing import (
    FunctionExecutorProcessContextManager,
    api_function_inputs,
    download_and_deserialize_so,
    initialize,
    rpc_channel,
    run_allocation,
)

# This import will be replaced by `import tensorlake` when we switch to the new SDK UX.
import tensorlake.applications.interface as tensorlake
from tensorlake.applications.user_data_serializer import (
    PickleUserDataSerializer,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutcomeCode,
    AllocationResult,
    GetRequestStateRequest,
    GetRequestStateResponse,
    InitializationOutcomeCode,
    InitializeResponse,
    RequestStateRequest,
    RequestStateResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
    SetRequestStateRequest,
    SetRequestStateResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

app: tensorlake.Application = tensorlake.define_application(name=__file__)


def request_state_client_stub(
    test_case: unittest.TestCase,
    stub: FunctionExecutorStub,
    expected_requests: List[RequestStateRequest],
    responses: List[RequestStateResponse],
) -> threading.Thread:
    server_request_iterator = stub.initialize_request_state_server(iter(responses))

    def loop():
        for expected_request in expected_requests:
            request = next(server_request_iterator)
            request: RequestStateRequest
            test_case.assertEqual(
                request.state_request_id, expected_request.state_request_id
            )
            test_case.assertEqual(request.allocation_id, expected_request.allocation_id)
            if request.HasField("set"):
                test_case.assertEqual(request.set.key, expected_request.set.key)
                # Two different serialized objects are not equal so we need to deserialize them and dump
                # into models that have corretly functioning equality operator.
                test_case.assertEqual(
                    PickleUserDataSerializer()
                    .deserialize(
                        request.set.value.data,
                        possible_types=[],
                    )
                    .model_dump(),
                    PickleUserDataSerializer()
                    .deserialize(
                        expected_request.set.value.data,
                        possible_types=[],
                    )
                    .model_dump(),
                )
            else:
                test_case.assertEqual(request.get.key, expected_request.get.key)

    request_state_client_thread = threading.Thread(target=loop)
    request_state_client_thread.start()
    return request_state_client_thread


@tensorlake.api()
@tensorlake.function()
def set_request_state(ctx: tensorlake.RequestContext, x: int) -> str:
    ctx.state.set(
        "test_state_key",
        StructuredState(
            string="hello",
            integer=x,
            structured=StructuredField(list=[1, 2, 3], dictionary={"a": 1, "b": 2}),
        ),
    )
    return "success"


class TestSetRequestState(unittest.TestCase):
    def _initialize_function_executor(self, stub: FunctionExecutorStub):
        initialize_response: InitializeResponse = initialize(
            stub,
            app=app,
            app_code_dir_path=APPLICATION_CODE_DIR_PATH,
            function_name="set_request_state",
        )
        self.assertEqual(
            initialize_response.outcome_code,
            InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
        )

    def test_success(self):
        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(stub)
                data: bytes = PickleUserDataSerializer().serialize(
                    StructuredState(
                        string="hello",
                        integer=42,
                        structured=StructuredField(
                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                        ),
                    )
                )
                expected_requests = [
                    RequestStateRequest(
                        state_request_id="0",
                        allocation_id="test-allocation",
                        set=SetRequestStateRequest(
                            key="test_state_key",
                            value=SerializedObject(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                    encoding_version=0,
                                    size=len(data),
                                    sha256_hash=hashlib.sha256(data).hexdigest(),
                                ),
                                data=data,
                            ),
                        ),
                    ),
                ]
                responses = [
                    RequestStateResponse(
                        state_request_id="0",
                        success=True,
                        set=SetRequestStateResponse(),
                    ),
                ]
                client_thread = request_state_client_stub(
                    self, stub, expected_requests, responses
                )
                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=api_function_inputs(42),
                )
                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                output: str = download_and_deserialize_so(
                    self,
                    alloc_result.value,
                    alloc_result.uploaded_function_outputs_blob,
                )
                self.assertEqual("success", output)

                print(
                    "Joining request state client thread, it should exit immediately..."
                )
                client_thread.join()

    def test_client_failure(self):
        with FunctionExecutorProcessContextManager(capture_std_outputs=True) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(stub)
                data: bytes = PickleUserDataSerializer().serialize(
                    StructuredState(
                        string="hello",
                        integer=42,
                        structured=StructuredField(
                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                        ),
                    )
                )
                expected_requests = [
                    RequestStateRequest(
                        state_request_id="0",
                        allocation_id="test-allocation",
                        set=SetRequestStateRequest(
                            key="test_state_key",
                            value=SerializedObject(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                    encoding_version=0,
                                    size=len(data),
                                    sha256_hash=hashlib.sha256(data).hexdigest(),
                                ),
                                data=data,
                            ),
                        ),
                    ),
                ]
                responses = [
                    RequestStateResponse(
                        state_request_id="0",
                        success=False,
                        set=SetRequestStateResponse(),
                    ),
                ]
                client_thread = request_state_client_stub(
                    self, stub, expected_requests, responses
                )
                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=api_function_inputs(42),
                )
                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )

                print(
                    "Joining request state client thread, it should exit immediately..."
                )
                client_thread.join()

        self.assertIn(
            'RuntimeError("failed to set the request state for key")',
            fe.read_stderr(),
        )


@tensorlake.api()
@tensorlake.function()
def check_request_state_is_expected(ctx: tensorlake.RequestContext, x: int) -> str:
    got_state: StructuredState = ctx.state.get("test_state_key")
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


@tensorlake.api()
@tensorlake.function()
def check_request_state_is_none(ctx: tensorlake.RequestContext, x: int) -> str:
    got_state: StructuredState = ctx.state.get("test_state_key")
    return "success" if got_state is None else "failure"


class TestGetInvocationState(unittest.TestCase):
    def test_success(self):
        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app=app,
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="check_request_state_is_expected",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                expected_requests = [
                    RequestStateRequest(
                        state_request_id="0",
                        allocation_id="test-allocation",
                        get=GetRequestStateRequest(
                            key="test_state_key",
                        ),
                    ),
                ]
                data: bytes = PickleUserDataSerializer().serialize(
                    StructuredState(
                        string="hello",
                        integer=33,
                        structured=StructuredField(
                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                        ),
                    )
                )
                responses = [
                    RequestStateResponse(
                        state_request_id="0",
                        success=True,
                        get=GetRequestStateResponse(
                            key="test_state_key",
                            value=SerializedObject(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                    encoding_version=0,
                                    size=len(data),
                                    sha256_hash=hashlib.sha256(data).hexdigest(),
                                ),
                                data=data,
                            ),
                        ),
                    ),
                ]
                client_thread = request_state_client_stub(
                    self, stub, expected_requests, responses
                )
                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=api_function_inputs(33),
                )
                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                output: str = download_and_deserialize_so(
                    self,
                    alloc_result.value,
                    alloc_result.uploaded_function_outputs_blob,
                )
                self.assertEqual("success", output)

                print(
                    "Joining request state client thread, it should exit immediately..."
                )
                client_thread.join()

    def test_success_none_value(self):
        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app=app,
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="check_request_state_is_none",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )
                expected_requests = [
                    RequestStateRequest(
                        state_request_id="0",
                        allocation_id="test-allocation",
                        get=GetRequestStateRequest(
                            key="test_state_key",
                        ),
                    ),
                ]
                responses = [
                    RequestStateResponse(
                        state_request_id="0",
                        success=True,
                        get=GetRequestStateResponse(
                            key="test_state_key",
                            value=None,
                        ),
                    ),
                ]
                client_thread = request_state_client_stub(
                    self, stub, expected_requests, responses
                )
                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=api_function_inputs(33),
                )
                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                output: str = download_and_deserialize_so(
                    self,
                    alloc_result.value,
                    alloc_result.uploaded_function_outputs_blob,
                )
                self.assertEqual("success", output)

                print(
                    "Joining request state client thread, it should exit immediately..."
                )
                client_thread.join()

    def test_client_failure(self):
        with FunctionExecutorProcessContextManager(capture_std_outputs=True) as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app=app,
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="check_request_state_is_expected",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )
                expected_requests = [
                    RequestStateRequest(
                        state_request_id="0",
                        allocation_id="test-allocation",
                        get=GetRequestStateRequest(
                            key="test_state_key",
                        ),
                    ),
                ]
                responses = [
                    RequestStateResponse(
                        state_request_id="0",
                        success=False,
                        get=GetRequestStateResponse(key="test_state_key"),
                    ),
                ]
                client_thread = request_state_client_stub(
                    self, stub, expected_requests, responses
                )
                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=api_function_inputs(14),
                )
                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                print(
                    "Joining request state client thread, it should exit immediately..."
                )
                client_thread.join()

        self.assertIn(
            'RuntimeError("failed to get the request state for key")',
            fe.read_stderr(),
        )


class TestRequestStateServerReconnect(unittest.TestCase):
    def test_second_initialize_request_state_server_request_fails(self):
        def infinite_response_generator() -> (
            Generator[RequestStateResponse, None, None]
        ):
            while True:
                yield RequestStateResponse(
                    state_request_id="0", success=True, set=SetRequestStateResponse()
                )

        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                first_request_iterator: Iterator[RequestStateRequest] = (
                    stub.initialize_request_state_server(infinite_response_generator())
                )
                # The fact that first request iterator works correctly is checked in other tests.

                # The second request should fail because there's already a proxy server running.
                second_request_iterator: Iterator[RequestStateRequest] = (
                    stub.initialize_request_state_server(infinite_response_generator())
                )
                try:
                    for request in second_request_iterator:
                        self.fail(
                            "Second request iterator should not return any requests but should raise an exception"
                        )
                except grpc.RpcError as e:
                    self.assertEqual(grpc.StatusCode.ALREADY_EXISTS, e.code())

    def test_second_initialize_request_state_server_request_succeeds_after_channel_close(
        self,
    ):
        def infinite_response_generator() -> (
            Generator[RequestStateResponse, None, None]
        ):
            while True:
                yield RequestStateResponse(
                    state_request_id="0", success=True, set=SetRequestStateResponse()
                )

        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel_1:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel_1)
                first_request_iterator: Iterator[RequestStateRequest] = (
                    stub.initialize_request_state_server(infinite_response_generator())
                )
                # On exit from this with block the channel is closed and proxy server should cleanely shutdown.

            time.sleep(5)  # Wait until the channel closes and proxy server shuts down.

            with rpc_channel(fe) as channel_2:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel_2)
                # The second request should succeed because the first channel was closed with results in clean proxy server shutdown.
                second_request_iterator: Iterator[RequestStateRequest] = (
                    stub.initialize_request_state_server(infinite_response_generator())
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
