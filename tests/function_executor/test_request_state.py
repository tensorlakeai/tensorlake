import os
import unittest
from typing import Iterator

import grpc
from models import StructuredField, StructuredState
from testing import (
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    create_tmp_blob,
    download_and_deserialize_so,
    initialize,
    read_tmp_blob_bytes,
    rpc_channel,
    run_allocation,
    wait_result_of_allocation_that_returns_output,
    write_tmp_blob_bytes,
)

from tensorlake.applications import (
    InternalError,
    RequestContext,
    application,
    function,
)
from tensorlake.applications.user_data_serializer import (
    PickleUserDataSerializer,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationOutcomeCode,
    AllocationRequestStateCommitWriteOperationResult,
    AllocationRequestStateOperation,
    AllocationRequestStateOperationResult,
    AllocationRequestStatePrepareReadOperationResult,
    AllocationRequestStatePrepareWriteOperationResult,
    AllocationResult,
    AllocationState,
    AllocationUpdate,
    CreateAllocationRequest,
    InitializationOutcomeCode,
    InitializeResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.status_pb2 import (
    Status,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


@application()
@function()
def set_request_state(x: int) -> str:
    ctx: RequestContext = RequestContext.get()
    try:
        ctx.state.set(
            "test_state_key",
            StructuredState(
                string="hello",
                integer=x,
                structured=StructuredField(list=[1, 2, 3], dictionary={"a": 1, "b": 2}),
            ),
        )
        return "success"
    except InternalError as e:
        return str(e)


class TestSetRequestState(unittest.TestCase):
    def _initialize_function_executor(self, stub: FunctionExecutorStub):
        initialize_response: InitializeResponse = initialize(
            stub,
            app_name="set_request_state",
            app_version="0.1",
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
                allocation_id: str = "test-allocation-id"
                allocation_states: Iterator[AllocationState] = run_allocation(
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(42),
                        ),
                    ),
                )

                request_state_blob: BLOB | None = None
                current_allocation_state = "wait_prepare_request_state_write_operation"
                for allocation_state in allocation_states:
                    allocation_state: AllocationState

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_write_operation"
                    ):
                        if len(allocation_state.request_state_operations) == 0:
                            continue
                        self.assertEqual(
                            len(allocation_state.request_state_operations), 1
                        )
                        operation: AllocationRequestStateOperation = (
                            allocation_state.request_state_operations[0]
                        )
                        self.assertTrue(operation.HasField("prepare_write"))
                        self.assertEqual(
                            operation.state_key,
                            "test_state_key",
                        )
                        request_state_blob = create_tmp_blob(
                            id=f"request_state/{operation.state_key}",
                            chunks_count=1,
                            chunk_size=operation.prepare_write.size,
                        )
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                request_state_operation_result=AllocationRequestStateOperationResult(
                                    operation_id=operation.operation_id,
                                    status=Status(
                                        code=grpc.StatusCode.OK.value[0],
                                    ),
                                    prepare_write=AllocationRequestStatePrepareWriteOperationResult(
                                        blob=request_state_blob,
                                    ),
                                ),
                            )
                        )
                        current_allocation_state = (
                            "wait_prepare_request_state_write_operation_deletion"
                        )

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_write_operation_deletion"
                    ):
                        found_prepare_write_operation: bool = False
                        for operation in allocation_state.request_state_operations:
                            operation: AllocationRequestStateOperation
                            if operation.HasField("prepare_write"):
                                found_prepare_write_operation = True
                                break
                        if found_prepare_write_operation:
                            continue
                        else:
                            current_allocation_state = (
                                "wait_commit_request_state_write_operation"
                            )

                    if (
                        current_allocation_state
                        == "wait_commit_request_state_write_operation"
                    ):
                        if len(allocation_state.request_state_operations) == 0:
                            continue
                        self.assertEqual(
                            len(allocation_state.request_state_operations), 1
                        )
                        operation: AllocationRequestStateOperation = (
                            allocation_state.request_state_operations[0]
                        )
                        self.assertTrue(operation.HasField("commit_write"))
                        self.assertEqual(
                            operation.state_key,
                            "test_state_key",
                        )
                        # Commit is a noop for local blob store.
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                request_state_operation_result=AllocationRequestStateOperationResult(
                                    operation_id=operation.operation_id,
                                    status=Status(
                                        code=grpc.StatusCode.OK.value[0],
                                    ),
                                    commit_write=AllocationRequestStateCommitWriteOperationResult(),
                                ),
                            )
                        )
                        break

                alloc_result: AllocationResult = (
                    wait_result_of_allocation_that_returns_output(
                        allocation_id,
                        self,
                        stub,
                        timeout_sec=None,
                    )
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

                self.assertIsNotNone(request_state_blob)
                serialized_request_state: bytes = read_tmp_blob_bytes(
                    request_state_blob, offset=0, size=request_state_blob.chunks[0].size
                )
                deserialized_request_state: (
                    StructuredState
                ) = PickleUserDataSerializer().deserialize(
                    serialized_request_state,
                    possible_types=[StructuredState],
                )
                self.assertEqual(
                    StructuredState(
                        string="hello",
                        integer=42,
                        structured=StructuredField(
                            list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                        ),
                    ).model_dump(),
                    deserialized_request_state.model_dump(),
                )

    def test_prepare_write_operation_failed(self):
        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                self._initialize_function_executor(stub)
                allocation_id: str = "test-allocation-id"
                allocation_states: Iterator[AllocationState] = run_allocation(
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(42),
                        ),
                    ),
                )

                current_allocation_state = "wait_prepare_request_state_write_operation"
                for allocation_state in allocation_states:
                    allocation_state: AllocationState

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_write_operation"
                    ):
                        if len(allocation_state.request_state_operations) == 0:
                            continue
                        self.assertEqual(
                            len(allocation_state.request_state_operations), 1
                        )
                        operation: AllocationRequestStateOperation = (
                            allocation_state.request_state_operations[0]
                        )
                        self.assertTrue(operation.HasField("prepare_write"))
                        self.assertEqual(
                            operation.state_key,
                            "test_state_key",
                        )
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                request_state_operation_result=AllocationRequestStateOperationResult(
                                    operation_id=operation.operation_id,
                                    status=Status(
                                        code=grpc.StatusCode.INTERNAL.value[0],
                                    ),
                                ),
                            )
                        )
                        break

                alloc_result: AllocationResult = (
                    wait_result_of_allocation_that_returns_output(
                        allocation_id,
                        self,
                        stub,
                        timeout_sec=None,
                    )
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
                self.assertEqual(
                    "Failed to set request state for key 'test_state_key'.",
                    output,
                )


@application()
@function()
def check_request_state_is_expected(x: int) -> str:
    ctx: RequestContext = RequestContext.get()
    try:
        got_state: StructuredState = ctx.state.get("test_state_key")
    except InternalError as e:
        return str(e)

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


@application()
@function()
def check_request_state_is_none(x: int) -> str:
    ctx: RequestContext = RequestContext.get()
    got_state: StructuredState = ctx.state.get("test_state_key")
    return "success" if got_state is None else "failure"


class TestGetRequestState(unittest.TestCase):
    def test_read_expected_state_value(self):
        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="check_request_state_is_expected",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="check_request_state_is_expected",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                allocation_id: str = "test-allocation"
                allocation_states: Iterator[AllocationState] = run_allocation(
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(33),
                        ),
                    ),
                )

                current_allocation_state = "wait_prepare_request_state_read_operation"
                for allocation_state in allocation_states:
                    allocation_state: AllocationState

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_read_operation"
                    ):
                        if len(allocation_state.request_state_operations) == 0:
                            continue
                        self.assertEqual(
                            len(allocation_state.request_state_operations), 1
                        )
                        operation: AllocationRequestStateOperation = (
                            allocation_state.request_state_operations[0]
                        )
                        self.assertTrue(operation.HasField("prepare_read"))
                        self.assertEqual(
                            operation.state_key,
                            "test_state_key",
                        )
                        request_state_read_result: (
                            bytes
                        ) = PickleUserDataSerializer().serialize(
                            StructuredState(
                                string="hello",
                                integer=33,
                                structured=StructuredField(
                                    list=[1, 2, 3], dictionary={"a": 1, "b": 2}
                                ),
                            )
                        )
                        request_state_blob: BLOB = create_tmp_blob(
                            id=f"request_state/{operation.state_key}",
                            chunks_count=1,
                            chunk_size=len(request_state_read_result),
                        )
                        write_tmp_blob_bytes(
                            request_state_blob, request_state_read_result
                        )
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                request_state_operation_result=AllocationRequestStateOperationResult(
                                    operation_id=operation.operation_id,
                                    status=Status(
                                        code=grpc.StatusCode.OK.value[0],
                                    ),
                                    prepare_read=AllocationRequestStatePrepareReadOperationResult(
                                        blob=request_state_blob,
                                    ),
                                ),
                            )
                        )
                        current_allocation_state = (
                            "wait_prepare_request_state_read_operation_deletion"
                        )

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_read_operation_deletion"
                    ):
                        found_prepare_read_operation: bool = False
                        for operation in allocation_state.request_state_operations:
                            operation: AllocationRequestStateOperation
                            if operation.HasField("prepare_read"):
                                found_prepare_read_operation = True
                                break
                        if found_prepare_read_operation:
                            continue
                        else:
                            break

                alloc_result: AllocationResult = (
                    wait_result_of_allocation_that_returns_output(
                        allocation_id,
                        self,
                        stub,
                        timeout_sec=None,
                    )
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

    def test_read_default_none(self):
        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="check_request_state_is_none",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="check_request_state_is_none",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                allocation_id: str = "test-allocation"
                allocation_states: Iterator[AllocationState] = run_allocation(
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(33),
                        ),
                    ),
                )

                current_allocation_state = "wait_prepare_request_state_read_operation"
                for allocation_state in allocation_states:
                    allocation_state: AllocationState

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_read_operation"
                    ):
                        if len(allocation_state.request_state_operations) == 0:
                            continue
                        self.assertEqual(
                            len(allocation_state.request_state_operations), 1
                        )
                        operation: AllocationRequestStateOperation = (
                            allocation_state.request_state_operations[0]
                        )
                        self.assertTrue(operation.HasField("prepare_read"))
                        self.assertEqual(
                            operation.state_key,
                            "test_state_key",
                        )
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                request_state_operation_result=AllocationRequestStateOperationResult(
                                    operation_id=operation.operation_id,
                                    status=Status(
                                        code=grpc.StatusCode.NOT_FOUND.value[0],
                                    ),
                                    prepare_read=AllocationRequestStatePrepareReadOperationResult(),
                                ),
                            )
                        )
                        current_allocation_state = (
                            "wait_prepare_request_state_read_operation_deletion"
                        )

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_read_operation_deletion"
                    ):
                        found_prepare_read_operation: bool = False
                        for operation in allocation_state.request_state_operations:
                            operation: AllocationRequestStateOperation
                            if operation.HasField("prepare_read"):
                                found_prepare_read_operation = True
                                break
                        if found_prepare_read_operation:
                            continue
                        else:
                            break

                alloc_result: AllocationResult = (
                    wait_result_of_allocation_that_returns_output(
                        allocation_id,
                        self,
                        stub,
                        timeout_sec=None,
                    )
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

    def test_prepare_read_operation_failed(self):
        with FunctionExecutorProcessContextManager() as fe:
            with rpc_channel(fe) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="check_request_state_is_expected",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="check_request_state_is_expected",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                allocation_id: str = "test-allocation"
                allocation_states: Iterator[AllocationState] = run_allocation(
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(33),
                        ),
                    ),
                )

                current_allocation_state = "wait_prepare_request_state_read_operation"
                for allocation_state in allocation_states:
                    allocation_state: AllocationState

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_read_operation"
                    ):
                        if len(allocation_state.request_state_operations) == 0:
                            continue
                        self.assertEqual(
                            len(allocation_state.request_state_operations), 1
                        )
                        operation: AllocationRequestStateOperation = (
                            allocation_state.request_state_operations[0]
                        )
                        self.assertTrue(operation.HasField("prepare_read"))
                        self.assertEqual(
                            operation.state_key,
                            "test_state_key",
                        )
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                request_state_operation_result=AllocationRequestStateOperationResult(
                                    operation_id=operation.operation_id,
                                    status=Status(
                                        code=grpc.StatusCode.INTERNAL.value[0],
                                    ),
                                    prepare_read=AllocationRequestStatePrepareReadOperationResult(),
                                ),
                            )
                        )
                        current_allocation_state = (
                            "wait_prepare_request_state_read_operation_deletion"
                        )

                    if (
                        current_allocation_state
                        == "wait_prepare_request_state_read_operation_deletion"
                    ):
                        found_prepare_read_operation: bool = False
                        for operation in allocation_state.request_state_operations:
                            operation: AllocationRequestStateOperation
                            if operation.HasField("prepare_read"):
                                found_prepare_read_operation = True
                                break
                        if found_prepare_read_operation:
                            continue
                        else:
                            break

                alloc_result: AllocationResult = (
                    wait_result_of_allocation_that_returns_output(
                        allocation_id,
                        self,
                        stub,
                        timeout_sec=None,
                    )
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
                self.assertEqual(
                    "Failed to get request state for key 'test_state_key'.",
                    output,
                )


if __name__ == "__main__":
    unittest.main()
