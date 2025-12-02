import hashlib
import json
import os
import random
import unittest
from typing import Iterator

import grpc
from testing import (
    FunctionExecutorProcessContextManager,
    create_request_error_blob,
    create_tmp_blob,
    initialize,
    rpc_channel,
    run_allocation,
    write_tmp_blob_bytes,
)

from tensorlake.applications import (
    RequestContext,
    application,
    function,
)
from tensorlake.applications.metadata import (
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ValueMetadata,
    serialize_metadata,
)
from tensorlake.applications.user_data_serializer import (
    PickleUserDataSerializer,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationOutcomeCode,
    AllocationOutputBLOB,
    AllocationOutputBLOBRequest,
    AllocationResult,
    AllocationState,
    AllocationUpdate,
    CreateAllocationRequest,
    FunctionInputs,
    InitializationOutcomeCode,
    InitializeResponse,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.status_pb2 import Status

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


@application()
@function()
def prints_progress_updates(total: int) -> int:
    ctx: RequestContext = RequestContext.get()
    attributes = {"key": "value"}
    for num in range(total):
        ctx.progress.update(
            current=num,
            total=total,
            attributes=attributes,
        )
    return total


@application()
@function()
def prints_progress_updates_with_message(total: int) -> int:
    ctx: RequestContext = RequestContext.get()
    attributes = {"key": "value"}
    for num in range(total):
        ctx.progress.update(
            current=num,
            total=total,
            message=f"this is step {num} of {total} steps in this function",
            attributes=attributes,
        )
    return total


class TestPrintProgressUpdates(unittest.TestCase):
    def test_function_prints_progres_update(self):
        arg = random.randint(1, 10)

        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="prints_progress_updates",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="prints_progress_updates",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                user_serializer: PickleUserDataSerializer = PickleUserDataSerializer()
                serialized_arg: bytes = user_serializer.serialize(arg)
                serialized_arg_metadata: bytes = serialize_metadata(
                    ValueMetadata(
                        id="arg_id",
                        cls=bytes,
                        serializer_name=user_serializer.name,
                        content_type=None,
                    )
                )

                serialized_args: bytes = b"".join(
                    [
                        serialized_arg_metadata,
                        serialized_arg,
                    ]
                )
                input_blob: BLOB = create_tmp_blob(id="input-blob-id")
                write_tmp_blob_bytes(
                    input_blob,
                    serialized_args,
                )

                allocation_id: str = "test-allocation-id"
                allocation_states: Iterator[AllocationState] = run_allocation(
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=FunctionInputs(
                                args=[
                                    SerializedObjectInsideBLOB(
                                        manifest=SerializedObjectManifest(
                                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                            encoding_version=0,
                                            size=len(serialized_arg_metadata)
                                            + len(serialized_arg),
                                            metadata_size=len(serialized_arg_metadata),
                                            sha256_hash=hashlib.sha256(
                                                serialized_arg_metadata + serialized_arg
                                            ).hexdigest(),
                                        ),
                                        offset=0,
                                    )
                                ],
                                arg_blobs=[input_blob],
                                function_call_metadata=serialize_metadata(
                                    FunctionCallMetadata(
                                        id="returns_argument_call",
                                        output_serializer_name_override=None,
                                        args=[
                                            FunctionCallArgumentMetadata(
                                                value_id="arg_id", collection=None
                                            ),
                                        ],
                                        kwargs={},
                                    )
                                ),
                                request_error_blob=create_request_error_blob(),
                            ),
                        ),
                    ),
                )
                current_allocation_state = "wait_blob_request"
                for allocation_state in allocation_states:
                    allocation_state: AllocationState

                    if current_allocation_state == "wait_blob_request":
                        if len(allocation_state.output_blob_requests) == 0:
                            continue  # Received empty initial AllocationState, keep waiting.

                        self.assertEqual(len(allocation_state.output_blob_requests), 1)
                        function_output_blob_request: AllocationOutputBLOBRequest = (
                            allocation_state.output_blob_requests[0]
                        )
                        # Simulate function output BLOB with 10 chunks.
                        function_output_blob: BLOB = create_tmp_blob(
                            id=function_output_blob_request.id,
                            chunks_count=10,
                            chunk_size=1024,
                        )
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                output_blob=AllocationOutputBLOB(
                                    status=Status(code=grpc.StatusCode.OK.value[0]),
                                    blob=function_output_blob,
                                ),
                            )
                        )
                        current_allocation_state = "wait_blob_deletion"

                    if current_allocation_state == "wait_blob_deletion":
                        if len(allocation_state.output_blob_requests) == 0:
                            current_allocation_state = "wait_result"

                    if current_allocation_state == "wait_result":
                        if allocation_state.HasField("result"):
                            alloc_result: AllocationResult = allocation_state.result
                            break

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )

        fe_stdout = process.read_stdout()
        self.assertIn(
            '"type": "ai.tensorlake.progress_update", "source": "/tensorlake/function_executor/runner"',
            fe_stdout,
        )

        for num in range(arg):
            data = {
                "RequestProgressUpdated": {
                    "request_id": "123",
                    "function_name": "prints_progress_updates",
                    "message": f"prints_progress_updates: executing step {num} of {arg}",
                    "step": num,
                    "total": arg,
                    "attributes": {"key": "value"},
                }
            }

            self.assertIn(
                json.dumps(data),
                fe_stdout,
            )

    def test_function_prints_progres_update_with_message(self):
        arg = random.randint(1, 10)

        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="prints_progress_updates_with_message",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="prints_progress_updates_with_message",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                user_serializer: PickleUserDataSerializer = PickleUserDataSerializer()
                serialized_arg: bytes = user_serializer.serialize(arg)
                serialized_arg_metadata: bytes = serialize_metadata(
                    ValueMetadata(
                        id="arg_id",
                        cls=bytes,
                        serializer_name=user_serializer.name,
                        content_type=None,
                    )
                )

                serialized_args: bytes = b"".join(
                    [
                        serialized_arg_metadata,
                        serialized_arg,
                    ]
                )
                input_blob: BLOB = create_tmp_blob(id="input-blob-id")
                write_tmp_blob_bytes(
                    input_blob,
                    serialized_args,
                )

                allocation_id: str = "test-allocation-id"
                allocation_states: Iterator[AllocationState] = run_allocation(
                    stub,
                    request=CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=FunctionInputs(
                                args=[
                                    SerializedObjectInsideBLOB(
                                        manifest=SerializedObjectManifest(
                                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                            encoding_version=0,
                                            size=len(serialized_arg_metadata)
                                            + len(serialized_arg),
                                            metadata_size=len(serialized_arg_metadata),
                                            sha256_hash=hashlib.sha256(
                                                serialized_arg_metadata + serialized_arg
                                            ).hexdigest(),
                                        ),
                                        offset=0,
                                    )
                                ],
                                arg_blobs=[input_blob],
                                function_call_metadata=serialize_metadata(
                                    FunctionCallMetadata(
                                        id="returns_argument_call",
                                        output_serializer_name_override=None,
                                        args=[
                                            FunctionCallArgumentMetadata(
                                                value_id="arg_id", collection=None
                                            ),
                                        ],
                                        kwargs={},
                                    )
                                ),
                                request_error_blob=create_request_error_blob(),
                            ),
                        ),
                    ),
                )
                current_allocation_state = "wait_blob_request"
                for allocation_state in allocation_states:
                    allocation_state: AllocationState

                    if current_allocation_state == "wait_blob_request":
                        if len(allocation_state.output_blob_requests) == 0:
                            continue  # Received empty initial AllocationState, keep waiting.

                        self.assertEqual(len(allocation_state.output_blob_requests), 1)
                        function_output_blob_request: AllocationOutputBLOBRequest = (
                            allocation_state.output_blob_requests[0]
                        )
                        # Simulate function output BLOB with 10 chunks.
                        function_output_blob: BLOB = create_tmp_blob(
                            id=function_output_blob_request.id,
                            chunks_count=10,
                            chunk_size=1024,
                        )
                        stub.send_allocation_update(
                            AllocationUpdate(
                                allocation_id=allocation_id,
                                output_blob=AllocationOutputBLOB(
                                    status=Status(code=grpc.StatusCode.OK.value[0]),
                                    blob=function_output_blob,
                                ),
                            )
                        )
                        current_allocation_state = "wait_blob_deletion"

                    if current_allocation_state == "wait_blob_deletion":
                        if len(allocation_state.output_blob_requests) == 0:
                            current_allocation_state = "wait_result"

                    if current_allocation_state == "wait_result":
                        if allocation_state.HasField("result"):
                            alloc_result: AllocationResult = allocation_state.result
                            break

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )

        fe_stdout = process.read_stdout()
        self.assertIn(
            '"type": "ai.tensorlake.progress_update", "source": "/tensorlake/function_executor/runner"',
            fe_stdout,
        )

        for num in range(arg):
            data = {
                "RequestProgressUpdated": {
                    "request_id": "123",
                    "function_name": "prints_progress_updates_with_message",
                    "message": f"this is step {num} of {arg} steps in this function",
                    "step": num,
                    "total": arg,
                    "attributes": {"key": "value"},
                }
            }

            self.assertIn(
                json.dumps(data),
                fe_stdout,
            )


if __name__ == "__main__":
    unittest.main()
