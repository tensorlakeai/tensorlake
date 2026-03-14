import hashlib
import os
import threading
import unittest
from typing import Iterator, List

import grpc
from models import FileChunk
from testing import (
    AllocationTestDriver,
    FunctionExecutorProcessContextManager,
    HTTPBodyPart,
    application_function_inputs,
    create_multipart_invoke_http_request,
    create_request_error_blob,
    create_tmp_blob,
    download_and_deserialize_so,
    initialize,
    read_so_metadata,
    read_tmp_blob_bytes,
    rpc_channel,
    run_allocation,
    run_allocation_that_fails,
    run_allocation_that_returns_output,
    write_new_application_payload_blob,
    write_tmp_blob_bytes,
)

from tensorlake.applications import (
    File,
    application,
    cls,
    function,
)
from tensorlake.applications.metadata import (
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ValueMetadata,
    deserialize_metadata,
    serialize_metadata,
)
from tensorlake.applications.user_data_serializer import (
    JSONUserDataSerializer,
    PickleUserDataSerializer,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    AdvanceAllocationExecutionLogBatchRequest,
    Allocation,
    AllocationEvent,
    AllocationEventFunctionCallCreated,
    AllocationEventFunctionCallWatcherResult,
    AllocationExecutionEvent,
    AllocationExecutionEventCreateFunctionCall,
    AllocationExecutionEventCreateFunctionCallWatcher,
    AllocationExecutionEventFinishAllocation,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationOutputBLOB,
    AllocationOutputBLOBRequest,
    AllocationState,
    AllocationUpdate,
    BLOBChunk,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    ExecutionPlanUpdate,
    ExecutionPlanUpdates,
    FunctionCall,
    FunctionCallWatcherStatus,
    FunctionInputs,
    FunctionRef,
    GetAllocationExecutionLogBatchRequest,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeResponse,
    ReadAllocationEventLogResponse,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
    WatchAllocationStateRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.status_pb2 import Status

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


@application()
@function()
def api_function_tail_call(url: str) -> List[FileChunk]:
    print(f"api_function_tail_call called with url: {url}")
    assert url == "https://example.com"
    assert isinstance(url, str)
    return file_chunker.future(
        File(content=bytes(b"hello"), content_type="text/plain"),
        num_chunks=3,
    )


@application()
@function()
def api_function_blocking_call(url: str) -> FileChunk:
    print(f"api_function_blocking_call called with url: {url}")
    assert url == "https://blocking-example.com"
    assert isinstance(url, str)
    return file_chunker(
        File(content=bytes(b"hello-blocking"), content_type="text/blocking-plain"),
        num_chunks=3,
    )[1]


@application()
@function()
def api_function_stringify_multiple_args(
    arg1: str, arg2: int, arg3: bool, arg4: None
) -> str:
    return f"arg1: {arg1}, arg2: {arg2}, arg3: {arg3}, arg4: {arg4}"


@function()
def file_chunker(file: File, num_chunks: int) -> List[FileChunk]:
    print(f"file_chunker called with file data: {file.content.decode()}")
    return [
        FileChunk(
            data=file.content[chunk_ix : chunk_ix + 1], start=chunk_ix, end=chunk_ix + 1
        )
        for chunk_ix in range(num_chunks)
    ]


@application()
@function()
def raises_exception(input: int):
    raise Exception("this extractor throws an exception.")


@function()
def returns_argument(arg: bytes) -> bytes:
    return arg


@cls()
class FunctionFailingOnInit:
    def __init__(self):
        raise Exception("This function fails on initialization")

    @function()
    def run(self, x: int) -> int:
        return x


class TestRunAllocation(unittest.TestCase):
    def test_api_function_tail_call(self):
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="api_function_tail_call",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="api_function_tail_call",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                allocation_id: str = "test-allocation-id"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(
                                "https://example.com", str
                            ),
                        ),
                    )
                )

                # Collect execution events using the driver, responding to
                # function call creation via event log.
                create_fc_event: AllocationExecutionEventCreateFunctionCall | None = (
                    None
                )

                def on_batch(events, driver):
                    nonlocal create_fc_event
                    for event in events:
                        if event.HasField("create_function_call"):
                            create_fc_event = event.create_function_call
                            fc_id = create_fc_event.updates.root_function_call_id
                            # Respond via event log: function call created OK.
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=allocation_id,
                                    entries=[
                                        AllocationEvent(
                                            clock=1,
                                            function_call_created=AllocationEventFunctionCallCreated(
                                                function_call_id=fc_id,
                                                status=Status(
                                                    code=grpc.StatusCode.OK.value[0]
                                                ),
                                            ),
                                        )
                                    ],
                                    last_clock=1,
                                    has_more=False,
                                )
                            )

                driver = AllocationTestDriver(stub, allocation_id)
                finish_event = driver.run(on_execution_event_batch=on_batch)

                self.assertIsNotNone(create_fc_event)
                self.assertEqual(
                    finish_event.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(finish_event.HasField("request_error_output"))
                self.assertTrue(finish_event.HasField("tail_call_durable_id"))

                # Validate function call from execution event.
                function_call_updates: ExecutionPlanUpdates = create_fc_event.updates
                self.assertTrue(function_call_updates.HasField("root_function_call_id"))
                self.assertFalse(function_call_updates.HasField("start_at"))
                function_call_update: ExecutionPlanUpdate = (
                    function_call_updates.updates[0]
                )
                self.assertTrue(function_call_update.HasField("function_call"))
                function_call: FunctionCall = function_call_update.function_call
                self.assertIsNotNone(function_call)
                self.assertIsNotNone(function_call.id)
                self.assertEqual(
                    function_call.target,
                    FunctionRef(
                        namespace="test",
                        application_name="api_function_tail_call",
                        application_version="0.1",
                        function_name="file_chunker",
                    ),
                )
                self.assertEqual(
                    function_call_updates.root_function_call_id, function_call.id
                )
                self.assertEqual(finish_event.tail_call_durable_id, function_call.id)

                self.assertEqual(len(function_call.args), 2)
                self.assertTrue(function_call.args[0].HasField("value"))
                self.assertTrue(function_call.args[1].HasField("value"))

                args_blob = create_fc_event.args_blob
                arg_0: File = download_and_deserialize_so(
                    self,
                    function_call.args[0].value,
                    args_blob,
                )
                self.assertEqual(arg_0.content, b"hello")
                self.assertEqual(arg_0.content_type, "text/plain")
                arg_0_metadata: ValueMetadata = read_so_metadata(
                    self, function_call.args[0].value, args_blob
                )
                arg_1: int = download_and_deserialize_so(
                    self,
                    function_call.args[1].value,
                    args_blob,
                )
                self.assertEqual(arg_1, 3)
                arg_1_metadata: ValueMetadata = read_so_metadata(
                    self, function_call.args[1].value, args_blob
                )

                function_call_metadata: FunctionCallMetadata = deserialize_metadata(
                    function_call.call_metadata
                )
                self.assertIsInstance(function_call_metadata, FunctionCallMetadata)

                self.assertEqual(len(function_call_metadata.args), 1)
                self.assertEqual(
                    function_call_metadata.args[0].value_id, arg_0_metadata.id
                )
                self.assertEqual(len(function_call_metadata.kwargs), 1)
                self.assertEqual(
                    function_call_metadata.kwargs["num_chunks"].value_id,
                    arg_1_metadata.id,
                )

                # Cleanup.
                stub.delete_allocation(
                    DeleteAllocationRequest(
                        allocation_id=allocation_id,
                    )
                )

        fe_stdout = process.read_stdout()
        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_finished", fe_stdout)
        self.assertIn("allocations_started", fe_stdout)
        self.assertIn("allocations_finished", fe_stdout)
        # Check function output to stdout
        self.assertIn(
            "api_function_tail_call called with url: https://example.com", fe_stdout
        )

    def test_api_function_blocking_call(self):
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="api_function_blocking_call",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="api_function_blocking_call",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                allocation_id: str = "test-allocation-id"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="123",
                            function_call_id="test-function-call",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(
                                "https://blocking-example.com", str
                            ),
                        ),
                    )
                )

                create_fc_event: AllocationExecutionEventCreateFunctionCall | None = (
                    None
                )
                create_watcher_event: (
                    AllocationExecutionEventCreateFunctionCallWatcher | None
                ) = None
                event_clock = 0

                def on_batch(events, driver):
                    nonlocal create_fc_event, create_watcher_event, event_clock
                    for event in events:
                        if event.HasField("create_function_call"):
                            create_fc_event = event.create_function_call
                            fc_id = create_fc_event.updates.root_function_call_id
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=allocation_id,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_created=AllocationEventFunctionCallCreated(
                                                function_call_id=fc_id,
                                                status=Status(
                                                    code=grpc.StatusCode.OK.value[0]
                                                ),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                        elif event.HasField("create_function_call_watcher"):
                            create_watcher_event = event.create_function_call_watcher
                            # Send the function call result back.
                            user_serializer: PickleUserDataSerializer = (
                                PickleUserDataSerializer()
                            )
                            serialized_function_call_output_metadata: bytes = (
                                serialize_metadata(
                                    ValueMetadata(
                                        id="function-call-output-id",
                                        type_hint=list[FileChunk],
                                        serializer_name=user_serializer.name,
                                        content_type=user_serializer.content_type,
                                    )
                                )
                            )
                            serialized_function_call_output: bytes = (
                                user_serializer.serialize(
                                    [
                                        FileChunk(data=b"h", start=0, end=1),
                                        FileChunk(data=b"e", start=1, end=2),
                                        FileChunk(data=b"l", start=2, end=3),
                                    ],
                                    type_hint=list[FileChunk],
                                )
                            )

                            function_call_output_blob_data: bytes = b"".join(
                                [
                                    serialized_function_call_output_metadata,
                                    serialized_function_call_output,
                                ]
                            )
                            function_call_output_blob: BLOB = create_tmp_blob(
                                id="function-call-output-blob",
                            )
                            write_tmp_blob_bytes(
                                function_call_output_blob,
                                function_call_output_blob_data,
                            )

                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=allocation_id,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                                function_call_id=create_watcher_event.function_call_id,
                                                outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                                watcher_status=FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
                                                value_output=SerializedObjectInsideBLOB(
                                                    manifest=SerializedObjectManifest(
                                                        encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                                        encoding_version=0,
                                                        size=len(
                                                            serialized_function_call_output_metadata
                                                        )
                                                        + len(
                                                            serialized_function_call_output
                                                        ),
                                                        metadata_size=len(
                                                            serialized_function_call_output_metadata
                                                        ),
                                                        sha256_hash=hashlib.sha256(
                                                            serialized_function_call_output_metadata
                                                            + serialized_function_call_output
                                                        ).hexdigest(),
                                                    ),
                                                    offset=0,
                                                ),
                                                value_blob=function_call_output_blob,
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )

                driver = AllocationTestDriver(stub, allocation_id)
                finish_event = driver.run(on_execution_event_batch=on_batch)

                self.assertIsNotNone(create_fc_event)
                self.assertIsNotNone(create_watcher_event)

                # Validate create_function_call event.
                self.assertTrue(create_fc_event.HasField("updates"))
                updates: ExecutionPlanUpdates = create_fc_event.updates
                self.assertTrue(updates.HasField("root_function_call_id"))
                self.assertFalse(updates.HasField("start_at"))
                self.assertEqual(len(updates.updates), 1)
                update: ExecutionPlanUpdate = updates.updates[0]

                function_call: FunctionCall = update.function_call
                self.assertEqual(updates.root_function_call_id, function_call.id)
                self.assertIsNotNone(function_call)
                self.assertIsNotNone(function_call.id)
                self.assertEqual(
                    function_call.target,
                    FunctionRef(
                        namespace="test",
                        application_name="api_function_blocking_call",
                        application_version="0.1",
                        function_name="file_chunker",
                    ),
                )

                self.assertEqual(len(function_call.args), 2)
                self.assertTrue(function_call.args[0].HasField("value"))
                self.assertTrue(function_call.args[1].HasField("value"))

                args_blob = create_fc_event.args_blob
                arg_0: File = download_and_deserialize_so(
                    self,
                    function_call.args[0].value,
                    args_blob,
                )
                self.assertEqual(arg_0.content, b"hello-blocking")
                self.assertEqual(arg_0.content_type, "text/blocking-plain")
                arg_0_metadata: ValueMetadata = read_so_metadata(
                    self, function_call.args[0].value, args_blob
                )
                arg_1: int = download_and_deserialize_so(
                    self,
                    function_call.args[1].value,
                    args_blob,
                )
                self.assertEqual(arg_1, 3)
                arg_1_metadata: ValueMetadata = read_so_metadata(
                    self, function_call.args[1].value, args_blob
                )

                function_call_metadata: FunctionCallMetadata = deserialize_metadata(
                    function_call.call_metadata
                )
                self.assertIsInstance(function_call_metadata, FunctionCallMetadata)

                self.assertEqual(len(function_call_metadata.args), 1)
                self.assertEqual(
                    function_call_metadata.args[0].value_id, arg_0_metadata.id
                )
                self.assertEqual(len(function_call_metadata.kwargs), 1)
                self.assertEqual(
                    function_call_metadata.kwargs["num_chunks"].value_id,
                    arg_1_metadata.id,
                )

                # Validate watcher event.
                self.assertEqual(
                    create_watcher_event.function_call_id,
                    function_call.id,
                )

                # Validate finish event.
                self.assertEqual(
                    finish_event.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(finish_event.HasField("request_error_output"))
                self.assertTrue(finish_event.HasField("value"))
                self.assertTrue(finish_event.HasField("uploaded_function_outputs_blob"))
                output: FileChunk = download_and_deserialize_so(
                    self,
                    finish_event.value,
                    finish_event.uploaded_function_outputs_blob,
                )
                self.assertEqual(output, FileChunk(data=b"e", start=1, end=2))

                # Cleanup.
                stub.delete_allocation(
                    DeleteAllocationRequest(
                        allocation_id=allocation_id,
                    )
                )

        fe_stdout = process.read_stdout()
        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_finished", fe_stdout)
        self.assertIn("allocations_started", fe_stdout)
        self.assertIn("allocations_finished", fe_stdout)
        # Check function output to stdout
        self.assertIn(
            "api_function_blocking_call called with url: https://blocking-example.com",
            fe_stdout,
        )

    def test_api_function_call_via_http_request_forwarding(self):
        # This mode is not currently used by Server so it's very important
        # to have a test for it to make sure that this mode works in all FEs
        # once we enable it in Server.
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="api_function_stringify_multiple_args",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="api_function_stringify_multiple_args",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                user_serializer: JSONUserDataSerializer = JSONUserDataSerializer()
                http_request_parts: List[HTTPBodyPart] = [
                    # arg1 is passed as first positional argument
                    HTTPBodyPart(
                        field_name="1",
                        content_type="application/json",
                        body=user_serializer.serialize("test-string-arg", str),
                    ),
                    # arg2 is passed as second positional argument
                    HTTPBodyPart(
                        field_name="2",
                        content_type="application/json",
                        body=user_serializer.serialize(777, int),
                    ),
                    # arg3 is passed as keyword argument
                    HTTPBodyPart(
                        field_name="arg3",
                        content_type="application/json",
                        body=user_serializer.serialize(True, bool),
                    ),
                    # arg4 is passed as keyword argument
                    HTTPBodyPart(
                        field_name="arg4",
                        content_type="application/json",
                        body=user_serializer.serialize(None, None),
                    ),
                ]
                serialized_http_request: bytes = create_multipart_invoke_http_request(
                    http_request_parts, boundary="magic-boundary-string"
                )
                api_payload_blob: BLOB = write_new_application_payload_blob(
                    serialized_http_request
                )

                allocation_id: str = "test-allocation-id"
                finish_event: AllocationExecutionEventFinishAllocation = (
                    run_allocation_that_returns_output(
                        self,
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
                                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW,
                                                encoding_version=0,
                                                size=len(serialized_http_request),
                                                metadata_size=0,  # No metadata for API function calls.
                                                sha256_hash=hashlib.sha256(
                                                    serialized_http_request
                                                ).hexdigest(),
                                                content_type="message/http",
                                            ),
                                            offset=0,
                                        )
                                    ],
                                    arg_blobs=[api_payload_blob],
                                    request_error_blob=create_request_error_blob(),
                                    function_call_metadata=b"",
                                ),
                            ),
                        ),
                    )
                )

                self.assertEqual(
                    finish_event.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(finish_event.HasField("request_error_output"))
                self.assertTrue(finish_event.HasField("value"))
                self.assertTrue(finish_event.HasField("uploaded_function_outputs_blob"))
                output: str = download_and_deserialize_so(
                    self,
                    finish_event.value,
                    finish_event.uploaded_function_outputs_blob,
                )
                self.assertEqual(
                    output,
                    "arg1: test-string-arg, arg2: 777, arg3: True, arg4: None",
                )

                # Cleanup.
                stub.delete_allocation(
                    DeleteAllocationRequest(
                        allocation_id=allocation_id,
                    )
                )

        fe_stdout = process.read_stdout()
        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_finished", fe_stdout)
        self.assertIn("allocations_started", fe_stdout)
        self.assertIn("allocations_finished", fe_stdout)

    def test_regular_function_call_with_multiple_chunks(self):
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="file_chunker",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="file_chunker",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                user_serializer: PickleUserDataSerializer = PickleUserDataSerializer()
                serialized_file_arg_metadata: bytes = serialize_metadata(
                    ValueMetadata(
                        id="file_arg_id",
                        type_hint=File,
                        serializer_name=None,
                        content_type="text/plain; charset=utf-8",
                    )
                )
                serialized_file_arg: bytes = (
                    "hello".encode()
                )  # File content is stored directly in the BLOB so users can read it over HTTP.
                serialized_num_chunks_arg_metadata: bytes = serialize_metadata(
                    ValueMetadata(
                        id="num_chunks_arg_id",
                        type_hint=int,
                        serializer_name=user_serializer.name,
                        content_type=user_serializer.content_type,
                    )
                )
                serialized_num_chunks_arg: bytes = user_serializer.serialize(5, int)

                serialized_args: bytes = b"".join(
                    [
                        serialized_file_arg_metadata,
                        serialized_file_arg,
                        serialized_num_chunks_arg_metadata,
                        serialized_num_chunks_arg,
                    ]
                )
                args_blob: BLOB = create_tmp_blob(id="args-blob-id")
                write_tmp_blob_bytes(
                    args_blob,
                    serialized_args,
                )

                allocation_id: str = "test-allocation-id"
                stub.create_allocation(
                    CreateAllocationRequest(
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
                                            size=len(serialized_file_arg_metadata)
                                            + len(serialized_file_arg),
                                            metadata_size=len(
                                                serialized_file_arg_metadata
                                            ),
                                            sha256_hash=hashlib.sha256(
                                                serialized_file_arg_metadata
                                                + serialized_file_arg
                                            ).hexdigest(),
                                        ),
                                        offset=0,
                                    ),
                                    SerializedObjectInsideBLOB(
                                        manifest=SerializedObjectManifest(
                                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                            encoding_version=0,
                                            size=len(serialized_num_chunks_arg_metadata)
                                            + len(serialized_num_chunks_arg),
                                            metadata_size=len(
                                                serialized_num_chunks_arg_metadata
                                            ),
                                            sha256_hash=hashlib.sha256(
                                                serialized_num_chunks_arg_metadata
                                                + serialized_num_chunks_arg
                                            ).hexdigest(),
                                        ),
                                        offset=len(serialized_file_arg_metadata)
                                        + len(serialized_file_arg),
                                    ),
                                ],
                                arg_blobs=[args_blob, args_blob],
                                request_error_blob=create_tmp_blob(
                                    id="request-error-blob"
                                ),
                                function_call_metadata=serialize_metadata(
                                    FunctionCallMetadata(
                                        id="file_chunker_call",
                                        function_name="file_chunker",
                                        output_serializer_name_override=None,
                                        output_type_hint_override=None,
                                        has_output_type_hint_override=False,
                                        args=[
                                            FunctionCallArgumentMetadata(
                                                value_id="file_arg_id",
                                            )
                                        ],
                                        kwargs={
                                            "num_chunks": FunctionCallArgumentMetadata(
                                                value_id="num_chunks_arg_id",
                                            ),
                                        },
                                        is_map_splitter=False,
                                        is_reduce_splitter=False,
                                        splitter_function_name=None,
                                        splitter_input_mode=None,
                                        is_map_concat=False,
                                    )
                                ),
                            ),
                        ),
                    ),
                )

                driver = AllocationTestDriver(stub, allocation_id)
                finish_event = driver.run()

                self.assertEqual(
                    finish_event.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(finish_event.HasField("request_error_output"))
                self.assertTrue(finish_event.HasField("value"))
                self.assertTrue(finish_event.HasField("uploaded_function_outputs_blob"))

                output = download_and_deserialize_so(
                    self,
                    finish_event.value,
                    finish_event.uploaded_function_outputs_blob,
                )
                self.assertEqual(len(output), 5)
                self.assertEqual(
                    output[0].model_dump(),
                    FileChunk(data=b"h", start=0, end=1).model_dump(),
                )
                self.assertEqual(
                    output[1].model_dump(),
                    FileChunk(data=b"e", start=1, end=2).model_dump(),
                )
                self.assertEqual(
                    output[2].model_dump(),
                    FileChunk(data=b"l", start=2, end=3).model_dump(),
                )
                self.assertEqual(
                    output[3].model_dump(),
                    FileChunk(data=b"l", start=3, end=4).model_dump(),
                )
                self.assertEqual(
                    output[4].model_dump(),
                    FileChunk(data=b"o", start=4, end=5).model_dump(),
                )

        fe_stdout = process.read_stdout()
        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_finished", fe_stdout)
        self.assertIn("allocations_started", fe_stdout)
        self.assertIn("allocations_finished", fe_stdout)
        # Check function output to stdout
        self.assertIn("file_chunker called with file data: hello", fe_stdout)

    def test_function_output_blob_with_multiple_chunks(self):
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="returns_argument",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="returns_argument",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                user_serializer: PickleUserDataSerializer = PickleUserDataSerializer()
                # 5 full chunks + 1 byte of output data out of 10 chunks
                arg: bytes = os.urandom(5 * 1024 + 1)
                serialized_arg: bytes = user_serializer.serialize(arg, bytes)
                serialized_arg_metadata: bytes = serialize_metadata(
                    ValueMetadata(
                        id="arg_id",
                        type_hint=bytes,
                        serializer_name=user_serializer.name,
                        content_type=user_serializer.content_type,
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
                request = CreateAllocationRequest(
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
                                    function_name="returns_argument",
                                    output_serializer_name_override=None,
                                    output_type_hint_override=None,
                                    has_output_type_hint_override=False,
                                    args=[
                                        FunctionCallArgumentMetadata(
                                            value_id="arg_id",
                                        ),
                                    ],
                                    kwargs={},
                                    is_map_splitter=False,
                                    is_reduce_splitter=False,
                                    splitter_function_name=None,
                                    splitter_input_mode=None,
                                    is_map_concat=False,
                                )
                            ),
                            request_error_blob=create_request_error_blob(),
                        ),
                    ),
                )
                stub.create_allocation(request)

                # Manually handle multi-chunk blobs via watch_allocation_state
                # and read the result from the execution log.
                function_output_blob: BLOB | None = None

                def handle_blobs():
                    nonlocal function_output_blob
                    allocation_states = stub.watch_allocation_state(
                        WatchAllocationStateRequest(allocation_id=allocation_id),
                    )
                    for allocation_state in allocation_states:
                        if len(allocation_state.output_blob_requests) > 0:
                            blob_request = allocation_state.output_blob_requests[0]
                            function_output_blob = create_tmp_blob(
                                id=blob_request.id,
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

                blob_thread = threading.Thread(target=handle_blobs, daemon=True)
                blob_thread.start()

                # Read result from execution log.
                finish_event = None
                while True:
                    response = stub.get_allocation_execution_log_batch(
                        GetAllocationExecutionLogBatchRequest(
                            allocation_id=allocation_id,
                        )
                    )
                    if len(response.events) == 0:
                        break
                    for event in response.events:
                        if event.HasField("finish_allocation"):
                            finish_event = event.finish_allocation
                    stub.advance_allocation_execution_log_batch(
                        AdvanceAllocationExecutionLogBatchRequest(
                            allocation_id=allocation_id,
                        )
                    )
                    if finish_event is not None:
                        break

                blob_thread.join(timeout=5)
                self.assertIsNotNone(finish_event)
                self.assertIsNotNone(function_output_blob)

                self.assertEqual(
                    finish_event.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(finish_event.HasField("request_error_output"))
                self.assertTrue(finish_event.HasField("value"))

                output: bytes = download_and_deserialize_so(
                    self,
                    finish_event.value,
                    finish_event.uploaded_function_outputs_blob,
                )
                self.assertEqual(arg, output)

                output_serialized_object: SerializedObjectInsideBLOB = (
                    finish_event.value
                )
                self.assertEqual(output_serialized_object.offset, 0)
                self.assertEqual(
                    output_serialized_object.manifest.size
                    - output_serialized_object.manifest.metadata_size,
                    len(serialized_arg),
                )

                # Verify that output BLOB chunks exactly match the output data
                # and the original BLOB chunks.
                chunks_count: int = output_serialized_object.manifest.size // 1024 + 1
                self.assertEqual(
                    len(finish_event.uploaded_function_outputs_blob.chunks),
                    chunks_count,
                )
                etags: List[str] = []
                for ix, uploaded_chunk in enumerate(
                    finish_event.uploaded_function_outputs_blob.chunks
                ):
                    uploaded_chunk: BLOBChunk
                    if ix < chunks_count - 1:
                        self.assertEqual(uploaded_chunk.size, 1024)
                    else:
                        # The 1 extra byte that should go to 6th chunk + Pickle
                        # header + value metadata. Both should fit into the last
                        # chunk.
                        self.assertEqual(
                            uploaded_chunk.size,
                            output_serialized_object.manifest.size % 1024,
                        )
                    self.assertIsNotNone(uploaded_chunk.etag)
                    self.assertNotIn(uploaded_chunk.etag, etags)
                    etags.append(uploaded_chunk.etag)
                    self.assertEqual(
                        uploaded_chunk.uri,
                        function_output_blob.chunks[ix].uri,
                    )

    def test_function_raises_error(self):
        with FunctionExecutorProcessContextManager(capture_std_outputs=True) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="raises_exception",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="raises_exception",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                allocation_id: str = "test-allocation-id"
                finish_event: AllocationExecutionEventFinishAllocation = (
                    run_allocation_that_fails(
                        stub,
                        request=CreateAllocationRequest(
                            allocation=Allocation(
                                request_id="123",
                                function_call_id="test-function-call",
                                allocation_id=allocation_id,
                                inputs=application_function_inputs(10, int),
                            ),
                        ),
                    )
                )

                self.assertEqual(
                    finish_event.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    finish_event.failure_reason,
                    AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
                )
                self.assertFalse(finish_event.HasField("request_error_output"))

        fe_stdout = process.read_stdout()
        # Check FE logs in stdout
        self.assertIn("running function", fe_stdout)
        self.assertIn("function finished", fe_stdout)
        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_finished", fe_stdout)
        self.assertIn("allocations_started", fe_stdout)
        self.assertIn("allocations_finished", fe_stdout)

        # Check original function exception is printed in stdout
        self.assertIn("this extractor throws an exception.", process.read_stdout())

    def test_function_initialization_raises_error(self):
        with FunctionExecutorProcessContextManager(capture_std_outputs=True) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="FunctionFailingOnInit.run",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="FunctionFailingOnInit.run",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    initialize_response.failure_reason,
                    InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
                )

        fe_stdout = process.read_stdout()
        # Check FE logs in stdout
        self.assertIn(
            "initializing function executor service",
            fe_stdout,
        )
        self.assertIn(
            "function executor service initialization failed",
            fe_stdout,
        )
        self.assertIn(
            "failed to load customer function",
            fe_stdout,
        )

        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_failed", fe_stdout)
        self.assertIn("This function fails on initialization", fe_stdout)


if __name__ == "__main__":
    unittest.main()
