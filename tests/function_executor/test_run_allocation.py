import hashlib
import os
import unittest
from typing import List

from pydantic import BaseModel
from testing import (
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    create_tmp_blob,
    download_and_deserialize_so,
    initialize,
    read_so_metadata,
    read_tmp_blob_bytes,
    rpc_channel,
    run_allocation,
    write_tmp_blob_bytes,
)

from tensorlake.applications import (
    File,
    application,
    cls,
    function,
)
from tensorlake.applications.ast.function_call_node import (
    ArgumentMetadata,
    RegularFunctionCallMetadata,
)
from tensorlake.applications.ast.value_node import ValueMetadata
from tensorlake.applications.user_data_serializer import (
    JSONUserDataSerializer,
    PickleUserDataSerializer,
)
from tensorlake.function_executor.handlers.run_function.function_call_node_metadata import (
    FunctionCallNodeMetadata,
    FunctionCallType,
)
from tensorlake.function_executor.handlers.run_function.value_node_metadata import (
    ValueNodeMetadata,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    BLOBChunk,
    ExecutionPlanUpdate,
    FunctionCall,
    FunctionInputs,
    FunctionRef,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeResponse,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


class FileChunk(BaseModel):
    data: bytes
    start: int
    end: int


@application()
@function()
def api_function(url: str) -> List[FileChunk]:
    print(f"api_function called with url: {url}")
    assert url == "https://example.com"
    assert isinstance(url, str)
    return file_chunker(
        File(content=bytes(b"hello"), content_type="text/plain"),
        num_chunks=3,
    )


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
    def test_api_function_success(self):
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = initialize(
                    stub,
                    app_name="api_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="api_function",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                user_serializer: JSONUserDataSerializer = JSONUserDataSerializer()
                serialized_api_payload: bytes = user_serializer.serialize(
                    "https://example.com"
                )
                api_payload_blob: BLOB = create_tmp_blob()
                write_tmp_blob_bytes(
                    api_payload_blob,
                    serialized_api_payload,
                )
                function_outputs_blob: BLOB = create_tmp_blob()
                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=FunctionInputs(
                        args=[
                            SerializedObjectInsideBLOB(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                                    encoding_version=0,
                                    size=len(serialized_api_payload),
                                    metadata_size=0,  # No metadata for API function calls.
                                    sha256_hash=hashlib.sha256(
                                        serialized_api_payload
                                    ).hexdigest(),
                                ),
                                offset=0,
                            )
                        ],
                        arg_blobs=[api_payload_blob],
                        function_outputs_blob=function_outputs_blob,
                        request_error_blob=create_tmp_blob(),
                        function_call_metadata=b"",
                    ),
                )

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(alloc_result.HasField("request_error_output"))

                updates: List[ExecutionPlanUpdate] = list(alloc_result.updates.updates)
                self.assertEqual(len(updates), 1)
                function_call: FunctionCall = updates[0].function_call
                self.assertEqual(
                    alloc_result.updates.root_function_call_id, function_call.id
                )
                self.assertIsNotNone(function_call)
                self.assertIsNotNone(function_call.id)
                self.assertEqual(
                    function_call.target,
                    FunctionRef(
                        namespace="test",
                        application_name="api_function",
                        application_version="0.1",
                        function_name="file_chunker",
                    ),
                )

                self.assertEqual(len(function_call.args), 2)
                self.assertTrue(function_call.args[0].HasField("value"))
                self.assertTrue(function_call.args[1].HasField("value"))
                arg_0: File = download_and_deserialize_so(
                    self,
                    function_call.args[0].value,
                    function_outputs_blob,
                )
                self.assertEqual(arg_0.content, b"hello")
                self.assertEqual(arg_0.content_type, "text/plain")
                arg_0_metadata = read_so_metadata(
                    self, function_call.args[0].value, function_outputs_blob
                )
                arg_1: int = download_and_deserialize_so(
                    self,
                    function_call.args[1].value,
                    function_outputs_blob,
                )
                self.assertEqual(arg_1, 3)
                arg_1_metadata = read_so_metadata(
                    self, function_call.args[1].value, function_outputs_blob
                )

                function_call_node_metadata = FunctionCallNodeMetadata.deserialize(
                    function_call.call_metadata
                )
                self.assertEqual(
                    function_call_node_metadata.type, FunctionCallType.REGULAR
                )
                function_call_metadata = RegularFunctionCallMetadata.deserialize(
                    function_call_node_metadata.metadata
                )
                self.assertEqual(len(function_call_metadata.args), 1)
                self.assertEqual(function_call_metadata.args[0].nid, arg_0_metadata.nid)
                self.assertEqual(function_call_metadata.args[0].flist, None)
                self.assertEqual(len(function_call_metadata.kwargs), 1)
                self.assertEqual(
                    function_call_metadata.kwargs["num_chunks"].nid, arg_1_metadata.nid
                )
                self.assertEqual(
                    function_call_metadata.kwargs["num_chunks"].flist, None
                )

        fe_stdout = process.read_stdout()
        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_finished", fe_stdout)
        self.assertIn("allocations_started", fe_stdout)
        self.assertIn("allocations_finished", fe_stdout)
        # Check function output to stdout
        self.assertIn("api_function called with url: https://example.com", fe_stdout)

    def test_regular_function_success(self):
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
                serialized_file_arg_metadata: bytes = ValueNodeMetadata(
                    nid="file_arg_id",
                    metadata=ValueMetadata(cls=File, extra="text/plain").serialize(),
                ).serialize()
                serialized_file_arg: bytes = (
                    "hello".encode()
                )  # File content is stored directly in the BLOB so users can read it over HTTP.
                serialized_num_chunks_arg_metadata: bytes = ValueNodeMetadata(
                    nid="num_chunks_arg_id",
                    metadata=ValueMetadata(
                        cls=int, extra=user_serializer.name
                    ).serialize(),
                ).serialize()
                serialized_num_chunks_arg: bytes = user_serializer.serialize(5)

                serialized_args: bytes = b"".join(
                    [
                        serialized_file_arg_metadata,
                        serialized_file_arg,
                        serialized_num_chunks_arg_metadata,
                        serialized_num_chunks_arg,
                    ]
                )
                args_blob: BLOB = create_tmp_blob()
                write_tmp_blob_bytes(
                    args_blob,
                    serialized_args,
                )
                function_outputs_blob: BLOB = create_tmp_blob()
                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=FunctionInputs(
                        args=[
                            SerializedObjectInsideBLOB(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                                    encoding_version=0,
                                    size=len(serialized_file_arg_metadata)
                                    + len(serialized_file_arg),
                                    metadata_size=len(serialized_file_arg_metadata),
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
                        function_outputs_blob=function_outputs_blob,
                        request_error_blob=create_tmp_blob(),
                        function_call_metadata=FunctionCallNodeMetadata(
                            nid="file_chunker_call",
                            type=FunctionCallType.REGULAR,
                            metadata=RegularFunctionCallMetadata(
                                args=[ArgumentMetadata(nid="file_arg_id", flist=None)],
                                kwargs={
                                    "num_chunks": ArgumentMetadata(
                                        nid="num_chunks_arg_id", flist=None
                                    ),
                                },
                                oso=None,
                            ).serialize(),
                        ).serialize(),
                    ),
                )

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(alloc_result.HasField("request_error_output"))
                self.assertTrue(alloc_result.HasField("value"))

                output = download_and_deserialize_so(
                    self, alloc_result.value, function_outputs_blob
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
                serialized_arg: bytes = user_serializer.serialize(arg)
                serialized_arg_metadata: bytes = ValueNodeMetadata(
                    nid="arg_id",
                    metadata=ValueMetadata(
                        cls=bytes, extra=user_serializer.name
                    ).serialize(),
                ).serialize()

                serialized_args: bytes = b"".join(
                    [
                        serialized_arg_metadata,
                        serialized_arg,
                    ]
                )
                input_blob: BLOB = create_tmp_blob()
                write_tmp_blob_bytes(
                    input_blob,
                    serialized_args,
                )
                function_outputs_blob: BLOB = create_tmp_blob(
                    chunks_count=10, chunk_size=1024
                )
                alloc_result: AllocationResult = run_allocation(
                    stub,
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
                        function_outputs_blob=function_outputs_blob,
                        request_error_blob=create_tmp_blob(),
                        function_call_metadata=FunctionCallNodeMetadata(
                            nid="returns_argument_call",
                            type=FunctionCallType.REGULAR,
                            metadata=RegularFunctionCallMetadata(
                                args=[ArgumentMetadata(nid="arg_id", flist=None)],
                                kwargs={},
                                oso=None,
                            ).serialize(),
                        ).serialize(),
                    ),
                )

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )

                serialized_output_metadata: bytes = read_tmp_blob_bytes(
                    function_outputs_blob,
                    alloc_result.value.offset,
                    alloc_result.value.manifest.metadata_size,
                )
                output: bytes = download_and_deserialize_so(
                    self, alloc_result.value, function_outputs_blob
                )
                self.assertEqual(arg, output)

                output_serialized_object: SerializedObjectInsideBLOB = (
                    alloc_result.value
                )
                self.assertEqual(output_serialized_object.offset, 0)
                self.assertEqual(
                    output_serialized_object.manifest.size
                    - output_serialized_object.manifest.metadata_size,
                    len(serialized_arg),
                )
                self.assertEqual(
                    output_serialized_object.manifest.sha256_hash,
                    hashlib.sha256(
                        serialized_output_metadata + serialized_arg
                    ).hexdigest(),
                )

                # Verify that output BLOB chunks exactly match the output data and the original BLOB chunks.
                chunks_count: int = output_serialized_object.manifest.size // 1024 + 1
                self.assertEqual(
                    len(alloc_result.uploaded_function_outputs_blob.chunks),
                    chunks_count,
                )
                etags: List[str] = []
                for ix, uploaded_chunk in enumerate(
                    alloc_result.uploaded_function_outputs_blob.chunks
                ):
                    uploaded_chunk: BLOBChunk
                    if ix < chunks_count - 1:
                        self.assertEqual(uploaded_chunk.size, 1024)
                    else:
                        # The 1 extra byte that should go to 6th chunk + Pickle header + value metadata.
                        # Both should fit into the last chunk.
                        self.assertEqual(
                            uploaded_chunk.size,
                            output_serialized_object.manifest.size % 1024,
                        )
                    self.assertIsNotNone(uploaded_chunk.etag)
                    self.assertNotIn(uploaded_chunk.etag, etags)
                    etags.append(uploaded_chunk.etag)
                    self.assertEqual(
                        uploaded_chunk.uri, function_outputs_blob.chunks[ix].uri
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

                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=application_function_inputs(10),
                )
                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    alloc_result.failure_reason,
                    AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
                )
                self.assertFalse(alloc_result.HasField("request_error_output"))

        fe_stdout = process.read_stdout()
        # Check FE logs in stdout
        self.assertIn("running function", fe_stdout)
        self.assertIn("function finished", fe_stdout)
        # Check FE events in stdout
        self.assertIn("function_executor_initialization_started", fe_stdout)
        self.assertIn("function_executor_initialization_finished", fe_stdout)
        self.assertIn("allocations_started", fe_stdout)
        self.assertIn("allocations_finished", fe_stdout)

        # Check function output in stderr
        self.assertIn("this extractor throws an exception.", process.read_stderr())

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
        self.assertIn("function_executor_initialization_finished", fe_stdout)

        # Check function output to stderr
        self.assertIn("This function fails on initialization", process.read_stderr())


if __name__ == "__main__":
    unittest.main()
