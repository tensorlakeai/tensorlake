import hashlib
import os
import unittest
from typing import List

from pydantic import BaseModel
from testing import (
    FunctionExecutorProcessContextManager,
    create_tmp_blob,
    deserialized_function_output,
    rpc_channel,
    run_task,
)

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    BLOBChunk,
    FunctionRef,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.workflows.remote.application.zip import zip_application_code

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

# This import will be replaced by `import tensorlake` when we switch to the new SDK UX.
import tensorlake.workflows.interface as tensorlake

app: tensorlake.Application = tensorlake.define_application(name=__file__)


class FileChunk(BaseModel):
    data: bytes
    start: int
    end: int


@tensorlake.api()
@tensorlake.function()
def api_function(url: str) -> List[FileChunk]:
    print(f"api_function called with url: {url}")
    assert url == "https://example.com"
    assert isinstance(url, str)
    return file_chunker(
        tensorlake.File(content=bytes(b"hello"), content_type="text/plain")
    )


@tensorlake.function()
def file_chunker(file: tensorlake.File) -> List[FileChunk]:
    print(f"file_chunker called with file data: {file.content.decode()}")
    return [
        FileChunk(data=file.content, start=0, end=5),
        FileChunk(data=file.content, start=5, end=len(file.content)),
    ]


@tensorlake.function()
def raises_exception(input: int):
    raise Exception("this extractor throws an exception.")


@tensorlake.function()
def returns_argument(arg: bytes) -> bytes:
    return arg


@tensorlake.function()
def returns_3x_argument(arg: bytes) -> List[bytes]:
    return [arg, arg, arg]


@tensorlake.cls()
class FunctionFailingOnInit:
    def __init__(self):
        raise Exception("This function fails on initialization")

    @tensorlake.function()
    def run(self, x: int) -> int:
        return x


class TestRunTask(unittest.TestCase):
    def test_api_function_success(self):
        application_zip: bytes = zip_application_code(
            code_dir_path=APPLICATION_CODE_DIR_PATH,
            ignored_absolute_paths=set(),
        )
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=False,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        function=FunctionRef(
                            namespace="test",
                            application_name=app.name,
                            application_version=app.version,
                            function_name="api_function",
                        ),
                        application_code=SerializedObject(
                            manifest=SerializedObjectManifest(
                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                                encoding_version=0,
                                size=len(application_zip),
                                sha256_hash=hashlib.sha256(application_zip).hexdigest(),
                            ),
                            data=application_zip,
                        ),
                    )
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                # Check FE logs (separated from function logs)
                # Skipping this right now because currently we print FE logs to FE stdout.
                # self.assertIn(
                #     "initializing function executor service",
                #     initialize_response.diagnostics.function_executor_log,
                # )
                # self.assertIn(
                #     "initialized function executor service",
                #     initialize_response.diagnostics.function_executor_log,
                # )

    #             function_outputs_blob: BLOB = create_tmp_blob()
    #             task_result: TaskResult = run_task(
    #                 stub,
    #                 function_name="extractor_b",
    #                 input=File(data=bytes(b"hello"), mime_type="text/plain"),
    #                 function_outputs_blob=function_outputs_blob,
    #                 invocation_error_blob=create_tmp_blob(),
    #             )

    #             self.assertEqual(
    #                 task_result.outcome_code,
    #                 TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
    #             )
    #             self.assertFalse(task_result.HasField("invocation_error_output"))
    #             # Check FE logs (separated from function logs)
    #             # Skipping this right now because currently we print FE logs to FE stdout.
    #             # self.assertIn(
    #             #     "running function", task_result.diagnostics.function_executor_log
    #             # )
    #             # self.assertIn(
    #             #     "function finished", task_result.diagnostics.function_executor_log
    #             # )

    #             fn_outputs = deserialized_function_output(
    #                 self, task_result.function_outputs, function_outputs_blob
    #             )
    #             self.assertEqual(len(fn_outputs), 2)
    #             expected = FileChunk(data=b"hello", start=5, end=5)

    #             self.assertEqual(expected.model_dump(), fn_outputs[1].model_dump())

    #     fe_stdout = process.read_stdout()
    #     # Check FE events in stdout
    #     self.assertIn("function_executor_initialization_started", fe_stdout)
    #     self.assertIn("function_executor_initialization_finished", fe_stdout)
    #     self.assertIn("task_allocations_started", fe_stdout)
    #     self.assertIn("task_allocations_finished", fe_stdout)
    #     # Check function output to stdout
    #     self.assertIn("extractor_b called with file data: hello", fe_stdout)

    # def test_function_output_blob_with_multiple_chunks(self):
    #     graph = Graph(
    #         name="test", description="test", start_node=extractor_returns_argument
    #     )
    #     graph_data: bytes = zip_graph_code(
    #         graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH
    #     )
    #     with FunctionExecutorProcessContextManager(
    #         capture_std_outputs=True,
    #     ) as process:
    #         with rpc_channel(process) as channel:
    #             stub: FunctionExecutorStub = FunctionExecutorStub(channel)
    #             initialize_response: InitializeResponse = stub.initialize(
    #                 InitializeRequest(
    #                     namespace="test",
    #                     graph_name="test",
    #                     graph_version="1",
    #                     function_name="extractor_returns_argument",
    #                     graph=SerializedObject(
    #                         manifest=SerializedObjectManifest(
    #                             encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
    #                             encoding_version=0,
    #                             size=len(graph_data),
    #                             sha256_hash=hashlib.sha256(graph_data).hexdigest(),
    #                         ),
    #                         data=graph_data,
    #                     ),
    #                 )
    #             )
    #             self.assertEqual(
    #                 initialize_response.outcome_code,
    #                 InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
    #             )

    #             function_outputs_blob: BLOB = create_tmp_blob(
    #                 chunks_count=10, chunk_size=1024
    #             )
    #             # 5 full chunks + 1 byte of output data out of 10 chunks
    #             input_data: bytes = os.urandom(5 * 1024 + 1)
    #             input_data_serialized: bytes = CloudPickleSerializer.serialize(
    #                 input_data
    #             )
    #             input_data_serialized_size: int = len(input_data_serialized)
    #             rtask_result: TaskResult = run_task(
    #                 stub,
    #                 function_name="extractor_returns_argument",
    #                 input=input_data,
    #                 function_outputs_blob=function_outputs_blob,
    #                 invocation_error_blob=create_tmp_blob(),
    #             )

    #             self.assertEqual(
    #                 rtask_result.outcome_code,
    #                 TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
    #             )

    #             fn_outputs = deserialized_function_output(
    #                 self, rtask_result.function_outputs, function_outputs_blob
    #             )
    #             self.assertEqual(len(fn_outputs), 1)
    #             self.assertEqual(input_data, fn_outputs[0])
    #             output_serialized_object: SerializedObjectInsideBLOB = (
    #                 rtask_result.function_outputs[0]
    #             )
    #             self.assertEqual(output_serialized_object.offset, 0)
    #             self.assertEqual(
    #                 output_serialized_object.manifest.size, len(input_data_serialized)
    #             )
    #             self.assertEqual(
    #                 output_serialized_object.manifest.sha256_hash,
    #                 hashlib.sha256(input_data_serialized).hexdigest(),
    #             )

    #             # Verify that output BLOB chunks exactly match the output data and the original BLOB chunks.
    #             chunks_count: int = input_data_serialized_size // 1024 + 1
    #             self.assertEqual(
    #                 len(rtask_result.uploaded_function_outputs_blob.chunks),
    #                 chunks_count,
    #             )
    #             etags: List[str] = []
    #             for ix, uploaded_chunk in enumerate(
    #                 rtask_result.uploaded_function_outputs_blob.chunks
    #             ):
    #                 uploaded_chunk: BLOBChunk
    #                 if ix < chunks_count - 1:
    #                     self.assertEqual(uploaded_chunk.size, 1024)
    #                 else:
    #                     # The 1 extra byte that should go to 6th chunk + CloudPickle header.
    #                     # Both should fit into the last chunk.
    #                     self.assertEqual(
    #                         uploaded_chunk.size,
    #                         input_data_serialized_size % 1024,
    #                     )
    #                 self.assertIsNotNone(uploaded_chunk.etag)
    #                 self.assertNotIn(uploaded_chunk.etag, etags)
    #                 etags.append(uploaded_chunk.etag)
    #                 self.assertEqual(
    #                     uploaded_chunk.uri, function_outputs_blob.chunks[ix].uri
    #                 )

    # def test_function_output_blob_with_multiple_chunks_and_function_outputs(self):
    #     graph = Graph(
    #         name="test", description="test", start_node=extractor_returns_3x_argument
    #     )
    #     graph_data: bytes = zip_graph_code(
    #         graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH
    #     )
    #     with FunctionExecutorProcessContextManager(
    #         capture_std_outputs=True,
    #     ) as process:
    #         with rpc_channel(process) as channel:
    #             stub: FunctionExecutorStub = FunctionExecutorStub(channel)
    #             initialize_response: InitializeResponse = stub.initialize(
    #                 InitializeRequest(
    #                     namespace="test",
    #                     graph_name="test",
    #                     graph_version="1",
    #                     function_name="extractor_returns_3x_argument",
    #                     graph=SerializedObject(
    #                         manifest=SerializedObjectManifest(
    #                             encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
    #                             encoding_version=0,
    #                             size=len(graph_data),
    #                             sha256_hash=hashlib.sha256(graph_data).hexdigest(),
    #                         ),
    #                         data=graph_data,
    #                     ),
    #                 )
    #             )
    #             self.assertEqual(
    #                 initialize_response.outcome_code,
    #                 InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
    #             )

    #             function_outputs_blob: BLOB = create_tmp_blob(
    #                 chunks_count=10, chunk_size=1024
    #             )
    #             # 3 full chunks + 1 byte of output data out of 10 chunks
    #             input_data: bytes = os.urandom(3 * 1024 + 1)
    #             input_data_serialized: bytes = CloudPickleSerializer.serialize(
    #                 input_data
    #             )
    #             input_data_serialized_size: int = len(input_data_serialized)
    #             task_result: TaskResult = run_task(
    #                 stub,
    #                 function_name="extractor_returns_3x_argument",
    #                 input=input_data,
    #                 function_outputs_blob=function_outputs_blob,
    #                 invocation_error_blob=create_tmp_blob(),
    #             )

    #             self.assertEqual(
    #                 task_result.outcome_code,
    #                 TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
    #             )

    #             fn_outputs = deserialized_function_output(
    #                 self, task_result.function_outputs, function_outputs_blob
    #             )
    #             self.assertEqual(len(fn_outputs), 3)
    #             for output_ix in range(3):
    #                 self.assertEqual(input_data, fn_outputs[output_ix])
    #                 output_serialized_object: SerializedObjectInsideBLOB = (
    #                     task_result.function_outputs[output_ix]
    #                 )
    #                 self.assertEqual(
    #                     output_serialized_object.offset,
    #                     output_ix * input_data_serialized_size,
    #                 )
    #                 self.assertEqual(
    #                     output_serialized_object.manifest.size,
    #                     input_data_serialized_size,
    #                 )
    #                 self.assertEqual(
    #                     output_serialized_object.manifest.sha256_hash,
    #                     hashlib.sha256(input_data_serialized).hexdigest(),
    #                 )

    #             # Verify that output BLOB chunks exactly match the output data and the original BLOB chunks.
    #             chunks_count: int = input_data_serialized_size * 3 // 1024 + 1
    #             self.assertEqual(
    #                 len(task_result.uploaded_function_outputs_blob.chunks),
    #                 chunks_count,
    #             )
    #             for ix, uploaded_chunk in enumerate(
    #                 task_result.uploaded_function_outputs_blob.chunks
    #             ):
    #                 uploaded_chunk: BLOBChunk
    #                 if ix < chunks_count - 1:
    #                     self.assertEqual(uploaded_chunk.size, 1024)
    #                 else:
    #                     self.assertEqual(
    #                         uploaded_chunk.size,
    #                         (input_data_serialized_size * 3) % 1024,
    #                     )
    #                 self.assertIsNotNone(uploaded_chunk.etag)
    #                 self.assertEqual(
    #                     uploaded_chunk.uri, function_outputs_blob.chunks[ix].uri
    #                 )

    # def test_function_raises_error(self):
    #     graph = Graph(name="test-exception", description="test", start_node=extractor_a)
    #     graph = graph.add_edge(extractor_a, extractor_exception)
    #     graph = graph.add_edge(extractor_exception, extractor_b)
    #     graph_data: bytes = zip_graph_code(
    #         graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH
    #     )

    #     with FunctionExecutorProcessContextManager(capture_std_outputs=True) as process:
    #         with rpc_channel(process) as channel:
    #             stub: FunctionExecutorStub = FunctionExecutorStub(channel)
    #             initialize_response: InitializeResponse = stub.initialize(
    #                 InitializeRequest(
    #                     namespace="test",
    #                     graph_name="test",
    #                     graph_version="1",
    #                     function_name="extractor_exception",
    #                     graph=SerializedObject(
    #                         manifest=SerializedObjectManifest(
    #                             encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
    #                             encoding_version=0,
    #                             size=len(graph_data),
    #                             sha256_hash=hashlib.sha256(graph_data).hexdigest(),
    #                         ),
    #                         data=graph_data,
    #                     ),
    #                 )
    #             )
    #             self.assertEqual(
    #                 initialize_response.outcome_code,
    #                 InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
    #             )

    #             function_outputs_blob: BLOB = create_tmp_blob()
    #             task_result: TaskResult = run_task(
    #                 stub,
    #                 function_name="extractor_exception",
    #                 input=10,
    #                 function_outputs_blob=function_outputs_blob,
    #                 invocation_error_blob=create_tmp_blob(),
    #             )
    #             self.assertEqual(
    #                 task_result.outcome_code,
    #                 TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
    #             )
    #             self.assertEqual(
    #                 task_result.failure_reason,
    #                 TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR,
    #             )
    #             self.assertFalse(task_result.HasField("invocation_error_output"))
    #             # Check FE logs (separated from function logs)
    #             # Skipping this right now because currently we print FE logs to FE stdout.
    #             # self.assertIn(
    #             #     "running function", task_result.diagnostics.function_executor_log
    #             # )
    #             # self.assertIn(
    #             #     "function finished", task_result.diagnostics.function_executor_log
    #             # )
    #             # # Verify that customer data is not printed in FE logs
    #             # self.assertNotIn(
    #             #     "this extractor throws an exception",
    #             #     task_result.diagnostics.function_executor_log,
    #             # )

    #     fe_stdout = process.read_stdout()
    #     # Check FE events in stdout
    #     self.assertIn("function_executor_initialization_started", fe_stdout)
    #     self.assertIn("function_executor_initialization_finished", fe_stdout)
    #     self.assertIn("task_allocations_started", fe_stdout)
    #     self.assertIn("task_allocations_finished", fe_stdout)
    #     # Check function output to stderr
    #     self.assertIn("this extractor throws an exception.", process.read_stderr())

    # def test_function_initialization_raises_error(self):
    #     graph = Graph(
    #         name="test-initialization-exception",
    #         description="test",
    #         start_node=FunctionFailingOnInit,
    #     )
    #     graph_data: bytes = zip_graph_code(
    #         graph=graph, code_dir_path=GRAPH_CODE_DIR_PATH
    #     )

    #     with FunctionExecutorProcessContextManager(capture_std_outputs=True) as process:
    #         with rpc_channel(process) as channel:
    #             stub: FunctionExecutorStub = FunctionExecutorStub(channel)
    #             initialize_response: InitializeResponse = stub.initialize(
    #                 InitializeRequest(
    #                     namespace="test",
    #                     graph_name="test",
    #                     graph_version="1",
    #                     function_name="FunctionFailingOnInit",
    #                     graph=SerializedObject(
    #                         manifest=SerializedObjectManifest(
    #                             encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
    #                             encoding_version=0,
    #                             size=len(graph_data),
    #                             sha256_hash=hashlib.sha256(graph_data).hexdigest(),
    #                         ),
    #                         data=graph_data,
    #                     ),
    #                 )
    #             )
    #             self.assertEqual(
    #                 initialize_response.outcome_code,
    #                 InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
    #             )
    #             self.assertEqual(
    #                 initialize_response.failure_reason,
    #                 InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
    #             )
    #             # Check FE logs (separated from function logs)
    #             # Skipping this right now because currently we print FE logs to FE stdout.
    #             # self.assertIn(
    #             #     "initializing function executor service",
    #             #     initialize_response.diagnostics.function_executor_log,
    #             # )
    #             # self.assertIn(
    #             #     "function executor service initialization failed",
    #             #     initialize_response.diagnostics.function_executor_log,
    #             # )
    #             # self.assertIn(
    #             #     "failed to load customer function",
    #             #     initialize_response.diagnostics.function_executor_log,
    #             # )
    #             # Verify that customer data is not printed in FE logs
    #             # self.assertNotIn(
    #             #     "This function fails on initialization",
    #             #     initialize_response.diagnostics.function_executor_log,
    #             # )

    #     fe_stdout = process.read_stdout()
    #     # Check FE events in stdout
    #     self.assertIn("function_executor_initialization_started", fe_stdout)
    #     self.assertIn("function_executor_initialization_finished", fe_stdout)
    #     # Check function output to stderr
    #     self.assertIn("This function fails on initialization", process.read_stderr())


if __name__ == "__main__":
    unittest.main()
