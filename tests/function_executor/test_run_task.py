import unittest
from typing import List, Mapping

from pydantic import BaseModel
from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    deserialized_function_output,
    read_local_blob_str,
    rpc_channel,
    run_task,
    tmp_local_file_blob,
)

from tensorlake import Graph
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
    TaskFailureReason,
    TaskOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.graph_serialization import (
    graph_code_dir_path,
    zip_graph_code,
)
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer

GRAPH_CODE_DIR_PATH = graph_code_dir_path(__file__)


@tensorlake_function()
def extractor_a(url: str) -> File:
    print(f"extractor_a called with url: {url}")
    assert url == "https://example.com"
    assert isinstance(url, str)
    return File(data=bytes(b"hello"), mime_type="text/plain")


class FileChunk(BaseModel):
    data: bytes
    start: int
    end: int


@tensorlake_function()
def extractor_b(file: File) -> List[FileChunk]:
    return [
        FileChunk(data=file.data, start=0, end=5),
        FileChunk(data=file.data, start=5, end=len(file.data)),
    ]


class SomeMetadata(BaseModel):
    metadata: Mapping[str, str]


@tensorlake_function()
def extractor_c(file_chunk: FileChunk) -> SomeMetadata:
    return SomeMetadata(metadata={"a": "b", "c": "d"})


@tensorlake_function()
def extractor_exception(a: int) -> int:
    raise Exception("this extractor throws an exception.")


def create_graph_a() -> Graph:
    graph = Graph(name="test", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_b)
    graph = graph.add_edge(extractor_b, extractor_c)
    return graph


def create_graph_exception() -> Graph:
    graph = Graph(name="test-exception", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_exception)
    graph = graph.add_edge(extractor_exception, extractor_b)
    return graph


class TestRunTask(unittest.TestCase):
    def test_function_success(self):
        graph_data: bytes = zip_graph_code(
            graph=create_graph_a(), code_dir_path=GRAPH_CODE_DIR_PATH
        )
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="extractor_b",
                        graph=SerializedObject(
                            manifest=SerializedObjectManifest(
                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                                encoding_version=0,
                                size=len(graph_data),
                            ),
                            data=graph_data,
                        ),
                        stdout=tmp_local_file_blob(),
                        stderr=tmp_local_file_blob(),
                    )
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
                )

                run_task_response: RunTaskResponse = run_task(
                    stub,
                    function_name="extractor_b",
                    input=File(data=bytes(b"hello"), mime_type="text/plain"),
                )

                self.assertEqual(
                    run_task_response.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
                )
                self.assertFalse(run_task_response.is_reducer)
                self.assertFalse(run_task_response.HasField("invocation_error_output"))

                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_outputs
                )
                self.assertEqual(len(fn_outputs), 2)
                expected = FileChunk(data=b"hello", start=5, end=5)

                self.assertEqual(expected.model_dump(), fn_outputs[1].model_dump())

    def test_function_raises_error(self):
        graph_data: bytes = zip_graph_code(
            graph=create_graph_exception(), code_dir_path=GRAPH_CODE_DIR_PATH
        )
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 1
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="extractor_exception",
                        graph=SerializedObject(
                            manifest=SerializedObjectManifest(
                                encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                                encoding_version=0,
                                size=len(graph_data),
                            ),
                            data=graph_data,
                        ),
                        stdout=tmp_local_file_blob(),
                        stderr=tmp_local_file_blob(),
                    )
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
                )

                stderr_blob = tmp_local_file_blob()
                run_task_response: RunTaskResponse = run_task(
                    stub,
                    function_name="extractor_exception",
                    input=10,
                    stderr_blob=stderr_blob,
                )

                self.assertEqual(
                    run_task_response.outcome_code,
                    TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    run_task_response.failure_reason,
                    TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR,
                )
                self.assertFalse(run_task_response.HasField("invocation_error_output"))
                self.assertFalse(run_task_response.is_reducer)
                self.assertTrue(
                    "this extractor throws an exception."
                    in read_local_blob_str(stderr_blob)
                )


if __name__ == "__main__":
    unittest.main()
