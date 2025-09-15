import hashlib
import os
import subprocess
import tempfile
import unittest
from typing import Any, Dict, List, Optional

import grpc

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationResult,
    AwaitAllocationProgress,
    AwaitAllocationRequest,
    BLOBChunk,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    FunctionInputs,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.server_configuration import GRPC_SERVER_OPTIONS
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer


class FunctionExecutorProcessContextManager:
    def __init__(
        self,
        port: int = 60000,
        extra_args: List[str] = [],
        extra_env: Dict[str, str] = {},
        capture_std_outputs: bool = False,
    ):
        self.port = port
        self._args = [
            "function-executor",
            "--address",
            f"localhost:{port}",
            "--executor-id",
            "test-executor",
            "--function-executor-id",
            "test-function-executor",
        ]
        self._args.extend(extra_args)
        self._extra_env = extra_env
        self._capture_std_outputs = capture_std_outputs
        self._process: Optional[subprocess.Popen] = None
        self._stdout: Optional[str] = None
        self._stderr: Optional[str] = None

    def __enter__(self) -> "FunctionExecutorProcessContextManager":
        kwargs = {}
        if self._extra_env is not None:
            kwargs["env"] = os.environ.copy()
            kwargs["env"].update(self._extra_env)
        if self._capture_std_outputs:
            kwargs["stdout"] = subprocess.PIPE
            kwargs["stderr"] = subprocess.PIPE
        self._process = subprocess.Popen(self._args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._process:
            self._process.terminate()
            if self._capture_std_outputs:
                self._stdout = self._process.stdout.read().decode("utf-8")
                self._stderr = self._process.stderr.read().decode("utf-8")
            self._process.__exit__(exc_type, exc_value, traceback)

    def read_stdout(self) -> Optional[str]:
        # Only call this after FE exits.
        return self._stdout

    def read_stderr(self) -> Optional[str]:
        # Only call this after FE exits.
        return self._stderr


def rpc_channel(context_manager: FunctionExecutorProcessContextManager) -> grpc.Channel:
    # The GRPC_SERVER_OPTIONS include the maximum message size which we need to set in the client channel.
    channel: grpc.Channel = grpc.insecure_channel(
        f"localhost:{context_manager.port}",
        options=GRPC_SERVER_OPTIONS,
    )
    try:
        SERVER_STARTUP_TIMEOUT_SEC = 5
        # This is not asyncio.Future but grpc.Future. It has a different interface.
        grpc.channel_ready_future(channel).result(timeout=SERVER_STARTUP_TIMEOUT_SEC)
        return channel
    except Exception as e:
        channel.close()
        raise Exception(
            f"Failed to connect to the gRPC server within {SERVER_STARTUP_TIMEOUT_SEC} seconds"
        ) from e


def run_task(
    stub: FunctionExecutorStub,
    function_name: str,
    input: Any,
    function_outputs_blob: BLOB,
    invocation_error_blob: BLOB,
    timeout_sec: Optional[int] = None,
) -> AllocationResult:
    function_input_blob: BLOB = create_tmp_blob()
    function_input_data: bytes = CloudPickleSerializer.serialize(input)
    write_tmp_blob_bytes(
        blob=function_input_blob,
        data=function_input_data,
    )

    task_id: str = "test-task"
    stub.create_allocation(
        CreateAllocationRequest(
            task=Allocation(
                namespace="test",
                graph_name="test",
                graph_version="1",
                function_name=function_name,
                graph_invocation_id="123",
                task_id=task_id,
                allocation_id="test-allocation",
                request=FunctionInputs(
                    function_input_blob=function_input_blob,
                    function_input=SerializedObjectInsideBLOB(
                        manifest=SerializedObjectManifest(
                            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                            encoding_version=0,
                            size=len(function_input_data),
                            sha256_hash=hashlib.sha256(function_input_data).hexdigest(),
                        ),
                        offset=0,
                    ),
                    function_outputs_blob=function_outputs_blob,
                    invocation_error_blob=invocation_error_blob,
                ),
            ),
        )
    )

    await_task_stream_rpc = stub.await_allocation(
        AwaitAllocationRequest(task_id=task_id), timeout=timeout_sec
    )
    result: Optional[AllocationResult] = None
    for progress in await_task_stream_rpc:
        progress: AwaitAllocationProgress
        if progress.WhichOneof("response") == "task_result":
            result: AllocationResult = progress.allocation_result
            break

    await_task_stream_rpc.cancel()
    stub.delete_allocation(DeleteAllocationRequest(task_id=task_id))

    if result is None:
        # Check in case if stream finished by FE without task_result.
        raise Exception("Allocation result was not received from the server.")

    return result


def deserialized_function_output(
    test_case: unittest.TestCase,
    function_outputs: List[SerializedObjectInsideBLOB],
    function_outputs_blob: BLOB,
) -> List[Any]:
    outputs: List[Any] = []
    for output in function_outputs:
        test_case.assertEqual(
            output.manifest.encoding,
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
        )
        data: bytes = read_tmp_blob_bytes(
            function_outputs_blob, output.offset, output.manifest.size
        )
        test_case.assertEqual(len(data), output.manifest.size)
        test_case.assertEqual(
            hashlib.sha256(data).hexdigest(), output.manifest.sha256_hash
        )
        outputs.append(CloudPickleSerializer.deserialize(data))
    return outputs


def create_tmp_blob(chunks_count: int = 5, chunk_size: int = 1 * 1024 * 1024) -> BLOB:
    """Returns a temporary local file backed blob for writing."""
    with tempfile.NamedTemporaryFile(delete=False) as blob_file:
        # blob_file.write(b"0" * chunk_size)
        blob_file_uri: str = f"file://{os.path.abspath(blob_file.name)}"
        chunks: List[BLOBChunk] = []
        for _ in range(chunks_count):
            chunks.append(
                BLOBChunk(
                    uri=blob_file_uri,
                    size=chunk_size,
                )
            )
        return BLOB(chunks=list(chunks))


def read_tmp_blob_bytes(blob: BLOB, offset: int, size: int) -> bytes:
    """Reads a local blob and returns its content as bytes."""
    blob_file_path: str = blob.chunks[0].uri.replace("file://", "", 1)
    with open(blob_file_path, "rb") as f:
        f.seek(offset)
        return f.read(size)


def write_tmp_blob_bytes(blob: BLOB, data: bytes) -> None:
    """Writes bytes to a local blob from its very beginning."""
    blob_file_path: str = blob.chunks[0].uri.replace("file://", "", 1)
    with open(blob_file_path, "wb") as f:
        return f.write(data)
