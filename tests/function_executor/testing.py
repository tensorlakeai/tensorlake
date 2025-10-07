import hashlib
import os
import subprocess
import tempfile
import unittest
from typing import Any, Dict, List

import grpc

from tensorlake.applications.ast.value_node import ValueMetadata
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.zip import zip_code
from tensorlake.applications.user_data_serializer import (
    JSONUserDataSerializer,
)
from tensorlake.function_executor.handlers.run_function.value_node_metadata import (
    ValueNodeMetadata,
)
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
    FunctionRef,
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
from tensorlake.function_executor.proto.server_configuration import GRPC_SERVER_OPTIONS


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
        self._process: subprocess.Popen | None = None
        self._stdout: str | None = None
        self._stderr: str | None = None

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

    def read_stdout(self) -> str | None:
        # Only call this after FE exits.
        return self._stdout

    def read_stderr(self) -> str | None:
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


def initialize(
    stub: FunctionExecutorStub,
    app_name: str,
    app_version: str,
    app_code_dir_path: str,
    function_name: str,
) -> InitializeResponse:
    application_zip: bytes = zip_code(
        code_dir_path=app_code_dir_path,
        ignored_absolute_paths=set(),
        all_functions=get_functions(),
    )
    return stub.initialize(
        InitializeRequest(
            function=FunctionRef(
                namespace="test",
                application_name=app_name,
                application_version=app_version,
                function_name=function_name,
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


def run_allocation(
    stub: FunctionExecutorStub,
    inputs: FunctionInputs,
    timeout_sec: int | None = None,
) -> AllocationResult:
    allocation_id: str = "test-allocation"
    stub.create_allocation(
        CreateAllocationRequest(
            allocation=Allocation(
                request_id="123",
                function_call_id="test-function-call",
                allocation_id=allocation_id,
                inputs=inputs,
            ),
        )
    )

    await_allocation_stream_rpc = stub.await_allocation(
        AwaitAllocationRequest(allocation_id=allocation_id), timeout=timeout_sec
    )
    result: AllocationResult | None = None
    for progress in await_allocation_stream_rpc:
        progress: AwaitAllocationProgress
        if progress.WhichOneof("response") == "allocation_result":
            result = progress.allocation_result
            break

    await_allocation_stream_rpc.cancel()
    stub.delete_allocation(DeleteAllocationRequest(allocation_id=allocation_id))

    if result is None:
        # Check in case if stream finished by FE without allocation_result.
        raise Exception("Allocation result was not received from the server.")

    return result


def application_function_inputs(payload: Any) -> FunctionInputs:
    user_serializer: JSONUserDataSerializer = JSONUserDataSerializer()
    serialized_payload: bytes = user_serializer.serialize(payload)
    payload_blob: BLOB = create_tmp_blob(
        chunks_count=1, chunk_size=len(serialized_payload)
    )
    write_tmp_blob_bytes(payload_blob, serialized_payload)

    return FunctionInputs(
        args=[
            SerializedObjectInsideBLOB(
                manifest=SerializedObjectManifest(
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                    encoding_version=0,
                    size=len(serialized_payload),
                    metadata_size=0,  # No metadata for API function calls.
                    sha256_hash=hashlib.sha256(serialized_payload).hexdigest(),
                ),
                offset=0,
            )
        ],
        arg_blobs=[payload_blob],
        function_outputs_blob=create_tmp_blob(),
        request_error_blob=create_tmp_blob(),
        function_call_metadata=b"",
    )


def download_and_deserialize_so(
    test_case: unittest.TestCase,
    so: SerializedObjectInsideBLOB,
    so_blob: BLOB,
) -> Any:
    serialized_node_metadata: bytes = read_tmp_blob_bytes(
        so_blob, so.offset, so.manifest.metadata_size
    )
    serialized_data: bytes = read_tmp_blob_bytes(
        so_blob,
        so.offset + so.manifest.metadata_size,
        so.manifest.size - so.manifest.metadata_size,
    )
    test_case.assertEqual(
        hashlib.sha256(serialized_node_metadata + serialized_data).hexdigest(),
        so.manifest.sha256_hash,
    )

    node_metadata: ValueNodeMetadata = ValueNodeMetadata.deserialize(
        serialized_node_metadata
    )
    return ValueMetadata.deserialize(node_metadata.metadata).deserialize_value(
        serialized_data
    )


def read_so_metadata(
    test_case: unittest.TestCase, so: SerializedObjectInsideBLOB, so_blob: BLOB
) -> ValueNodeMetadata:
    serialized_node_metadata: bytes = read_tmp_blob_bytes(
        so_blob, so.offset, so.manifest.metadata_size
    )
    return ValueNodeMetadata.deserialize(serialized_node_metadata)


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
