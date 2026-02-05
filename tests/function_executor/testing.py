import hashlib
import os
import subprocess
import tempfile
import unittest
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List

import grpc

from tensorlake.applications.function.user_data_serializer import (
    deserialize_value_with_metadata,
)
from tensorlake.applications.metadata import ValueMetadata, deserialize_metadata
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.zip import zip_code
from tensorlake.applications.user_data_serializer import (
    JSONUserDataSerializer,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    AllocationOutputBLOB,
    AllocationOutputBLOBRequest,
    AllocationResult,
    AllocationState,
    AllocationUpdate,
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
    WatchAllocationStateRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.server_configuration import GRPC_SERVER_OPTIONS
from tensorlake.function_executor.proto.status_pb2 import Status


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
    request: CreateAllocationRequest,
    timeout_sec: float | None = None,
) -> Iterator[AllocationState]:
    stub.create_allocation(request)
    return stub.watch_allocation_state(
        WatchAllocationStateRequest(allocation_id=request.allocation.allocation_id),
        timeout=timeout_sec,
    )


def run_allocation_that_returns_output(
    test_case: unittest.TestCase,
    stub: FunctionExecutorStub,
    request: CreateAllocationRequest,
    timeout_sec: float | None = None,
) -> AllocationResult:
    stub.create_allocation(request)
    return wait_result_of_allocation_that_returns_output(
        request.allocation.allocation_id,
        test_case,
        stub,
        timeout_sec=timeout_sec,
    )


def wait_result_of_allocation_that_returns_output(
    allocation_id: str,
    test_case: unittest.TestCase,
    stub: FunctionExecutorStub,
    timeout_sec: float | None,
) -> AllocationResult:
    current_allocation_state: str = "wait_blob_request"

    allocation_states: Iterator[AllocationState] = stub.watch_allocation_state(
        WatchAllocationStateRequest(allocation_id=allocation_id),
        timeout=timeout_sec,
    )

    for allocation_state in allocation_states:
        allocation_state: AllocationState

        if current_allocation_state == "wait_blob_request":
            if len(allocation_state.output_blob_requests) == 0:
                continue  # Received empty initial AllocationState, keep waiting.

            test_case.assertEqual(len(allocation_state.output_blob_requests), 1)
            function_output_blob_request: AllocationOutputBLOBRequest = (
                allocation_state.output_blob_requests[0]
            )
            function_output_blob: BLOB = create_tmp_blob(
                id=function_output_blob_request.id,
                chunks_count=1,
                chunk_size=function_output_blob_request.size,
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
                return allocation_state.result


def run_allocation_that_fails(
    stub: FunctionExecutorStub,
    request: CreateAllocationRequest,
    timeout_sec: float | None = None,
) -> AllocationResult:
    allocation_states: Iterator[AllocationState] = run_allocation(
        stub,
        request=request,
        timeout_sec=timeout_sec,
    )
    for allocation_state in allocation_states:
        allocation_state: AllocationState

        if allocation_state.HasField("result"):
            return allocation_state.result


def delete_allocation(
    stub: FunctionExecutorStub,
    allocation_id: str,
) -> None:
    stub.delete_allocation(
        DeleteAllocationRequest(
            allocation_id=allocation_id,
        )
    )


def application_function_inputs(payload: Any, type_hint: Any) -> FunctionInputs:
    user_serializer: JSONUserDataSerializer = JSONUserDataSerializer()
    serialized_payload: bytes = user_serializer.serialize(payload, type_hint)
    payload_blob: BLOB = write_new_application_payload_blob(serialized_payload)

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
        request_error_blob=create_request_error_blob(),
        function_call_metadata=b"",
    )


def create_request_error_blob() -> BLOB:
    return create_tmp_blob(id="request-error-blob")


def write_new_application_payload_blob(serialized_payload: bytes) -> BLOB:
    blob: BLOB = create_tmp_blob(
        id="application-payload-blob",
        chunks_count=1,
        chunk_size=len(serialized_payload),
    )
    write_tmp_blob_bytes(blob, serialized_payload)
    return blob


def download_and_deserialize_so(
    test_case: unittest.TestCase,
    so: SerializedObjectInsideBLOB,
    so_blob: BLOB,
) -> Any:
    serialized_value_metadata: bytes = read_tmp_blob_bytes(
        so_blob, so.offset, so.manifest.metadata_size
    )
    serialized_data: bytes = read_tmp_blob_bytes(
        so_blob,
        so.offset + so.manifest.metadata_size,
        so.manifest.size - so.manifest.metadata_size,
    )
    test_case.assertEqual(
        hashlib.sha256(serialized_value_metadata + serialized_data).hexdigest(),
        so.manifest.sha256_hash,
    )

    metadata: ValueMetadata = deserialize_metadata(serialized_value_metadata)
    return deserialize_value_with_metadata(
        serialized_value=serialized_data, metadata=metadata
    )


def read_so_metadata(
    test_case: unittest.TestCase, so: SerializedObjectInsideBLOB, so_blob: BLOB
) -> ValueMetadata:
    serialized_value_metadata: bytes = read_tmp_blob_bytes(
        so_blob, so.offset, so.manifest.metadata_size
    )
    metadata: ValueMetadata = deserialize_metadata(serialized_value_metadata)
    test_case.assertIsInstance(metadata, ValueMetadata)
    return metadata


def create_tmp_blob(
    id: str, chunks_count: int = 5, chunk_size: int = 1 * 1024 * 1024
) -> BLOB:
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
        return BLOB(id=id, chunks=list(chunks))


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


@dataclass
class HTTPBodyPart:
    content_type: str
    field_name: str
    body: bytes


def create_multipart_invoke_http_request(
    parts: list[HTTPBodyPart], boundary: str
) -> bytes:
    """Creates a multipart HTTP request with the given parts and boundary."""
    lines: list[bytes] = [
        b"POST /invoke HTTP/1.1",
        b"Host: localhost",
        f'Content-Type: multipart/form-data; boundary="{boundary}"'.encode("utf-8"),
        b"",  # Empty line between headers and body
    ]
    boundary_bytes: bytes = boundary.encode("utf-8")

    for part in parts:
        lines.append(b"--" + boundary_bytes)
        lines.append(
            f'Content-Disposition: form-data; name="{part.field_name}"'.encode("utf-8")
        )
        lines.append(f"Content-Type: {part.content_type}".encode("utf-8"))
        lines.append(b"")  # Empty line between headers and body
        lines.append(part.body)

    lines.append(b"--" + boundary_bytes + b"--")
    lines.append(b"")

    return b"\r\n".join(lines)
