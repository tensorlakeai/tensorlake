import os
import subprocess
import tempfile
import unittest
from typing import Any, Dict, List, Optional

import grpc

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    RunTaskRequest,
    RunTaskResponse,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.server_configuration import GRPC_SERVER_OPTIONS
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer

# Default Executor range is 50000:51000.
# Use a value outside of this range to not conflict with other tests.
DEFAULT_FUNCTION_EXECUTOR_PORT: int = 60000


class FunctionExecutorProcessContextManager:
    def __init__(
        self,
        port: int = DEFAULT_FUNCTION_EXECUTOR_PORT,
        extra_args: List[str] = [],
        keep_std_outputs: bool = True,
        extra_env: Dict[str, str] = {},
    ):
        self.port = port
        self._args = [
            "function-executor",
            "--dev",
            "--address",
            f"localhost:{port}",
            "--executor-id",
            "test-executor",
            "--function-executor-id",
            "test-function-executor",
        ]
        self._args.extend(extra_args)
        self._keep_std_outputs = keep_std_outputs
        self._extra_env = extra_env
        self._process: Optional[subprocess.Popen] = None

    def __enter__(self) -> "FunctionExecutorProcessContextManager":
        kwargs = {}
        if not self._keep_std_outputs:
            kwargs["stdout"] = subprocess.DEVNULL
            kwargs["stderr"] = subprocess.DEVNULL
        if self._extra_env is not None:
            kwargs["env"] = os.environ.copy()
            kwargs["env"].update(self._extra_env)
        self._process = subprocess.Popen(self._args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._process:
            self._process.terminate()
            self._process.wait()


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
    stdout_blob: Optional[BLOB] = None,
    stderr_blob: Optional[BLOB] = None,
    timeout_sec: Optional[int] = None,
) -> RunTaskResponse:
    function_input_blob: BLOB = tmp_local_file_blob()
    function_input_path: str = function_input_blob.uri.replace("file://", "", 1)
    function_input_data: bytes = CloudPickleSerializer.serialize(input)
    with open(function_input_path, "wb") as f:
        f.write(function_input_data)

    if stdout_blob is None:
        stdout_blob = tmp_local_file_blob()
    if stderr_blob is None:
        stderr_blob = tmp_local_file_blob()

    return stub.run_task(
        RunTaskRequest(
            namespace="test",
            graph_name="test",
            graph_version="1",
            function_name=function_name,
            graph_invocation_id="123",
            task_id="test-task",
            allocation_id="test-allocation",
            function_input=SerializedObjectInsideBLOB(
                manifest=SerializedObjectManifest(
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
                    encoding_version=0,
                    size=len(function_input_data),
                ),
                blob=function_input_blob,
                offset=0,
            ),
            stdout=stdout_blob,
            stderr=stderr_blob,
            function_outputs=tmp_local_file_blob(),
        ),
        timeout=timeout_sec,
    )


def deserialized_function_output(
    test_case: unittest.TestCase, function_outputs: List[SerializedObjectInsideBLOB]
) -> List[Any]:
    outputs: List[Any] = []
    for output in function_outputs:
        test_case.assertEqual(
            output.manifest.encoding,
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
        )
        output_path: str = output.blob.uri.replace("file://", "", 1)
        with open(output_path, "rb") as f:
            f.seek(output.offset)
            data: bytes = f.read(output.manifest.size)
        outputs.append(CloudPickleSerializer.deserialize(data))
    return outputs


def tmp_local_file_blob() -> BLOB:
    """Returns a temporary local file blob."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()
    return BLOB(
        uri=f"file://{os.path.abspath(temp_file.name)}",
    )


def read_local_blob_str(blob: BLOB) -> str:
    """Reads a local blob and returns its content as a string."""
    file_path: str = blob.uri.replace("file://", "", 1)
    with open(file_path, "r") as f:
        return f.read()
