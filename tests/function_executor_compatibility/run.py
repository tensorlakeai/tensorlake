"""Runs a shared protocol-parity matrix against both function executors."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import pickle
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from collections import Counter
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SOURCE_ROOT = REPOSITORY_ROOT / "src"
FIXTURE_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PYTHON_SOURCE_ROOT))
sys.path.insert(0, str(FIXTURE_ROOT))

import grpc
import python_application  # noqa: F401  # Included in the Python fixture ZIP.

from tensorlake.applications import File as PythonFile
from tensorlake.applications import FunctionError as PythonFunctionError
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value_with_metadata,
)
from tensorlake.applications.metadata import (
    ValueMetadata,
    deserialize_metadata,
    serialize_metadata,
)
from tensorlake.applications.metadata.function_call import FunctionCallMetadata
from tensorlake.applications.user_data_serializer import PickleUserDataSerializer
from tensorlake.function_executor.proto.function_executor_pb2 import (
    ALLOCATION_OUTCOME_CODE_FAILURE,
    ALLOCATION_OUTCOME_CODE_SUCCESS,
    BLOB,
    FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
    FUNCTION_CALL_WATCHER_STATUS_TIMEDOUT,
    INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
    INITIALIZATION_OUTCOME_CODE_FAILURE,
    INITIALIZATION_OUTCOME_CODE_SUCCESS,
    REPLAY_MODE_NONE,
    REPLAY_MODE_STRICT,
    AdvanceAllocationExecutionLogBatchRequest,
    Allocation,
    AllocationEvent,
    AllocationEventFunctionCallCreated,
    AllocationEventFunctionCallWatcherCreated,
    AllocationEventFunctionCallWatcherResult,
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationOutputBlob,
    AllocationRequestStateCommitWriteOperationResult,
    AllocationRequestStateOperationResult,
    AllocationRequestStatePrepareReadOperationResult,
    AllocationRequestStatePrepareWriteOperationResult,
    AllocationUpdate,
    BLOBChunk,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    FunctionInputs,
    FunctionRef,
    GetAllocationExecutionLogBatchRequest,
    HealthCheckRequest,
    HttpRequestHeader,
    InfoRequest,
    InitializeRequest,
    ListAllocationsRequest,
    ReadAllocationEventLogResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    RequestContext as ProtocolRequestContext,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
    WatchAllocationEventLogReads,
    WatchAllocationStateRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.status_pb2 import Status

APPLICATION_VERSION = "compatibility-v1"
RPC_TIMEOUT_SECONDS = 10
ALLOCATION_TIMEOUT_SECONDS = 20
REQUEST_ERROR_BLOB_SIZE = 4096
CHUNKED_HTTP_BODY = bytes(range(256)) * 1024

DOUBLE_FUNCTION = "parity_double"
ADD_FUNCTION = "parity_add"
FAILING_FUNCTION = "parity_failing_child"
REQUEST_FAILING_FUNCTION = "parity_request_failing_child"
IDENTITY_FILE_FUNCTION = "parity_identity_file"
MISSING_FUNCTION = "parity_missing_after_import"

# Mirrors the typed-value branches in the orchestration server's
# prepare_data_payload and string_to_data_payload_encoding conversions.
SERVER_CONTENT_TYPE_BY_TYPED_ENCODING = {
    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON: "application/json",
    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT: "text/plain",
    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE: (
        "application/python-pickle"
    ),
    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP: "application/zip",
}
SERVER_TYPED_ENCODING_BY_CONTENT_TYPE = {
    content_type: encoding
    for encoding, content_type in SERVER_CONTENT_TYPE_BY_TYPED_ENCODING.items()
}

FUNCTION_NAMES = (
    DOUBLE_FUNCTION,
    ADD_FUNCTION,
    FAILING_FUNCTION,
    REQUEST_FAILING_FUNCTION,
    IDENTITY_FILE_FUNCTION,
    "parity_value",
    "parity_multipart",
    "parity_child",
    "parity_wait_first_failure_after_success",
    "parity_wait_first_failure_after_success_and_failure",
    "parity_wait_causal_replay",
    "parity_wait_batched_results",
    "parity_map",
    "parity_reduce",
    "parity_reduce_large",
    "parity_reduce_no_initial",
    "parity_map_reduce",
    "parity_tail_call",
    "parity_handled_child_failure",
    "parity_handled_child_request_error",
    "parity_handled_child_timeout",
    "parity_handled_creation_failure",
    "parity_watcher_creation_failure",
    "parity_request_error",
    "parity_function_error",
    "parity_file",
    "parity_json_file",
    "parity_http_body",
    "parity_state",
    "parity_progress_validation",
    "parity_context_validation",
    "parity_replay_mismatch",
    "parity_http_envelope",
    "parity_http_envelope_default",
    "parity_file_input",
    "parity_multipart_http_body",
    "parity_empty_http_body",
    "parity_malformed_json",
    "parity_chunked_http_body",
    "parity_wait_all_completed",
    "parity_wait_timeout",
    "parity_run_later",
    "parity_detached_future",
    "parity_future_reuse",
    "parity_map_empty",
    "parity_reduce_empty_initial",
    "parity_map_failure",
    "parity_reduce_failure",
    "parity_unhandled_child_failure",
    "parity_unhandled_child_request_error",
    "parity_context_events",
    "parity_state_failure",
)


@dataclass(frozen=True)
class Scenario:
    name: str
    input: Any
    expected_terminal: dict[str, Any]
    behavior: str = "none"
    expected_calls: tuple[int, int] = (0, 0)
    typescript_expected_calls: tuple[int, int] | None = None
    expected_state_operations: tuple[str, ...] = ()
    expected_progress: tuple[tuple[float, float], ...] = ()
    replay_success: bool = False
    replay_mismatch: bool = False
    multipart: bool = False
    raw_body: bool = False
    raw_content_type: str | None = None
    http_message: bool = False
    empty_body: bool = False
    input_chunk_size: int | None = None
    request_headers: tuple[tuple[str, str], ...] = ()
    expected_scheduled_calls: int = 0
    expected_metrics: tuple[tuple[str, str, float], ...] = ()
    expected_progress_events: tuple[
        tuple[float, float, str, tuple[tuple[str, str], ...]], ...
    ] = ()

    def call_counts(self, language: str) -> tuple[int, int]:
        if language == "typescript" and self.typescript_expected_calls is not None:
            return self.typescript_expected_calls
        return self.expected_calls


SCENARIOS = (
    Scenario(
        name="parity_value",
        input=21,
        expected_terminal={"outcome": "success", "value": {"value": 21}},
    ),
    Scenario(
        name="parity_multipart",
        input=[6, 7],
        expected_terminal={"outcome": "success", "value": 42},
        multipart=True,
    ),
    Scenario(
        name="parity_child",
        input=21,
        expected_terminal={"outcome": "success", "value": 42},
        behavior="double",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_wait_first_failure_after_success",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"done": 2, "not_done": 0},
        },
        behavior="wait_first_failure",
        expected_calls=(2, 2),
        replay_success=True,
    ),
    Scenario(
        name="parity_wait_first_failure_after_success_and_failure",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"done": 2, "not_done": 0},
        },
        behavior="wait_first_failure_with_failure",
        expected_calls=(2, 2),
        replay_success=True,
    ),
    Scenario(
        name="parity_wait_causal_replay",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {
                "done": 1,
                "not_done": 1,
                "marker": 46,
                "results": [42, 44],
            },
        },
        behavior="wait_causal_replay",
        expected_calls=(3, 3),
        replay_success=True,
    ),
    Scenario(
        name="parity_wait_batched_results",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {
                "done": 1,
                "not_done": 1,
                "results": [42, 44],
            },
        },
        behavior="wait_batched_results",
        expected_calls=(2, 2),
        replay_success=True,
    ),
    Scenario(
        name="parity_map",
        input=1,
        expected_terminal={"outcome": "success", "value": [2, 4, 6]},
        behavior="map",
        expected_calls=(1, 1),
        typescript_expected_calls=(3, 3),
        replay_success=True,
    ),
    Scenario(
        name="parity_reduce",
        input=1,
        expected_terminal={"outcome": "success", "value": 16},
        behavior="reduce",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_reduce_large",
        input=1,
        expected_terminal={"outcome": "success", "value": 131842},
        behavior="reduce_large",
        expected_calls=(1, 1),
        typescript_expected_calls=(2, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_reduce_no_initial",
        input=1,
        expected_terminal={"outcome": "success", "value": 6},
        behavior="reduce_no_initial",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_map_reduce",
        input=1,
        expected_terminal={"outcome": "success", "value": 12},
        behavior="map_reduce",
        expected_calls=(2, 2),
        typescript_expected_calls=(4, 4),
        replay_success=True,
    ),
    Scenario(
        name="parity_tail_call",
        input=21,
        expected_terminal={
            "outcome": "success",
            "tail_call": {"function": DOUBLE_FUNCTION, "arguments": [21]},
        },
        behavior="tail_call",
        expected_calls=(1, 0),
    ),
    Scenario(
        name="parity_handled_child_failure",
        input=21,
        expected_terminal={"outcome": "success", "value": "caught:function_error"},
        behavior="watcher_failure",
        expected_calls=(1, 1),
    ),
    Scenario(
        name="parity_handled_child_request_error",
        input=21,
        expected_terminal={"outcome": "success", "value": "caught:request_error"},
        behavior="watcher_request_error",
        expected_calls=(1, 1),
    ),
    Scenario(
        name="parity_handled_child_timeout",
        input=21,
        expected_terminal={"outcome": "success", "value": "caught:timeout"},
        behavior="watcher_timeout",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_handled_creation_failure",
        input=21,
        expected_terminal={"outcome": "success", "value": "caught:creation_error"},
        behavior="creation_failure",
        expected_calls=(1, 0),
    ),
    Scenario(
        name="parity_watcher_creation_failure",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "function_error",
        },
        behavior="watcher_creation_failure",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_request_error",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "request_error",
            "request_error": "invalid value: 21",
        },
    ),
    Scenario(
        name="parity_function_error",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "function_error",
        },
    ),
    Scenario(
        name="parity_file",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {
                "file": {
                    "content_hex": b"parity-file-21".hex(),
                    "content_type": "text/plain",
                }
            },
        },
    ),
    Scenario(
        name="parity_json_file",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {
                "content": '{"value":21}',
                "content_type": "application/json",
                "is_file": True,
            },
        },
        behavior="json_file",
        expected_calls=(1, 1),
    ),
    Scenario(
        name="parity_http_body",
        input=b' { "event": "created", "id": 42 }\n',
        expected_terminal={
            "outcome": "success",
            "value": {
                "content_hex": b' { "event": "created", "id": 42 }\n'.hex(),
                "content_type": "application/cloudevents+json; charset=utf-8",
                "header": "second",
                "header_values": ["first", "second"],
                "is_http_body": True,
                "json": {"event": "created", "id": 42},
                "text": ' { "event": "created", "id": 42 }\n',
            },
        },
        raw_body=True,
        raw_content_type="application/cloudevents+json; charset=utf-8",
        request_headers=(
            ("X-Tensorlake-Test", "first"),
            ("x-tensorlake-test", "second"),
        ),
    ),
    Scenario(
        name="parity_state",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {
                "missing": {"value": -1},
                "request_id": "compatibility-parity_state-request",
                "stored": {"value": 21},
            },
        },
        expected_state_operations=(
            "prepare_read",
            "prepare_write",
            "commit_write",
            "prepare_read",
        ),
        expected_progress=((2.0, 3.0),),
        expected_progress_events=(
            (
                2.0,
                3.0,
                "parity progress",
                (("runtime", "shared-harness"),),
            ),
        ),
    ),
    Scenario(
        name="parity_progress_validation",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"rejected": 5},
        },
    ),
    Scenario(
        name="parity_context_validation",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"rejected": 9},
        },
    ),
    Scenario(
        name="parity_replay_mismatch",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "replay_event_history_mismatch",
        },
        behavior="replay_mismatch",
        replay_mismatch=True,
    ),
    Scenario(
        name="parity_http_envelope",
        input={"value": 21},
        expected_terminal={
            "outcome": "success",
            "value": {
                "header": "public-header",
                "value": {"value": 21},
            },
        },
        http_message=True,
        request_headers=(("X-Public-Invocation", "public-header"),),
        replay_success=True,
    ),
    Scenario(
        name="parity_http_envelope_default",
        input=None,
        expected_terminal={"outcome": "success", "value": "Hello, world!"},
        http_message=True,
        empty_body=True,
        replay_success=True,
    ),
    Scenario(
        name="parity_file_input",
        input=b"\x00compatibility-file\xff",
        expected_terminal={
            "outcome": "success",
            "value": {
                "content_hex": b"\x00compatibility-file\xff".hex(),
                "content_type": "application/octet-stream",
                "is_file": True,
            },
        },
        raw_body=True,
        raw_content_type="application/octet-stream",
        replay_success=True,
    ),
    Scenario(
        name="parity_multipart_http_body",
        input=[
            PythonFile(b"\x00\x01\x02\xff", "application/vnd.tensorlake.event"),
            {"source": "partner", "attempt": 2},
        ],
        expected_terminal={
            "outcome": "success",
            "value": {
                "body_hex": b"\x00\x01\x02\xff".hex(),
                "body_type": "application/vnd.tensorlake.event",
                "metadata": {"source": "partner", "attempt": 2},
            },
        },
        multipart=True,
        replay_success=True,
    ),
    Scenario(
        name="parity_empty_http_body",
        input=b"",
        expected_terminal={
            "outcome": "success",
            "value": {
                "content_hex": "",
                "content_type": None,
            },
        },
        raw_body=True,
        replay_success=True,
    ),
    Scenario(
        name="parity_malformed_json",
        input=b'{"value":',
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "function_error",
        },
        raw_body=True,
        raw_content_type="application/json",
        replay_success=True,
    ),
    Scenario(
        name="parity_chunked_http_body",
        input=CHUNKED_HTTP_BODY,
        expected_terminal={
            "outcome": "success",
            "value": {
                "sha256": hashlib.sha256(CHUNKED_HTTP_BODY).hexdigest(),
                "size": len(CHUNKED_HTTP_BODY),
            },
        },
        raw_body=True,
        raw_content_type="application/octet-stream",
        input_chunk_size=65_537,
        replay_success=True,
    ),
    Scenario(
        name="parity_wait_all_completed",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"done": 2, "failures": 1, "not_done": 0},
        },
        behavior="wait_all_completed",
        expected_calls=(2, 2),
        replay_success=True,
    ),
    Scenario(
        name="parity_wait_timeout",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"done": 1, "failures": 1, "not_done": 0},
        },
        behavior="wait_timeout",
        expected_calls=(1, 1),
    ),
    Scenario(
        name="parity_run_later",
        input=21,
        expected_terminal={"outcome": "success", "value": 42},
        behavior="run_later",
        expected_calls=(1, 1),
        expected_scheduled_calls=1,
        replay_success=True,
    ),
    Scenario(
        name="parity_detached_future",
        input=21,
        expected_terminal={"outcome": "success", "value": "started"},
        behavior="detached_future",
        # The child creation is the customer-visible contract. Depending on
        # scheduling, TypeScript may also install a watcher before the parent
        # returns even though the application never awaits the future.
        expected_calls=(1, -1),
    ),
    Scenario(
        name="parity_future_reuse",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"first": 42, "second": 42},
        },
        behavior="future_reuse",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_map_empty",
        input=21,
        expected_terminal={"outcome": "success", "value": []},
        behavior="map_empty",
        expected_calls=(1, 1),
        typescript_expected_calls=(0, 0),
        replay_success=True,
    ),
    Scenario(
        name="parity_reduce_empty_initial",
        input=21,
        expected_terminal={"outcome": "success", "value": 21},
        behavior="reduce_empty",
        expected_calls=(1, 1),
        typescript_expected_calls=(0, 0),
        replay_success=True,
    ),
    Scenario(
        name="parity_map_failure",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "function_error",
        },
        behavior="map_failure",
        expected_calls=(1, 1),
        # Promise.all can observe any one of the concurrently failing watchers
        # first. The externally relevant contract is the terminal graph failure,
        # not how many sibling watchers happened to be installed beforehand.
        typescript_expected_calls=(3, -1),
    ),
    Scenario(
        name="parity_reduce_failure",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "function_error",
        },
        behavior="reduce_failure",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_unhandled_child_failure",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "function_error",
        },
        behavior="unhandled_child_failure",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_unhandled_child_request_error",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "request_error",
            "request_error": "child request failed for 21",
        },
        behavior="unhandled_child_request_error",
        expected_calls=(1, 1),
        replay_success=True,
    ),
    Scenario(
        name="parity_context_events",
        input=21,
        expected_terminal={
            "outcome": "success",
            "value": {"stored": {"value": 21}},
        },
        expected_state_operations=(
            "prepare_write",
            "commit_write",
            "prepare_write",
            "commit_write",
            "prepare_read",
        ),
        expected_progress=((1.0, 2.0), (2.0, 2.0)),
        expected_metrics=(
            ("counter", "processed_items", 21.0),
            ("timer", "processing_seconds", 1.25),
        ),
        expected_progress_events=(
            (1.0, 2.0, "context halfway", (("phase", "half"),)),
            (2.0, 2.0, "context complete", (("phase", "done"),)),
        ),
        replay_success=True,
    ),
    Scenario(
        name="parity_state_failure",
        input=21,
        expected_terminal={
            "outcome": "failure",
            "failure_reason": "function_error",
        },
        behavior="state_failure",
        expected_state_operations=("prepare_read",),
        replay_success=True,
    ),
)


@dataclass(frozen=True)
class ExecutorSpec:
    language: str
    command: tuple[str, ...]
    application_archive: bytes
    failed_initialization_archive: bytes


@dataclass(frozen=True)
class ChildResult:
    outcome: str
    value: Any = None
    request_error: str | None = None


class ExecutorProcess:
    def __init__(self, spec: ExecutorSpec):
        self.spec = spec
        self.port = available_port()
        self.address = f"127.0.0.1:{self.port}"
        self._log = tempfile.TemporaryFile(mode="w+b")
        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join(
            filter(None, [str(PYTHON_SOURCE_ROOT), env.get("PYTHONPATH", "")])
        )
        self._process = subprocess.Popen(
            [
                *spec.command,
                "--address",
                self.address,
                "--executor-id",
                "compatibility-executor",
                "--function-executor-id",
                f"compatibility-{spec.language}",
            ],
            cwd=REPOSITORY_ROOT,
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        self.channel = grpc.insecure_channel(
            self.address,
            options=[
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
            ],
        )
        try:
            grpc.channel_ready_future(self.channel).result(timeout=RPC_TIMEOUT_SECONDS)
        except BaseException as error:
            self.close()
            raise RuntimeError(
                f"{spec.language} function executor did not start:\n{self.logs()}"
            ) from error
        self.stub = FunctionExecutorStub(self.channel)

    def close(self) -> None:
        self.channel.close()
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)

    def request_graceful_shutdown(self) -> None:
        if self._process.poll() is not None:
            raise AssertionError(
                f"{self.spec.language} executor exited before shutdown was requested:\n"
                f"{self.logs()}"
            )
        self._process.terminate()

    def wait_for_graceful_shutdown(
        self, *, pending_operation: str, timeout: float = 5
    ) -> dict[str, Any]:
        try:
            self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            logs = self.logs()
            self._process.kill()
            self._process.wait(timeout=5)
            raise AssertionError(
                f"{self.spec.language} executor did not stop within {timeout}s "
                f"while {pending_operation} was pending:\n{logs}"
            ) from error
        return_code = self._process.returncode
        logs = self.logs()
        if return_code != 0:
            raise AssertionError(
                f"{self.spec.language} executor did not complete graceful shutdown "
                f"while {pending_operation} was pending (exit={return_code}):\n"
                f"{logs}"
            )
        if self.spec.language == "typescript":
            if '"message":"stopped TypeScript function executor"' not in logs:
                raise AssertionError(
                    "TypeScript executor exited without logging completed graceful "
                    f"shutdown while {pending_operation} was pending:\n{logs}"
                )
            if (
                '"message":"forcing TypeScript function executor shutdown after grace period"'
                in logs
            ):
                raise AssertionError(
                    "TypeScript executor forced shutdown while "
                    f"{pending_operation} was pending:\n{logs}"
                )
        else:
            if "stopped function executor server" not in logs:
                raise AssertionError(
                    "Python executor exited without logging completed graceful "
                    f"shutdown while {pending_operation} was pending:\n{logs}"
                )
        return {"exit_mode": "graceful", "exit_code": return_code}

    def terminate_gracefully(
        self, *, pending_operation: str, timeout: float = 5
    ) -> dict[str, Any]:
        self.request_graceful_shutdown()
        return self.wait_for_graceful_shutdown(
            pending_operation=pending_operation,
            timeout=timeout,
        )

    def logs(self) -> str:
        return self.all_logs()[-16_000:]

    def all_logs(self) -> str:
        self._log.flush()
        self._log.seek(0)
        return self._log.read().decode("utf-8", errors="replace")

    def __enter__(self) -> ExecutorProcess:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
        if exc_value is not None:
            print(
                f"\n{self.spec.language} executor logs:\n{self.logs()}",
                file=sys.stderr,
            )


class ProtocolDriver:
    def __init__(
        self,
        spec: ExecutorSpec,
        stub: FunctionExecutorStub,
        scenario: Scenario,
        allocation_id: str,
        directory: Path,
        replay_events: list[AllocationEvent] | None = None,
        fail_output_blob: bool = False,
    ):
        self.spec = spec
        self.stub = stub
        self.scenario = scenario
        self.allocation_id = allocation_id
        self.directory = directory
        self.event_responses: queue.Queue[ReadAllocationEventLogResponse | None] = (
            queue.Queue()
        )
        self.stop = threading.Event()
        self.error: BaseException | None = None
        self.threads: list[threading.Thread] = []
        self.clock = 0
        self.event_history: list[AllocationEvent] = []
        self.child_calls: list[dict[str, Any]] = []
        self.scheduled_call_count = 0
        self.child_results: dict[str, ChildResult] = {}
        self.reduce_last_steps: dict[str, tuple[int, str, Any]] = {}
        self.reduce_step_counts: dict[str, int] = {}
        self.delayed_watcher_results: list[AllocationEvent] = []
        self.call_metadata: dict[str, bytes] = {}
        self.batch_kinds: list[list[str]] = []
        self.terminal_count = 0
        self.state_operation_kinds: list[str] = []
        self.progress: list[tuple[float, float]] = []
        self.state_values: dict[str, BLOB] = {}
        self.fail_output_blob = fail_output_blob
        if replay_events is not None:
            self.event_responses.put(
                ReadAllocationEventLogResponse(
                    allocation_id=self.allocation_id,
                    entries=replay_events,
                    last_clock=max((event.clock for event in replay_events), default=0),
                    has_more=False,
                )
            )

    def run(self) -> dict[str, Any]:
        self.threads = [
            threading.Thread(target=self._serve_allocation_state, daemon=True),
            threading.Thread(target=self._serve_event_reads, daemon=True),
        ]
        for thread in self.threads:
            thread.start()

        deadline = time.monotonic() + ALLOCATION_TIMEOUT_SECONDS
        finish = None
        while finish is None:
            if self.error is not None:
                raise self.error
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"{self.spec.language} {self.scenario.name} allocation did not finish"
                )
            response = self.stub.get_allocation_execution_log_batch(
                GetAllocationExecutionLogBatchRequest(allocation_id=self.allocation_id),
                timeout=max(0.1, deadline - time.monotonic()),
            )
            if not response.events:
                raise AssertionError(
                    "executor closed its execution log without a terminal event"
                )
            kinds: list[str] = []
            for event in response.events:
                kind = event.WhichOneof("event")
                if kind is None:
                    raise AssertionError("execution event has no event payload")
                kinds.append(kind)
                if kind == "create_function_call":
                    self._accept_child_call(event.create_function_call)
                elif kind == "create_function_call_watcher":
                    self._accept_child_watcher(event.create_function_call_watcher)
                elif kind == "finish_allocation":
                    self.terminal_count += 1
                    finish = event.finish_allocation
            self.batch_kinds.append(kinds)
            self.stub.advance_allocation_execution_log_batch(
                AdvanceAllocationExecutionLogBatchRequest(
                    allocation_id=self.allocation_id
                ),
                timeout=RPC_TIMEOUT_SECONDS,
            )

        final_batch = self.stub.get_allocation_execution_log_batch(
            GetAllocationExecutionLogBatchRequest(allocation_id=self.allocation_id),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        if final_batch.events:
            raise AssertionError("executor emitted events after its terminal batch")
        self.stop.set()
        self.event_responses.put(None)
        for thread in self.threads:
            thread.join(timeout=1)
        return {
            "terminal": self._normalize_finish(finish),
            "batch_kinds": self.batch_kinds,
            "event_counts": dict(
                sorted(kind_counts(self.batch_kinds).items(), key=lambda item: item[0])
            ),
            "child_calls": self.child_calls,
            "scheduled_call_count": self.scheduled_call_count,
            "terminal_count": self.terminal_count,
            "state_operations": self.state_operation_kinds,
            "progress": self.progress,
        }

    def _accept_child_call(self, creation) -> None:
        updates = creation.updates
        if updates.HasField("start_at"):
            self.scheduled_call_count += 1
        first_metadata = None
        if updates.updates and updates.updates[0].HasField("function_call"):
            first_metadata = decode_function_call_metadata(
                updates.updates[0].function_call.call_metadata
            )
        if (
            len(updates.updates) > 1
            or isinstance(first_metadata, dict)
            and first_metadata.get("operation") == "reduce"
        ):
            self._accept_reduce_chain(creation)
            return
        if len(updates.updates) != 1:
            raise AssertionError("expected one operation in the creation batch")
        operation = updates.updates[0]
        durable_id = updates.root_function_call_id
        if operation.HasField("function_call"):
            call = operation.function_call
            arguments_proto = call.args
            call_metadata = call.call_metadata
            logical_function = call.target.function_name
            special = None
        elif operation.HasField("reduce"):
            raise AssertionError(
                "executor emitted deprecated ReduceOp instead of a function-call chain"
            )
        else:
            raise AssertionError("creation batch contains no supported operation")
        if call.id != durable_id:
            raise AssertionError("operation ID and root durable ID differ")
        arguments: list[Any] = []
        for argument in arguments_proto:
            if not argument.HasField("value"):
                raise AssertionError(
                    "parity fixture unexpectedly emitted a function-call reference"
                )
            validate_typed_value_server_round_trip(argument.value.manifest)
            arguments.append(
                decode_serialized_value(argument.value, creation.args_blob)
            )

        metadata = decode_function_call_metadata(call_metadata)
        positional = arguments
        keyword_arguments: dict[str, Any] = {}
        if special == "reduce":
            positional = arguments[1:]
            keyword_arguments = {"initial": arguments[0]}
        elif isinstance(metadata, FunctionCallMetadata):
            positional = arguments[: len(metadata.args)]
            keyword_arguments = dict(
                zip(metadata.kwargs.keys(), arguments[len(metadata.args) :])
            )
            if metadata.is_map_splitter:
                special = "map"
                logical_function = metadata.splitter_function_name
            elif metadata.is_reduce_splitter:
                special = "reduce"
                logical_function = metadata.splitter_function_name

        child_call = {
            "durable_id": durable_id,
            "function": logical_function,
            "arguments": normalize_value(positional),
            "keyword_arguments": normalize_value(keyword_arguments),
            "special": special,
        }
        self.child_calls.append(child_call)
        self.call_metadata[durable_id] = bytes(call_metadata)

        if self.scenario.behavior == "creation_failure":
            metadata = (
                pickle.dumps(PythonFunctionError("creation failed"))
                if self.spec.language == "python"
                else b""
            )
            self._queue_events(
                [
                    AllocationEvent(
                        function_call_created=AllocationEventFunctionCallCreated(
                            function_call_id=durable_id,
                            status=Status(code=13, message="creation failed"),
                            metadata=metadata,
                        )
                    )
                ]
            )
            return

        self.child_results[durable_id] = self._result_for_call(child_call)
        self._queue_events(
            [
                AllocationEvent(
                    function_call_created=AllocationEventFunctionCallCreated(
                        function_call_id=durable_id,
                        status=Status(code=0),
                        metadata=call_metadata,
                    )
                )
            ]
        )

    def _accept_reduce_chain(self, creation) -> None:
        updates = creation.updates
        calls = []
        for operation in updates.updates:
            if not operation.HasField("function_call"):
                raise AssertionError(
                    "reduce chain contains a non-function-call operation"
                )
            calls.append(operation.function_call)
        durable_id = updates.root_function_call_id
        if calls[-1].id != durable_id:
            raise AssertionError("reduce chain root is not its final function call")

        plan_initial = None
        plan_has_initial = False
        values: list[Any] = []
        logical_function = calls[0].target.function_name
        reduce_root_id = None
        previous_step = None
        previous_id = None
        previous_result = None
        for call in calls:
            if call.target.function_name != logical_function or len(call.args) != 2:
                raise AssertionError("reduce chain has inconsistent reducer calls")
            metadata = decode_function_call_metadata(call.call_metadata)
            if not isinstance(metadata, dict) or metadata.get("operation") != "reduce":
                raise AssertionError(
                    "reduce chain is missing TypeScript reduce metadata"
                )
            current_root_id = metadata.get("reduceRootId")
            step = metadata.get("reduceStep")
            step_count = metadata.get("reduceStepCount")
            if (
                not isinstance(current_root_id, str)
                or not current_root_id
                or not isinstance(step, int)
                or isinstance(step, bool)
                or not isinstance(step_count, int)
                or isinstance(step_count, bool)
                or step_count <= 0
            ):
                raise AssertionError("reduce chain has invalid step metadata")
            if reduce_root_id is None:
                reduce_root_id = current_root_id
                previous = self.reduce_last_steps.get(reduce_root_id)
                if previous is None:
                    previous_step, previous_id, previous_result = -1, None, None
                else:
                    previous_step, previous_id, previous_result = previous
                known_step_count = self.reduce_step_counts.setdefault(
                    reduce_root_id, step_count
                )
                if known_step_count != step_count:
                    raise AssertionError(
                        "reduce chain step count changed between plans"
                    )
            elif current_root_id != reduce_root_id:
                raise AssertionError("reduce chain root changed within a plan")
            if step_count != self.reduce_step_counts[reduce_root_id]:
                raise AssertionError("reduce chain step count changed within a plan")
            if step != previous_step + 1:
                raise AssertionError("reduce chain steps are not ordered")
            accumulator = call.args[0]
            if step == 0:
                if not accumulator.HasField("value"):
                    raise AssertionError(
                        "reduce chain initial accumulator is not inline"
                    )
                validate_typed_value_server_round_trip(accumulator.value.manifest)
                accumulator_value = decode_serialized_value(
                    accumulator.value, creation.args_blob
                )
            else:
                if (
                    not accumulator.HasField("function_call_id")
                    or accumulator.function_call_id != previous_id
                ):
                    raise AssertionError(
                        "reduce chain accumulator does not reference the prior step"
                    )
                accumulator_value = previous_result
            if not plan_has_initial:
                plan_initial = accumulator_value
                plan_has_initial = True
            if not call.args[1].HasField("value"):
                raise AssertionError("reduce chain item is not inline")
            validate_typed_value_server_round_trip(call.args[1].value.manifest)
            item = decode_serialized_value(call.args[1].value, creation.args_blob)
            values.append(item)
            previous_result = accumulator_value + item
            previous_step = step
            previous_id = call.id

        if reduce_root_id is None or previous_step is None or previous_id is None:
            raise AssertionError("reduce chain is empty")
        if previous_step == self.reduce_step_counts[reduce_root_id] - 1:
            if previous_id != reduce_root_id:
                raise AssertionError("final reduce step does not match reduce root")
        elif previous_id == reduce_root_id:
            raise AssertionError("non-final reduce plan unexpectedly uses reduce root")
        self.reduce_last_steps[reduce_root_id] = (
            previous_step,
            previous_id,
            previous_result,
        )
        child_call = {
            "durable_id": durable_id,
            "function": logical_function,
            "arguments": normalize_value(values),
            "keyword_arguments": {"initial": normalize_value(plan_initial)},
            "special": "reduce",
        }
        self.child_calls.append(child_call)
        self.call_metadata[durable_id] = bytes(calls[-1].call_metadata)
        self.child_results[durable_id] = (
            ChildResult(outcome="failure")
            if self.scenario.behavior == "reduce_failure"
            else ChildResult(outcome="success", value=previous_result)
        )
        self._queue_events(
            [
                AllocationEvent(
                    function_call_created=AllocationEventFunctionCallCreated(
                        function_call_id=durable_id,
                        status=Status(code=0),
                        metadata=calls[-1].call_metadata,
                    )
                )
            ]
        )

    def _result_for_call(self, call: dict[str, Any]) -> ChildResult:
        behavior = self.scenario.behavior
        arguments = call["arguments"]
        if behavior == "watcher_failure":
            return ChildResult(outcome="failure")
        if behavior == "watcher_request_error":
            return ChildResult(
                outcome="failure",
                request_error=f"child request failed for {arguments[0]}",
            )
        if behavior == "wait_first_failure_with_failure":
            if call["function"] == FAILING_FUNCTION:
                return ChildResult(outcome="failure")
            return ChildResult(outcome="success", value=arguments[0] * 2)
        if behavior == "wait_all_completed":
            if call["function"] == FAILING_FUNCTION:
                return ChildResult(outcome="failure")
            return ChildResult(outcome="success", value=arguments[0] * 2)
        if behavior in ("map", "map_reduce", "map_empty", "map_failure"):
            if behavior == "map_failure":
                return ChildResult(outcome="failure")
            if call["function"] == ADD_FUNCTION:
                initial = call["keyword_arguments"].get("initial")
                values = list(arguments)
                if initial is None:
                    initial, values = values[0], values[1:]
                return ChildResult(outcome="success", value=initial + sum(values))
            if call["special"] == "map":
                return ChildResult(
                    outcome="success", value=[value * 2 for value in arguments]
                )
            return ChildResult(outcome="success", value=arguments[0] * 2)
        if behavior in (
            "reduce",
            "reduce_large",
            "reduce_no_initial",
            "reduce_empty",
            "reduce_failure",
        ):
            if behavior == "reduce_failure":
                return ChildResult(outcome="failure")
            if call["special"] == "reduce":
                initial = call["keyword_arguments"].get("initial")
                values = list(arguments)
                if initial is None:
                    initial, values = values[0], values[1:]
                return ChildResult(outcome="success", value=initial + sum(values))
            return ChildResult(outcome="success", value=arguments[0] + arguments[1])
        if behavior == "json_file":
            serialized_file = arguments[0]["file"]
            return ChildResult(
                outcome="success",
                value=PythonFile(
                    bytes.fromhex(serialized_file["content_hex"]),
                    serialized_file["content_type"],
                ),
            )
        if behavior in (
            "double",
            "tail_call",
            "wait_first_failure",
            "wait_causal_replay",
            "wait_batched_results",
            "watcher_creation_failure",
            "watcher_timeout",
            "wait_timeout",
            "run_later",
            "detached_future",
            "future_reuse",
            "unhandled_child_failure",
            "unhandled_child_request_error",
        ):
            if behavior == "unhandled_child_failure":
                return ChildResult(outcome="failure")
            if behavior == "unhandled_child_request_error":
                return ChildResult(
                    outcome="failure",
                    request_error=f"child request failed for {arguments[0]}",
                )
            return ChildResult(outcome="success", value=arguments[0] * 2)
        raise AssertionError(f"unexpected child call in {self.scenario.name}: {call!r}")

    def _accept_child_watcher(self, watcher) -> None:
        durable_id = watcher.function_call_id
        if durable_id not in self.child_results:
            raise AssertionError(f"watcher references unknown child call {durable_id}")
        if self.scenario.behavior == "watcher_creation_failure":
            self._queue_events(
                [
                    AllocationEvent(
                        function_call_watcher_created=(
                            AllocationEventFunctionCallWatcherCreated(
                                function_call_id=durable_id,
                                status=Status(
                                    code=grpc.StatusCode.INTERNAL.value[0],
                                    message="watcher creation failed",
                                ),
                            )
                        )
                    )
                ]
            )
            return
        watcher_created = AllocationEvent(
            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                function_call_id=durable_id,
                status=Status(code=0),
            )
        )
        result = self.child_results[durable_id]
        watcher_result = AllocationEventFunctionCallWatcherResult(
            function_call_id=durable_id,
            watcher_status=(
                FUNCTION_CALL_WATCHER_STATUS_TIMEDOUT
                if self.scenario.behavior in ("watcher_timeout", "wait_timeout")
                else FUNCTION_CALL_WATCHER_STATUS_COMPLETED
            ),
        )
        if self.scenario.behavior in ("watcher_timeout", "wait_timeout"):
            watcher_result.outcome_code = ALLOCATION_OUTCOME_CODE_FAILURE
        elif result.outcome == "success":
            value, blob = encode_value_for_language(
                self.spec.language,
                result.value,
                self.directory,
                f"watcher-result-{durable_id}",
            )
            watcher_result.outcome_code = ALLOCATION_OUTCOME_CODE_SUCCESS
            watcher_result.value_output.CopyFrom(value)
            watcher_result.value_blob.CopyFrom(blob)
        elif result.request_error is not None:
            value, blob = encode_text_value(
                result.request_error,
                self.directory,
                f"watcher-request-error-{durable_id}",
            )
            watcher_result.outcome_code = ALLOCATION_OUTCOME_CODE_FAILURE
            watcher_result.request_error_output.CopyFrom(value)
            watcher_result.request_error_blob.CopyFrom(blob)
        else:
            watcher_result.outcome_code = ALLOCATION_OUTCOME_CODE_FAILURE
        watcher_result_event = AllocationEvent(
            function_call_watcher_result=watcher_result
        )

        if self.scenario.behavior == "wait_causal_replay":
            call_index = next(
                index
                for index, call in enumerate(self.child_calls)
                if call["durable_id"] == durable_id
            )
            if call_index == 1:
                self._queue_events([watcher_created])
                self.delayed_watcher_results.append(watcher_result_event)
                return
            if call_index == 2:
                self._queue_events(
                    [
                        watcher_created,
                        watcher_result_event,
                        *self.delayed_watcher_results,
                    ]
                )
                self.delayed_watcher_results.clear()
                return
        if self.scenario.behavior == "wait_batched_results":
            call_index = next(
                index
                for index, call in enumerate(self.child_calls)
                if call["durable_id"] == durable_id
            )
            if call_index == 0:
                self._queue_events([watcher_created])
                self.delayed_watcher_results.append(watcher_result_event)
                return
            self._queue_events(
                [
                    watcher_created,
                    *self.delayed_watcher_results,
                    watcher_result_event,
                ]
            )
            self.delayed_watcher_results.clear()
            return

        self._queue_events([watcher_created])
        self._queue_events([watcher_result_event])

    def _queue_events(self, events: list[AllocationEvent]) -> None:
        history_events: list[AllocationEvent] = []
        for event in events:
            self.clock += 1
            event.clock = self.clock
            history_event = AllocationEvent()
            history_event.CopyFrom(event)
            self.event_history.append(history_event)
            history_events.append(history_event)
        self.event_responses.put(
            ReadAllocationEventLogResponse(
                allocation_id=self.allocation_id,
                entries=history_events,
                last_clock=self.clock,
                has_more=False,
            )
        )

    def _serve_allocation_state(self) -> None:
        responded_blobs: set[str] = set()
        responded_operations: set[str] = set()
        try:
            states: Iterator = self.stub.watch_allocation_state(
                WatchAllocationStateRequest(allocation_id=self.allocation_id)
            )
            for state in states:
                if state.HasField("progress"):
                    progress = (
                        float(state.progress.current),
                        float(state.progress.total),
                    )
                    if not self.progress or self.progress[-1] != progress:
                        self.progress.append(progress)
                for request in state.output_blob_requests:
                    if request.id in responded_blobs:
                        continue
                    responded_blobs.add(request.id)
                    if self.fail_output_blob:
                        output_blob = AllocationOutputBlob(
                            status=Status(
                                code=13,
                                message="compatibility output BLOB creation failure",
                            )
                        )
                    else:
                        destination = self.directory / f"output-{request.id}"
                        destination.touch()
                        output_blob = AllocationOutputBlob(
                            status=Status(code=0),
                            blob=BLOB(
                                id=request.id,
                                chunks=[
                                    BLOBChunk(
                                        uri=destination.as_uri(),
                                        size=request.size,
                                    )
                                ],
                            ),
                        )
                    self.stub.send_allocation_update(
                        AllocationUpdate(
                            allocation_id=self.allocation_id,
                            output_blob=output_blob,
                        ),
                        timeout=RPC_TIMEOUT_SECONDS,
                    )
                for operation in state.request_state_operations:
                    if operation.operation_id in responded_operations:
                        continue
                    responded_operations.add(operation.operation_id)
                    self._respond_to_state_operation(operation)
                if self.stop.is_set():
                    return
        except grpc.RpcError as error:
            if not self.stop.is_set():
                self.error = error
        except BaseException as error:
            self.error = error

    def _respond_to_state_operation(self, operation) -> None:
        kind = operation.WhichOneof("operation")
        if kind is None:
            raise AssertionError("request-state operation has no payload")
        self.state_operation_kinds.append(kind)
        result = AllocationRequestStateOperationResult(
            operation_id=operation.operation_id,
            status=Status(code=0),
        )
        if self.scenario.behavior == "state_failure":
            result.status.code = 13
            result.status.message = "compatibility state service failure"
        elif kind == "prepare_write":
            destination = self.directory / f"state-{operation.operation_id}"
            destination.touch()
            result.prepare_write.CopyFrom(
                AllocationRequestStatePrepareWriteOperationResult(
                    blob=BLOB(
                        id=f"state-{operation.operation_id}",
                        chunks=[
                            BLOBChunk(
                                uri=destination.as_uri(),
                                size=operation.prepare_write.size,
                            )
                        ],
                    )
                )
            )
        elif kind == "commit_write":
            stored = BLOB()
            stored.CopyFrom(operation.commit_write.blob)
            self.state_values[operation.state_key] = stored
            result.commit_write.CopyFrom(
                AllocationRequestStateCommitWriteOperationResult()
            )
        elif operation.state_key in self.state_values:
            result.prepare_read.CopyFrom(
                AllocationRequestStatePrepareReadOperationResult(
                    blob=self.state_values[operation.state_key]
                )
            )
        else:
            result.status.code = 5
            result.status.message = "state key not found"
        self.stub.send_allocation_update(
            AllocationUpdate(
                allocation_id=self.allocation_id,
                request_state_operation_result=result,
            ),
            timeout=RPC_TIMEOUT_SECONDS,
        )

    def _serve_event_reads(self) -> None:
        try:
            reads = self.stub.watch_allocation_event_log_reads(
                WatchAllocationEventLogReads(allocation_id=self.allocation_id)
            )
            for _request in reads:
                response = self.event_responses.get(timeout=ALLOCATION_TIMEOUT_SECONDS)
                if response is None:
                    return
                self.stub.send_allocation_event_log_read_response(
                    response, timeout=RPC_TIMEOUT_SECONDS
                )
        except grpc.RpcError as error:
            if not self.stop.is_set():
                self.error = error
        except BaseException as error:
            self.error = error

    def _normalize_finish(self, finish) -> dict[str, Any]:
        if self.terminal_count != 1:
            raise AssertionError(
                f"expected one terminal event, received {self.terminal_count}"
            )
        outcome = enum_suffix(
            AllocationOutcomeCode.Name(finish.outcome_code),
            "ALLOCATION_OUTCOME_CODE_",
        )
        terminal: dict[str, Any] = {"outcome": outcome}
        if finish.outcome_code == ALLOCATION_OUTCOME_CODE_SUCCESS:
            if finish.HasField("value"):
                if not finish.HasField("uploaded_function_outputs_blob"):
                    raise AssertionError("successful value output has no uploaded BLOB")
                terminal["value"] = normalize_value(
                    decode_serialized_value(
                        finish.value, finish.uploaded_function_outputs_blob
                    )
                )
            elif finish.HasField("tail_call_durable_id"):
                matching = [
                    call
                    for call in self.child_calls
                    if call["durable_id"] == finish.tail_call_durable_id
                ]
                if len(matching) != 1:
                    raise AssertionError(
                        "tail-call output does not match one child call"
                    )
                terminal["tail_call"] = {
                    "function": matching[0]["function"],
                    "arguments": matching[0]["arguments"],
                }
            else:
                raise AssertionError("successful allocation has no value or tail call")
        else:
            terminal["failure_reason"] = enum_suffix(
                AllocationFailureReason.Name(finish.failure_reason),
                "ALLOCATION_FAILURE_REASON_",
            )
            if finish.HasField("request_error_output"):
                if not finish.HasField("uploaded_request_error_blob"):
                    raise AssertionError("request error output has no uploaded BLOB")
                terminal["request_error"] = decode_serialized_value(
                    finish.request_error_output, finish.uploaded_request_error_blob
                )
        return terminal


def available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_python_archive(*, stale: bool = False) -> bytes:
    manifest = {
        "functions": {
            name: {"name": name, "module_import_name": "python_application"}
            for name in (*FUNCTION_NAMES, MISSING_FUNCTION)
        }
    }
    source = (FIXTURE_ROOT / "python_application.py").read_bytes()
    if stale:
        stale_definitions = []
        for name in FUNCTION_NAMES:
            parameters = "left, right" if name == "parity_multipart" else "value"
            stale_definitions.append(
                "\n".join(
                    [
                        "@application()",
                        "@function()",
                        f"def {name}({parameters}):",
                        f'    return {{"stale_archive": "{name}"}}',
                    ]
                )
            )
        source += (
            "\n\n# Definitions used only by the failed-initialization retry probe.\n"
            + "\n\n".join(stale_definitions)
            + "\n"
        ).encode()
    return build_zip(
        {
            ".tensorlake_code_manifest.json": json.dumps(manifest).encode(),
            "python_application.py": source,
        }
    )


def build_typescript_archive(*, stale: bool = False) -> bytes:
    sdk_module = (
        REPOSITORY_ROOT / "typescript" / "dist" / "applications" / "index.js"
    ).as_uri()
    runtime = f"""
// Initialization retry archive variant: {"stale" if stale else "current"}.
import {{ createHash }} from "node:crypto";
import {{ EventEmitter }} from "node:events";
import {{
  File,
  FunctionError,
  Future,
  HttpBody,
  RequestContext,
  RequestError,
  SDKUsageError,
  TimeoutError,
  getFunction,
  registerApplication,
  registerFunction,
  schema,
}} from {json.dumps(sdk_module)};

const preinitializedTicks = new EventEmitter();
setInterval(() => preinitializedTicks.emit("tick"), 25).unref();

const numberParameter = [schema.parameter("value", schema.number())];
const double = registerFunction(
  {json.dumps(DOUBLE_FUNCTION)},
  async (value) => value * 2,
);
const add = registerFunction(
  {json.dumps(ADD_FUNCTION)},
  async (accumulator, value) => accumulator + value,
);
const failingChild = registerFunction({json.dumps(FAILING_FUNCTION)}, async (value) => {{
  throw new Error(`child failed for ${{value}}`);
}});
const requestFailingChild = registerFunction(
  {json.dumps(REQUEST_FAILING_FUNCTION)},
  async (value) => {{
    throw new RequestError(`child request failed for ${{value}}`);
  }},
);
const identityFile = registerFunction(async (value) => value, {{
  name: {json.dumps(IDENTITY_FILE_FUNCTION)},
  parameters: [schema.parameter("value", schema.file())],
  returns: schema.file(),
}});

registerApplication("parity_value", async (value) => ({{ value }}));
registerApplication("parity_multipart", async (left, right) => left * right);
registerApplication("parity_child", async (value) => double(value));
registerApplication("parity_wait_first_failure_after_success", async (value) => {{
  const completed = double.future(value).run();
  await completed.result();
  const pending = double.future(value + 1);
  const waited = await Future.wait([completed, pending], {{
    returnWhen: "first_failure",
  }});
  return {{ done: waited.done.length, not_done: waited.notDone.length }};
}});
registerApplication(
  "parity_wait_first_failure_after_success_and_failure",
  async (value) => {{
    const completed = double.future(value).run();
    await completed.result();
    const failing = failingChild.future(value + 1);
    const waited = await Future.wait([completed, failing], {{
      returnWhen: "first_failure",
    }});
    return {{ done: waited.done.length, not_done: waited.notDone.length }};
  }},
);
registerApplication("parity_wait_causal_replay", async (value) => {{
  const first = double.future(value).run();
  const second = double.future(value + 1).run();
  const waited = await Future.wait([first, second], {{
    returnWhen: "first_completed",
  }});
  await new Promise((resolve) => preinitializedTicks.once("tick", resolve));
  const marker = await double(value + 2);
  return {{
    done: waited.done.length,
    not_done: waited.notDone.length,
    marker,
    results: [await first, await second],
  }};
}});
registerApplication("parity_wait_batched_results", async (value) => {{
  const first = double.future(value).run();
  const second = double.future(value + 1).run();
  const waited = await Future.wait([first, second], {{
    returnWhen: "first_completed",
  }});
  return {{
    done: waited.done.length,
    not_done: waited.notDone.length,
    results: [await first, await second],
  }};
}});
registerApplication(
  "parity_map",
  async (value) => double.map([
    new Promise((resolve) => setTimeout(() => resolve(value), 25)),
    Promise.resolve(value + 1),
    Promise.resolve(value + 2),
  ]),
);
registerApplication(
  "parity_reduce",
  async (value) => add.reduce([value, value + 1, value + 2], 10),
);
registerApplication(
  "parity_reduce_large",
  async (value) => add.reduce(
    Array.from({{ length: 513 }}, (_, index) => index + 1),
    value,
  ),
);
registerApplication(
  "parity_reduce_no_initial",
  async (value) => add.reduce([value, value + 1, value + 2]),
);
registerApplication(
  "parity_map_reduce",
  async (value) => add.reduce(double.map([value, value + 1, value + 2]), 0),
);
registerApplication("parity_tail_call", async (value) => double.tailCall(value));
registerApplication("parity_handled_child_failure", async (value) => {{
  try {{
    await failingChild(value);
  }} catch (error) {{
    if (!(error instanceof FunctionError)) throw error;
    return "caught:function_error";
  }}
  return "unexpected:success";
}});
registerApplication("parity_handled_child_request_error", async (value) => {{
  try {{
    await requestFailingChild(value);
  }} catch (error) {{
    if (!(error instanceof RequestError)) throw error;
    return "caught:request_error";
  }}
  return "unexpected:success";
}});
registerApplication("parity_handled_child_timeout", async (value) => {{
  try {{
    await double(value);
  }} catch (error) {{
    if (!(error instanceof TimeoutError)) throw error;
    return "caught:timeout";
  }}
  return "unexpected:success";
}});
registerApplication("parity_handled_creation_failure", async (value) => {{
  try {{
    await failingChild(value);
  }} catch (error) {{
    if (!(error instanceof FunctionError)) throw error;
    return "caught:creation_error";
  }}
  return "unexpected:success";
}});
registerApplication(
  "parity_watcher_creation_failure",
  async (value) => double(value),
);
registerApplication("parity_request_error", async (value) => {{
  throw new RequestError(`invalid value: ${{value}}`);
}});
registerApplication("parity_function_error", async (value) => {{
  throw new Error(`function failed for ${{value}}`);
}});
registerApplication(async (value) => new File(
  new TextEncoder().encode(`parity-file-${{value}}`),
  "text/plain",
), {{
  name: "parity_file",
  parameters: numberParameter,
  returns: schema.file(),
}});
registerApplication("parity_json_file", async (value) => {{
  const content = new TextEncoder().encode(JSON.stringify({{ value }}));
  const result = await identityFile(new File(content, "application/json"));
  return {{
    content: new TextDecoder().decode(result.content),
    content_type: result.contentType,
    is_file: result instanceof File,
  }};
}});
registerApplication(async (body) => ({{
  header: RequestContext.get().headers.get("x-tensorlake-test"),
  header_values: RequestContext.get().headers.getAll("x-tensorlake-test"),
  content_hex: Buffer.from(body.content).toString("hex"),
  content_type: body.contentType,
  is_http_body: body instanceof HttpBody,
  json: body.json(),
  text: body.text(),
}}), {{
  name: "parity_http_body",
  parameters: [schema.parameter("body", schema.httpBody())],
  returns: schema.object({{
    content_hex: schema.string(),
    content_type: schema.string(),
    header: schema.string(),
    header_values: schema.array(schema.string()),
    is_http_body: schema.boolean(),
    json: schema.object({{
      event: schema.string(),
      id: schema.integer(),
    }}),
    text: schema.string(),
  }}),
}});
registerApplication("parity_state", async (value) => {{
  const context = RequestContext.get();
  const missing = await context.state.get("missing", {{ value: -1 }});
  await context.state.set("answer", {{ value }});
  const stored = await context.state.get("answer");
  await context.progress.update(2, 3, {{
    message: "parity progress",
    attributes: {{ runtime: "shared-harness" }},
  }});
  return {{ missing, request_id: context.requestId, stored }};
}});
registerApplication("parity_progress_validation", async (value) => {{
  const progress = RequestContext.get().progress;
  const invalidUpdates = [
    [-1, 1, undefined],
    [1, Number.POSITIVE_INFINITY, undefined],
    [1, 1, {{ message: value }}],
    [1, 1, {{ attributes: ["invalid"] }}],
    [1, 1, {{ attributes: new Date() }}],
  ];
  let rejected = 0;
  for (const [current, total, options] of invalidUpdates) {{
    try {{
      await progress.update(current, total, options);
    }} catch (error) {{
      if (!(error instanceof SDKUsageError)) throw error;
      rejected += 1;
    }}
  }}
  return {{ rejected }};
}});
registerApplication("parity_context_validation", async (value) => {{
  const context = RequestContext.get();
  const invalidOperations = [
    () => context.state.get(value),
    () => context.state.set(value, "invalid"),
    () => context.metrics.counter(value),
    () => context.metrics.counter("counter", 1.5),
    () => context.metrics.counter("counter", true),
    () => context.metrics.timer(value, 1),
    () => context.metrics.timer("timer", "invalid"),
    () => context.metrics.timer("timer", true),
    () => context.metrics.timer("timer", Number.POSITIVE_INFINITY),
  ];
  let rejected = 0;
  for (const operation of invalidOperations) {{
    try {{
      await operation();
    }} catch (error) {{
      if (!(error instanceof SDKUsageError)) throw error;
      rejected += 1;
    }}
  }}
  return {{ rejected }};
}});
registerApplication("parity_replay_mismatch", async (value) => double(value));
registerApplication("parity_http_envelope", async (value) => ({{
  header: RequestContext.get().headers.get("x-public-invocation"),
  value,
}}));
registerApplication(
  "parity_http_envelope_default",
  async (name = "world") => `Hello, ${{name}}!`,
);
registerApplication(async (file) => ({{
  content_hex: Buffer.from(file.content).toString("hex"),
  content_type: file.contentType,
  is_file: file instanceof File,
}}), {{
  name: "parity_file_input",
  parameters: [schema.parameter("file", schema.file())],
  returns: schema.object({{
    content_hex: schema.string(),
    content_type: schema.string(),
    is_file: schema.boolean(),
  }}),
}});
registerApplication(async (body, metadata) => ({{
  body_hex: Buffer.from(body.content).toString("hex"),
  body_type: body.contentType,
  metadata,
}}), {{
  name: "parity_multipart_http_body",
  parameters: [
    schema.parameter("body", schema.httpBody()),
    schema.parameter("metadata", schema.object({{
      source: schema.string(),
      attempt: schema.integer(),
    }})),
  ],
  returns: schema.object({{
    body_hex: schema.string(),
    body_type: schema.string(),
    metadata: schema.object({{
      source: schema.string(),
      attempt: schema.integer(),
    }}),
  }}),
}});
registerApplication(async (body) => ({{
  content_hex: Buffer.from(body.content).toString("hex"),
  content_type: body.contentType ?? null,
}}), {{
  name: "parity_empty_http_body",
  parameters: [schema.parameter("body", schema.httpBody())],
  returns: schema.object({{
    content_hex: schema.string(),
    content_type: schema.union(schema.string(), schema.null()),
  }}),
}});
registerApplication("parity_malformed_json", async (value) => value);
registerApplication(async (body) => ({{
  sha256: createHash("sha256").update(body.content).digest("hex"),
  size: body.content.byteLength,
}}), {{
  name: "parity_chunked_http_body",
  parameters: [schema.parameter("body", schema.httpBody())],
  returns: schema.object({{
    sha256: schema.string(),
    size: schema.integer(),
  }}),
}});
registerApplication("parity_wait_all_completed", async (value) => {{
  const successful = double.future(value);
  const failing = failingChild.future(value + 1);
  const waited = await Future.wait([successful, failing], {{
    returnWhen: "all_completed",
  }});
  return {{
    done: waited.done.length,
    failures: waited.done.filter((future) => future.exception != null).length,
    not_done: waited.notDone.length,
  }};
}});
registerApplication("parity_wait_timeout", async (value) => {{
  const pending = double.future(value);
  const waited = await Future.wait([pending], {{
    timeout: 0.05,
    returnWhen: "all_completed",
  }});
  return {{
    done: waited.done.length,
    failures: waited.done.filter((future) => future.exception != null).length,
    not_done: waited.notDone.length,
  }};
}});
registerApplication(
  "parity_run_later",
  async (value) => double.future(value).runLater(0.05).result(),
);
registerApplication("parity_detached_future", async (value) => {{
  double.future(value).run();
  return "started";
}});
registerApplication("parity_future_reuse", async (value) => {{
  const future = double.future(value);
  const first = await future.result();
  const second = await future.result();
  return {{ first, second }};
}});
registerApplication("parity_map_empty", async () => double.map([]));
registerApplication(
  "parity_reduce_empty_initial",
  async (value) => add.reduce([], value),
);
registerApplication(
  "parity_map_failure",
  async (value) => failingChild.map([value, value + 1, value + 2]),
);
registerApplication(
  "parity_reduce_failure",
  async (value) => add.reduce([value, value + 1], 0),
);
registerApplication(
  "parity_unhandled_child_failure",
  async (value) => failingChild(value),
);
registerApplication(
  "parity_unhandled_child_request_error",
  async (value) => requestFailingChild(value),
);
registerApplication("parity_context_events", async (value) => {{
  const context = RequestContext.get();
  await context.state.set("version", {{ value: 1 }});
  await context.state.set("version", {{ value }});
  const stored = await context.state.get("version");
  await context.metrics.counter("processed_items", value);
  await context.metrics.timer("processing_seconds", 1.25);
  await context.progress.update(1, 2, {{
    message: "context halfway",
    attributes: {{ phase: "half" }},
  }});
  await context.progress.update(2, 2, {{
    message: "context complete",
    attributes: {{ phase: "done" }},
  }});
  return {{ stored }};
}});
registerApplication("parity_state_failure", async (value) => {{
  await RequestContext.get().state.get("unavailable");
  return value;
}});

export function __tensorlakeGetFunction(name) {{
  return getFunction(name);
}}
""".encode()
    manifest = {
        "format_version": 2,
        "runtime": "typescript",
        "minimum_node_major": 24,
        "module": "runtime.mjs",
        "functions": {
            name: {"name": name} for name in (*FUNCTION_NAMES, MISSING_FUNCTION)
        },
    }
    return build_zip(
        {
            ".tensorlake_code_manifest.json": json.dumps(manifest).encode(),
            "runtime.mjs": runtime,
        }
    )


def build_zip(files: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, contents in files.items():
            archive.writestr(name, contents)
    return output.getvalue()


def run_manifest_contract_probe() -> dict[str, Any]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(PYTHON_SOURCE_ROOT), environment.get("PYTHONPATH", "")])
    )
    commands = {
        "python": (
            sys.executable,
            str(FIXTURE_ROOT / "python_manifest_probe.py"),
        ),
        "typescript": (
            "node",
            str(FIXTURE_ROOT / "typescript_manifest_probe.mjs"),
        ),
    }
    manifests: dict[str, Any] = {}
    for language, command in commands.items():
        completed = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise AssertionError(
                f"{language} manifest probe failed:\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            )
        try:
            manifests[language] = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise AssertionError(
                f"{language} manifest probe did not emit one JSON manifest:\n"
                f"{completed.stdout}"
            ) from error
    if manifests["python"] != manifests["typescript"]:
        raise AssertionError(
            "Python and TypeScript application manifest contracts differ:\n"
            f"{json.dumps(manifests, indent=2, sort_keys=True)}"
        )
    manifest = manifests["python"]
    if manifest["allow"] != ["unauthenticated_requests"]:
        raise AssertionError(
            "application manifest omitted the public invocation capability"
        )
    if manifest["functions"]["manifest_application"]["parameter_types"] != [
        "tensorlake_http_body"
    ]:
        raise AssertionError("application manifest omitted the HttpBody parameter type")
    return manifest


def input_for(directory: Path, scenario: Scenario) -> FunctionInputs:
    content_type: str | None = "application/json"
    encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
    if scenario.multipart:
        boundary = "tensorlake-parity-boundary"
        parts: list[bytes] = []
        for index, value in enumerate(scenario.input):
            if isinstance(value, PythonFile):
                part_content_type = value.content_type
                part_data = value.content
            else:
                part_content_type = "application/json"
                part_data = json.dumps(value, separators=(",", ":")).encode()
            parts.extend(
                [
                    f"--{boundary}".encode(),
                    (
                        f'Content-Disposition: form-data; name="{index}"; '
                        f'filename="{index}"'
                    ).encode(),
                    f"Content-Type: {part_content_type}".encode(),
                    b"",
                    part_data,
                ]
            )
        parts.extend([f"--{boundary}--".encode(), b""])
        data = b"\r\n".join(parts)
        content_type = f'multipart/form-data; boundary="{boundary}"'
        encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW
    elif scenario.http_message:
        if scenario.empty_body:
            body = b""
            body_content_type = None
        elif isinstance(scenario.input, bytes):
            body = scenario.input
            body_content_type = scenario.raw_content_type
        else:
            body = json.dumps(scenario.input, separators=(",", ":")).encode()
            body_content_type = scenario.raw_content_type or "application/json"
        headers = [b"POST /invoke HTTP/1.1", b"Host: compatibility.tensorlake"]
        if body_content_type is not None:
            headers.append(f"Content-Type: {body_content_type}".encode())
        data = b"\r\n".join([*headers, b"", body])
        content_type = "message/http"
        encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW
    elif scenario.raw_body:
        if not isinstance(scenario.input, bytes):
            raise AssertionError("raw body scenarios require bytes input")
        data = scenario.input
        content_type = scenario.raw_content_type
        encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW
    else:
        data = json.dumps(scenario.input, separators=(",", ":")).encode()
    chunk_size = scenario.input_chunk_size or max(1, len(data))
    source = directory / "input"
    source.write_bytes(data)
    input_chunks = [
        BLOBChunk(
            uri=source.as_uri(),
            size=min(chunk_size, len(data) - offset),
        )
        for offset in range(0, len(data), chunk_size)
    ]
    if not input_chunks:
        input_chunks.append(BLOBChunk(uri=source.as_uri(), size=0))
    request_error = directory / "request-error"
    request_error.touch()
    manifest = SerializedObjectManifest(
        encoding=encoding,
        encoding_version=0,
        size=len(data),
        metadata_size=0,
        sha256_hash=hashlib.sha256(data).hexdigest(),
    )
    if content_type is not None:
        manifest.content_type = content_type
    return FunctionInputs(
        args=[
            SerializedObjectInsideBLOB(
                manifest=manifest,
                offset=0,
            )
        ],
        arg_blobs=[
            BLOB(
                id="compatibility-input",
                chunks=input_chunks,
            )
        ],
        request_error_blob=BLOB(
            id="compatibility-request-error",
            chunks=[
                BLOBChunk(uri=request_error.as_uri(), size=REQUEST_ERROR_BLOB_SIZE)
            ],
        ),
        request_context=ProtocolRequestContext(
            headers=[
                HttpRequestHeader(name=name, value=value)
                for name, value in scenario.request_headers
            ]
        ),
        function_call_metadata=b"",
    )


def encode_value_for_language(
    language: str, value: Any, directory: Path, name: str
) -> tuple[SerializedObjectInsideBLOB, BLOB]:
    if isinstance(value, PythonFile):
        data = value.content
        content_type = value.content_type
        if language == "python":
            metadata = serialize_metadata(
                ValueMetadata(
                    id=name,
                    type_hint=PythonFile,
                    serializer_name=None,
                    content_type=content_type,
                )
            )
        else:
            metadata = json.dumps(
                {"format": "tensorlake.typescript.value.v1"}, separators=(",", ":")
            ).encode()
        encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW
    elif language == "python":
        serializer = PickleUserDataSerializer()
        metadata = serialize_metadata(
            ValueMetadata(
                id=name,
                type_hint=type(value),
                serializer_name=serializer.name,
                content_type=serializer.content_type,
            )
        )
        data = serializer.serialize(value, type(value))
        encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
        content_type = serializer.content_type
    else:
        metadata = json.dumps(
            {"format": "tensorlake.typescript.value.v1"}, separators=(",", ":")
        ).encode()
        data = json.dumps(value, separators=(",", ":")).encode()
        encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
        content_type = "application/json"
    return write_serialized_value(
        name, metadata, data, encoding, content_type, directory
    )


def encode_text_value(
    value: str, directory: Path, name: str
) -> tuple[SerializedObjectInsideBLOB, BLOB]:
    return write_serialized_value(
        name,
        b"",
        value.encode(),
        SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT,
        "text/plain",
        directory,
    )


def write_serialized_value(
    name: str,
    metadata: bytes,
    data: bytes,
    encoding: int,
    content_type: str,
    directory: Path,
) -> tuple[SerializedObjectInsideBLOB, BLOB]:
    contents = metadata + data
    destination = directory / name
    destination.write_bytes(contents)
    return (
        SerializedObjectInsideBLOB(
            manifest=SerializedObjectManifest(
                encoding=encoding,
                encoding_version=0,
                size=len(contents),
                metadata_size=len(metadata),
                sha256_hash=hashlib.sha256(contents).hexdigest(),
                content_type=content_type,
            ),
            offset=0,
        ),
        BLOB(
            id=name,
            chunks=[BLOBChunk(uri=destination.as_uri(), size=len(contents))],
        ),
    )


def validate_typed_value_server_round_trip(
    manifest: SerializedObjectManifest,
) -> None:
    canonical_content_type = SERVER_CONTENT_TYPE_BY_TYPED_ENCODING.get(
        manifest.encoding
    )
    if canonical_content_type is None:
        return
    effective_content_type = manifest.content_type or canonical_content_type
    round_tripped_encoding = SERVER_TYPED_ENCODING_BY_CONTENT_TYPE.get(
        effective_content_type,
        SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW,
    )
    if round_tripped_encoding == manifest.encoding:
        return
    emitted_name = SerializedObjectEncoding.Name(manifest.encoding)
    round_tripped_name = SerializedObjectEncoding.Name(round_tripped_encoding)
    raise AssertionError(
        f"serialized value encoding {emitted_name} with content type "
        f"{manifest.content_type!r} would return from the orchestration server as "
        f"{round_tripped_name}"
    )


def decode_serialized_value(value: SerializedObjectInsideBLOB, blob: BLOB) -> Any:
    contents = read_blob(blob, value.offset, value.manifest.size)
    digest = hashlib.sha256(contents).hexdigest()
    if value.manifest.sha256_hash and digest != value.manifest.sha256_hash:
        raise AssertionError(
            f"serialized value digest mismatch: expected "
            f"{value.manifest.sha256_hash}, got {digest}"
        )
    metadata_size = value.manifest.metadata_size
    metadata_bytes = contents[:metadata_size]
    data = contents[metadata_size:]
    if metadata_bytes:
        if metadata_bytes.startswith(b"{"):
            metadata = json.loads(metadata_bytes)
            if metadata != {"format": "tensorlake.typescript.value.v1"}:
                raise AssertionError(f"unknown TypeScript value metadata: {metadata!r}")
        else:
            metadata = deserialize_metadata(metadata_bytes)
            return deserialize_value_with_metadata(data, metadata)
    encoding = value.manifest.encoding
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT:
        return data.decode("utf-8")
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON:
        return json.loads(data)
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW:
        return {
            "file": {
                "content_hex": data.hex(),
                "content_type": value.manifest.content_type,
            }
        }
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE:
        return pickle.loads(data)
    raise AssertionError(f"unsupported compatibility value encoding {encoding}")


def decode_function_call_metadata(data: bytes) -> Any | None:
    if not data:
        return None
    if data.startswith(b"{"):
        return json.loads(data)
    return deserialize_metadata(data)


def normalize_value(value: Any) -> Any:
    if isinstance(value, PythonFile):
        return {
            "file": {
                "content_hex": value.content.hex(),
                "content_type": value.content_type,
            }
        }
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, dict):
        return {str(key): normalize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    return value


def read_blob(blob: BLOB, offset: int, size: int) -> bytes:
    remaining = size
    skipped = offset
    output = bytearray()
    for chunk in blob.chunks:
        chunk_path = Path(chunk.uri.removeprefix("file://"))
        chunk_contents = chunk_path.read_bytes()
        if skipped >= len(chunk_contents):
            skipped -= len(chunk_contents)
            continue
        part = chunk_contents[skipped : skipped + remaining]
        output.extend(part)
        remaining -= len(part)
        skipped = 0
        if remaining == 0:
            break
    if remaining != 0:
        raise AssertionError(
            f"BLOB {blob.id} was {remaining} bytes shorter than declared"
        )
    return bytes(output)


def assert_structured_initialization_rejection(
    executor: ExecutorProcess,
    request: InitializeRequest,
    description: str,
) -> dict[str, Any]:
    try:
        response = executor.stub.initialize(request, timeout=RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as error:
        raise AssertionError(
            f"{executor.spec.language} rejected {description} with gRPC "
            f"{error.code().name} instead of InitializeResponse"
        ) from error
    if response.outcome_code != INITIALIZATION_OUTCOME_CODE_FAILURE:
        raise AssertionError(
            f"{executor.spec.language} accepted {description}: {response!r}"
        )
    if (
        not response.HasField("failure_reason")
        or response.failure_reason != INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR
    ):
        raise AssertionError(
            f"{executor.spec.language} returned the wrong failure reason for "
            f"{description}: {response!r}"
        )
    if not response.HasField("error_message") or not response.error_message:
        raise AssertionError(
            f"{executor.spec.language} omitted the initialization error message "
            f"for {description}"
        )
    return {
        "transport": "initialize_response",
        "outcome": "failure",
        "failure_reason": "function_error",
        "has_error_message": True,
    }


def initialize_executor(
    executor: ExecutorProcess, scenario: Scenario
) -> dict[str, Any]:
    info = executor.stub.get_info(InfoRequest(), timeout=RPC_TIMEOUT_SECONDS)
    if info.sdk_language != executor.spec.language:
        raise AssertionError(
            f"expected {executor.spec.language} GetInfo language, got {info.sdk_language}"
        )

    preinitialization_allocation = CreateAllocationRequest(
        allocation=Allocation(
            request_id="compatibility-preinitialization-request",
            function_call_id="compatibility-preinitialization-call",
            allocation_id="compatibility-preinitialization-allocation",
            inputs=FunctionInputs(
                args=[],
                arg_blobs=[],
                request_error_blob=BLOB(id="unused-request-error"),
            ),
        )
    )
    try:
        executor.stub.create_allocation(
            preinitialization_allocation,
            timeout=RPC_TIMEOUT_SECONDS,
        )
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.FAILED_PRECONDITION:
            raise AssertionError(
                f"{executor.spec.language} rejected a pre-initialization "
                f"allocation with {error.code().name}, expected FAILED_PRECONDITION"
            ) from error
        if error.details() != "Function Executor is not initialized":
            raise AssertionError(
                f"{executor.spec.language} returned an unexpected "
                f"pre-initialization error: {error.details()!r}"
            ) from error
    else:
        raise AssertionError(
            f"{executor.spec.language} accepted an allocation before initialization"
        )

    def initialization_request(
        archive: bytes,
        function_name: str = scenario.name,
        namespace: str = "compatibility",
    ) -> InitializeRequest:
        return InitializeRequest(
            function=FunctionRef(
                namespace=namespace,
                application_name=scenario.name,
                application_version=APPLICATION_VERSION,
                function_name=function_name,
            ),
            application_code=SerializedObject(
                manifest=SerializedObjectManifest(
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                    encoding_version=0,
                    size=len(archive),
                    metadata_size=0,
                    sha256_hash=hashlib.sha256(archive).hexdigest(),
                ),
                data=archive,
            ),
        )

    empty_reference_rejection = assert_structured_initialization_rejection(
        executor,
        initialization_request(
            executor.spec.application_archive,
            namespace="",
        ),
        "an empty function-reference namespace",
    )
    post_import_rejection = assert_structured_initialization_rejection(
        executor,
        initialization_request(
            executor.spec.failed_initialization_archive,
            function_name=MISSING_FUNCTION,
        ),
        "an application function absent after module import",
    )

    initialized = executor.stub.initialize(
        initialization_request(executor.spec.application_archive),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    if initialized.outcome_code != INITIALIZATION_OUTCOME_CODE_SUCCESS:
        raise AssertionError(
            f"{executor.spec.language} initialization failed: "
            f"reason={initialized.failure_reason} "
            f"message={initialized.error_message!r}"
        )
    duplicate_rejection = assert_structured_initialization_rejection(
        executor,
        initialization_request(executor.spec.application_archive),
        "a second initialization after success",
    )
    health = executor.stub.check_health(
        HealthCheckRequest(), timeout=RPC_TIMEOUT_SECONDS
    )
    if not health.healthy:
        raise AssertionError(f"{executor.spec.language} executor is not healthy")
    return {
        "protocol_version": info.version,
        "initialization_empty_reference_rejected": empty_reference_rejection,
        "initialization_post_import_rejected": post_import_rejection,
        "initialization_changed_archive_retry": (
            executor.spec.failed_initialization_archive
            != executor.spec.application_archive
        ),
        "initialization_duplicate_rejected": duplicate_rejection,
        "preinitialization_allocation_status": "FAILED_PRECONDITION",
        "initialized": True,
        "healthy": True,
    }


def validate_malformed_allocations(
    executor: ExecutorProcess,
) -> dict[str, str]:
    malformed_requests = (
        (
            "empty_request_id",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="",
                    function_call_id="compatibility-empty-id-call",
                    allocation_id="compatibility-empty-id-allocation",
                    inputs=FunctionInputs(
                        args=[],
                        arg_blobs=[],
                        request_error_blob=BLOB(id="unused-request-error"),
                    ),
                )
            ),
        ),
        (
            "argument_blob_count",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="compatibility-malformed-count-request",
                    function_call_id="compatibility-malformed-count-call",
                    allocation_id="compatibility-malformed-count-allocation",
                    inputs=FunctionInputs(
                        args=[
                            SerializedObjectInsideBLOB(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                                    encoding_version=0,
                                    size=0,
                                    metadata_size=0,
                                    sha256_hash=hashlib.sha256(b"").hexdigest(),
                                    content_type="application/json",
                                ),
                                offset=0,
                            )
                        ],
                        arg_blobs=[],
                        request_error_blob=BLOB(id="unused-request-error"),
                    ),
                )
            ),
        ),
        (
            "missing_request_error_blob",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="compatibility-missing-error-request",
                    function_call_id="compatibility-missing-error-call",
                    allocation_id="compatibility-missing-error-allocation",
                    inputs=FunctionInputs(args=[], arg_blobs=[]),
                )
            ),
        ),
        (
            "missing_argument_manifest",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="compatibility-missing-manifest-request",
                    function_call_id="compatibility-missing-manifest-call",
                    allocation_id="compatibility-missing-manifest-allocation",
                    inputs=FunctionInputs(
                        args=[SerializedObjectInsideBLOB(offset=0)],
                        arg_blobs=[BLOB(id="unused-input")],
                        request_error_blob=BLOB(id="unused-request-error"),
                    ),
                )
            ),
        ),
        (
            "missing_blob_chunk_uri",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="compatibility-missing-uri-request",
                    function_call_id="compatibility-missing-uri-call",
                    allocation_id="compatibility-missing-uri-allocation",
                    inputs=FunctionInputs(
                        args=[
                            SerializedObjectInsideBLOB(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                                    encoding_version=0,
                                    size=0,
                                    metadata_size=0,
                                    sha256_hash=hashlib.sha256(b"").hexdigest(),
                                    content_type="application/json",
                                ),
                                offset=0,
                            )
                        ],
                        arg_blobs=[
                            BLOB(id="invalid-input", chunks=[BLOBChunk(size=0)])
                        ],
                        request_error_blob=BLOB(id="unused-request-error"),
                    ),
                )
            ),
        ),
        (
            "missing_metadata_size",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="compatibility-missing-metadata-request",
                    function_call_id="compatibility-missing-metadata-call",
                    allocation_id="compatibility-missing-metadata-allocation",
                    inputs=FunctionInputs(
                        args=[
                            SerializedObjectInsideBLOB(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                                    encoding_version=0,
                                    size=0,
                                    sha256_hash=hashlib.sha256(b"").hexdigest(),
                                    content_type="application/json",
                                ),
                                offset=0,
                            )
                        ],
                        arg_blobs=[BLOB(id="unused-input")],
                        request_error_blob=BLOB(id="unused-request-error"),
                    ),
                )
            ),
        ),
        (
            "oversized_metadata",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="compatibility-oversized-metadata-request",
                    function_call_id="compatibility-oversized-metadata-call",
                    allocation_id="compatibility-oversized-metadata-allocation",
                    inputs=FunctionInputs(
                        args=[
                            SerializedObjectInsideBLOB(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                                    encoding_version=0,
                                    size=0,
                                    metadata_size=1,
                                    sha256_hash=hashlib.sha256(b"").hexdigest(),
                                    content_type="application/json",
                                ),
                                offset=0,
                            )
                        ],
                        arg_blobs=[BLOB(id="unused-input")],
                        request_error_blob=BLOB(id="unused-request-error"),
                    ),
                )
            ),
        ),
        (
            "unsafe_argument_size",
            CreateAllocationRequest(
                allocation=Allocation(
                    request_id="compatibility-unsafe-size-request",
                    function_call_id="compatibility-unsafe-size-call",
                    allocation_id="compatibility-unsafe-size-allocation",
                    inputs=FunctionInputs(
                        args=[
                            SerializedObjectInsideBLOB(
                                manifest=SerializedObjectManifest(
                                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                                    encoding_version=0,
                                    size=(1 << 53),
                                    metadata_size=0,
                                    sha256_hash=hashlib.sha256(b"").hexdigest(),
                                    content_type="application/json",
                                ),
                                offset=0,
                            )
                        ],
                        arg_blobs=[BLOB(id="unused-input")],
                        request_error_blob=BLOB(id="unused-request-error"),
                    ),
                )
            ),
        ),
    )
    rejection_statuses = {}
    for name, request in malformed_requests:
        try:
            executor.stub.create_allocation(request, timeout=RPC_TIMEOUT_SECONDS)
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.INVALID_ARGUMENT:
                raise AssertionError(
                    f"{executor.spec.language} rejected malformed allocation "
                    f"{request.allocation.allocation_id} with "
                    f"{error.code().name}, expected INVALID_ARGUMENT"
                ) from error
            rejection_statuses[name] = error.code().name.lower()
            continue
        raise AssertionError(
            f"{executor.spec.language} accepted malformed allocation "
            f"{request.allocation.allocation_id}"
        )
    listed = executor.stub.list_allocations(
        ListAllocationsRequest(), timeout=RPC_TIMEOUT_SECONDS
    )
    if listed.allocations:
        raise AssertionError(
            f"{executor.spec.language} retained a rejected malformed allocation"
        )
    return rejection_statuses


def drive_allocation(
    executor: ExecutorProcess,
    scenario: Scenario,
    directory: Path,
    suffix: str,
    replay_mode: int,
    replay_events: list[AllocationEvent] | None = None,
    *,
    fail_output_blob: bool = False,
    malformed_event_log_page: bool = False,
) -> tuple[dict[str, Any], list[AllocationEvent]]:
    allocation_id = f"compatibility-{scenario.name}-{executor.spec.language}-{suffix}"
    executor.stub.create_allocation(
        CreateAllocationRequest(
            allocation=Allocation(
                request_id=f"compatibility-{scenario.name}-request",
                function_call_id=f"compatibility-{scenario.name}-root-call",
                allocation_id=allocation_id,
                inputs=input_for(directory, scenario),
                replay_mode=replay_mode,
            )
        ),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    driver = ProtocolDriver(
        executor.spec,
        executor.stub,
        scenario,
        allocation_id,
        directory,
        replay_events=replay_events,
        fail_output_blob=fail_output_blob,
    )
    if malformed_event_log_page:
        driver.event_responses.put(
            ReadAllocationEventLogResponse(
                allocation_id=allocation_id,
                entries=[
                    AllocationEvent(
                        clock=0,
                        function_call_created=AllocationEventFunctionCallCreated(
                            function_call_id="invalid-clock",
                            status=Status(code=0),
                        ),
                    )
                ],
                last_clock=0,
                has_more=False,
            )
        )
    trace = driver.run()
    listed = executor.stub.list_allocations(
        ListAllocationsRequest(), timeout=RPC_TIMEOUT_SECONDS
    )
    if len(listed.allocations) != 1:
        raise AssertionError(
            f"expected one live allocation, received {len(listed.allocations)}"
        )
    executor.stub.delete_allocation(
        DeleteAllocationRequest(allocation_id=allocation_id),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    after_delete = executor.stub.list_allocations(
        ListAllocationsRequest(), timeout=RPC_TIMEOUT_SECONDS
    )
    if after_delete.allocations:
        raise AssertionError("allocation remained listed after deletion")
    return trace, driver.event_history


def run_output_blob_failure_probe(
    spec: ExecutorSpec, scenario: Scenario
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix=f"tensorlake-fe-output-blob-failure-{spec.language}-"
    ) as temporary:
        directory = Path(temporary)
        with ExecutorProcess(spec) as executor:
            initialize_executor(executor, scenario)
            trace, history = drive_allocation(
                executor,
                scenario,
                directory,
                "output-blob-failure",
                REPLAY_MODE_NONE,
                fail_output_blob=True,
            )
            if history:
                raise AssertionError(
                    f"{spec.language} output-BLOB failure probe unexpectedly "
                    "produced durable event history"
                )
            expected_terminal = {
                "outcome": "failure",
                "failure_reason": "internal_error",
            }
            if trace["terminal"] != expected_terminal:
                raise AssertionError(
                    f"{spec.language} output-BLOB failure did not terminate as "
                    f"an internal error: {trace['terminal']!r}"
                )
            if trace["event_counts"] != {"finish_allocation": 1}:
                raise AssertionError(
                    f"{spec.language} output-BLOB failure emitted unexpected "
                    f"execution events: {trace['event_counts']!r}"
                )
            return {
                "terminal": trace["terminal"],
                "terminal_event_count": trace["terminal_count"],
            }


def run_malformed_event_log_probe(
    spec: ExecutorSpec, scenario: Scenario
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix=f"tensorlake-fe-malformed-event-log-{spec.language}-"
    ) as temporary:
        directory = Path(temporary)
        with ExecutorProcess(spec) as executor:
            initialize_executor(executor, scenario)
            expected_by_mode = {
                "live": "internal_error",
                "strict": "replay_event_history_mismatch",
            }
            results: dict[str, Any] = {}
            for mode, expected_failure_reason in expected_by_mode.items():
                trace, _history = drive_allocation(
                    executor,
                    scenario,
                    directory,
                    f"malformed-event-log-{mode}",
                    REPLAY_MODE_STRICT if mode == "strict" else REPLAY_MODE_NONE,
                    malformed_event_log_page=True,
                )
                expected_terminal = {
                    "outcome": "failure",
                    "failure_reason": expected_failure_reason,
                }
                if trace["terminal"] != expected_terminal:
                    raise AssertionError(
                        f"{spec.language} malformed event-log page in {mode} mode "
                        f"did not produce {expected_terminal!r}: "
                        f"{trace['terminal']!r}"
                    )
                if trace["terminal_count"] != 1:
                    raise AssertionError(
                        f"{spec.language} malformed event-log page in {mode} mode "
                        f"emitted {trace['terminal_count']} terminal events"
                    )
                results[mode] = trace["terminal"]
            return results


def run_replay_corruption_probe(
    spec: ExecutorSpec, scenario: Scenario
) -> dict[str, Any]:
    def copied_events(events: list[AllocationEvent]) -> list[AllocationEvent]:
        copies = []
        for clock, event in enumerate(events, start=1):
            copied = AllocationEvent()
            copied.CopyFrom(event)
            copied.clock = clock
            copies.append(copied)
        return copies

    with tempfile.TemporaryDirectory(
        prefix=f"tensorlake-fe-replay-corruption-{spec.language}-"
    ) as temporary:
        directory = Path(temporary)
        with ExecutorProcess(spec) as executor:
            initialize_executor(executor, scenario)
            initial_trace, history = drive_allocation(
                executor,
                scenario,
                directory,
                "replay-corruption-source",
                REPLAY_MODE_NONE,
            )
            validate_trace(spec.language, scenario, initial_trace)
            kinds = [event.WhichOneof("event") for event in history]
            if kinds != [
                "function_call_created",
                "function_call_watcher_created",
                "function_call_watcher_result",
            ]:
                raise AssertionError(
                    f"{spec.language} replay corruption source history differs: "
                    f"{kinds!r}"
                )
            duplicate_result = copied_events([*history, history[-1]])
            result_before_watcher = copied_events([history[0], history[2], history[1]])
            variants = {
                "unknown_event": [AllocationEvent(clock=1)],
                "result_before_watcher": result_before_watcher,
                "duplicate_result": duplicate_result,
            }
            expected_terminal = {
                "outcome": "failure",
                "failure_reason": "replay_event_history_mismatch",
            }
            results = {}
            for name, replay_events in variants.items():
                trace, live_history = drive_allocation(
                    executor,
                    scenario,
                    directory,
                    f"replay-corruption-{name}",
                    REPLAY_MODE_STRICT,
                    replay_events,
                )
                if live_history:
                    raise AssertionError(
                        f"{spec.language} {name} corruption emitted live history"
                    )
                if (
                    trace["terminal"] != expected_terminal
                    or trace["event_counts"] != {"finish_allocation": 1}
                    or trace["terminal_count"] != 1
                ):
                    raise AssertionError(
                        f"{spec.language} {name} corruption was not rejected "
                        f"exactly once: {trace!r}"
                    )
                results[name] = trace["terminal"]
            return results


def run_scenario(spec: ExecutorSpec, scenario: Scenario) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix=f"tensorlake-fe-{spec.language}-{scenario.name}-"
    ) as temporary:
        directory = Path(temporary)
        with ExecutorProcess(spec) as executor:
            lifecycle = initialize_executor(executor, scenario)
            malformed_allocations = validate_malformed_allocations(executor)
            replay_events = None
            replay_mode = REPLAY_MODE_NONE
            if scenario.replay_mismatch:
                replay_mode = REPLAY_MODE_STRICT
                replay_events = [
                    AllocationEvent(
                        clock=1,
                        function_call_created=AllocationEventFunctionCallCreated(
                            function_call_id="not-the-expected-durable-id",
                            status=Status(code=0),
                        ),
                    )
                ]
            trace, history = drive_allocation(
                executor,
                scenario,
                directory,
                "initial",
                replay_mode,
                replay_events,
            )
            validate_trace(spec.language, scenario, trace)
            context_events = validate_context_events(
                spec.language,
                scenario,
                executor.all_logs(),
            )
            result: dict[str, Any] = {
                **lifecycle,
                "malformed_allocations": malformed_allocations,
                "terminal": trace["terminal"],
                "event_counts": trace["event_counts"],
                "state_operations": trace["state_operations"],
                "progress": trace["progress"],
                **context_events,
            }
            if scenario.replay_success:
                replay_trace, replay_history = drive_allocation(
                    executor,
                    scenario,
                    directory,
                    "replay",
                    REPLAY_MODE_STRICT,
                    history,
                )
                if replay_history:
                    raise AssertionError(
                        "strict replay unexpectedly emitted new live events"
                    )
                validate_replay_trace(scenario, replay_trace)
                result["strict_replay"] = {
                    "terminal": replay_trace["terminal"],
                    "event_counts": replay_trace["event_counts"],
                }
            return result


def validate_context_events(
    language: str,
    scenario: Scenario,
    logs: str,
) -> dict[str, Any]:
    request_id = f"compatibility-{scenario.name}-request"
    function_run_id = f"compatibility-{scenario.name}-root-call"
    allocation_id = f"compatibility-{scenario.name}-{language}-initial"
    metrics: list[tuple[str, str, float]] = []
    progress_events: list[tuple[float, float, str, tuple[tuple[str, str], ...]]] = []
    for line in logs.splitlines():
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(event, dict):
            continue
        event_type = event.get("type")
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        if event_type in (
            "ai.tensorlake.metric.counter.inc",
            "ai.tensorlake.metric.timer",
        ):
            if data.get("request_id") != request_id:
                continue
            if data.get("function_name") != scenario.name:
                raise AssertionError(
                    f"{language} {scenario.name} metric has the wrong function name"
                )
            if event_type.endswith("counter.inc"):
                metrics.append(
                    (
                        "counter",
                        str(data.get("counter_name")),
                        float(data.get("counter_inc")),
                    )
                )
            else:
                metrics.append(
                    (
                        "timer",
                        str(data.get("timer_name")),
                        float(data.get("timer_value")),
                    )
                )
        elif event_type == "ai.tensorlake.progress_update":
            progress_data = data.get("RequestProgressUpdated")
            if not isinstance(progress_data, dict):
                continue
            if progress_data.get("request_id") != request_id:
                continue
            identifiers = {
                "function_name": scenario.name,
                "function_run_id": function_run_id,
                "allocation_id": allocation_id,
            }
            if any(
                progress_data.get(key) != value for key, value in identifiers.items()
            ):
                raise AssertionError(
                    f"{language} {scenario.name} progress event identifiers differ: "
                    f"{progress_data!r}"
                )
            attributes = progress_data.get("attributes") or {}
            if not isinstance(attributes, dict):
                raise AssertionError(
                    f"{language} {scenario.name} progress attributes are not an object"
                )
            progress_events.append(
                (
                    float(progress_data.get("step")),
                    float(progress_data.get("total")),
                    str(progress_data.get("message")),
                    tuple(
                        sorted(
                            (str(key), str(value)) for key, value in attributes.items()
                        )
                    ),
                )
            )
    expected_metrics = list(scenario.expected_metrics)
    if metrics != expected_metrics:
        raise AssertionError(
            f"{language} {scenario.name} metric events differ: "
            f"expected={expected_metrics!r}, actual={metrics!r}"
        )
    expected_progress_events = list(scenario.expected_progress_events)
    if progress_events != expected_progress_events:
        raise AssertionError(
            f"{language} {scenario.name} progress events differ: "
            f"expected={expected_progress_events!r}, actual={progress_events!r}"
        )
    return {
        "metrics": metrics,
        "progress_events": progress_events,
    }


def validate_shutdown_execution_batch(
    spec: ExecutorSpec, batch_future: Any
) -> dict[str, Any]:
    try:
        response = batch_future.result(timeout=RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as error:
        raise AssertionError(
            f"{spec.language} executor closed the execution-log RPC without "
            "delivering a terminal allocation event"
        ) from error
    if len(response.events) != 1:
        raise AssertionError(
            f"{spec.language} executor must deliver exactly one terminal event during "
            f"shutdown, got {len(response.events)}"
        )
    event = response.events[0]
    if event.WhichOneof("event") != "finish_allocation":
        raise AssertionError(
            f"{spec.language} executor returned a non-terminal execution event during "
            f"shutdown: {event!r}"
        )
    finish = event.finish_allocation
    if (
        finish.outcome_code != ALLOCATION_OUTCOME_CODE_FAILURE
        or finish.failure_reason
        != AllocationFailureReason.Value("ALLOCATION_FAILURE_REASON_FUNCTION_ERROR")
    ):
        raise AssertionError(
            f"{spec.language} executor shutdown terminal event is not a function-error "
            f"failure: {finish!r}"
        )
    return {
        "terminal_delivery": "function_error",
        "terminal_event_count": 1,
    }


def run_shutdown_during_input_probe(
    spec: ExecutorSpec, scenario: Scenario
) -> dict[str, Any]:
    request_started = threading.Event()
    release_request = threading.Event()

    class BlockingBLOBHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            request_started.set()
            release_request.wait(timeout=10)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), BlockingBLOBHandler)
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        with tempfile.TemporaryDirectory(
            prefix=f"tensorlake-fe-shutdown-{spec.language}-"
        ) as temporary:
            directory = Path(temporary)
            with ExecutorProcess(spec) as executor:
                initialize_executor(executor, scenario)
                allocation_id = f"compatibility-shutdown-{spec.language}-allocation"
                inputs = input_for(directory, scenario)
                if len(inputs.arg_blobs) != 1 or len(inputs.arg_blobs[0].chunks) != 1:
                    raise AssertionError("shutdown probe requires one input BLOB chunk")
                inputs.arg_blobs[0].chunks[
                    0
                ].uri = f"http://127.0.0.1:{server.server_port}/blocked-input"
                create_request = CreateAllocationRequest(
                    allocation=Allocation(
                        request_id=f"compatibility-shutdown-{spec.language}-request",
                        function_call_id=f"compatibility-shutdown-{spec.language}-call",
                        allocation_id=allocation_id,
                        inputs=inputs,
                        replay_mode=REPLAY_MODE_NONE,
                    )
                )
                executor.stub.create_allocation(
                    create_request,
                    timeout=RPC_TIMEOUT_SECONDS,
                )
                if not request_started.wait(timeout=RPC_TIMEOUT_SECONDS):
                    raise AssertionError(
                        f"{spec.language} executor did not start the blocking "
                        f"input BLOB request:\n{executor.logs()}"
                    )
                admission_statuses = {}
                try:
                    executor.stub.create_allocation(
                        create_request,
                        timeout=RPC_TIMEOUT_SECONDS,
                    )
                except grpc.RpcError as error:
                    admission_statuses["duplicate_create"] = error.code().name
                else:
                    raise AssertionError(
                        f"{spec.language} accepted a duplicate active allocation"
                    )
                try:
                    executor.stub.delete_allocation(
                        DeleteAllocationRequest(allocation_id=allocation_id),
                        timeout=RPC_TIMEOUT_SECONDS,
                    )
                except grpc.RpcError as error:
                    admission_statuses["delete_active"] = error.code().name
                else:
                    raise AssertionError(
                        f"{spec.language} deleted an active allocation"
                    )
                expected_admission = {
                    "duplicate_create": "ALREADY_EXISTS",
                    "delete_active": "FAILED_PRECONDITION",
                }
                if admission_statuses != expected_admission:
                    raise AssertionError(
                        f"{spec.language} active allocation admission differs: "
                        f"{admission_statuses!r}"
                    )
                batch_future = executor.stub.get_allocation_execution_log_batch.future(
                    GetAllocationExecutionLogBatchRequest(allocation_id=allocation_id),
                    timeout=RPC_TIMEOUT_SECONDS,
                )
                process_result = executor.terminate_gracefully(
                    pending_operation="input BLOB download"
                )
                return {
                    **process_result,
                    **validate_shutdown_execution_batch(spec, batch_future),
                    "active_allocation_admission": admission_statuses,
                }
    finally:
        release_request.set()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=1)


def run_shutdown_during_output_probe(
    spec: ExecutorSpec, scenario: Scenario
) -> dict[str, Any]:
    request_started = threading.Event()
    release_request = threading.Event()

    class BlockingBLOBHandler(BaseHTTPRequestHandler):
        def do_PUT(self) -> None:
            request_started.set()
            release_request.wait(timeout=10)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), BlockingBLOBHandler)
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        with tempfile.TemporaryDirectory(
            prefix=f"tensorlake-fe-output-shutdown-{spec.language}-"
        ) as temporary:
            directory = Path(temporary)
            with ExecutorProcess(spec) as executor:
                initialize_executor(executor, scenario)
                allocation_id = (
                    f"compatibility-output-shutdown-{spec.language}-allocation"
                )
                executor.stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id=f"compatibility-output-shutdown-{spec.language}-request",
                            function_call_id=f"compatibility-output-shutdown-{spec.language}-call",
                            allocation_id=allocation_id,
                            inputs=input_for(directory, scenario),
                            replay_mode=REPLAY_MODE_NONE,
                        )
                    ),
                    timeout=RPC_TIMEOUT_SECONDS,
                )
                state_stream = executor.stub.watch_allocation_state(
                    WatchAllocationStateRequest(allocation_id=allocation_id),
                    timeout=RPC_TIMEOUT_SECONDS,
                )
                output_request = None
                for state in state_stream:
                    if state.output_blob_requests:
                        output_request = state.output_blob_requests[0]
                        break
                if output_request is None:
                    raise AssertionError(
                        f"{spec.language} executor did not request an output BLOB"
                    )
                executor.stub.send_allocation_update(
                    AllocationUpdate(
                        allocation_id=allocation_id,
                        output_blob=AllocationOutputBlob(
                            status=Status(code=0),
                            blob=BLOB(
                                id=output_request.id,
                                chunks=[
                                    BLOBChunk(
                                        uri=(
                                            f"http://127.0.0.1:{server.server_port}"
                                            "/blocked-output"
                                        ),
                                        size=output_request.size,
                                    )
                                ],
                            ),
                        ),
                    ),
                    timeout=RPC_TIMEOUT_SECONDS,
                )
                if not request_started.wait(timeout=RPC_TIMEOUT_SECONDS):
                    raise AssertionError(
                        f"{spec.language} executor did not start the blocking "
                        f"output BLOB request:\n{executor.logs()}"
                    )
                batch_future = executor.stub.get_allocation_execution_log_batch.future(
                    GetAllocationExecutionLogBatchRequest(allocation_id=allocation_id),
                    timeout=RPC_TIMEOUT_SECONDS,
                )
                process_result = executor.terminate_gracefully(
                    pending_operation="output BLOB upload"
                )
                return {
                    **process_result,
                    **validate_shutdown_execution_batch(spec, batch_future),
                }
    finally:
        release_request.set()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=1)


def run_shutdown_with_queued_execution_batch_probe(
    spec: ExecutorSpec, scenario: Scenario
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix=f"tensorlake-fe-queued-shutdown-{spec.language}-"
    ) as temporary:
        directory = Path(temporary)
        with ExecutorProcess(spec) as executor:
            initialize_executor(executor, scenario)
            allocation_id = f"compatibility-queued-shutdown-{spec.language}-allocation"
            executor.stub.create_allocation(
                CreateAllocationRequest(
                    allocation=Allocation(
                        request_id=f"compatibility-queued-shutdown-{spec.language}-request",
                        function_call_id=f"compatibility-queued-shutdown-{spec.language}-call",
                        allocation_id=allocation_id,
                        inputs=input_for(directory, scenario),
                        replay_mode=REPLAY_MODE_NONE,
                    )
                ),
                timeout=RPC_TIMEOUT_SECONDS,
            )
            state_driver = ProtocolDriver(
                spec,
                executor.stub,
                scenario,
                allocation_id,
                directory,
            )
            state_thread = threading.Thread(
                target=state_driver._serve_allocation_state,
                daemon=True,
            )
            state_thread.start()
            # Establish the event-read stream before polling execution batches.
            # The child call then blocks waiting for its creation acknowledgement,
            # leaving a real nonterminal batch at the head of the FIFO.
            event_reads = executor.stub.watch_allocation_event_log_reads(
                WatchAllocationEventLogReads(allocation_id=allocation_id),
                timeout=RPC_TIMEOUT_SECONDS,
            )
            event_read_received = threading.Event()

            def receive_first_event_read() -> None:
                try:
                    next(event_reads)
                    event_read_received.set()
                except (grpc.RpcError, StopIteration):
                    pass

            event_read_thread = threading.Thread(
                target=receive_first_event_read,
                daemon=True,
            )
            event_read_thread.start()
            if not event_read_received.wait(timeout=RPC_TIMEOUT_SECONDS):
                raise AssertionError(
                    f"{spec.language} queued-shutdown probe did not receive an "
                    f"event-log read request:\n{executor.logs()}"
                )
            first = executor.stub.get_allocation_execution_log_batch(
                GetAllocationExecutionLogBatchRequest(allocation_id=allocation_id),
                timeout=RPC_TIMEOUT_SECONDS,
            )
            if (
                len(first.events) != 1
                or first.events[0].WhichOneof("event") != "create_function_call"
            ):
                raise AssertionError(
                    f"{spec.language} queued-shutdown probe expected one child-call "
                    f"batch, got {first!r}"
                )

            state_driver.stop.set()
            executor.request_graceful_shutdown()
            executor.stub.advance_allocation_execution_log_batch(
                AdvanceAllocationExecutionLogBatchRequest(allocation_id=allocation_id),
                timeout=RPC_TIMEOUT_SECONDS,
            )
            terminal_future = executor.stub.get_allocation_execution_log_batch.future(
                GetAllocationExecutionLogBatchRequest(allocation_id=allocation_id),
                timeout=RPC_TIMEOUT_SECONDS,
            )
            process_result = executor.wait_for_graceful_shutdown(
                pending_operation="an older execution batch awaiting acknowledgement"
            )
            event_reads.cancel()
            event_read_thread.join(timeout=1)
            state_thread.join(timeout=1)
            if state_driver.error is not None:
                raise state_driver.error
            return {
                **process_result,
                **validate_shutdown_execution_batch(spec, terminal_future),
            }


def validate_trace(language: str, scenario: Scenario, trace: dict[str, Any]) -> None:
    if trace["terminal"] != scenario.expected_terminal:
        raise AssertionError(
            f"{language} {scenario.name} terminal mismatch:\n"
            f"expected={scenario.expected_terminal!r}\n"
            f"actual={trace['terminal']!r}"
        )
    expected_creations, expected_watchers = scenario.call_counts(language)
    counts = trace["event_counts"]
    if counts.get("create_function_call", 0) != expected_creations:
        raise AssertionError(
            f"{language} {scenario.name} expected {expected_creations} child "
            f"creations, got {counts.get('create_function_call', 0)}"
        )
    if (
        expected_watchers >= 0
        and counts.get("create_function_call_watcher", 0) != expected_watchers
    ):
        raise AssertionError(
            f"{language} {scenario.name} expected {expected_watchers} child "
            f"watchers, got {counts.get('create_function_call_watcher', 0)}"
        )
    if counts.get("finish_allocation", 0) != 1 or trace["terminal_count"] != 1:
        raise AssertionError(f"{language} {scenario.name} did not finish exactly once")
    if tuple(trace["state_operations"]) != scenario.expected_state_operations:
        raise AssertionError(
            f"{language} {scenario.name} request-state operations differ: "
            f"{trace['state_operations']!r}"
        )
    if tuple(trace["progress"]) != scenario.expected_progress:
        raise AssertionError(
            f"{language} {scenario.name} progress differs: {trace['progress']!r}"
        )
    if trace["scheduled_call_count"] != scenario.expected_scheduled_calls:
        raise AssertionError(
            f"{language} {scenario.name} scheduled "
            f"{trace['scheduled_call_count']} calls, expected "
            f"{scenario.expected_scheduled_calls}"
        )
    validate_child_calls(language, scenario, trace["child_calls"])


def validate_child_calls(
    language: str, scenario: Scenario, calls: list[dict[str, Any]]
) -> None:
    if len(calls) != scenario.call_counts(language)[0]:
        raise AssertionError(
            f"{language} {scenario.name} child-call trace is incomplete"
        )
    if not calls:
        return
    if scenario.behavior in (
        "double",
        "tail_call",
        "wait_first_failure",
        "wait_first_failure_with_failure",
        "wait_causal_replay",
        "wait_batched_results",
        "watcher_failure",
        "watcher_request_error",
        "watcher_timeout",
        "creation_failure",
        "watcher_creation_failure",
    ):
        expected_functions = (
            [DOUBLE_FUNCTION, FAILING_FUNCTION]
            if scenario.behavior == "wait_first_failure_with_failure"
            else [
                (
                    REQUEST_FAILING_FUNCTION
                    if scenario.behavior == "watcher_request_error"
                    else (
                        FAILING_FUNCTION
                        if scenario.behavior in ("watcher_failure", "creation_failure")
                        else DOUBLE_FUNCTION
                    )
                )
            ]
            * len(calls)
        )
        expected_arguments = (
            [
                [scenario.input],
                [scenario.input + 1],
                [scenario.input + 2],
            ]
            if scenario.behavior == "wait_causal_replay"
            else (
                [[scenario.input], [scenario.input + 1]]
                if scenario.behavior
                in (
                    "wait_first_failure",
                    "wait_first_failure_with_failure",
                    "wait_batched_results",
                )
                else [[scenario.input]] * len(calls)
            )
        )
        if [call["arguments"] for call in calls] != expected_arguments or any(
            call["function"] != expected_function or call["special"] is not None
            for call, expected_function in zip(calls, expected_functions)
        ):
            raise AssertionError(
                f"{language} {scenario.name} child-call arguments differ: {calls!r}"
            )
    elif scenario.behavior == "wait_all_completed":
        actual = [
            (call["function"], call["arguments"], call["special"]) for call in calls
        ]
        expected = [
            (DOUBLE_FUNCTION, [scenario.input], None),
            (FAILING_FUNCTION, [scenario.input + 1], None),
        ]
        if actual != expected:
            raise AssertionError(f"{language} all-completed calls differ: {actual!r}")
    elif scenario.behavior in (
        "wait_timeout",
        "run_later",
        "detached_future",
        "future_reuse",
        "unhandled_child_failure",
        "unhandled_child_request_error",
    ):
        expected_function = (
            FAILING_FUNCTION
            if scenario.behavior == "unhandled_child_failure"
            else (
                REQUEST_FAILING_FUNCTION
                if scenario.behavior == "unhandled_child_request_error"
                else DOUBLE_FUNCTION
            )
        )
        if any(
            call["function"] != expected_function
            or call["arguments"] != [scenario.input]
            or call["special"] is not None
            for call in calls
        ):
            raise AssertionError(
                f"{language} {scenario.name} child-call intent differs: {calls!r}"
            )
    elif scenario.behavior == "map_empty":
        actual = without_durable_ids(calls)
        expected = [
            {
                "function": DOUBLE_FUNCTION,
                "arguments": [],
                "keyword_arguments": {},
                "special": "map",
            }
        ]
        if language != "python" or actual != expected:
            raise AssertionError(
                f"{language} empty-map call intent differs: {actual!r}"
            )
    elif scenario.behavior == "reduce_empty":
        actual = without_durable_ids(calls)
        expected = [
            {
                "function": ADD_FUNCTION,
                "arguments": [],
                "keyword_arguments": {"initial": scenario.input},
                "special": "reduce",
            }
        ]
        if language != "python" or actual != expected:
            raise AssertionError(
                f"{language} empty-reduce call intent differs: {actual!r}"
            )
    elif scenario.behavior == "map_failure":
        if language == "python":
            expected = [
                {
                    "function": FAILING_FUNCTION,
                    "arguments": [
                        scenario.input,
                        scenario.input + 1,
                        scenario.input + 2,
                    ],
                    "keyword_arguments": {},
                    "special": "map",
                }
            ]
        else:
            expected = [
                {
                    "function": FAILING_FUNCTION,
                    "arguments": [value],
                    "keyword_arguments": {},
                    "special": None,
                }
                for value in (
                    scenario.input,
                    scenario.input + 1,
                    scenario.input + 2,
                )
            ]
        actual = without_durable_ids(calls)
        if actual != expected:
            raise AssertionError(
                f"{language} failing-map call intent differs: {actual!r}"
            )
    elif scenario.behavior == "reduce_failure":
        actual = without_durable_ids(calls)
        expected = [
            {
                "function": ADD_FUNCTION,
                "arguments": [scenario.input, scenario.input + 1],
                "keyword_arguments": {"initial": 0},
                "special": "reduce",
            }
        ]
        if actual != expected:
            raise AssertionError(
                f"{language} failing-reduce call intent differs: {actual!r}"
            )
    elif scenario.behavior == "map":
        if language == "python":
            expected = [
                {
                    "function": DOUBLE_FUNCTION,
                    "arguments": [1, 2, 3],
                    "keyword_arguments": {},
                    "special": "map",
                }
            ]
            actual = without_durable_ids(calls)
            if actual != expected:
                raise AssertionError(f"Python map protocol differs: {actual!r}")
        else:
            values = [call["arguments"][0] for call in calls]
            if values != [1, 2, 3] or any(
                call["function"] != DOUBLE_FUNCTION or call["special"] is not None
                for call in calls
            ):
                raise AssertionError(f"TypeScript map protocol differs: {calls!r}")
    elif scenario.behavior == "reduce_large":
        if language == "python":
            expected = [
                {
                    "function": ADD_FUNCTION,
                    "arguments": list(range(1, 514)),
                    "keyword_arguments": {"initial": scenario.input},
                    "special": "reduce",
                }
            ]
        else:
            expected = [
                {
                    "function": ADD_FUNCTION,
                    "arguments": list(range(1, 513)),
                    "keyword_arguments": {"initial": scenario.input},
                    "special": "reduce",
                },
                {
                    "function": ADD_FUNCTION,
                    "arguments": [513],
                    "keyword_arguments": {
                        "initial": scenario.input + sum(range(1, 513))
                    },
                    "special": "reduce",
                },
            ]
        actual = without_durable_ids(calls)
        if actual != expected:
            raise AssertionError(
                f"{language} large reduce protocol differs: {actual!r}"
            )
    elif scenario.behavior in ("reduce", "reduce_no_initial"):
        if scenario.behavior == "reduce":
            expected = [
                {
                    "function": ADD_FUNCTION,
                    "arguments": [1, 2, 3],
                    "keyword_arguments": {"initial": 10},
                    "special": "reduce",
                }
            ]
        elif language == "python":
            expected = [
                {
                    "function": ADD_FUNCTION,
                    "arguments": [1, 2, 3],
                    "keyword_arguments": {},
                    "special": "reduce",
                }
            ]
        else:
            expected = [
                {
                    "function": ADD_FUNCTION,
                    "arguments": [2, 3],
                    "keyword_arguments": {"initial": 1},
                    "special": "reduce",
                }
            ]
        actual = without_durable_ids(calls)
        if actual != expected:
            raise AssertionError(f"{language} reduce protocol differs: {actual!r}")
    elif scenario.behavior == "map_reduce":
        if language == "python":
            expected = [
                {
                    "function": DOUBLE_FUNCTION,
                    "arguments": [1, 2, 3],
                    "keyword_arguments": {},
                    "special": "map",
                },
                {
                    "function": ADD_FUNCTION,
                    "arguments": [2, 4, 6],
                    "keyword_arguments": {"initial": 0},
                    "special": "reduce",
                },
            ]
        else:
            expected = [
                {
                    "function": DOUBLE_FUNCTION,
                    "arguments": [value],
                    "keyword_arguments": {},
                    "special": None,
                }
                for value in (1, 2, 3)
            ]
            expected.append(
                {
                    "function": ADD_FUNCTION,
                    "arguments": [2, 4, 6],
                    "keyword_arguments": {"initial": 0},
                    "special": "reduce",
                }
            )
        actual = without_durable_ids(calls)
        if actual != expected:
            raise AssertionError(f"{language} map/reduce protocol differs: {actual!r}")


def validate_replay_trace(scenario: Scenario, trace: dict[str, Any]) -> None:
    if trace["terminal"] != scenario.expected_terminal:
        raise AssertionError(
            f"strict replay terminal differs: expected {scenario.expected_terminal!r}, "
            f"got {trace['terminal']!r}"
        )
    if trace["event_counts"] != {"finish_allocation": 1}:
        raise AssertionError(
            f"strict replay emitted live durable operations: {trace['event_counts']!r}"
        )
    if trace["terminal_count"] != 1:
        raise AssertionError("strict replay did not finish exactly once")
    if tuple(trace["state_operations"]) != scenario.expected_state_operations:
        raise AssertionError(
            "strict replay request-state operations differ: "
            f"{trace['state_operations']!r}"
        )
    if tuple(trace["progress"]) != scenario.expected_progress:
        raise AssertionError(f"strict replay progress differs: {trace['progress']!r}")


def without_durable_ids(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {key: value for key, value in call.items() if key != "durable_id"}
        for call in calls
    ]


def kind_counts(batches: list[list[str]]) -> Counter[str]:
    return Counter(kind for batch in batches for kind in batch)


def enum_suffix(value: str, prefix: str) -> str:
    if not value.startswith(prefix):
        raise AssertionError(f"unexpected enum name {value!r}")
    return value.removeprefix(prefix).lower()


def comparable_result(result: dict[str, Any]) -> dict[str, Any]:
    comparable = {
        "protocol_version": result["protocol_version"],
        "initialization_empty_reference_rejected": result[
            "initialization_empty_reference_rejected"
        ],
        "initialization_post_import_rejected": result[
            "initialization_post_import_rejected"
        ],
        "initialization_changed_archive_retry": result[
            "initialization_changed_archive_retry"
        ],
        "initialization_duplicate_rejected": result[
            "initialization_duplicate_rejected"
        ],
        "initialized": result["initialized"],
        "healthy": result["healthy"],
        "malformed_allocations": result["malformed_allocations"],
        "terminal": result["terminal"],
        "state_operations": result["state_operations"],
        "progress": result["progress"],
        "metrics": result["metrics"],
        "progress_events": result["progress_events"],
    }
    if "strict_replay" in result:
        comparable["strict_replay"] = result["strict_replay"]
    return comparable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=[scenario.name for scenario in SCENARIOS],
        help="Run only this scenario; may be supplied more than once.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selected = [
        scenario
        for scenario in SCENARIOS
        if args.scenario is None or scenario.name in args.scenario
    ]
    typescript_entrypoint = (
        REPOSITORY_ROOT / "typescript" / "bin" / "function-executor.js"
    )
    typescript_build = (
        REPOSITORY_ROOT / "typescript" / "dist" / "function-executor" / "main.js"
    )
    applications_build = (
        REPOSITORY_ROOT / "typescript" / "dist" / "applications" / "index.js"
    )
    if not typescript_build.exists() or not applications_build.exists():
        raise SystemExit(
            "TypeScript SDK and function executor are not built; run "
            "`npm --prefix typescript run build:sdk` first"
        )
    manifest_contract = run_manifest_contract_probe()
    print(
        "application_manifest: " f"{json.dumps(manifest_contract, sort_keys=True)}",
        flush=True,
    )
    specs = [
        ExecutorSpec(
            language="python",
            command=(sys.executable, "-m", "tensorlake.function_executor.main"),
            application_archive=build_python_archive(),
            failed_initialization_archive=build_python_archive(stale=True),
        ),
        ExecutorSpec(
            language="typescript",
            command=("node", str(typescript_entrypoint)),
            application_archive=build_typescript_archive(),
            failed_initialization_archive=build_typescript_archive(stale=True),
        ),
    ]
    shutdown_scenario = next(
        scenario for scenario in SCENARIOS if scenario.name == "parity_child"
    )
    output_blob_failure_results = {
        spec.language: run_output_blob_failure_probe(spec, SCENARIOS[0])
        for spec in specs
    }
    if (
        output_blob_failure_results["python"]
        != output_blob_failure_results["typescript"]
    ):
        raise AssertionError(
            "Python and TypeScript output-BLOB failure behavior differs: "
            f"{output_blob_failure_results!r}"
        )
    print(
        "output_blob_failure: "
        f"{json.dumps(output_blob_failure_results, sort_keys=True)}",
        flush=True,
    )
    malformed_event_log_results = {
        spec.language: run_malformed_event_log_probe(spec, shutdown_scenario)
        for spec in specs
    }
    if (
        malformed_event_log_results["python"]
        != malformed_event_log_results["typescript"]
    ):
        raise AssertionError(
            "Python and TypeScript malformed event-log behavior differs: "
            f"{malformed_event_log_results!r}"
        )
    print(
        "malformed_event_log: "
        f"{json.dumps(malformed_event_log_results, sort_keys=True)}",
        flush=True,
    )
    replay_corruption_results = {
        spec.language: run_replay_corruption_probe(spec, shutdown_scenario)
        for spec in specs
    }
    if replay_corruption_results["python"] != replay_corruption_results["typescript"]:
        raise AssertionError(
            "Python and TypeScript replay corruption behavior differs: "
            f"{replay_corruption_results!r}"
        )
    print(
        "replay_corruption: "
        f"{json.dumps(replay_corruption_results, sort_keys=True)}",
        flush=True,
    )
    shutdown_results = {}
    for spec in specs:
        shutdown_results[spec.language] = {
            "input_blob": run_shutdown_during_input_probe(spec, SCENARIOS[0]),
            "output_blob": run_shutdown_during_output_probe(spec, SCENARIOS[0]),
            "execution_batch": run_shutdown_with_queued_execution_batch_probe(
                spec,
                shutdown_scenario,
            ),
        }
        shutdown_core_results = {
            json.dumps(
                {
                    key: value
                    for key, value in result.items()
                    if key != "active_allocation_admission"
                },
                sort_keys=True,
            )
            for result in shutdown_results[spec.language].values()
        }
        if len(shutdown_core_results) != 1:
            raise AssertionError(
                f"{spec.language} executor shutdown differs by pending operation: "
                f"{shutdown_results[spec.language]!r}"
            )
    if shutdown_results["python"] != shutdown_results["typescript"]:
        raise AssertionError(
            "Python and TypeScript executor shutdown behavior differs: "
            f"{shutdown_results!r}"
        )
    print(
        "executor_shutdown: " f"{json.dumps(shutdown_results, sort_keys=True)}",
        flush=True,
    )

    results: dict[str, dict[str, Any]] = {}
    for scenario in selected:
        language_results = {
            spec.language: run_scenario(spec, scenario) for spec in specs
        }
        comparable = {
            language: comparable_result(result)
            for language, result in language_results.items()
        }
        if comparable["python"] != comparable["typescript"]:
            print(
                json.dumps(language_results, indent=2, sort_keys=True), file=sys.stderr
            )
            raise AssertionError(f"{scenario.name} runtime results differ")
        results[scenario.name] = language_results
        print(f"{scenario.name}: passed", flush=True)
    print(json.dumps(results, indent=2, sort_keys=True))
    print(
        f"Python and TypeScript function executors passed "
        f"{len(selected)} shared parity scenarios."
    )


if __name__ == "__main__":
    main()
