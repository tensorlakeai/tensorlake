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
    InfoRequest,
    InitializeRequest,
    ListAllocationsRequest,
    ReadAllocationEventLogResponse,
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

DOUBLE_FUNCTION = "parity_double"
ADD_FUNCTION = "parity_add"
FAILING_FUNCTION = "parity_failing_child"
IDENTITY_FILE_FUNCTION = "parity_identity_file"

FUNCTION_NAMES = (
    DOUBLE_FUNCTION,
    ADD_FUNCTION,
    FAILING_FUNCTION,
    IDENTITY_FILE_FUNCTION,
    "parity_value",
    "parity_multipart",
    "parity_child",
    "parity_map",
    "parity_reduce",
    "parity_tail_call",
    "parity_handled_child_failure",
    "parity_handled_creation_failure",
    "parity_request_error",
    "parity_function_error",
    "parity_file",
    "parity_json_file",
    "parity_state",
    "parity_replay_mismatch",
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
        name="parity_handled_creation_failure",
        input=21,
        expected_terminal={"outcome": "success", "value": "caught:creation_error"},
        behavior="creation_failure",
        expected_calls=(1, 0),
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
            },
        },
        behavior="json_file",
        expected_calls=(1, 1),
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
)


@dataclass(frozen=True)
class ExecutorSpec:
    language: str
    command: tuple[str, ...]
    application_archive: bytes


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

    def logs(self) -> str:
        self._log.flush()
        self._log.seek(0)
        return self._log.read().decode("utf-8", errors="replace")[-16_000:]

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
        self.child_results: dict[str, ChildResult] = {}
        self.call_metadata: dict[str, bytes] = {}
        self.batch_kinds: list[list[str]] = []
        self.terminal_count = 0
        self.state_operation_kinds: list[str] = []
        self.progress: list[tuple[float, float]] = []
        self.state_values: dict[str, BLOB] = {}
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
            "terminal_count": self.terminal_count,
            "state_operations": self.state_operation_kinds,
            "progress": self.progress,
        }

    def _accept_child_call(self, creation) -> None:
        updates = creation.updates
        if len(updates.updates) > 1:
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

        initial = None
        values: list[Any] = []
        previous_id = None
        logical_function = calls[0].target.function_name
        for index, call in enumerate(calls):
            if call.target.function_name != logical_function or len(call.args) != 2:
                raise AssertionError("reduce chain has inconsistent reducer calls")
            metadata = decode_function_call_metadata(call.call_metadata)
            if not isinstance(metadata, dict) or metadata.get("operation") != "reduce":
                raise AssertionError(
                    "reduce chain is missing TypeScript reduce metadata"
                )
            if metadata.get("reduceStep") != index:
                raise AssertionError("reduce chain steps are not ordered")
            accumulator = call.args[0]
            if index == 0:
                if not accumulator.HasField("value"):
                    raise AssertionError(
                        "reduce chain initial accumulator is not inline"
                    )
                initial = decode_serialized_value(accumulator.value, creation.args_blob)
            elif (
                not accumulator.HasField("function_call_id")
                or accumulator.function_call_id != previous_id
            ):
                raise AssertionError(
                    "reduce chain accumulator does not reference the prior step"
                )
            if not call.args[1].HasField("value"):
                raise AssertionError("reduce chain item is not inline")
            values.append(
                decode_serialized_value(call.args[1].value, creation.args_blob)
            )
            previous_id = call.id

        child_call = {
            "durable_id": durable_id,
            "function": logical_function,
            "arguments": normalize_value(values),
            "keyword_arguments": {"initial": normalize_value(initial)},
            "special": "reduce",
        }
        self.child_calls.append(child_call)
        self.call_metadata[durable_id] = bytes(calls[-1].call_metadata)
        self.child_results[durable_id] = self._result_for_call(child_call)
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
        if behavior == "map":
            if call["special"] == "map":
                return ChildResult(
                    outcome="success", value=[value * 2 for value in arguments]
                )
            return ChildResult(outcome="success", value=arguments[0] * 2)
        if behavior == "reduce":
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
        if behavior in ("double", "tail_call"):
            return ChildResult(outcome="success", value=arguments[0] * 2)
        raise AssertionError(f"unexpected child call in {self.scenario.name}: {call!r}")

    def _accept_child_watcher(self, watcher) -> None:
        durable_id = watcher.function_call_id
        if durable_id not in self.child_results:
            raise AssertionError(f"watcher references unknown child call {durable_id}")
        self._queue_events(
            [
                AllocationEvent(
                    function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                        function_call_id=durable_id,
                        status=Status(code=0),
                    )
                )
            ]
        )
        result = self.child_results[durable_id]
        watcher_result = AllocationEventFunctionCallWatcherResult(
            function_call_id=durable_id,
            watcher_status=FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
        )
        if result.outcome == "success":
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
        self._queue_events(
            [AllocationEvent(function_call_watcher_result=watcher_result)]
        )

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
                    destination = self.directory / f"output-{request.id}"
                    destination.touch()
                    self.stub.send_allocation_update(
                        AllocationUpdate(
                            allocation_id=self.allocation_id,
                            output_blob=AllocationOutputBlob(
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
                            ),
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
        if kind == "prepare_write":
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


def build_python_archive() -> bytes:
    manifest = {
        "functions": {
            name: {"name": name, "module_import_name": "python_application"}
            for name in FUNCTION_NAMES
        }
    }
    return build_zip(
        {
            ".tensorlake_code_manifest.json": json.dumps(manifest).encode(),
            "python_application.py": (
                FIXTURE_ROOT / "python_application.py"
            ).read_bytes(),
        }
    )


def build_typescript_archive() -> bytes:
    sdk_module = (
        REPOSITORY_ROOT / "typescript" / "dist" / "applications" / "index.js"
    ).as_uri()
    runtime = f"""
import {{
  File,
  FunctionError,
  RequestContext,
  RequestError,
  getFunction,
  registerApplication,
  registerFunction,
  schema,
}} from {json.dumps(sdk_module)};

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
const identityFile = registerFunction(async (value) => value, {{
  name: {json.dumps(IDENTITY_FILE_FUNCTION)},
  parameters: [schema.parameter("value", schema.file())],
  returns: schema.file(),
}});

registerApplication("parity_value", async (value) => ({{ value }}));
registerApplication("parity_multipart", async (left, right) => left * right);
registerApplication("parity_child", async (value) => double(value));
registerApplication(
  "parity_map",
  async (value) => double.map([value, value + 1, value + 2]),
);
registerApplication(
  "parity_reduce",
  async (value) => add.reduce([value, value + 1, value + 2], 10),
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
registerApplication("parity_handled_creation_failure", async (value) => {{
  try {{
    await failingChild(value);
  }} catch (error) {{
    if (!(error instanceof FunctionError)) throw error;
    return "caught:creation_error";
  }}
  return "unexpected:success";
}});
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
  }};
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
registerApplication("parity_replay_mismatch", async (value) => double(value));

export function __tensorlakeGetFunction(name) {{
  return getFunction(name);
}}
""".encode()
    manifest = {
        "format_version": 2,
        "runtime": "typescript",
        "minimum_node_major": 24,
        "module": "runtime.mjs",
        "functions": {name: {"name": name} for name in FUNCTION_NAMES},
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


def input_for(directory: Path, scenario: Scenario) -> FunctionInputs:
    content_type = "application/json"
    encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
    if scenario.multipart:
        boundary = "tensorlake-parity-boundary"
        parts: list[bytes] = []
        for index, value in enumerate(scenario.input):
            parts.extend(
                [
                    f"--{boundary}".encode(),
                    (
                        f'Content-Disposition: form-data; name="{index}"; '
                        f'filename="{index}"'
                    ).encode(),
                    b"Content-Type: application/json",
                    b"",
                    json.dumps(value, separators=(",", ":")).encode(),
                ]
            )
        parts.extend([f"--{boundary}--".encode(), b""])
        data = b"\r\n".join(parts)
        content_type = f'multipart/form-data; boundary="{boundary}"'
        encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW
    else:
        data = json.dumps(scenario.input, separators=(",", ":")).encode()
    source = directory / "input"
    source.write_bytes(data)
    request_error = directory / "request-error"
    request_error.touch()
    return FunctionInputs(
        args=[
            SerializedObjectInsideBLOB(
                manifest=SerializedObjectManifest(
                    encoding=encoding,
                    encoding_version=0,
                    size=len(data),
                    metadata_size=0,
                    sha256_hash=hashlib.sha256(data).hexdigest(),
                    content_type=content_type,
                ),
                offset=0,
            )
        ],
        arg_blobs=[
            BLOB(
                id="compatibility-input",
                chunks=[BLOBChunk(uri=source.as_uri(), size=len(data))],
            )
        ],
        request_error_blob=BLOB(
            id="compatibility-request-error",
            chunks=[
                BLOBChunk(uri=request_error.as_uri(), size=REQUEST_ERROR_BLOB_SIZE)
            ],
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


def initialize_executor(
    executor: ExecutorProcess, scenario: Scenario
) -> dict[str, Any]:
    info = executor.stub.get_info(InfoRequest(), timeout=RPC_TIMEOUT_SECONDS)
    if info.sdk_language != executor.spec.language:
        raise AssertionError(
            f"expected {executor.spec.language} GetInfo language, got {info.sdk_language}"
        )
    initialized = executor.stub.initialize(
        InitializeRequest(
            function=FunctionRef(
                namespace="compatibility",
                application_name=scenario.name,
                application_version=APPLICATION_VERSION,
                function_name=scenario.name,
            ),
            application_code=SerializedObject(
                manifest=SerializedObjectManifest(
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                    encoding_version=0,
                    size=len(executor.spec.application_archive),
                    metadata_size=0,
                    sha256_hash=hashlib.sha256(
                        executor.spec.application_archive
                    ).hexdigest(),
                ),
                data=executor.spec.application_archive,
            ),
        ),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    if initialized.outcome_code != INITIALIZATION_OUTCOME_CODE_SUCCESS:
        raise AssertionError(
            f"{executor.spec.language} initialization failed: "
            f"reason={initialized.failure_reason} "
            f"message={initialized.error_message!r}"
        )
    health = executor.stub.check_health(
        HealthCheckRequest(), timeout=RPC_TIMEOUT_SECONDS
    )
    if not health.healthy:
        raise AssertionError(f"{executor.spec.language} executor is not healthy")
    return {
        "protocol_version": info.version,
        "initialized": True,
        "healthy": True,
    }


def drive_allocation(
    executor: ExecutorProcess,
    scenario: Scenario,
    directory: Path,
    suffix: str,
    replay_mode: int,
    replay_events: list[AllocationEvent] | None = None,
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


def run_scenario(spec: ExecutorSpec, scenario: Scenario) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix=f"tensorlake-fe-{spec.language}-{scenario.name}-"
    ) as temporary:
        directory = Path(temporary)
        with ExecutorProcess(spec) as executor:
            lifecycle = initialize_executor(executor, scenario)
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
            result: dict[str, Any] = {
                **lifecycle,
                "terminal": trace["terminal"],
                "event_counts": trace["event_counts"],
                "state_operations": trace["state_operations"],
                "progress": trace["progress"],
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
    if counts.get("create_function_call_watcher", 0) != expected_watchers:
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
        "watcher_failure",
        "creation_failure",
    ):
        expected_function = (
            FAILING_FUNCTION
            if scenario.behavior in ("watcher_failure", "creation_failure")
            else DOUBLE_FUNCTION
        )
        if any(
            call["function"] != expected_function
            or call["arguments"] != [scenario.input]
            or call["special"] is not None
            for call in calls
        ):
            raise AssertionError(
                f"{language} {scenario.name} child-call arguments differ: {calls!r}"
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
    elif scenario.behavior == "reduce":
        expected = [
            {
                "function": ADD_FUNCTION,
                "arguments": [1, 2, 3],
                "keyword_arguments": {"initial": 10},
                "special": "reduce",
            }
        ]
        actual = without_durable_ids(calls)
        if actual != expected:
            raise AssertionError(f"{language} reduce protocol differs: {actual!r}")


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
        "initialized": result["initialized"],
        "healthy": result["healthy"],
        "terminal": result["terminal"],
        "state_operations": result["state_operations"],
        "progress": result["progress"],
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
    specs = [
        ExecutorSpec(
            language="python",
            command=(sys.executable, "-m", "tensorlake.function_executor.main"),
            application_archive=build_python_archive(),
        ),
        ExecutorSpec(
            language="typescript",
            command=("node", str(typescript_entrypoint)),
            application_archive=build_typescript_archive(),
        ),
    ]
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
