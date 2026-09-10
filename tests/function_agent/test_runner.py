from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import io
import json
import os
import pickle
import threading
import unittest
import zipfile
from typing import Any, Coroutine

from tensorlake._cloud_sdk import FunctionAgentCore
from tensorlake.applications.request_context.request_state import (
    REQUEST_STATE_USER_DATA_SERIALIZER,
)
from tensorlake.applications.user_data_serializer import (
    APPLICATION_FUNCTION_CALL_SERIALIZER_NAME,
    serializer_by_name,
)
from tensorlake.function_agent.runner import ProtocolWriter, PythonFunctionRunner
from tests.function_agent.benchmark_protocol_writer import SerialProtocolWriter, measure


class FakeNativeCore:
    """The language-side native contract, without duplicating Rust behavior."""

    def __init__(self) -> None:
        self.inputs: asyncio.Queue[str] = asyncio.Queue()
        self.outputs: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def next_input(self) -> str:
        return await self.inputs.get()

    async def submit_output(self, output_json: str) -> None:
        await self.outputs.put(json.loads(output_json))

    def push(self, message: dict[str, Any]) -> None:
        self.inputs.put_nowait(json.dumps(message))


class LoopBoundNativeCore:
    """Models PyO3 future creation, which requires the running loop thread."""

    def __init__(self) -> None:
        self.outputs: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    def submit_output(self, output_json: str) -> Coroutine[Any, Any, None]:
        asyncio.get_running_loop()

        async def submit() -> None:
            await self.outputs.put(json.loads(output_json))

        return submit()


class GatedNativeCore(FakeNativeCore):
    """Explicit native completion gates, without implementing a second WAL."""

    def __init__(self) -> None:
        super().__init__()
        self.entered: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.pending: dict[str, asyncio.Future[None]] = {}

    def submit_output(self, output_json: str) -> Coroutine[Any, Any, None]:
        loop = asyncio.get_running_loop()
        message = json.loads(output_json)

        async def submit() -> None:
            completion = loop.create_future()
            self.pending[message["test_id"]] = completion
            self.entered.put_nowait(message)
            await completion

        return submit()

    def release_all(self) -> None:
        for completion in self.pending.values():
            if not completion.done():
                completion.set_result(None)


class ProtocolWriterConcurrencyTest(unittest.IsolatedAsyncioTestCase):
    async def test_benchmark_timeout_drains_actual_workers_without_blocking_loop(
        self,
    ) -> None:
        for writer_type in (SerialProtocolWriter, ProtocolWriter):
            measured = asyncio.create_task(
                measure(
                    writer_type,
                    0,
                    native_delay_seconds=60,
                    ack_timeout_seconds=0.02,
                )
            )
            done, _ = await asyncio.wait({measured}, timeout=3)
            if not done:
                measured.cancel()
                self.fail("benchmark timeout failed to finish cleanup within 3s")
            with self.assertRaises(asyncio.TimeoutError):
                measured.result()

    async def asyncSetUp(self) -> None:
        self.core = GatedNativeCore()
        self.writer = ProtocolWriter(self.core, asyncio.get_running_loop())  # type: ignore[arg-type]
        self.writes: list[asyncio.Task[None]] = []
        self.workers_finished: list[threading.Event] = []

    async def asyncTearDown(self) -> None:
        # Bounded teardown also releases any next same-attempt writer admitted
        # by completing its predecessor; no worker is left waiting on the loop.
        async def drain() -> None:
            while any(not write.done() for write in self.writes) or any(
                not finished.is_set() for finished in self.workers_finished
            ):
                self.core.release_all()
                await asyncio.sleep(0.001)
            self.core.release_all()
            await asyncio.gather(*self.writes, return_exceptions=True)

        await asyncio.wait_for(drain(), timeout=2)

    def submit(
        self,
        test_id: str,
        attempt_id: str | None = None,
        started: threading.Event | None = None,
    ) -> asyncio.Task[None]:
        message: dict[str, Any] = {"type": "initialized", "test_id": test_id}
        if attempt_id is not None:
            message.update(type="failure", attempt_id=attempt_id)
        finished = threading.Event()
        self.workers_finished.append(finished)

        def write() -> None:
            try:
                if started is not None:
                    started.set()
                self.writer.write(message)
            finally:
                finished.set()

        task = asyncio.create_task(asyncio.to_thread(write))
        self.writes.append(task)
        return task

    async def entered(self, test_id: str) -> None:
        message = await asyncio.wait_for(self.core.entered.get(), timeout=2)
        self.assertEqual(message["test_id"], test_id)

    async def test_independent_attempt_does_not_wait_for_native_ack(self) -> None:
        first = self.submit("first", "a")
        await self.entered("first")
        second = self.submit("second", "b")
        await self.entered("second")
        self.assertFalse(first.done())
        self.assertFalse(second.done())
        self.core.pending["second"].set_result(None)
        await asyncio.wait_for(second, timeout=2)
        self.assertFalse(first.done())
        self.core.pending["first"].set_result(None)
        await asyncio.wait_for(first, timeout=2)
        self.assertEqual(len(self.writer._attempt_locks), 0)

    async def test_shutdown_input_progresses_while_writes_await_native_outcomes(
        self,
    ) -> None:
        runner = PythonFunctionRunner(self.writer)
        first = self.submit("first", "a")
        await self.entered("first")
        second = self.submit("second", "b")
        await self.entered("second")
        self.core.push({"type": "shutdown"})
        await asyncio.wait_for(runner.serve(self.core), timeout=2)  # type: ignore[arg-type]
        self.assertFalse(first.done())
        self.assertFalse(second.done())
        # The input loop does not fabricate write acknowledgments on shutdown;
        # actual native outcomes still release the blocked writer threads.
        for pending in self.core.pending.values():
            pending.set_exception(RuntimeError("native agent stopped"))
        for write in (first, second):
            with self.assertRaisesRegex(RuntimeError, "native agent stopped"):
                await asyncio.wait_for(write, timeout=2)
        self.assertEqual(len(self.writer._attempt_locks), 0)

    async def test_same_attempt_waits_through_native_error_and_reclaims_lock(
        self,
    ) -> None:
        first = self.submit("first", "a")
        await self.entered("first")
        second = self.submit("second", "a")
        independent = self.submit("independent", "b")
        await self.entered("independent")
        self.assertNotIn("second", self.core.pending)
        error = RuntimeError("native durable write failed")
        self.core.pending["first"].set_exception(error)
        with self.assertRaises(RuntimeError) as raised:
            await asyncio.wait_for(first, timeout=2)
        self.assertIs(raised.exception, error)
        await self.entered("second")
        self.core.release_all()
        await asyncio.wait_for(asyncio.gather(second, independent), timeout=2)
        # Keep the native exception alive: its traceback must not pin the key.
        self.assertEqual(len(self.writer._attempt_locks), 0)

    async def test_cancelled_waiter_does_not_release_same_attempt_order(self) -> None:
        first = self.submit("first", "a")
        await self.entered("first")
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        second = self.submit("second", "a")
        independent = self.submit("independent", "b")
        await self.entered("independent")
        self.assertNotIn("second", self.core.pending)
        self.assertFalse(self.core.pending["first"].cancelled())
        self.core.pending["first"].set_result(None)
        await self.entered("second")
        self.core.release_all()
        await asyncio.wait_for(asyncio.gather(second, independent), timeout=2)
        self.assertEqual(len(self.writer._attempt_locks), 0)

    async def test_lifecycle_lock_is_separate_and_serialized(self) -> None:
        first = self.submit("initialized-1")
        await self.entered("initialized-1")
        second = self.submit("initialized-2")
        attempt = self.submit("attempt", "initialized")
        await self.entered("attempt")
        self.assertNotIn("initialized-2", self.core.pending)
        self.core.pending["initialized-1"].set_result(None)
        await self.entered("initialized-2")
        self.core.release_all()
        await asyncio.wait_for(asyncio.gather(first, second, attempt), timeout=2)

    async def test_cancelled_queued_writer_stays_ordered_until_its_native_ack(
        self,
    ) -> None:
        first = self.submit("first", "a")
        await self.entered("first")
        started = threading.Event()
        queued = self.submit("queued", "a", started=started)
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        queued.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await queued
        self.assertNotIn("queued", self.core.pending)
        self.core.pending["first"].set_result(None)
        await self.entered("queued")
        third = self.submit("third", "a")
        independent = self.submit("independent", "b")
        await self.entered("independent")
        self.assertNotIn("third", self.core.pending)
        self.assertEqual(len(self.writer._attempt_locks), 2)
        self.core.pending["queued"].set_result(None)
        await self.entered("third")
        self.core.release_all()
        await asyncio.wait_for(asyncio.gather(first, third, independent), timeout=2)
        self.assertEqual(len(self.writer._attempt_locks), 0)

    async def test_sequential_attempt_history_does_not_retain_ordering_keys(
        self,
    ) -> None:
        for index in range(128):
            key = str(index)
            write = self.submit(key, key)
            await self.entered(key)
            self.assertEqual(len(self.writer._attempt_locks), 1)
            self.core.pending[key].set_result(None)
            await asyncio.wait_for(write, timeout=2)
            self.assertEqual(len(self.writer._attempt_locks), 0)

    async def test_invalid_identity_is_forwarded_to_native_validation(self) -> None:
        for invalid in ([], {}, 42, None):
            write = asyncio.create_task(
                asyncio.to_thread(
                    self.writer.write,
                    {"type": "failure", "attempt_id": invalid, "test_id": "invalid"},
                )
            )
            self.writes.append(write)
            await self.entered("invalid")
            self.core.pending["invalid"].set_exception(ValueError("invalid identity"))
            with self.assertRaisesRegex(ValueError, "invalid identity"):
                await asyncio.wait_for(write, timeout=2)
        self.assertEqual(len(self.writer._attempt_locks), 0)


class PythonFunctionRunnerTest(unittest.IsolatedAsyncioTestCase):
    async def test_writer_propagates_real_native_errors_without_retaining_attempt(
        self,
    ) -> None:
        core = FunctionAgentCore(
            "http://127.0.0.1:9",
            "test-token",
            registration_attempts=1,
            registration_retry_ms=1,
            request_timeout_ms=50,
        )
        protocol = ProtocolWriter(core, asyncio.get_running_loop())
        # These are rejected by the actual PyO3/core JSON boundary, not a fake
        # extension. Both errors must cross the synchronous bridge unchanged.
        for message_type in ("failure", "success"):
            with self.assertRaisesRegex(RuntimeError, "missing field"):
                await asyncio.wait_for(
                    asyncio.to_thread(
                        protocol.write,
                        {"type": message_type, "attempt_id": "same-attempt"},
                    ),
                    timeout=2,
                )
            self.assertEqual(len(protocol._attempt_locks), 0)
        await asyncio.sleep(0.1)

    async def test_native_core_can_start_inside_the_python_event_loop(self) -> None:
        core = FunctionAgentCore(
            "http://127.0.0.1:9",
            "test-token",
            registration_attempts=1,
            registration_retry_ms=1,
            request_timeout_ms=50,
        )
        self.assertTrue(callable(core.next_input))
        self.assertTrue(callable(core.submit_output))
        await asyncio.sleep(0.1)

    async def test_protocol_writer_invokes_native_core_on_event_loop(self) -> None:
        core = LoopBoundNativeCore()
        protocol = ProtocolWriter(core, asyncio.get_running_loop())  # type: ignore[arg-type]

        await asyncio.wait_for(
            asyncio.to_thread(protocol.write, {"type": "initialized"}), timeout=2
        )

        self.assertEqual(await core.outputs.get(), {"type": "initialized"})

    async def test_runner_stops_cleanly_on_shutdown(self) -> None:
        core = FakeNativeCore()
        protocol = ProtocolWriter(core, asyncio.get_running_loop())  # type: ignore[arg-type]
        runner = PythonFunctionRunner(protocol)
        core.push({"type": "shutdown"})

        await asyncio.wait_for(runner.serve(core), timeout=2)  # type: ignore[arg-type]

    async def test_resolved_environment_is_set_before_application_import(self) -> None:
        function_name = "embedded_agent_import_secret_test"
        module_name = "embedded_agent_import_secret_test_module"
        target = "TL_TEST_IMPORT_SECRET"
        canary = "credential-canary-value"
        self.addCleanup(os.environ.pop, target, None)
        code = self._code_zip(
            f"""\
import os
from tensorlake.applications import function

IMPORTED_VALUE = os.environ.get("{target}")

@function()
def {function_name}() -> str:
    return IMPORTED_VALUE
""",
            function_name,
            module_name,
        )
        core = FakeNativeCore()
        protocol = ProtocolWriter(core, asyncio.get_running_loop())  # type: ignore[arg-type]
        runner = PythonFunctionRunner(protocol)
        serve = asyncio.create_task(runner.serve(core))  # type: ignore[arg-type]
        self.addAsyncCleanup(self._stop, serve)
        core.push(
            {
                "type": "assignment",
                "assignment": {
                    "attempt_id": "attempt-secret",
                    "fence_token": 3,
                    "function_run_id": "run-secret",
                    "request_id": "request-secret",
                    "namespace": "default",
                    "application": "secret-test",
                    "application_version": "v1",
                    "function": function_name,
                    "timeout_ms": 5_000,
                    "initialization_timeout_ms": 5_000,
                    "inputs": [
                        {
                            "data_base64": "",
                            "metadata_base64": "",
                            "content_type": "application/octet-stream",
                        }
                    ],
                    "request_headers": [],
                    "call_metadata_base64": "",
                    "application_code_base64": base64.b64encode(code).decode("ascii"),
                    "application_code_sha256": hashlib.sha256(code).hexdigest(),
                    "resolved_environment": [{"target": target, "value": canary}],
                },
            }
        )

        initialized = await self._output(core)
        result = await self._output(core)
        self.assertEqual(initialized, {"type": "initialized"})
        self.assertEqual(result["type"], "success")
        self.assertEqual(
            pickle.loads(
                base64.b64decode(result["result"]["output_base64"], validate=True)
            ),
            canary,
        )
        self.assertNotIn(canary, json.dumps([initialized, result]))

    async def test_application_state_round_trip_and_value_result(self) -> None:
        function_name = "embedded_agent_stateful_test"
        module_name = "embedded_agent_stateful_test_module"
        code = self._code_zip(
            f"""\
from tensorlake.applications import RequestContext, application, function

@application()
@function()
def {function_name}(value: dict) -> dict:
    context = RequestContext.get()
    context.state.set("saved", value)
    return context.state.get("saved")
""",
            function_name,
            module_name,
        )
        serializer = serializer_by_name(APPLICATION_FUNCTION_CALL_SERIALIZER_NAME)
        expected = {"value": 42}
        core = FakeNativeCore()
        protocol = ProtocolWriter(core, asyncio.get_running_loop())  # type: ignore[arg-type]
        runner = PythonFunctionRunner(protocol)
        serve = asyncio.create_task(runner.serve(core))  # type: ignore[arg-type]
        self.addAsyncCleanup(self._stop, serve)

        core.push(
            {
                "type": "assignment",
                "assignment": {
                    "attempt_id": "attempt-state",
                    "fence_token": 7,
                    "function_run_id": "run-state",
                    "request_id": "request-state",
                    "namespace": "default",
                    "application": "stateful-test",
                    "application_version": "v1",
                    "function": function_name,
                    "timeout_ms": 5_000,
                    "initialization_timeout_ms": 5_000,
                    "inputs": [
                        {
                            "data_base64": base64.b64encode(
                                serializer.serialize(expected, dict)
                            ).decode("ascii"),
                            "metadata_base64": "",
                            "content_type": serializer.content_type,
                        }
                    ],
                    "request_headers": [],
                    "call_metadata_base64": "",
                    "application_code_base64": base64.b64encode(code).decode("ascii"),
                    "application_code_sha256": hashlib.sha256(code).hexdigest(),
                },
            }
        )

        self.assertEqual(await self._output(core), {"type": "initialized"})
        state_set = await self._output(core)
        self.assertEqual(state_set["type"], "request_state")
        self.assertEqual(state_set["operation"]["operation"], "set")
        serialized_state = base64.b64decode(
            state_set["operation"]["value_base64"], validate=True
        )
        self.assertEqual(
            REQUEST_STATE_USER_DATA_SERIALIZER.deserialize(
                serialized_state, type_hint=dict
            ),
            expected,
        )
        core.push(
            {
                "type": "request_state_result",
                "result": {
                    "operation_id": state_set["operation_id"],
                    "attempt_id": "attempt-state",
                    "fence_token": 7,
                    "result": "set",
                },
            }
        )

        state_get = await self._output(core)
        self.assertEqual(state_get["type"], "request_state")
        self.assertEqual(state_get["operation"]["operation"], "get")
        core.push(
            {
                "type": "request_state_result",
                "result": {
                    "operation_id": state_get["operation_id"],
                    "attempt_id": "attempt-state",
                    "fence_token": 7,
                    "result": "get",
                    "value_base64": base64.b64encode(serialized_state).decode("ascii"),
                },
            }
        )

        result = await self._output(core)
        self.assertEqual(result["type"], "success")
        self.assertEqual(
            serializer.deserialize(
                base64.b64decode(result["result"]["output_base64"], validate=True),
                dict,
            ),
            expected,
        )

    async def test_async_application_awaits_child_function_result(self) -> None:
        function_name = "embedded_agent_async_parent"
        child_name = "embedded_agent_async_child"
        module_name = "embedded_agent_async_test_module"
        code = self._code_zip(
            f"""\
from tensorlake.applications import RequestError, application, function

@function()
async def {child_name}(value: int) -> int:
    return value * 2

@application()
@function()
async def {function_name}() -> int:
    try:
        await {child_name}(5)
    except RequestError:
        return 10
    raise AssertionError("expected child RequestError")
""",
            function_name,
            module_name,
        )
        serializer = serializer_by_name(APPLICATION_FUNCTION_CALL_SERIALIZER_NAME)
        core = FakeNativeCore()
        protocol = ProtocolWriter(core, asyncio.get_running_loop())  # type: ignore[arg-type]
        runner = PythonFunctionRunner(protocol)
        serve = asyncio.create_task(runner.serve(core))  # type: ignore[arg-type]
        self.addAsyncCleanup(self._stop, serve)
        core.push(
            {
                "type": "assignment",
                "assignment": {
                    "attempt_id": "attempt-async",
                    "fence_token": 11,
                    "function_run_id": "run-async",
                    "request_id": "request-async",
                    "namespace": "default",
                    "application": "async-test",
                    "application_version": "v1",
                    "function": function_name,
                    "timeout_ms": 5_000,
                    "initialization_timeout_ms": 5_000,
                    # Empty base64 fields are omitted by the Rust protocol's
                    # serde defaults for a no-argument application request.
                    "inputs": [{}],
                    "request_headers": [],
                    "call_metadata_base64": "",
                    "application_code_base64": base64.b64encode(code).decode("ascii"),
                    "application_code_sha256": hashlib.sha256(code).hexdigest(),
                },
            }
        )

        self.assertEqual(await self._output(core), {"type": "initialized"})
        call_batch = await self._output(core)
        self.assertEqual(call_batch["type"], "call_batch")
        self.assertEqual(len(call_batch["calls"]), 1)
        self.assertEqual(call_batch["calls"][0]["function_name"], child_name)
        watch = await self._output(core)
        self.assertEqual(watch["type"], "watch")
        self.assertEqual(
            await self._output(core),
            {
                "type": "suspend",
                "attempt_id": "attempt-async",
            },
        )

        core.push(
            {
                "type": "call_result",
                "attempt_id": "attempt-async",
                "function_call_id": call_batch["calls"][0]["function_call_id"],
                "outcome": "failure",
                "reason": "request_error",
            }
        )

        self.assertEqual(
            await self._output(core),
            {
                "type": "resume",
                "attempt_id": "attempt-async",
            },
        )
        result = await self._output(core)
        self.assertEqual(result["type"], "success")
        self.assertEqual(
            serializer.deserialize(
                base64.b64decode(result["result"]["output_base64"], validate=True),
                int,
            ),
            10,
        )

    @staticmethod
    async def _output(core: FakeNativeCore) -> dict[str, Any]:
        return await asyncio.wait_for(core.outputs.get(), timeout=2)

    @staticmethod
    async def _stop(task: asyncio.Task[None]) -> None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @staticmethod
    def _code_zip(source: str, function_name: str, module_name: str) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(
                ".tensorlake_code_manifest.json",
                json.dumps(
                    {
                        "functions": {
                            function_name: {
                                "name": function_name,
                                "module_import_name": module_name,
                            }
                        }
                    }
                ),
            )
            archive.writestr(f"{module_name}.py", source)
        return buffer.getvalue()
