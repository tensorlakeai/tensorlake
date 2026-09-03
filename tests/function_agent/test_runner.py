from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import io
import json
import os
import pickle
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


class PythonFunctionRunnerTest(unittest.IsolatedAsyncioTestCase):
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
