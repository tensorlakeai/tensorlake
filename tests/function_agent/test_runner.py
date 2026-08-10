from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import io
import json
import unittest
import zipfile
from typing import Any

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
