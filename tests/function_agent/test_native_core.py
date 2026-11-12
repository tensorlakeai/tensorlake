"""Smoke test requiring a full SDK build with the native Function Agent."""

import asyncio
import unittest

from tensorlake._cloud_sdk import FunctionAgentCore


class FunctionAgentCoreTest(unittest.IsolatedAsyncioTestCase):
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
