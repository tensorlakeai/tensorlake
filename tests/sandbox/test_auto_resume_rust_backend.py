"""Public sync/async auto-resume paths; transport and retry policy are tested in Rust."""

import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake.sandbox import AsyncSandbox, Sandbox
from tensorlake.sandbox.async_client import AsyncSandboxClient
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.exceptions import RemoteAPIError, SandboxError
from tensorlake.sandbox.models import SandboxInfo
from tests.sandbox.test_client_rust_backend import _FakeRustClient
from tests.sandbox.test_client_rust_backend_async import (
    _FakeAsyncRustClient as _AsyncFakeRustClient,
)


class TestAutoResume(unittest.TestCase):
    def test_create_and_pending_preserve_optional_boolean(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake
        for value in (None, False, True):
            for wait in (True, False):
                with self.subTest(value=value, wait=wait):
                    client.create(name="agent", auto_resume=value, wait=wait)
                    body = json.loads(fake.create_request_json)
                    self.assertEqual(body.get("auto_resume"), value)
                    self.assertEqual("auto_resume" in body, value is not None)
        client.close()

    def test_patch_and_legacy_response(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake
        for value in (False, True):
            client.update_sandbox("agent", auto_resume=value)
            self.assertEqual(
                json.loads(fake.update_request_json), {"auto_resume": value}
            )
        client.update_sandbox("agent", exposed_ports=[8080])
        self.assertNotIn("auto_resume", json.loads(fake.update_request_json))
        self.assertTrue(client.get("agent").auto_resume)
        payload = json.loads(fake.get_sandbox_json("agent")[1])
        payload.update(auto_resume=False, status="suspended")
        self.assertFalse(SandboxInfo.model_validate(payload).auto_resume)
        with self.assertRaisesRegex(SandboxError, "cannot be combined"):
            client.update_sandbox("agent", auto_resume=False, cpus=2)
        self.assertEqual(fake.resume_calls, [])
        client.close()

    def test_connected_create_and_handle_wrappers(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake
        sandbox = client.create_and_connect(name="agent", auto_resume=False)
        self.assertFalse(json.loads(fake.create_request_json)["auto_resume"])
        sandbox.update(auto_resume=False)
        self.assertEqual(json.loads(fake.update_request_json), {"auto_resume": False})
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            for wait in (True, False):
                Sandbox.create(name="agent", auto_resume=False, wait=wait)
                self.assertFalse(json.loads(fake.create_request_json)["auto_resume"])
        client.close()

    def test_get_or_create_forwards_only_when_creating(self):
        from tensorlake.sandbox.exceptions import SandboxNotFoundError

        created = MagicMock(spec=Sandbox)
        with (
            patch.object(Sandbox, "connect", side_effect=SandboxNotFoundError("agent")),
            patch.object(Sandbox, "create", return_value=created) as create,
        ):
            Sandbox.get_or_create("agent", auto_resume=False)
            self.assertIs(create.call_args.kwargs["auto_resume"], False)

    def test_proxy_code_is_available_without_changing_raw_message(self):
        body = '{"error":"Automatic resume disabled","code":"SANDBOX_AUTO_RESUME_DISABLED"}'
        error = RemoteAPIError(409, body)
        self.assertEqual(error.status_code, 409)
        self.assertEqual(error.code, "SANDBOX_AUTO_RESUME_DISABLED")
        self.assertEqual(error.message, body)
        for body in ("plain", "[]", '{"code":123}'):
            self.assertIsNone(RemoteAPIError(409, body).code)


class TestAsyncAutoResume(unittest.IsolatedAsyncioTestCase):
    async def test_create_and_pending_preserve_optional_boolean(self):
        client = AsyncSandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _AsyncFakeRustClient()
        client._rust_client = fake
        for value in (None, False, True):
            for wait in (True, False):
                with self.subTest(value=value, wait=wait):
                    await client.create(name="agent", auto_resume=value, wait=wait)
                    body = json.loads(fake.create_request_json)
                    self.assertEqual(body.get("auto_resume"), value)
                    self.assertEqual("auto_resume" in body, value is not None)
        await client.close()

    async def test_patch_and_legacy_response(self):
        client = AsyncSandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _AsyncFakeRustClient()
        client._rust_client = fake
        for value in (False, True):
            await client.update_sandbox("agent", auto_resume=value)
            self.assertEqual(
                json.loads(fake.update_request_json), {"auto_resume": value}
            )
        await client.update_sandbox("agent", exposed_ports=[8080])
        self.assertNotIn("auto_resume", json.loads(fake.update_request_json))
        self.assertTrue((await client.get("agent")).auto_resume)
        with self.assertRaisesRegex(SandboxError, "cannot be combined"):
            await client.update_sandbox("agent", auto_resume=False, cpus=2)
        self.assertEqual(fake.resume_calls, [])
        await client.close()

    async def test_connected_create_and_handle_wrappers(self):
        client = AsyncSandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _AsyncFakeRustClient()
        client._rust_client = fake
        sandbox = await client.create_and_connect(name="agent", auto_resume=False)
        self.assertFalse(json.loads(fake.create_request_json)["auto_resume"])
        await sandbox.update(auto_resume=False)
        self.assertEqual(json.loads(fake.update_request_json), {"auto_resume": False})
        with patch(
            "tensorlake.sandbox.async_client.AsyncSandboxClient", return_value=client
        ):
            for wait in (True, False):
                await AsyncSandbox.create(name="agent", auto_resume=False, wait=wait)
                self.assertFalse(json.loads(fake.create_request_json)["auto_resume"])
        await client.close()

    async def test_get_or_create_forwards_only_when_creating(self):
        from tensorlake.sandbox.exceptions import SandboxNotFoundError

        created = MagicMock(spec=AsyncSandbox)
        with (
            patch.object(
                AsyncSandbox, "connect", side_effect=SandboxNotFoundError("agent")
            ),
            patch.object(
                AsyncSandbox, "create", new_callable=AsyncMock, return_value=created
            ) as create,
        ):
            await AsyncSandbox.get_or_create("agent", auto_resume=False)
            self.assertIs(create.call_args.kwargs["auto_resume"], False)


if __name__ == "__main__":
    unittest.main()
