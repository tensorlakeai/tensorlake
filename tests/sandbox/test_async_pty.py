import asyncio
import json
import types
import unittest
from unittest.mock import AsyncMock, patch

from tensorlake.sandbox import AsyncPty, AsyncSandbox


class _FakeAsyncRustProxyClient:
    def __init__(self, *args, **kwargs):
        self.start_payload_json = None

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"

    async def create_pty_session_json_async(self, payload_json):
        self.start_payload_json = payload_json
        return ("trace-async", json.dumps({"session_id": "sess-1", "token": "tok-1"}))


class _FakeAsyncWebSocketConnection:
    def __init__(self):
        self.sent = []
        self._frames: asyncio.Queue = asyncio.Queue()
        self.close_code = 1000
        self.close_reason = ""
        self._closed = False

    async def send(self, data):
        self.sent.append(bytes(data))

    async def close(self, code=1000, reason="client disconnect"):
        self.close_code = code
        self.close_reason = reason
        self._closed = True
        await self._frames.put(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._frames.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item
        return item


def _make_async_sandbox():
    import tensorlake.sandbox.async_sandbox as async_sandbox_module

    fake = _FakeAsyncRustProxyClient()
    with (
        patch.object(
            async_sandbox_module, "_RUST_SANDBOX_PROXY_CLIENT_AVAILABLE", True
        ),
        patch.object(
            async_sandbox_module,
            "RustCloudSandboxProxyClient",
            side_effect=lambda **kwargs: fake,
        ),
    ):
        sandbox = AsyncSandbox(
            sandbox_id="sbx-1",
            proxy_url="http://localhost:9443",
            api_key="secret",
        )
    sandbox._rust_client = fake
    sandbox._base_url = fake.base_url()
    return sandbox, fake


class TestAsyncPty(unittest.IsolatedAsyncioTestCase):
    async def test_create_pty_returns_connected_handle(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, fake_rust = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        seen_data = []
        seen_exit = []

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(
                command="/bin/bash",
                on_data=seen_data.append,
                on_exit=seen_exit.append,
            )

            self.assertIsInstance(pty, AsyncPty)
            self.assertIn("token=tok-1", pty._ws_url)
            self.assertEqual(pty._ws_headers["X-PTY-Token"], "tok-1")
            # Give the reader task a chance to run before we assert sends.
            await asyncio.sleep(0)
            self.assertEqual(fake_ws.sent[0], b"\x02")

            await pty.send_input("pwd\n")
            await pty.resize(120, 40)
            self.assertEqual(fake_ws.sent[1], b"\x00pwd\n")
            self.assertEqual(fake_ws.sent[2], b"\x01\x00x\x00(")

            await fake_ws._frames.put(b"\x00hi")
            await fake_ws._frames.put(b"\x03\x00\x00\x00\x07")

            self.assertEqual(await pty.wait(timeout=1), 7)
            self.assertEqual(seen_data, [b"hi"])
            self.assertEqual(seen_exit, [7])

        payload = json.loads(fake_rust.start_payload_json)
        self.assertEqual(payload["command"], "/bin/bash")

    async def test_disconnect_and_reconnect(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        first = _FakeAsyncWebSocketConnection()
        second = _FakeAsyncWebSocketConnection()
        sockets = [first, second]
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(side_effect=lambda *a, **k: sockets.pop(0)),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")
            await pty.disconnect()
            for _ in range(100):
                if not pty.connected:
                    break
                await asyncio.sleep(0)
            await pty.connect()

            second.close_reason = "exit:0"
            await second._frames.put(None)

            self.assertEqual(await pty.wait(timeout=1), 0)
            self.assertEqual(first.sent[0], b"\x02")
            self.assertEqual(second.sent[0], b"\x02")

    async def test_kill_pty_uses_http_api(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")

            fake_response = types.SimpleNamespace(
                is_success=True, status_code=204, text=""
            )
            fake_client = AsyncMock()
            fake_client.delete = AsyncMock(return_value=fake_response)
            fake_client.__aenter__ = AsyncMock(return_value=fake_client)
            fake_client.__aexit__ = AsyncMock(return_value=None)

            with patch.object(
                pty_module.httpx,
                "AsyncClient",
                return_value=fake_client,
            ):
                await pty.kill()

                fake_client.delete.assert_called_once()
                url_arg = fake_client.delete.call_args.args[0]
                self.assertIn("/api/v1/pty/sess-1", url_arg)

    async def test_create_pty_cleans_up_session_when_attach_fails(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(side_effect=OSError("mock websocket connect failure")),
        )

        with (
            patch.object(pty_module, "_async_ws_client", fake_module),
            patch.object(
                sandbox, "_delete_pty_session", new_callable=AsyncMock
            ) as delete_mock,
        ):
            with self.assertRaisesRegex(Exception, "mock websocket connect failure"):
                await sandbox.create_pty(command="/bin/bash")

            delete_mock.assert_called_once_with("sess-1", timeout=10.0)


if __name__ == "__main__":
    unittest.main()
