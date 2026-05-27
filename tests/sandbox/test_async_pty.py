import asyncio
import json
import types
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from tensorlake.sandbox import AsyncPty, AsyncSandbox
from tensorlake.sandbox.exceptions import (
    RemoteAPIError,
    SandboxConnectionError,
    SandboxError,
)
from tensorlake.sandbox.pty import _prepare_async_ws_connect


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

    async def test_wait_timeout_raises(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")

            with self.assertRaises(TimeoutError):
                await pty.wait(timeout=0.05)

    async def test_reader_loop_session_terminated(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")
            fake_ws.close_reason = "session terminated"
            await fake_ws._frames.put(None)

            with self.assertRaisesRegex(SandboxError, "PTY session terminated"):
                await pty.wait(timeout=1)

    async def test_reader_loop_close_error_wraps_as_connection_error(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")
            await fake_ws._frames.put(RuntimeError("ws read boom"))

            with self.assertRaisesRegex(
                SandboxConnectionError,
                r"PTY websocket closed unexpectedly:.*ws read boom",
            ):
                await pty.wait(timeout=1)

    async def test_reader_loop_fallback_unexpected_close(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")
            # No exit frame, no close_error, no recognized close_reason — falls
            # through to the generic SandboxError branch.
            await fake_ws._frames.put(None)

            with self.assertRaisesRegex(
                SandboxError,
                r"PTY websocket closed unexpectedly:",
            ) as caught:
                await pty.wait(timeout=1)

            # Must be the bare SandboxError fallback, not the connection-error
            # or terminated branches.
            self.assertNotIsInstance(caught.exception, SandboxConnectionError)
            self.assertNotIn("session terminated", str(caught.exception))

    async def test_kill_raises_remote_api_error_on_non_success(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")

            fake_response = types.SimpleNamespace(
                is_success=False, status_code=500, text="boom"
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
                with self.assertRaises(RemoteAPIError) as caught:
                    await pty.kill()

            self.assertEqual(caught.exception.status_code, 500)

    async def test_kill_wraps_httpx_error_as_connection_error(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")

            fake_client = AsyncMock()
            fake_client.delete = AsyncMock(
                side_effect=httpx.ConnectError("no route to host")
            )
            fake_client.__aenter__ = AsyncMock(return_value=fake_client)
            fake_client.__aexit__ = AsyncMock(return_value=None)

            with patch.object(
                pty_module.httpx,
                "AsyncClient",
                return_value=fake_client,
            ):
                with self.assertRaisesRegex(
                    SandboxConnectionError,
                    r"Failed to kill PTY session:.*no route to host",
                ):
                    await pty.kill()

    async def test_send_input_before_connect_raises(self):
        pty = AsyncPty(
            session_id="s",
            token="t",
            ws_url="wss://x/ws",
            ws_headers={},
            http_url="https://x",
            http_headers={},
        )
        with self.assertRaisesRegex(SandboxError, "PTY is not connected"):
            await pty.send_input("hello")

    async def test_resize_before_connect_raises(self):
        pty = AsyncPty(
            session_id="s",
            token="t",
            ws_url="wss://x/ws",
            ws_headers={},
            http_url="https://x",
            http_headers={},
        )
        with self.assertRaisesRegex(SandboxError, "PTY is not connected"):
            await pty.resize(120, 40)

    async def test_connect_after_exit_raises(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")
            await fake_ws._frames.put(b"\x03\x00\x00\x00\x00")
            self.assertEqual(await pty.wait(timeout=1), 0)

            with self.assertRaisesRegex(
                SandboxError, "PTY session has already exited"
            ):
                await pty.connect()

    async def test_connect_without_websockets_library_raises_install_hint(self):
        import tensorlake.sandbox.pty as pty_module

        pty = AsyncPty(
            session_id="s",
            token="t",
            ws_url="wss://x/ws",
            ws_headers={},
            http_url="https://x",
            http_headers={},
        )
        with patch.object(pty_module, "_async_ws_client", None):
            with self.assertRaisesRegex(
                SandboxError, "websockets is required for AsyncPty"
            ):
                await pty.connect()

    async def test_on_exit_handler_added_after_exit_runs_immediately(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(return_value=fake_ws),
        )

        with patch.object(pty_module, "_async_ws_client", fake_module):
            pty = await sandbox.create_pty(command="/bin/bash")
            await fake_ws._frames.put(b"\x03\x00\x00\x00\x2a")
            self.assertEqual(await pty.wait(timeout=1), 42)

            late_calls = []
            pty.on_exit(late_calls.append)
            self.assertEqual(late_calls, [42])

    async def test_create_pty_session_payload_includes_all_fields(self):
        sandbox, fake_rust = _make_async_sandbox()

        await sandbox.create_pty_session(
            command="/bin/zsh",
            args=["-l", "-c", "echo hi"],
            env={"FOO": "bar", "PATH": "/usr/bin"},
            working_dir="/workspace",
            rows=50,
            cols=132,
        )

        payload = json.loads(fake_rust.start_payload_json)
        self.assertEqual(payload["command"], "/bin/zsh")
        self.assertEqual(payload["args"], ["-l", "-c", "echo hi"])
        self.assertEqual(payload["env"], {"FOO": "bar", "PATH": "/usr/bin"})
        self.assertEqual(payload["working_dir"], "/workspace")
        self.assertEqual(payload["rows"], 50)
        self.assertEqual(payload["cols"], 132)

    async def test_create_pty_session_payload_omits_optional_fields(self):
        sandbox, fake_rust = _make_async_sandbox()

        await sandbox.create_pty_session(command="/bin/sh")

        payload = json.loads(fake_rust.start_payload_json)
        self.assertEqual(
            set(payload.keys()), {"command", "rows", "cols"}
        )


class TestPtyWsUrl(unittest.TestCase):
    """Scheme-rewrite for AsyncSandbox.pty_ws_url."""

    def _sandbox_with_base(self, base_url: str) -> AsyncSandbox:
        sandbox, _ = _make_async_sandbox()
        sandbox._base_url = base_url
        return sandbox

    def test_https_becomes_wss(self):
        sandbox = self._sandbox_with_base("https://sandbox.example.com:9443")
        self.assertEqual(
            sandbox.pty_ws_url("sess-1", "tok"),
            "wss://sandbox.example.com:9443/api/v1/pty/sess-1/ws",
        )

    def test_http_becomes_ws(self):
        sandbox = self._sandbox_with_base("http://localhost:9443")
        self.assertEqual(
            sandbox.pty_ws_url("sess-1", "tok"),
            "ws://localhost:9443/api/v1/pty/sess-1/ws",
        )

    def test_unknown_scheme_passes_through(self):
        sandbox = self._sandbox_with_base("unix:///var/run/sandbox.sock")
        self.assertEqual(
            sandbox.pty_ws_url("sess-1", "tok"),
            "unix:///var/run/sandbox.sock/api/v1/pty/sess-1/ws",
        )

    def test_trailing_slash_in_base_url_is_stripped(self):
        sandbox = self._sandbox_with_base("https://sandbox.example.com/")
        self.assertEqual(
            sandbox.pty_ws_url("sess-1", "tok"),
            "wss://sandbox.example.com/api/v1/pty/sess-1/ws",
        )


class TestPrepareAsyncWsConnect(unittest.TestCase):
    """Pure-function tests for the Host-header / netloc workaround."""

    def test_no_host_header_is_passthrough(self):
        ws_url, headers, kwargs = _prepare_async_ws_connect(
            "wss://sandbox.example.com:9443/api/v1/pty/sess-1/ws",
            {"X-PTY-Token": "tok"},
        )
        self.assertEqual(
            ws_url, "wss://sandbox.example.com:9443/api/v1/pty/sess-1/ws"
        )
        self.assertEqual(headers, [("X-PTY-Token", "tok")])
        self.assertEqual(kwargs, {})

    def test_host_header_rewrites_netloc_and_pins_host_port(self):
        ws_url, headers, kwargs = _prepare_async_ws_connect(
            "wss://10.0.0.5:9443/api/v1/pty/sess-1/ws",
            {"Host": "sandbox.example.com", "X-PTY-Token": "tok"},
        )
        self.assertEqual(
            ws_url, "wss://sandbox.example.com/api/v1/pty/sess-1/ws"
        )
        # Host header must be stripped — websockets builds it from the URI.
        self.assertEqual(headers, [("X-PTY-Token", "tok")])
        # TCP target stays pinned to the original host & port.
        self.assertEqual(kwargs, {"host": "10.0.0.5", "port": 9443})

    def test_host_header_without_explicit_port_omits_port_kwarg(self):
        ws_url, headers, kwargs = _prepare_async_ws_connect(
            "wss://10.0.0.5/api/v1/pty/sess-1/ws",
            {"Host": "sandbox.example.com"},
        )
        self.assertEqual(
            ws_url, "wss://sandbox.example.com/api/v1/pty/sess-1/ws"
        )
        self.assertEqual(headers, [])
        self.assertEqual(kwargs, {"host": "10.0.0.5"})


if __name__ == "__main__":
    unittest.main()
