import asyncio
import types
import unittest
from unittest.mock import AsyncMock, patch

import tensorlake.sandbox.async_sandbox as async_sandbox_module
import tensorlake.sandbox.tunnel as tunnel_module
from tensorlake.sandbox import AsyncSandbox, AsyncTcpTunnel, TunnelAddress
from tensorlake.sandbox.exceptions import SandboxError


class _FakeAsyncRustProxyClient:
    def __init__(self, *args, **kwargs):
        pass

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"


class _FakeAsyncWebSocketConnection:
    """Stands in for ``websockets.asyncio.client.ClientConnection``."""

    def __init__(self):
        self.sent = []
        self.frames: asyncio.Queue = asyncio.Queue()
        self.closed = False

    async def send(self, data):
        self.sent.append(bytes(data))

    async def close(self, code=1000, reason=""):
        if not self.closed:
            self.closed = True
            await self.frames.put(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.frames.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item
        return item


class _StuckCloseWebSocket(_FakeAsyncWebSocketConnection):
    """``close()`` blocks in drain() because the proxy stopped reading."""

    def __init__(self):
        super().__init__()
        self.aborted = False
        self.transport = types.SimpleNamespace(abort=self._abort)

    def _abort(self):
        self.aborted = True
        # A real abort tears down the connection, which ends the
        # ws->local read loop.
        self.frames.put_nowait(None)

    async def close(self, code=1000, reason=""):
        await asyncio.Event().wait()


class _TailOnCloseWebSocket(_FakeAsyncWebSocketConnection):
    """The proxy sends its last frames together with its close frame."""

    TAIL = [bytes([65 + i]) * (1 << 20) for i in range(8)]

    async def close(self, code=1000, reason=""):
        if not self.closed:
            self.closed = True
            for chunk in self.TAIL:
                self.frames.put_nowait(chunk)
            self.frames.put_nowait(None)


def _make_async_sandbox(proxy_url="http://localhost:9443", sandbox_id="sbx-1"):
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
            sandbox_id=sandbox_id,
            proxy_url=proxy_url,
            api_key="secret",
        )
    sandbox._rust_client = fake
    sandbox._base_url = proxy_url
    return sandbox


async def _wait_until(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return predicate()


class TestAsyncTcpTunnel(unittest.IsolatedAsyncioTestCase):
    async def connect_client(self, tunnel):
        reader, writer = await asyncio.open_connection(
            tunnel.local_host, tunnel.local_port
        )

        async def cleanup():
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        self.addAsyncCleanup(cleanup)
        return reader, writer

    async def test_binds_local_port_and_relays_bytes(self):
        sandbox = _make_async_sandbox(proxy_url="https://sandbox.tensorlake.ai")
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            tunnel = await sandbox.create_tunnel(8080, local_port=0)
            self.addAsyncCleanup(tunnel.close)

            self.assertIsInstance(tunnel, AsyncTcpTunnel)
            self.assertEqual(tunnel.remote_port, 8080)
            self.assertNotEqual(tunnel.local_port, 0)
            self.assertEqual(
                tunnel.address(),
                TunnelAddress(host="127.0.0.1", port=tunnel.local_port),
            )

            reader, writer = await self.connect_client(tunnel)
            self.assertTrue(await _wait_until(lambda: fake_module.connect.called))
            args, kwargs = fake_module.connect.call_args
            self.assertEqual(
                args[0], "wss://sandbox.tensorlake.ai/api/v1/tunnels/tcp?port=8080"
            )
            self.assertIn(
                ("Authorization", "Bearer secret"), kwargs["additional_headers"]
            )
            self.assertEqual(kwargs["open_timeout"], 10.0)

            writer.write(b"hello")
            await writer.drain()
            self.assertTrue(await _wait_until(lambda: fake_ws.sent))
            self.assertEqual(fake_ws.sent[0], b"hello")

            await fake_ws.frames.put(b"world")
            self.assertEqual(await asyncio.wait_for(reader.read(1024), 2), b"world")

            # Closing the local client closes the websocket.
            writer.close()
            await writer.wait_closed()
            self.assertTrue(await _wait_until(lambda: fake_ws.closed))

            await tunnel.close()
            self.assertTrue(tunnel.closed)

    async def test_uses_sandbox_host_override_for_localhost_proxy(self):
        sandbox = _make_async_sandbox(
            proxy_url="http://localhost:9443", sandbox_id="sbx-local"
        )
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            tunnel = await sandbox.create_tunnel(5901, local_port=0)
            self.addAsyncCleanup(tunnel.close)
            await self.connect_client(tunnel)
            self.assertTrue(await _wait_until(lambda: fake_module.connect.called))
            args, kwargs = fake_module.connect.call_args
            # The Host override is embedded in the URI; the TCP target is pinned
            # back to the proxy host and port via explicit kwargs.
            self.assertEqual(
                args[0], "ws://sbx-local.local/api/v1/tunnels/tcp?port=5901"
            )
            self.assertEqual(kwargs["host"], "localhost")
            self.assertEqual(kwargs["port"], 9443)
            header_names = [name for name, _ in kwargs["additional_headers"]]
            self.assertNotIn("Host", header_names)

    async def test_sandbox_close_closes_local_connection(self):
        sandbox = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            tunnel = await sandbox.create_tunnel(8080, local_port=0)
            self.addAsyncCleanup(tunnel.close)
            reader, _writer = await self.connect_client(tunnel)
            self.assertTrue(await _wait_until(lambda: fake_module.connect.called))

            await fake_ws.close()
            self.assertEqual(await asyncio.wait_for(reader.read(1024), 2), b"")

    async def test_websocket_connect_failure_closes_local_connection(self):
        sandbox = _make_async_sandbox()
        fake_module = types.SimpleNamespace(
            connect=AsyncMock(side_effect=ConnectionRefusedError("nope"))
        )

        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            tunnel = await sandbox.create_tunnel(8080, local_port=0)
            self.addAsyncCleanup(tunnel.close)
            reader, _writer = await self.connect_client(tunnel)
            self.assertEqual(await asyncio.wait_for(reader.read(1024), 2), b"")

    async def test_close_stops_listener_and_active_connections(self):
        sandbox = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            async with await sandbox.create_tunnel(8080, local_port=0) as tunnel:
                reader, _writer = await self.connect_client(tunnel)
                self.assertTrue(await _wait_until(lambda: fake_module.connect.called))
                address = (tunnel.local_host, tunnel.local_port)

            self.assertTrue(tunnel.closed)
            self.assertEqual(await asyncio.wait_for(reader.read(1024), 2), b"")
            self.assertTrue(fake_ws.closed)
            with self.assertRaises(OSError):
                await asyncio.wait_for(asyncio.open_connection(*address), 1)

            # Closing twice is a no-op.
            await tunnel.close()

    async def test_close_does_not_hang_when_local_client_stops_reading(self):
        sandbox = _make_async_sandbox()
        fake_ws = _FakeAsyncWebSocketConnection()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            tunnel = await sandbox.create_tunnel(8080, local_port=0)
            self.addAsyncCleanup(tunnel.close)
            _reader, _writer = await self.connect_client(tunnel)
            self.assertTrue(await _wait_until(lambda: fake_module.connect.called))

            # The sandbox sends far more than the local socket buffers hold,
            # and the local client never reads. The relay blocks in drain().
            for _ in range(16):
                await fake_ws.frames.put(b"x" * (1 << 20))
            await asyncio.sleep(0.2)

            await asyncio.wait_for(tunnel.close(), 3)
            self.assertTrue(tunnel.closed)

    async def test_close_does_not_hang_when_proxy_stops_reading(self):
        sandbox = _make_async_sandbox()
        fake_ws = _StuckCloseWebSocket()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with (
            patch.object(tunnel_module, "_async_ws_client", fake_module),
            patch.object(tunnel_module, "TUNNEL_CLOSE_TIMEOUT", 0.2),
        ):
            tunnel = await sandbox.create_tunnel(8080, local_port=0)
            self.addAsyncCleanup(tunnel.close)
            reader, writer = await self.connect_client(tunnel)
            self.assertTrue(await _wait_until(lambda: fake_module.connect.called))

            writer.write(b"upload")
            await writer.drain()
            self.assertTrue(await _wait_until(lambda: fake_ws.sent))

            await asyncio.wait_for(tunnel.close(), 3)
            self.assertTrue(tunnel.closed)
            self.assertTrue(fake_ws.aborted)
            self.assertEqual(await asyncio.wait_for(reader.read(1024), 2), b"")

    async def test_local_eof_does_not_hang_when_proxy_stops_reading(self):
        sandbox = _make_async_sandbox()
        fake_ws = _StuckCloseWebSocket()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with (
            patch.object(tunnel_module, "_async_ws_client", fake_module),
            patch.object(tunnel_module, "TUNNEL_CLOSE_TIMEOUT", 0.2),
        ):
            tunnel = await sandbox.create_tunnel(8080, local_port=0)
            self.addAsyncCleanup(tunnel.close)
            _reader, writer = await self.connect_client(tunnel)
            self.assertTrue(await _wait_until(lambda: fake_module.connect.called))

            # Local client finishes its upload. The relay must not stay stuck
            # in ws.close() forever.
            writer.close()
            await writer.wait_closed()
            self.assertTrue(await _wait_until(lambda: fake_ws.aborted, timeout=3))
            self.assertTrue(await _wait_until(lambda: not tunnel._relays, timeout=3))

    async def test_local_half_close_delivers_frames_sent_before_close(self):
        sandbox = _make_async_sandbox()
        fake_ws = _TailOnCloseWebSocket()
        fake_module = types.SimpleNamespace(connect=AsyncMock(return_value=fake_ws))

        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            tunnel = await sandbox.create_tunnel(8080, local_port=0)
            self.addAsyncCleanup(tunnel.close)
            reader, writer = await self.connect_client(tunnel)
            self.assertTrue(await _wait_until(lambda: fake_module.connect.called))

            # The client sends its request and half-closes, then waits for
            # the response. Frames that arrive before the proxy's close
            # frame must still reach the client.
            writer.write(b"request")
            await writer.drain()
            writer.write_eof()
            self.assertEqual(
                await asyncio.wait_for(reader.read(), 3), b"".join(fake_ws.TAIL)
            )

    async def test_rejects_invalid_arguments(self):
        sandbox = _make_async_sandbox()
        fake_module = types.SimpleNamespace(connect=AsyncMock())
        with patch.object(tunnel_module, "_async_ws_client", fake_module):
            with self.assertRaises(SandboxError):
                await sandbox.create_tunnel(0)
            with self.assertRaises(SandboxError):
                await sandbox.create_tunnel(8080, local_port=70000)
            with self.assertRaises(SandboxError):
                await sandbox.create_tunnel(8080, local_port=0, connect_timeout=-1)
            with self.assertRaises(SandboxError):
                await sandbox.create_tunnel(8080, local_port=0, connect_timeout=0)

    async def test_requires_websockets(self):
        sandbox = _make_async_sandbox()
        with patch.object(tunnel_module, "_async_ws_client", None):
            with self.assertRaises(SandboxError) as ctx:
                await sandbox.create_tunnel(8080, local_port=0)
        self.assertIn("websockets is required", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
