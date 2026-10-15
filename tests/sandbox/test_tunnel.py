"""Binding regressions against real sockets; transport stress tests live in Rust."""

import asyncio
import concurrent.futures
import inspect
import socket
import unittest
from unittest.mock import patch

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from tensorlake.sandbox import AsyncTcpTunnel, TcpTunnel, TunnelAddress
from tensorlake.sandbox.exceptions import SandboxError
from tensorlake.sandbox.tunnel import build_tunnel_ws_url


class TunnelTestsMixin:
    tunnel_class = TcpTunnel

    async def start(self, base_url="http://127.0.0.1:1", **kwargs):
        kwargs = {
            "base_url": base_url,
            "ws_headers": {},
            "remote_port": 9000,
            "local_port": 0,
            **kwargs,
        }
        tunnel = self.tunnel_class.listen(**kwargs)
        if inspect.isawaitable(tunnel):
            tunnel = await tunnel
        self.addAsyncCleanup(self.close_tunnel, tunnel)
        return tunnel

    async def close_tunnel(self, tunnel):
        result = tunnel.close()
        if inspect.isawaitable(result):
            await result

    async def server(self, handler):
        server = await serve(handler, "127.0.0.1", 0, close_timeout=0.1)

        async def close():
            server.close()
            await server.wait_closed()

        self.addAsyncCleanup(close)
        return f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"

    async def connect(self, tunnel):
        reader, writer = await asyncio.open_connection(
            tunnel.local_host, tunnel.local_port
        )
        self.addCleanup(writer.close)
        return reader, writer

    async def test_routing_headers_binary_data_and_remote_close(self):
        headers = {}

        async def peer(ws):
            headers.update(ws.request.headers)
            self.assertEqual(ws.request.path, "/api/v1/tunnels/tcp?port=9000")
            self.assertEqual(await ws.recv(), b"hello")
            await ws.send(bytes([0, 255, 128, 1]))

        url = await self.server(peer)
        tunnel = await self.start(
            url,
            ws_headers={
                "Host": "sbx.local",
                "Authorization": "Bearer test-key",
                "x-tensorlake-sandbox-id": "sbx-test",
                "x-tensorlake-route-hint": "route-test",
            },
        )
        reader, writer = await self.connect(tunnel)
        writer.write(b"hello")
        self.assertEqual(
            await asyncio.wait_for(reader.read(), 3), bytes([0, 255, 128, 1])
        )
        self.assertEqual(headers["host"], "sbx.local")
        self.assertEqual(headers["authorization"], "Bearer test-key")
        self.assertEqual(headers["x-tensorlake-sandbox-id"], "sbx-test")
        self.assertEqual(headers["x-tensorlake-route-hint"], "route-test")
        self.assertEqual(tunnel.remote_port, 9000)
        self.assertEqual(
            tunnel.address(), TunnelAddress("127.0.0.1", tunnel.local_port)
        )

    async def test_concurrent_immediate_writes_and_large_payloads(self):
        async def echo(ws):
            try:
                async for data in ws:
                    await ws.send(data)
            except ConnectionClosed:
                pass

        tunnel = await self.start(await self.server(echo))

        def exercise():
            def one(n):
                data = bytes([n]) * (256 * 1024 if n % 10 == 0 else 128)
                with socket.create_connection(
                    (tunnel.local_host, tunnel.local_port), timeout=5
                ) as client:
                    client.sendall(data)
                    received = bytearray()
                    while len(received) < len(data):
                        chunk = client.recv(len(data) - len(received))
                        self.assertTrue(chunk, f"premature EOF on connection {n}")
                        received.extend(chunk)
                    self.assertEqual(received, data)

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as clients:
                list(clients.map(one, range(100)))

        await asyncio.wait_for(asyncio.to_thread(exercise), 30)

    async def test_close_stops_listener_and_active_connections(self):
        connected = asyncio.Event()

        async def peer(ws):
            connected.set()
            await ws.wait_closed()

        tunnel = await self.start(await self.server(peer))
        reader, _ = await self.connect(tunnel)
        await asyncio.wait_for(connected.wait(), 3)
        await asyncio.wait_for(self.close_tunnel(tunnel), 1)
        self.assertTrue(tunnel.closed)
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
        with self.assertRaises(OSError):
            await asyncio.open_connection(tunnel.local_host, tunnel.local_port)
        await self.close_tunnel(tunnel)

    async def test_close_during_handshake(self):
        connected = asyncio.Event()

        async def stalled(reader, writer):
            connected.set()
            await reader.read()
            writer.close()

        server = await asyncio.start_server(stalled, "127.0.0.1", 0)
        self.addCleanup(server.close)
        tunnel = await self.start(
            f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        )
        reader, _ = await self.connect(tunnel)
        await asyncio.wait_for(connected.wait(), 3)
        await asyncio.wait_for(self.close_tunnel(tunnel), 1)
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")

    async def test_connect_failure_closes_only_that_connection(self):
        tunnel = await self.start()
        reader, _ = await self.connect(tunnel)
        self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
        self.assertFalse(tunnel.closed)

    async def test_bind_failure_is_sandbox_error(self):
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen()
            with self.assertRaisesRegex(SandboxError, "failed to bind"):
                await self.start(local_port=occupied.getsockname()[1])

    async def test_rejects_invalid_arguments(self):
        for args in [{"remote_port": p} for p in [0, -1, 65536, True, "80"]] + [
            {"local_port": -1},
            {"connect_timeout": 0},
            {"connect_timeout": -1},
            {"connect_timeout": float("nan")},
            {"connect_timeout": "invalid"},
        ]:
            with self.subTest(args=args), self.assertRaises(SandboxError):
                await self.start(**args)

    async def test_local_port_defaults_to_remote_port(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        tunnel = await self.start(remote_port=port, local_port=None)
        self.assertEqual(tunnel.local_port, port)

    async def test_context_manager(self):
        tunnel = await self.start()
        if isinstance(tunnel, AsyncTcpTunnel):
            async with tunnel as entered:
                self.assertIs(entered, tunnel)
        else:
            with tunnel as entered:
                self.assertIs(entered, tunnel)
        self.assertTrue(tunnel.closed)

    async def test_sandbox_create_tunnel_preserves_host_routing(self):
        import tensorlake.sandbox.async_sandbox as async_module
        import tensorlake.sandbox.sandbox as sync_module
        from tensorlake.sandbox import AsyncSandbox, Sandbox

        is_async = self.tunnel_class is AsyncTcpTunnel
        module, cls = (
            (async_module, AsyncSandbox) if is_async else (sync_module, Sandbox)
        )

        class FakeProxy:
            def __init__(self, **kwargs):
                pass

            def close(self):
                pass

            def base_url(self):
                return url

        key = "RustCloudSandboxProxyClient"

        async def peer(ws):
            self.assertEqual(ws.request.headers["host"], "sbx-test.local")
            await ws.send(b"ready")
            await ws.wait_closed()

        url = await self.server(peer)
        with (
            patch.object(module, "_RUST_SANDBOX_PROXY_CLIENT_AVAILABLE", True),
            patch.object(module, key, FakeProxy),
        ):
            sandbox = cls(sandbox_id="sbx-test", proxy_url=url, api_key="test-key")
        tunnel = sandbox.create_tunnel(9000, local_port=0)
        if is_async:
            tunnel = await tunnel
        self.addAsyncCleanup(self.close_tunnel, tunnel)
        reader, _ = await self.connect(tunnel)
        self.assertEqual(await asyncio.wait_for(reader.readexactly(5), 3), b"ready")
        result = sandbox.close()
        if inspect.isawaitable(result):
            await result
        await self.close_tunnel(tunnel)
        self.assertTrue(tunnel.closed)


class TestTcpTunnel(TunnelTestsMixin, unittest.IsolatedAsyncioTestCase):
    def test_build_tunnel_ws_url(self):
        self.assertEqual(
            build_tunnel_ws_url("https://sandbox.tensorlake.ai", 8080),
            "wss://sandbox.tensorlake.ai/api/v1/tunnels/tcp?port=8080",
        )
        self.assertEqual(
            build_tunnel_ws_url("http://localhost:9443/", 5901),
            "ws://localhost:9443/api/v1/tunnels/tcp?port=5901",
        )


if __name__ == "__main__":
    unittest.main()
