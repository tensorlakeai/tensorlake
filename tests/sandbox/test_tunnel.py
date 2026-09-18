import errno
import queue
import socket
import threading
import time
import unittest
from unittest.mock import patch

import tensorlake.sandbox.sandbox as sandbox_module
import tensorlake.sandbox.tunnel as tunnel_module
from tensorlake.sandbox import Sandbox, TcpTunnel, TunnelAddress
from tensorlake.sandbox.exceptions import SandboxError
from tensorlake.sandbox.tunnel import build_tunnel_ws_url

_WS_BINARY = 0x2
_WS_CLOSE = 0x8
_WS_PONG = 0xA


class _FakeRustProxyClient:
    def __init__(self, *args, **kwargs):
        pass

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"


class _FakeWebSocketConnection:
    """Stands in for ``websocket.WebSocket`` in the sync relay."""

    def __init__(self):
        self.sent = []
        self.frames = queue.Queue()
        self.close_sent = False
        self.shutdown_called = False
        self.pings = 0
        self.timeout = None
        self.send_error = None
        self.send_close_error = None
        self.answer_close = True

    def settimeout(self, timeout):
        self.timeout = timeout

    def send_binary(self, data):
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(bytes(data))

    def send_close(self, status=1000, reason=b""):
        if self.send_close_error is not None:
            raise self.send_close_error
        self.close_sent = True
        if self.answer_close:
            # Server answers the close handshake.
            self.frames.put((_WS_CLOSE, b""))

    def ping(self, payload=b""):
        self.pings += 1

    def recv_data(self, control_frame=False):
        item = self.frames.get(timeout=5)
        if item is None:
            raise ConnectionError("websocket closed")
        if isinstance(item, Exception):
            raise item
        return item

    def shutdown(self):
        self.shutdown_called = True
        self.frames.put(None)


class _FlakyListener:
    """Wraps a listening socket so ``accept`` raises scripted errors first."""

    def __init__(self, listener, errors):
        self._listener = listener
        self._errors = list(errors)
        self.accept_calls = 0

    def accept(self):
        self.accept_calls += 1
        if self._errors:
            raise self._errors.pop(0)
        return self._listener.accept()

    def __getattr__(self, name):
        return getattr(self._listener, name)


class _FakeWebSocketModule:
    def __init__(self, ws=None, error=None):
        self.ws = ws
        self.error = error
        self.calls = []
        # Set by tests that need the connect to block until released.
        self.connecting = threading.Event()
        self.release = threading.Event()
        self.release.set()

    def create_connection(self, url, header=None, timeout=None, **kwargs):
        self.connecting.set()
        self.release.wait(timeout=5)
        self.calls.append(
            {
                "url": url,
                "header": list(header or []),
                "timeout": timeout,
                "host": kwargs.get("host"),
            }
        )
        if self.error is not None:
            raise self.error
        return self.ws


def _wait_until(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class TestTcpTunnel(unittest.TestCase):
    def make_sandbox(self, proxy_url="http://localhost:9443", sandbox_id="sbx-1"):
        fake = _FakeRustProxyClient()
        with (
            patch.object(sandbox_module, "_RUST_SANDBOX_PROXY_CLIENT_AVAILABLE", True),
            patch.object(
                sandbox_module,
                "RustCloudSandboxProxyClient",
                side_effect=lambda **kwargs: fake,
            ),
        ):
            sandbox = Sandbox(
                sandbox_id=sandbox_id,
                proxy_url=proxy_url,
                api_key="secret",
            )
        sandbox._rust_client = fake
        sandbox._base_url = proxy_url
        return sandbox

    def connect_client(self, tunnel):
        client = socket.create_connection((tunnel.local_host, tunnel.local_port))
        client.settimeout(2)
        self.addCleanup(client.close)
        return client

    def test_build_tunnel_ws_url(self):
        self.assertEqual(
            build_tunnel_ws_url("https://sandbox.tensorlake.ai", 8080),
            "wss://sandbox.tensorlake.ai/api/v1/tunnels/tcp?port=8080",
        )
        self.assertEqual(
            build_tunnel_ws_url("http://localhost:9443/", 5901),
            "ws://localhost:9443/api/v1/tunnels/tcp?port=5901",
        )

    def test_binds_local_port_and_relays_bytes(self):
        sandbox = self.make_sandbox(proxy_url="https://sandbox.tensorlake.ai")
        fake_ws = _FakeWebSocketConnection()
        fake_module = _FakeWebSocketModule(ws=fake_ws)

        with patch.object(tunnel_module, "websocket", fake_module):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)

            self.assertIsInstance(tunnel, TcpTunnel)
            self.assertEqual(tunnel.remote_port, 8080)
            self.assertEqual(tunnel.local_host, "127.0.0.1")
            self.assertNotEqual(tunnel.local_port, 0)
            self.assertEqual(
                tunnel.address(),
                TunnelAddress(host="127.0.0.1", port=tunnel.local_port),
            )

            client = self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_module.calls))
            call = fake_module.calls[0]
            self.assertEqual(
                call["url"], "wss://sandbox.tensorlake.ai/api/v1/tunnels/tcp?port=8080"
            )
            self.assertIn("Authorization: Bearer secret", call["header"])
            self.assertEqual(call["timeout"], 10.0)

            client.sendall(b"hello")
            self.assertTrue(_wait_until(lambda: fake_ws.sent))
            self.assertEqual(fake_ws.sent[0], b"hello")

            fake_ws.frames.put((_WS_BINARY, b"world"))
            self.assertEqual(client.recv(1024), b"world")

            # Closing the local client sends a close frame to the sandbox.
            client.close()
            self.assertTrue(_wait_until(lambda: fake_ws.close_sent))
            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))

            tunnel.close()
            self.assertTrue(tunnel.closed)

    def test_uses_sandbox_host_override_for_localhost_proxy(self):
        sandbox = self.make_sandbox(
            proxy_url="http://localhost:9443", sandbox_id="sbx-local"
        )
        fake_ws = _FakeWebSocketConnection()
        fake_module = _FakeWebSocketModule(ws=fake_ws)

        with patch.object(tunnel_module, "websocket", fake_module):
            tunnel = sandbox.create_tunnel(5901, local_port=0)
            self.addCleanup(tunnel.close)
            self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_module.calls))
            call = fake_module.calls[0]
            self.assertEqual(
                call["url"], "ws://localhost:9443/api/v1/tunnels/tcp?port=5901"
            )
            # websocket-client writes its own Host line and appends ``header``
            # verbatim, so the override must go through the ``host`` option.
            self.assertEqual(call["host"], "sbx-local.local")
            self.assertFalse(
                [line for line in call["header"] if line.startswith("Host:")]
            )

    def test_local_port_defaults_to_remote_port(self):
        sandbox = self.make_sandbox()
        port = _free_port()
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule()):
            tunnel = sandbox.create_tunnel(port)
            self.addCleanup(tunnel.close)
            self.assertEqual(tunnel.local_port, port)
            self.assertEqual(tunnel.remote_port, port)

    def test_sandbox_close_closes_local_connection(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_ws.timeout is not None))

            fake_ws.frames.put((_WS_CLOSE, b""))
            self.assertEqual(client.recv(1024), b"")
            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))

    def test_local_disconnect_without_close_reply_closes_relay(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        # The proxy never answers the close frame. Without a deadline the
        # reader thread would ping forever and the relay would leak.
        fake_ws.answer_close = False
        with (
            patch.object(tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)),
            patch.object(tunnel_module, "TUNNEL_CLOSE_TIMEOUT", 0.2),
        ):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_ws.timeout is not None))
            self.assertEqual(len(tunnel._relays), 1)

            client.close()
            self.assertTrue(_wait_until(lambda: fake_ws.close_sent))
            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))
            self.assertTrue(_wait_until(lambda: not tunnel._relays))
            self.assertTrue(
                _wait_until(
                    lambda: not any(
                        t.name.startswith("tensorlake-tunnel-relay-")
                        for t in threading.enumerate()
                    )
                )
            )

    def test_close_frame_send_failure_closes_relay(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        fake_ws.send_close_error = TimeoutError("send timed out")
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_ws.timeout is not None))

            client.close()
            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))
            self.assertTrue(_wait_until(lambda: not tunnel._relays))

    def test_websocket_send_failure_closes_relay(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        # The websocket stays readable: recv_data keeps blocking on the
        # frame queue. Only sends fail, as on a send timeout.
        fake_ws.send_error = TimeoutError("send timed out")
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_ws.timeout is not None))

            client.sendall(b"hello")
            # Both directions stop: the websocket is shut down and the
            # local client sees the connection end.
            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))
            self.assertEqual(client.recv(1024), b"")
            self.assertEqual(fake_ws.sent, [])
            self.assertTrue(_wait_until(lambda: not tunnel._relays))

    def test_close_during_websocket_connect_shuts_down_websocket(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        # The peer never answers a close frame, so a leaked relay would
        # keep its reader thread alive.
        fake_ws.answer_close = False
        fake_module = _FakeWebSocketModule(ws=fake_ws)
        fake_module.release.clear()
        with patch.object(tunnel_module, "websocket", fake_module):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertTrue(fake_module.connecting.wait(timeout=2))

            # Close the tunnel while the websocket connect is pending, then
            # let the connect succeed.
            tunnel.close()
            self.assertEqual(client.recv(1024), b"")
            fake_module.release.set()

            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))
            # The relay never started pumping on the late websocket.
            self.assertFalse(fake_ws.close_sent)
            self.assertEqual(tunnel._relays, set())

    def test_websocket_connect_failure_closes_local_connection(self):
        sandbox = self.make_sandbox()
        fake_module = _FakeWebSocketModule(error=ConnectionRefusedError("nope"))
        with patch.object(tunnel_module, "websocket", fake_module):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertEqual(client.recv(1024), b"")
            self.assertEqual(len(fake_module.calls), 1)

    def test_close_stops_listener_and_active_connections(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)):
            with sandbox.create_tunnel(8080, local_port=0) as tunnel:
                client = self.connect_client(tunnel)
                self.assertTrue(_wait_until(lambda: fake_ws.timeout is not None))
                address = (tunnel.local_host, tunnel.local_port)

            self.assertTrue(tunnel.closed)
            self.assertEqual(client.recv(1024), b"")
            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))
            with self.assertRaises(OSError):
                socket.create_connection(address, timeout=1)

            # Closing twice is a no-op.
            tunnel.close()

    def test_rejects_invalid_arguments(self):
        sandbox = self.make_sandbox()
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule()):
            with self.assertRaises(SandboxError):
                sandbox.create_tunnel(0)
            with self.assertRaises(SandboxError):
                sandbox.create_tunnel(70000)
            with self.assertRaises(SandboxError):
                sandbox.create_tunnel(8080, local_port=70000)
            with self.assertRaises(SandboxError):
                sandbox.create_tunnel(8080, local_port=0, connect_timeout=-1)
            # Zero makes the websocket connect non-blocking and fail at once.
            with self.assertRaises(SandboxError):
                sandbox.create_tunnel(8080, local_port=0, connect_timeout=0)

    def make_tunnel_with_listener_errors(self, fake_ws, errors):
        """Build a tunnel whose listener raises ``errors`` before accepting."""
        tunnel = TcpTunnel(
            base_url="http://localhost:9443",
            ws_headers={},
            remote_port=8080,
            local_port=0,
        )
        self.addCleanup(tunnel.close)
        listener = _FlakyListener(tunnel._listener, errors)
        tunnel._listener = listener
        patcher = patch.object(
            tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        tunnel._start()
        return tunnel, listener

    def test_accept_loop_survives_transient_accept_errors(self):
        fake_ws = _FakeWebSocketConnection()
        with patch.object(tunnel_module, "TUNNEL_ACCEPT_RETRY_DELAY", 0.01):
            tunnel, listener = self.make_tunnel_with_listener_errors(
                fake_ws,
                [
                    ConnectionAbortedError(errno.ECONNABORTED, "aborted"),
                    OSError(errno.EMFILE, "too many open files"),
                ],
            )
            client = self.connect_client(tunnel)
            client.sendall(b"still alive")
            self.assertTrue(_wait_until(lambda: fake_ws.sent == [b"still alive"]))
        self.assertGreaterEqual(listener.accept_calls, 3)
        self.assertFalse(tunnel.closed)

    def test_accept_loop_closes_tunnel_on_listener_failure(self):
        fake_ws = _FakeWebSocketConnection()
        tunnel, _ = self.make_tunnel_with_listener_errors(
            fake_ws, [OSError(errno.EIO, "listener broke")]
        )
        # The tunnel reports closed and the port is released.
        self.assertTrue(_wait_until(lambda: tunnel.closed))
        address = (tunnel.local_host, tunnel.local_port)
        with self.assertRaises(OSError):
            socket.create_connection(address, timeout=1)

    def test_unanswered_ping_closes_relay(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_ws.timeout is not None))

            # First read timeout: the relay pings and keeps waiting.
            fake_ws.frames.put(socket.timeout("read timed out"))
            self.assertTrue(_wait_until(lambda: fake_ws.pings == 1))
            self.assertFalse(fake_ws.shutdown_called)

            # Second read timeout with no frame in between: the peer is dead.
            fake_ws.frames.put(socket.timeout("read timed out"))
            self.assertTrue(_wait_until(lambda: fake_ws.shutdown_called))
            self.assertEqual(client.recv(1024), b"")
            self.assertTrue(_wait_until(lambda: not tunnel._relays))

    def test_pong_keeps_idle_relay_alive(self):
        sandbox = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule(ws=fake_ws)):
            tunnel = sandbox.create_tunnel(8080, local_port=0)
            self.addCleanup(tunnel.close)
            client = self.connect_client(tunnel)
            self.assertTrue(_wait_until(lambda: fake_ws.timeout is not None))

            for expected_pings in (1, 2, 3):
                fake_ws.frames.put(socket.timeout("read timed out"))
                self.assertTrue(_wait_until(lambda: fake_ws.pings == expected_pings))
                # The peer answers, so the next timeout pings again.
                fake_ws.frames.put((_WS_PONG, b""))

            self.assertFalse(fake_ws.shutdown_called)
            fake_ws.frames.put((_WS_BINARY, b"data"))
            self.assertEqual(client.recv(1024), b"data")
            self.assertEqual(len(tunnel._relays), 1)

    def test_bind_failure_raises_sandbox_error(self):
        sandbox = self.make_sandbox()
        holder = socket.create_server(("127.0.0.1", 0))
        self.addCleanup(holder.close)
        taken = holder.getsockname()[1]
        with patch.object(tunnel_module, "websocket", _FakeWebSocketModule()):
            with self.assertRaises(SandboxError) as ctx:
                sandbox.create_tunnel(8080, local_port=taken)
        self.assertIn("failed to bind local tunnel listener", str(ctx.exception))

    def test_requires_websocket_client(self):
        sandbox = self.make_sandbox()
        with patch.object(tunnel_module, "websocket", None):
            with self.assertRaises(SandboxError) as ctx:
                sandbox.create_tunnel(8080, local_port=0)
        self.assertIn("websocket-client is required", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
