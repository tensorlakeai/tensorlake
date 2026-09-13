"""Exercise readiness deadlines and connection reuse through the native binding."""

import asyncio
import json
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from tensorlake.sandbox import AsyncSandbox, Sandbox, SandboxError


class ReadinessServer(ThreadingHTTPServer):
    def __init__(self) -> None:
        self.connections = 0
        self.metadata_reads = 0
        self.pending_reads = 3
        self.requests: list[tuple[str, str | None]] = []
        super().__init__(("127.0.0.1", 0), ReadinessHandler)

    def get_request(self) -> tuple[socket.socket, Any]:
        connection, address = super().get_request()
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.connections += 1
        return connection, address


class ReadinessHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: ReadinessServer

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        self.server.requests.append(
            (self.path, self.headers.get("X-Tensorlake-Request-Timeout-Ms"))
        )
        if self.path == "/api/v1/health":
            value = {"healthy": True}
        else:
            self.server.metadata_reads += 1
            value = {
                "id": "transport-test",
                "namespace": "default",
                "status": (
                    "pending"
                    if self.server.metadata_reads <= self.server.pending_reads
                    else "running"
                ),
                "resources": {"cpus": 1, "memory_mb": 1024, "disk_mb": 1024},
                "created_at": 0,
                "sandbox_url": f"http://127.0.0.1:{self.server.server_port}",
            }
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class TestConnectTransport(unittest.TestCase):
    def setUp(self) -> None:
        self.server = ReadinessServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.api_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())

    def assert_reused_transport(self) -> None:
        self.assertEqual(self.server.connections, 1)
        self.assertEqual(self.server.metadata_reads, 5)
        deadlines = [
            int(timeout)
            for path, timeout in self.server.requests
            if path != "/api/v1/health" and timeout is not None
        ]
        self.assertEqual(len(deadlines), 5)
        self.assertGreater(deadlines[0], deadlines[-1])
        self.assertTrue(all(0 < timeout <= 2000 for timeout in deadlines))
        # A readiness deadline must not leak into subsequent proxy operations.
        self.assertEqual(self.server.requests[-1], ("/api/v1/health", "2000"))

    def test_sync_connect_reuses_transport_with_independent_deadlines(self) -> None:
        sandbox = Sandbox.connect(
            "transport-test", api_url=self.api_url, api_key=None, request_timeout=2
        )
        try:
            self.assertTrue(sandbox.health().healthy)
            self.assert_reused_transport()
        finally:
            sandbox.close()

    def test_async_connect_reuses_transport_with_independent_deadlines(self) -> None:
        async def connect() -> None:
            sandbox = await AsyncSandbox.connect(
                "transport-test", api_url=self.api_url, api_key=None, request_timeout=2
            )
            try:
                self.assertTrue((await sandbox.health()).healthy)
                self.assert_reused_transport()
            finally:
                sandbox.close()

        asyncio.run(connect())

    def test_pending_connect_expires_without_probing_or_rebuilding_transport(
        self,
    ) -> None:
        self.server.pending_reads = 100
        with self.assertRaisesRegex(SandboxError, "within 0.3s"):
            Sandbox.connect(
                "transport-test",
                api_url=self.api_url,
                api_key=None,
                request_timeout=0.3,
            )
        self.assertEqual(self.server.connections, 1)
        self.assertGreaterEqual(self.server.metadata_reads, 2)
        self.assertTrue(
            all(path != "/api/v1/health" for path, _ in self.server.requests)
        )
