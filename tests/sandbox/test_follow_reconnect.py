"""Reconnect of ``follow_*`` streams after a mid-stream connection drop.

Runs the real native stream against a local SSE server. The server sends a
few events, then closes the socket without ending the chunked body, which
the SDK sees as a dropped stream. The sandbox daemon replays a process's
whole output on every follow request, so the server does the same on the
next request. The consumer must receive each line exactly once, in order.
"""

import asyncio
import json
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tensorlake.sandbox import AsyncSandbox, Sandbox

_PATH = "/api/v1/processes/101/output/follow"


def _event(index: int) -> bytes:
    payload = {
        "line": f"line {index}",
        "timestamp": "2026-10-21T00:00:00Z",
        "stream": "stdout",
    }
    return f"data: {json.dumps(payload)}\n\n".encode()


class _DroppingHandler(BaseHTTPRequestHandler):
    """Serves ``server.script``: one ``(event_count, drop)`` step per request.

    A ``drop`` step writes the events and then closes the socket with no
    terminating chunk. A clean step ends the body, as the daemon does when
    the process exits. The last step repeats for any further request.
    """

    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler API
        return None

    def _chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()

    def do_GET(self):
        if self.path != _PATH:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        step = min(len(self.server.requests), len(self.server.script) - 1)
        self.server.requests.append(self.path)
        event_count, drop = self.server.script[step]

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        for index in range(1, event_count + 1):
            self._chunk(_event(index))
        self.close_connection = True
        if drop:
            self.connection.shutdown(socket.SHUT_RDWR)
        else:
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()


class _ServerMixin:
    def start_server(self, script):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _DroppingHandler)
        self.server.script = script
        self.server.requests = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return f"http://127.0.0.1:{self.server.server_port}"

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()


class TestFollowReconnectSync(_ServerMixin, unittest.TestCase):
    def tearDown(self):
        self.stop_server()

    def test_resumes_after_drop_without_repeating_events(self):
        url = self.start_server([(3, True), (5, False)])
        sandbox = Sandbox(sandbox_id="sbx-test", proxy_url=url, api_key="k")
        try:
            lines = [event.line for event in sandbox.follow_output(101)]
        finally:
            sandbox.close()
        self.assertEqual(lines, [f"line {i}" for i in range(1, 6)])
        self.assertEqual(len(self.server.requests), 2)

    def test_resumes_after_drop_before_first_event(self):
        url = self.start_server([(0, True), (2, False)])
        sandbox = Sandbox(sandbox_id="sbx-test", proxy_url=url, api_key="k")
        try:
            lines = [event.line for event in sandbox.follow_output(101)]
        finally:
            sandbox.close()
        self.assertEqual(lines, ["line 1", "line 2"])
        self.assertEqual(len(self.server.requests), 2)

    def test_resumes_across_two_drops(self):
        url = self.start_server([(2, True), (3, True), (4, False)])
        sandbox = Sandbox(sandbox_id="sbx-test", proxy_url=url, api_key="k")
        try:
            lines = [event.line for event in sandbox.follow_output(101)]
        finally:
            sandbox.close()
        self.assertEqual(lines, [f"line {i}" for i in range(1, 5)])
        self.assertEqual(len(self.server.requests), 3)


class TestFollowReconnectAsync(_ServerMixin, unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await asyncio.to_thread(self.stop_server)

    async def test_resumes_after_drop_without_repeating_events(self):
        url = self.start_server([(3, True), (5, False)])
        sandbox = AsyncSandbox(sandbox_id="sbx-test", proxy_url=url, api_key="k")
        try:
            lines = [event.line async for event in sandbox.follow_output(101)]
        finally:
            sandbox.close()
        self.assertEqual(lines, [f"line {i}" for i in range(1, 6)])
        self.assertEqual(len(self.server.requests), 2)

    async def test_collect_resumes_after_drop(self):
        url = self.start_server([(3, True), (5, False)])
        sandbox = AsyncSandbox(sandbox_id="sbx-test", proxy_url=url, api_key="k")
        try:
            events = await sandbox.follow_output(101)
        finally:
            sandbox.close()
        self.assertEqual([e.line for e in events], [f"line {i}" for i in range(1, 6)])
        self.assertIsInstance(events.trace_id, str)
        self.assertEqual(len(self.server.requests), 2)


if __name__ == "__main__":
    unittest.main()
