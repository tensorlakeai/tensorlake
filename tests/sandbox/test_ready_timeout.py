"""``PendingSandbox.ready`` honours its timeout through the real Rust binding.

A scripted HTTP server acknowledges the create at once, answers the first
readiness poll with ``pending`` and then stalls the second poll. ``ready``
must return within its budget plus a small margin, raising ``SandboxPending``
without waiting for the stalled answer and without issuing a DELETE.
"""

import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from tensorlake.sandbox import (
    PendingSandbox,
    RemoteAPIError,
    SandboxClient,
    SandboxError,
    SandboxPending,
)

_SECOND_POLL_DELAY_SEC = 2.0


class _ScriptedHandler(BaseHTTPRequestHandler):
    """POST → 202 pending record; GET #1 → pending (or 503 when the server's
    ``fail_first_poll`` is set); GET #2 → pending after a long delay. Every
    request is recorded on the server."""

    def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler API
        return None

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.server.requests.append(("POST", self.path, self.rfile.read(length)))
        self._send(
            202,
            {"sandbox_id": "sbx-1", "state": "pending", "pending_reason": "scheduling"},
        )

    def do_GET(self):
        self.server.requests.append(("GET", self.path, b""))
        polls = sum(1 for method, _, _ in self.server.requests if method == "GET")
        if polls == 1 and self.server.fail_first_poll:
            self._send(503, {"message": "busy"})
            return
        if polls > 1:
            # Emulate a slow answer: a client that waits for it overshoots.
            time.sleep(_SECOND_POLL_DELAY_SEC)
        self._send(
            200,
            {
                "id": "sbx-1",
                "namespace": "default",
                "status": "pending",
                "pending_reason": "no_resources_available",
                "resources": {"cpus": 1.0, "memory_mb": 1024, "disk_mb": 10240},
            },
        )

    def do_DELETE(self):
        self.server.requests.append(("DELETE", self.path, b""))
        self._send(200, {})


class TestReadyTimeout(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), _ScriptedHandler)
        self.server.requests = []
        self.server.fail_first_poll = False
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = SandboxClient.for_localhost(
            api_url=f"http://127.0.0.1:{self.server.server_port}",
            request_timeout=30.0,
        )

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()

    def test_ready_returns_within_its_budget_despite_a_stalled_poll(self):
        pending = self.client.create(image="python:3.11", wait=False)
        self.assertIsInstance(pending, PendingSandbox)
        self.assertEqual(pending.sandbox_id, "sbx-1")
        self.assertIs(json.loads(self.server.requests[0][2])["wait"], False)

        budget = 0.05
        started = time.monotonic()
        with self.assertRaises(SandboxPending) as caught:
            pending.ready(timeout=budget, poll_interval=0.01)
        elapsed = time.monotonic() - started

        self.assertLess(
            elapsed, budget + 0.3, f"ready overshot its budget: {elapsed:.3f}s"
        )
        self.assertEqual(caught.exception.sandbox_id, "sbx-1")
        self.assertEqual(caught.exception.pending_reason, "no_resources_available")
        self.assertEqual(caught.exception.timeout, budget)
        # A stalled poll is abandoned, never a DELETE.
        self.assertNotIn("DELETE", [method for method, _, _ in self.server.requests])

    def test_ready_stops_at_the_deadline_after_a_failed_first_poll(self):
        # A 503 on the first poll, then a stalled poll: the retry backoff
        # consumes the budget and no further poll may be granted the
        # first-poll floor. The last error surfaces within the budget.
        self.server.fail_first_poll = True
        pending = self.client.create(image="python:3.11", wait=False)

        budget = 0.05
        started = time.monotonic()
        with self.assertRaises(SandboxError) as caught:
            pending.ready(timeout=budget, poll_interval=0.01)
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.3, f"ready overshot its budget: {elapsed:.3f}s")
        self.assertIsInstance(caught.exception, RemoteAPIError)
        self.assertEqual(caught.exception.status_code, 503)
        self.assertNotIn("DELETE", [method for method, _, _ in self.server.requests])


if __name__ == "__main__":
    unittest.main()
