"""Exercise output limits through the real Python native binding over HTTP."""

import asyncio
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tensorlake.sandbox import AsyncSandbox, Sandbox
from tensorlake.sandbox.exceptions import SandboxConnectionError


class TestOutputLimit(unittest.TestCase):
    def setUp(self):
        self.requests = []
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["content-length"])))
                test.requests.append(body)
                command = body["command"]
                events = [
                    {"line": "abcdefghij", "stream": "stdout", "timestamp": 1},
                    {"line": "ERROR", "stream": "stderr", "timestamp": 2},
                    {"line": "é界🙂", "stream": "stdout", "timestamp": 3},
                ]
                if command == "empty":
                    events = []
                if command != "reset":
                    events.append({"signal": 9, "reason": "timed_out"})
                data = b"".join(
                    f"data: {json.dumps(event)}\n\n".encode() for event in events
                )
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    async def exercise(self, sandbox_type):
        sandbox = sandbox_type(sandbox_id="output-limit", proxy_url=self.url)

        async def run(*args, **kwargs):
            result = sandbox.run(*args, **kwargs)
            return await result if sandbox_type is AsyncSandbox else result

        try:
            for limit, stdout, stderr, truncated in [
                (None, "abcdefghij\né界🙂", "ERROR", None),
                (25, "abcdefghij\né界🙂", "ERROR", False),
                (15, "\né界🙂", "ERROR", True),
                (8, "界🙂", "", True),
                (0, "", "", True),
            ]:
                with self.subTest(limit=limit):
                    result = await run("mixed", max_output_bytes=limit, timeout=1)
                    self.assertEqual(result.stdout, stdout)
                    self.assertEqual(result.stderr, stderr)
                    self.assertEqual(result.output_truncated, truncated)
                    self.assertEqual(result.exit_code, -9)
                    self.assertTrue(result.timed_out)
                    self.assertTrue(result.trace_id)
                    self.assertEqual(self.requests[-1]["timeout"], 1)
                    self.assertNotIn("max_output_bytes", self.requests[-1])
            empty = await run("empty", max_output_bytes=0)
            self.assertFalse(empty.output_truncated)
            before = len(self.requests)
            for invalid in [-1, True, 1.5, "8", 2**32]:
                with self.subTest(invalid=invalid):
                    with self.assertRaisesRegex(ValueError, "max_output_bytes"):
                        await run("invalid", max_output_bytes=invalid)
            self.assertEqual(len(self.requests), before)
            with self.assertRaises(SandboxConnectionError):
                await run("reset", max_output_bytes=8)
            self.assertEqual(len(self.requests), before + 1)
        finally:
            sandbox.close()

    def test_sync_output_limit(self):
        asyncio.run(self.exercise(Sandbox))

    def test_async_output_limit(self):
        asyncio.run(self.exercise(AsyncSandbox))


if __name__ == "__main__":
    unittest.main()
