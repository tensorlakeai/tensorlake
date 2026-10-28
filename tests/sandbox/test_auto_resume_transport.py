"""Exercise the public SDK through the native binding against a local contract server.

This verifies SDK transport and error behavior, not real server/proxy lifecycle behavior.
"""

import asyncio
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tensorlake.sandbox import AsyncSandbox, Sandbox
from tensorlake.sandbox.exceptions import RemoteAPIError


class TestAutoResumeTransport(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.flag = True
        self.status = "running"
        self.legacy = False
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle_request(self):
                length = int(self.headers.get("content-length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                test.requests.append((self.command, self.path, body))
                code = 200
                payload = {}
                if self.path.endswith("/resume"):
                    test.status = "running"
                elif self.path.endswith("/suspend"):
                    test.status = "suspended"
                elif self.path.startswith("/api/v1/files"):
                    code = 409
                    payload = {
                        "error": "Automatic resume disabled",
                        "code": "SANDBOX_AUTO_RESUME_DISABLED",
                    }
                elif self.command == "POST":
                    test.flag = body.get("auto_resume", True)
                    payload = {"sandbox_id": "sbx-1", "status": "running"}
                else:
                    if self.command == "PATCH":
                        test.flag = body.get("auto_resume", test.flag)
                    payload = {
                        "id": "sbx-1",
                        "namespace": "default",
                        "name": "parked-agent",
                        "status": test.status,
                        "resources": {"cpus": 1, "memory_mb": 512, "disk_mb": 1024},
                        "auto_resume": test.flag,
                        "sandbox_url": test.url,
                    }
                if test.legacy:
                    payload.pop("auto_resume", None)
                encoded = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            do_GET = handle_request
            do_POST = handle_request
            do_PATCH = handle_request

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def test_sync_documented_park_resume_release_flow(self):
        sandbox = Sandbox.create(
            name="parked-agent",
            auto_resume=False,
            api_url=self.url,
            proxy_url=self.url,
            api_key="test",
        )
        try:
            self.assertFalse(sandbox.info().auto_resume)
            sandbox.suspend()
            self.assertEqual(sandbox.info().status, "suspended")
            before = len(self.requests)
            with self.assertRaises(RemoteAPIError) as caught:
                sandbox.read_file("/test")
            self.assertEqual(caught.exception.code, "SANDBOX_AUTO_RESUME_DISABLED")
            self.assertEqual(caught.exception.status_code, 409)
            self.assertEqual(len(self.requests), before + 1)
            sandbox.resume()
            self.assertFalse(sandbox.info().auto_resume)
            sandbox.suspend()
            sandbox.update(auto_resume=True)
            self.assertTrue(sandbox.info().auto_resume)
            self.assertEqual(self.status, "suspended")
            self.legacy = True
            self.assertTrue(sandbox.info().auto_resume)
            self.assertEqual(
                sum(path.endswith("/resume") for _, path, _ in self.requests), 1
            )
        finally:
            sandbox.close()

    def test_async_documented_park_resume_release_flow(self):
        async def run():
            sandbox = await AsyncSandbox.create(
                name="parked-agent",
                auto_resume=False,
                api_url=self.url,
                proxy_url=self.url,
                api_key="test",
            )
            try:
                self.assertFalse((await sandbox.info()).auto_resume)
                await sandbox.suspend()
                before = len(self.requests)
                with self.assertRaises(RemoteAPIError) as caught:
                    await sandbox.read_file("/test")
                self.assertEqual(caught.exception.code, "SANDBOX_AUTO_RESUME_DISABLED")
                self.assertEqual(caught.exception.status_code, 409)
                self.assertEqual(len(self.requests), before + 1)
                await sandbox.resume()
                self.assertFalse((await sandbox.info()).auto_resume)
                await sandbox.suspend()
                await sandbox.update(auto_resume=True)
                self.assertTrue((await sandbox.info()).auto_resume)
                self.assertEqual(self.status, "suspended")
                self.assertEqual(
                    sum(path.endswith("/resume") for _, path, _ in self.requests), 1
                )
            finally:
                sandbox.close()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
