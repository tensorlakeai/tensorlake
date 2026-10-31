"""Recoverable resume contract through the checked-out native binding and HTTP.

The backend's lifecycle responses are empty 200/202 or diagnostic-only 409.
State comes from GET; diagnosis content must never determine retry safety.
"""

import asyncio
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from tensorlake.sandbox import (
    AsyncSandboxClient,
    RemoteAPIError,
    SandboxClient,
    SandboxError,
    SandboxPending,
    SandboxResumeError,
    SandboxStatus,
)


class TestResumeFailed(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.status = "resume_failed"
        self.diagnosis = {"message": "restore failed", "debug_data": {"attempt_id": 17}}
        self.reply = 409
        self.body = b"restore failed"  # Indexify's actual conflict shape
        self.next_status = "resume_failed"
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle_request(self):
                path = urlsplit(self.path).path
                test.requests.append((self.command, path))
                size = int(self.headers.get("content-length", 0))
                self.rfile.read(size)
                code, payload = 200, None
                if path.endswith("/resume"):
                    test.status = test.next_status
                    if test.status == "running":
                        test.diagnosis = None
                    code, payload = test.reply, test.body
                elif self.command == "POST":
                    payload = {"sandbox_id": "sbx-1", "state": "pending"}
                    code = 202
                else:
                    info = {
                        "id": "sbx-1",
                        "namespace": "default",
                        "status": test.status,
                        "snapshot_id": "snap-1",
                        "error_details": test.diagnosis,
                        "resources": {"cpus": 1, "memory_mb": 512, "disk_mb": 1024},
                        "sandbox_url": test.url,
                    }
                    payload = (
                        {"sandboxes": [info]} if path.endswith("/sandboxes") else info
                    )
                encoded = (
                    payload
                    if isinstance(payload, bytes)
                    else json.dumps(payload).encode()
                )
                self.send_response(code)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            do_GET = handle_request
            do_POST = handle_request
            do_DELETE = handle_request

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.assertFalse(any(method == "DELETE" for method, _ in self.requests))

    def check_error(self, error):
        self.assertIsInstance(error, SandboxError)
        self.assertNotIsInstance(error, SandboxPending)
        self.assertEqual(error.sandbox_id, "sbx-1")
        self.assertEqual(error.status, SandboxStatus.RESUME_FAILED)
        self.assertEqual(error.snapshot_id, "snap-1")
        self.assertEqual(error.error_details, self.diagnosis)

    def test_sync_get_list_readiness_connect_and_explicit_retry(self):
        with SandboxClient.for_localhost(api_url=self.url) as client:
            info = client.get("sbx-1")
            self.assertEqual(info.status, SandboxStatus.RESUME_FAILED)
            self.assertEqual(info.snapshot_id, "snap-1")
            self.assertEqual(list(client.list())[0].error_details, self.diagnosis)
            pending = client.create(wait=False)
            for operation in [
                lambda: pending.ready(timeout=0, cancel_on_timeout=True),
                lambda: client.connect("sbx-1"),
            ]:
                with self.assertRaises(SandboxResumeError) as caught:
                    operation()
                self.check_error(caught.exception)
            self.assertFalse(any(path.endswith("/resume") for _, path in self.requests))
            # Docs' get-before-resume shape; one explicit attempt, not a loop.
            if client.get("sbx-1").status == SandboxStatus.RESUME_FAILED:
                with self.assertRaises(SandboxResumeError) as caught:
                    client.resume("sbx-1", timeout=120)
                self.check_error(caught.exception)
            self.reply, self.body, self.next_status = 200, b"", "running"
            client.resume("sbx-1", timeout=120)
            self.assertEqual(client.get("sbx-1").status, SandboxStatus.RUNNING)
            self.assertIsNone(client.get("sbx-1").error_details)
            self.assertEqual(
                sum(path.endswith("/resume") for _, path in self.requests), 2
            )

    def test_async_get_list_readiness_connect_and_explicit_retry(self):
        async def run():
            async with AsyncSandboxClient.for_localhost(api_url=self.url) as client:
                info = await client.get("sbx-1")
                self.assertEqual(info.status, SandboxStatus.RESUME_FAILED)
                self.assertEqual(info.snapshot_id, "snap-1")
                self.assertEqual(
                    list(await client.list())[0].error_details, self.diagnosis
                )
                pending = await client.create(wait=False)
                with self.assertRaises(SandboxResumeError) as caught:
                    await pending.ready(timeout=0, cancel_on_timeout=True)
                self.check_error(caught.exception)
                with self.assertRaises(SandboxResumeError):
                    await client.connect("sbx-1")
                with self.assertRaises(SandboxResumeError) as caught:
                    await client.resume("sbx-1", timeout=120)
                self.check_error(caught.exception)
                self.reply, self.body, self.next_status = 202, b"", "running"
                await client.resume("sbx-1", timeout=120)
                self.assertEqual(
                    (await client.get("sbx-1")).status, SandboxStatus.RUNNING
                )
                self.assertIsNone((await client.get("sbx-1")).error_details)
                self.assertEqual(
                    sum(path.endswith("/resume") for _, path in self.requests), 2
                )

        asyncio.run(run())

    def test_empty_accepted_poll_failure_and_absent_diagnosis(self):
        self.reply, self.body, self.diagnosis = 202, b"", None
        with SandboxClient.for_localhost(api_url=self.url) as client:
            with self.assertRaises(SandboxResumeError) as caught:
                client.resume("sbx-1", timeout=120)
            self.check_error(caught.exception)

    def test_uncertain_pending_is_not_safe_to_retry(self):
        self.next_status = "pending"
        self.body = b'{"message":"Resident resume failed"}'
        with SandboxClient.for_localhost(api_url=self.url) as client:
            with self.assertRaises(RemoteAPIError) as caught:
                client.resume("sbx-1", timeout=120)
            self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(sum(path.endswith("/resume") for _, path in self.requests), 1)

    def test_connected_handle_sync_and_async_resume_failure(self):
        self.status = "running"
        with SandboxClient.for_localhost(api_url=self.url) as client:
            sandbox = client.connect("sbx-1")
            try:
                with self.assertRaises(SandboxResumeError) as caught:
                    sandbox.resume(timeout=120)
                self.check_error(caught.exception)
            finally:
                sandbox.close()

        async def run():
            self.status = "running"
            async with AsyncSandboxClient.for_localhost(api_url=self.url) as client:
                sandbox = await client.connect("sbx-1")
                try:
                    with self.assertRaises(SandboxResumeError) as caught:
                        await sandbox.resume(timeout=120)
                    self.check_error(caught.exception)
                finally:
                    sandbox.close()

        asyncio.run(run())

    def test_fresh_start_termination_remains_distinct(self):
        self.status = "terminated"
        with SandboxClient.for_localhost(api_url=self.url) as client:
            pending = client.create(wait=False)
            with self.assertRaises(SandboxError) as caught:
                pending.ready(timeout=0)
            self.assertNotIsInstance(caught.exception, SandboxResumeError)
            self.assertIn("terminated", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
