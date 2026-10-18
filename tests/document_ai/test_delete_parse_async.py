import json
import threading
import unittest
from unittest.mock import patch

from tensorlake.documentai import DocumentAI, _base, files
from tensorlake.documentai.models import DocumentAIError


class _FakeRustClient:
    def __init__(self, status="successful", error_method=None):
        self.status = status
        self.error_method = error_method
        self.requests = []
        self.event_loop_thread = threading.get_ident()

    def request_json(self, method, path, body_json=None):
        self.requests.append((method, path))
        if threading.get_ident() == self.event_loop_thread:
            raise AssertionError("Blocking transport ran on the event loop thread")
        if method == self.error_method:
            raise ConnectionError(f"{method} failed")
        body = (
            {
                "parse_id": "parse-1",
                "parsed_pages_count": 1,
                "status": self.status,
                "created_at": "2026-01-01T00:00:00Z",
            }
            if method == "GET"
            else {}
        )
        return json.dumps({"status_code": 200, "headers": {}, "body": json.dumps(body)})

    def close(self):
        pass


class TestDeleteParseAsync(unittest.IsolatedAsyncioTestCase):
    def _client(self, **kwargs):
        fake = _FakeRustClient(**kwargs)
        with (
            patch.object(_base, "_RUST_DOCUMENT_AI_CLIENT_AVAILABLE", True),
            patch.object(_base, "RustCloudDocumentAIClient", return_value=fake),
            patch.object(files, "_RUST_DOCUMENT_AI_CLIENT_AVAILABLE", True),
            patch.object(files, "RustCloudDocumentAIClient", return_value=fake),
        ):
            client = DocumentAI(api_key="test-key")
        self.addCleanup(client.close)
        return client, fake

    async def test_terminal_parse_deleted_without_blocking_event_loop(self):
        for status in ("successful", "failure"):
            with self.subTest(status=status):
                client, fake = self._client(status=status)
                result = await client.delete_parse_async("parse-1")
                self.assertIsNone(result)
                self.assertEqual(
                    fake.requests,
                    [("GET", "parse/parse-1"), ("DELETE", "parse/parse-1")],
                )

    async def test_in_progress_parse_not_deleted(self):
        for status in ("pending", "processing"):
            with self.subTest(status=status):
                client, fake = self._client(status=status)
                with self.assertRaisesRegex(ValueError, "still pending or processing"):
                    await client.delete_parse_async("parse-1")
                self.assertEqual(fake.requests, [("GET", "parse/parse-1")])

    async def test_failed_lookup_does_not_delete(self):
        client, fake = self._client(error_method="GET")
        with self.assertRaisesRegex(DocumentAIError, "GET failed"):
            await client.delete_parse_async("parse-1")
        self.assertEqual(fake.requests, [("GET", "parse/parse-1")])

    async def test_delete_failure_propagates(self):
        client, fake = self._client(error_method="DELETE")
        with self.assertRaisesRegex(DocumentAIError, "DELETE failed"):
            await client.delete_parse_async("parse-1")
        self.assertEqual(
            fake.requests, [("GET", "parse/parse-1"), ("DELETE", "parse/parse-1")]
        )


if __name__ == "__main__":
    unittest.main()
