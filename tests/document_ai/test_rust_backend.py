import json
import tempfile
import unittest
from pathlib import Path

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import DocumentAIError, ParseStatus


def _response(status_code: int, body: dict, headers: dict | None = None) -> str:
    return json.dumps(
        {
            "status_code": status_code,
            "headers": headers or {},
            "body": json.dumps(body),
        }
    )


class _FakeRustDocumentAIClient:
    def __init__(self):
        self.requests = []
        self.uploads = []

    def close(self):
        return None

    def request_json(self, method, path, body_json=None):
        self.requests.append((method, path, body_json))
        if method == "POST" and path == "/parse":
            return _response(200, {"parse_id": "parse-1"})
        if method == "GET" and path.startswith("parse/"):
            return _response(
                200,
                {
                    "parse_id": "parse-1",
                    "parsed_pages_count": 1,
                    "status": "successful",
                    "created_at": "2026-01-01T00:00:00Z",
                    "finished_at": "2026-01-01T00:00:01Z",
                },
            )
        return _response(200, {})

    def parse_events_json(self, parse_id):
        self.requests.append(("GET", f"parse/{parse_id} [sse]", None))
        return [
            json.dumps(
                {
                    "event": "parse_update",
                    "data": json.dumps(
                        {
                            "parse_id": parse_id,
                            "parsed_pages_count": 0,
                            "status": "processing",
                            "created_at": "2026-01-01T00:00:00Z",
                        }
                    ),
                }
            ),
            json.dumps(
                {
                    "event": "parse_done",
                    "data": json.dumps(
                        {
                            "parse_id": parse_id,
                            "parsed_pages_count": 1,
                            "status": "successful",
                            "created_at": "2026-01-01T00:00:00Z",
                            "finished_at": "2026-01-01T00:00:01Z",
                        }
                    ),
                }
            ),
        ]

    def upload_file_json(self, file_name, content):
        self.uploads.append((file_name, content))
        return _response(200, {"file_id": "tensorlake-123"})


class _UnauthorizedRustDocumentAIClient(_FakeRustDocumentAIClient):
    def request_json(self, method, path, body_json=None):
        return _response(401, {"message": "unauthorized", "code": "INTERNAL_ERROR"})


class TestDocumentAIRustBackend(unittest.TestCase):
    def test_parse_uses_rust_backend(self):
        doc_ai = DocumentAI(api_key="k", server_url="http://localhost:8900")
        fake = _FakeRustDocumentAIClient()
        doc_ai._rust_client = fake

        parse_id = doc_ai.parse(file_id="tensorlake-123")

        self.assertEqual(parse_id, "parse-1")
        method, path, body_json = fake.requests[0]
        self.assertEqual(method, "POST")
        self.assertEqual(path, "/parse")
        self.assertEqual(json.loads(body_json)["file_id"], "tensorlake-123")

    def test_wait_for_completion_uses_rust_sse_events(self):
        doc_ai = DocumentAI(api_key="k", server_url="http://localhost:8900")
        fake = _FakeRustDocumentAIClient()
        doc_ai._rust_client = fake

        parse_result = doc_ai.wait_for_completion("parse-1")

        self.assertEqual(parse_result.parse_id, "parse-1")
        self.assertEqual(parse_result.status, ParseStatus.SUCCESSFUL)

    def test_upload_uses_rust_backend(self):
        doc_ai = DocumentAI(api_key="k", server_url="http://localhost:8900")
        fake = _FakeRustDocumentAIClient()
        doc_ai._uploader._rust_client = fake

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"hello")
            tmp_path = Path(tmp.name)

        try:
            file_id = doc_ai.upload(path=str(tmp_path))
        finally:
            tmp_path.unlink(missing_ok=True)

        self.assertEqual(file_id, "tensorlake-123")
        self.assertEqual(len(fake.uploads), 1)
        self.assertEqual(fake.uploads[0][0], tmp_path.name)

    def test_unauthorized_maps_to_document_ai_error(self):
        doc_ai = DocumentAI(api_key="k", server_url="http://localhost:8900")
        doc_ai._rust_client = _UnauthorizedRustDocumentAIClient()

        with self.assertRaises(DocumentAIError) as ctx:
            doc_ai.parse(file_id="tensorlake-123")

        self.assertEqual(ctx.exception.code, "unauthorized")


if __name__ == "__main__":
    unittest.main()
