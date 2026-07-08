import unittest

from tensorlake.applications.request_context.http_client.context import (
    RequestContextHTTPClient,
)


class _Logger:
    def bind(self, **kwargs):
        return self


class TestRequestContextHeaders(unittest.TestCase):
    def test_headers_returns_copy(self):
        headers = {"x-trace-id": "trace-123"}
        context = RequestContextHTTPClient(
            request_id="request-1",
            allocation_id="allocation-1",
            function_name="fn",
            function_run_id="run-1",
            server_base_url="http://127.0.0.1:1",
            http_client=None,
            blob_store=None,
            logger=_Logger(),
            headers=headers,
        )

        headers["x-trace-id"] = "changed"
        context.headers["x-trace-id"] = "mutated"

        self.assertEqual(context.headers, {"x-trace-id": "trace-123"})


if __name__ == "__main__":
    unittest.main()
