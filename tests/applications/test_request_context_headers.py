import pickle
import unittest

from tensorlake.applications.request_context.http_client.context import (
    RequestContextHTTPClient,
)


class _Logger:
    def bind(self, **kwargs):
        return self


def _context(headers):
    return RequestContextHTTPClient(
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


class TestRequestContextHeaders(unittest.TestCase):
    def test_headers_are_immutable_and_case_insensitive(self):
        source_headers = {"X-Trace-Id": "trace-123"}
        context = _context(source_headers)

        source_headers["X-Trace-Id"] = "changed"

        self.assertEqual(context.headers["x-trace-id"], "trace-123")
        self.assertEqual(context.headers["X-TRACE-ID"], "trace-123")
        self.assertEqual(context.headers.get("x-trace-id"), "trace-123")
        self.assertEqual(context.headers.getlist("x-trace-id"), ["trace-123"])
        self.assertEqual(dict(context.headers.items()), {"X-Trace-Id": "trace-123"})
        with self.assertRaises(TypeError):
            context.headers["x-trace-id"] = "mutated"

    def test_headers_preserve_duplicate_values(self):
        context = _context([("X-Token", "first"), ("x-token", "second")])

        self.assertEqual(context.headers["X-Token"], "second")
        self.assertEqual(context.headers.getlist("x-token"), ["first", "second"])

    def test_headers_survive_request_context_pickling(self):
        context = _context([("X-Token", "first"), ("x-token", "second")])

        restored_context = pickle.loads(pickle.dumps(context))

        self.assertEqual(
            restored_context.headers.getlist("x-token"),
            ["first", "second"],
        )


if __name__ == "__main__":
    unittest.main()
