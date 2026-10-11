"""Unit tests for S3BLOBStore retry classification.

These tests mock the S3 HTTP responses with respx so they need no AWS credentials and no
network, unlike test_s3_blob_store.py which talks to a real bucket.
"""

import unittest

import httpx
import respx

from tensorlake.applications.blob_store.s3_blob_store import (
    S3BLOBStore,
    _is_retriable_exception,
)
from tensorlake.applications.interface.exceptions import InternalError
from tensorlake.applications.internal_logger import InternalLogger

_URI = "s3://test-bucket.s3.amazonaws.com/test-key"
_URL = "https://test-bucket.s3.amazonaws.com/test-key"
_PAYLOAD = b"0123456789"


def _s3_error_body(code: str, message: str = "") -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<Error><Code>{code}</Code><Message>{message}</Message>"
        "<RequestId>REQ</RequestId><HostId>HOST</HostId></Error>"
    )


# The exact body S3 returns when it closes a connection that went idle mid request.
_REQUEST_TIMEOUT_BODY = _s3_error_body(
    "RequestTimeout",
    "Your socket connection to the server was not read from or written to within "
    "the timeout period. Idle connections will be closed.",
)


def _status_error(status_code: int, body: str) -> httpx.HTTPStatusError:
    request = httpx.Request("PUT", _URL)
    response = httpx.Response(status_code, text=body, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


class TestIsRetriableException(unittest.TestCase):
    def test_transient_s3_error_codes_are_retriable(self):
        for status_code, code in [
            (400, "RequestTimeout"),
            (403, "RequestTimeTooSkewed"),
            (400, "SlowDown"),
            (400, "InternalError"),
            (400, "ServiceUnavailable"),
        ]:
            with self.subTest(status_code=status_code, code=code):
                self.assertTrue(
                    _is_retriable_exception(
                        _status_error(status_code, _s3_error_body(code))
                    )
                )

    def test_permanent_s3_error_codes_are_not_retriable(self):
        for status_code, code in [
            (400, "InvalidRequest"),
            (400, "EntityTooLarge"),
            (403, "AccessDenied"),
            (403, "SignatureDoesNotMatch"),
            (404, "NoSuchKey"),
        ]:
            with self.subTest(status_code=status_code, code=code):
                self.assertFalse(
                    _is_retriable_exception(
                        _status_error(status_code, _s3_error_body(code))
                    )
                )

    def test_unreadable_body_stays_non_retriable(self):
        # Without an error code there is no way to tell a transient 400 from a permanent one,
        # so the status code decides and the request is not retried.
        self.assertFalse(_is_retriable_exception(_status_error(400, "")))
        self.assertFalse(_is_retriable_exception(_status_error(400, "not xml at all")))

    def test_server_errors_are_retriable(self):
        for status_code in [429, 500, 502, 503, 504]:
            with self.subTest(status_code=status_code):
                self.assertTrue(_is_retriable_exception(_status_error(status_code, "")))

    def test_transport_errors_are_retriable(self):
        # This is what a connection closed by S3 while pooled looks like.
        self.assertTrue(
            _is_retriable_exception(
                httpx.RemoteProtocolError(
                    "Server disconnected without sending a response."
                )
            )
        )
        self.assertTrue(_is_retriable_exception(httpx.ConnectTimeout("timed out")))
        self.assertTrue(_is_retriable_exception(httpx.ReadError("reset")))


class TestS3BLOBStoreRetries(unittest.TestCase):
    def setUp(self):
        self.store = S3BLOBStore(io_workers_count=2)
        self.logger = InternalLogger.get_logger()

    def _put(self) -> str:
        return self.store.put(_URI, [memoryview(_PAYLOAD)], self.logger)

    def _get(self) -> bytes:
        destination = memoryview(bytearray(len(_PAYLOAD)))
        self.store.get(_URI, 0, destination, self.logger)
        return bytes(destination)

    @respx.mock
    def test_put_retries_400_request_timeout(self):
        # Regression test for issue #469: S3 answers a stalled upload with HTTP 400
        # RequestTimeout, which used to be classified as a permanent client error.
        route = respx.put(_URL).mock(
            side_effect=[
                httpx.Response(400, text=_REQUEST_TIMEOUT_BODY),
                httpx.Response(400, text=_REQUEST_TIMEOUT_BODY),
                httpx.Response(200, headers={"ETag": '"etag-value"'}),
            ]
        )

        self.assertEqual(self._put(), '"etag-value"')
        self.assertEqual(route.call_count, 3)

    @respx.mock
    def test_put_resends_the_whole_body_on_every_attempt(self):
        # A retry is only correct if the body is re-sent in full; memoryviews must not be
        # consumed by the first attempt.
        route = respx.put(_URL).mock(
            side_effect=[
                httpx.Response(400, text=_REQUEST_TIMEOUT_BODY),
                httpx.Response(200, headers={"ETag": '"etag-value"'}),
            ]
        )

        self._put()

        self.assertEqual(route.call_count, 2)
        for call in route.calls:
            self.assertEqual(call.request.content, _PAYLOAD)
            self.assertEqual(call.request.headers["Content-Length"], str(len(_PAYLOAD)))

    @respx.mock
    def test_put_does_not_retry_permanent_400(self):
        route = respx.put(_URL).mock(
            return_value=httpx.Response(400, text=_s3_error_body("InvalidRequest"))
        )

        with self.assertRaises(InternalError):
            self._put()

        self.assertEqual(route.call_count, 1)

    @respx.mock
    def test_put_does_not_retry_access_denied(self):
        route = respx.put(_URL).mock(
            return_value=httpx.Response(403, text=_s3_error_body("AccessDenied"))
        )

        with self.assertRaises(InternalError):
            self._put()

        self.assertEqual(route.call_count, 1)

    @respx.mock
    def test_put_gives_up_after_max_retries(self):
        route = respx.put(_URL).mock(
            return_value=httpx.Response(400, text=_REQUEST_TIMEOUT_BODY)
        )

        with self.assertRaises(InternalError):
            self._put()

        # The initial attempt plus _MAX_RETRIES retries.
        self.assertEqual(route.call_count, 4)

    @respx.mock
    def test_put_retries_disconnected_connection(self):
        # A connection S3 already closed fails before any response is received.
        route = respx.put(_URL).mock(
            side_effect=[
                httpx.RemoteProtocolError(
                    "Server disconnected without sending a response."
                ),
                httpx.Response(200, headers={"ETag": '"etag-value"'}),
            ]
        )

        self.assertEqual(self._put(), '"etag-value"')
        self.assertEqual(route.call_count, 2)

    @respx.mock
    def test_get_retries_400_request_timeout(self):
        route = respx.get(_URL).mock(
            side_effect=[
                httpx.Response(400, text=_REQUEST_TIMEOUT_BODY),
                httpx.Response(206, content=_PAYLOAD),
            ]
        )

        self.assertEqual(self._get(), _PAYLOAD)
        self.assertEqual(route.call_count, 2)

    @respx.mock
    def test_get_does_not_retry_missing_key(self):
        route = respx.get(_URL).mock(
            return_value=httpx.Response(404, text=_s3_error_body("NoSuchKey"))
        )

        with self.assertRaises(InternalError):
            self._get()

        self.assertEqual(route.call_count, 1)

    @respx.mock
    def test_get_tells_transient_and_permanent_400_apart(self):
        # get() streams responses. Unless the error body is read inside the stream context,
        # reading it afterwards raises httpx.StreamClosed, the S3 error code is lost, and every
        # 400 looks alike to the retry layer. Two 400s classified differently prove the body
        # reached the classifier.
        route = respx.get(_URL).mock(
            return_value=httpx.Response(400, text=_s3_error_body("InvalidRequest"))
        )
        with self.assertRaises(InternalError):
            self._get()
        self.assertEqual(route.call_count, 1)

        respx.reset()
        route = respx.get(_URL).mock(
            return_value=httpx.Response(400, text=_REQUEST_TIMEOUT_BODY)
        )
        with self.assertRaises(InternalError):
            self._get()
        self.assertEqual(route.call_count, 4)


class TestConnectionPoolConfiguration(unittest.TestCase):
    def test_keepalive_expiry_is_shorter_than_s3_idle_timeout(self):
        # S3 closes idle connections after tens of seconds. Holding them client side for
        # longer than that makes the pool hand out sockets S3 has already closed, which is
        # what produced the HTTP 400 RequestTimeout and "Server disconnected" failures in
        # issue #469.
        from tensorlake.applications.blob_store.s3_blob_store import (
            _CONNECTION_KEEP_ALIVE_EXPIRY_SEC,
        )

        self.assertLessEqual(_CONNECTION_KEEP_ALIVE_EXPIRY_SEC, 20.0)


if __name__ == "__main__":
    unittest.main()
