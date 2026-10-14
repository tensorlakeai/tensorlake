"""Unit tests for Document AI error response deserialization.

These are pure unit tests: unlike the rest of tests/document_ai they need no API key and
make no network calls.
"""

import io
import json
import unittest
from contextlib import redirect_stderr

from tensorlake.documentai._base import (
    _deserialize_error_response,
    _RustHTTPResponse,
)
from tensorlake.documentai.models import ErrorCode, ErrorResponse


def _response(body, status_code: int = 400, headers: dict = None) -> _RustHTTPResponse:
    if not isinstance(body, str):
        body = json.dumps(body)
    return _RustHTTPResponse(status_code=status_code, headers=headers or {}, body=body)


class TestErrorResponseModel(unittest.TestCase):
    def test_known_code_is_parsed_as_enum(self):
        response = ErrorResponse.model_validate(
            {"message": "bad mime type", "code": "INVALID_MIME_TYPE"}
        )

        self.assertEqual(response.code, ErrorCode.INVALID_MIME_TYPE)
        self.assertEqual(response.message, "bad mime type")

    def test_null_code_keeps_the_message(self):
        # The payload from issue #408. A null code used to fail validation, which cost the
        # user the message and produced a pydantic error about the SDK's own model instead.
        response = ErrorResponse.model_validate(
            {
                "message": "Expecting value: line 1 column 1 (char 0)",
                "code": None,
                "details": None,
            }
        )

        self.assertEqual(response.code, ErrorCode.UNKNOWN)
        self.assertEqual(response.message, "Expecting value: line 1 column 1 (char 0)")

    def test_unrecognized_code_keeps_the_message(self):
        # The service must be able to add an error code without breaking error reporting
        # in SDK versions that predate it.
        response = ErrorResponse.model_validate(
            {"message": "Too many requests", "code": "SOME_FUTURE_ERROR_CODE"}
        )

        self.assertEqual(response.code, ErrorCode.UNKNOWN)
        self.assertEqual(response.message, "Too many requests")

    def test_missing_code_keeps_the_message(self):
        response = ErrorResponse.model_validate({"message": "something broke"})

        self.assertEqual(response.code, ErrorCode.UNKNOWN)
        self.assertEqual(response.message, "something broke")

    def test_optional_fields_are_preserved(self):
        response = ErrorResponse.model_validate(
            {
                "message": "quota exceeded",
                "code": "QUOTA_EXCEEDED",
                "trace_id": "trace-123",
                "details": {"field": "page_range"},
            }
        )

        self.assertEqual(response.code, ErrorCode.QUOTA_EXCEEDED)
        self.assertEqual(response.trace_id, "trace-123")
        self.assertEqual(response.details, {"field": "page_range"})

    def test_message_is_still_required(self):
        # An error response without a message carries no information, so it must fall
        # through to the raw body handling in _deserialize_error_response().
        with self.assertRaises(Exception):
            ErrorResponse.model_validate({"code": "INVALID_MIME_TYPE"})


class TestDeserializeErrorResponse(unittest.TestCase):
    def test_well_formed_error_response(self):
        result = _deserialize_error_response(
            _response({"message": "bad mime type", "code": "INVALID_MIME_TYPE"})
        )

        self.assertEqual(result.code, ErrorCode.INVALID_MIME_TYPE)
        self.assertEqual(result.message, "bad mime type")

    def test_null_code_surfaces_the_service_message(self):
        # Regression test for issue #408: the message used to be replaced by the whole raw
        # JSON body because the response failed to deserialize.
        result = _deserialize_error_response(
            _response(
                {
                    "message": "Expecting value: line 1 column 1 (char 0)",
                    "code": None,
                    "details": None,
                }
            )
        )

        self.assertEqual(result.message, "Expecting value: line 1 column 1 (char 0)")
        self.assertNotIn("{", result.message)

    def test_body_without_a_message_field_falls_back_to_the_raw_body(self):
        body = {"unexpected": "shape"}
        result = _deserialize_error_response(_response(body))

        self.assertEqual(result.code, ErrorCode.UNKNOWN)
        self.assertEqual(result.message, json.dumps(body))

    def test_non_json_body_falls_back_to_the_raw_body(self):
        result = _deserialize_error_response(_response("502 Bad Gateway"))

        self.assertEqual(result.code, ErrorCode.UNKNOWN)
        self.assertEqual(result.message, "502 Bad Gateway")

    def test_empty_body_does_not_raise(self):
        result = _deserialize_error_response(_response(""))

        self.assertEqual(result.code, ErrorCode.UNKNOWN)
        self.assertEqual(result.message, "")

    def test_alternative_message_keys_are_recovered(self):
        for key in ("error", "detail"):
            with self.subTest(key=key):
                result = _deserialize_error_response(
                    _response({key: "upstream failed"})
                )

                self.assertEqual(result.message, "upstream failed")

    def test_trace_id_is_read_from_headers_case_insensitively(self):
        result = _deserialize_error_response(
            _response({"unexpected": "shape"}, headers={"X-Trace-ID": "trace-abc"})
        )

        self.assertEqual(result.trace_id, "trace-abc")

    def test_nothing_is_printed_to_stderr(self):
        # Deserialization failures used to be printed straight to stderr from library
        # code. The reported error is the caller's to surface, not the SDK's to narrate.
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            _deserialize_error_response(_response("not json at all"))
            _deserialize_error_response(_response({"unexpected": "shape"}))

        self.assertEqual(stderr.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
