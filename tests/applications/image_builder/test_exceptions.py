"""Tests for unified exception hierarchy."""

import pytest

from tensorlake.applications.image_builder.exceptions import (
    ImageBuilderBuildError,
    ImageBuilderClientV3BadRequestError,
    ImageBuilderClientV3Error,
    ImageBuilderClientV3InternalError,
    ImageBuilderClientV3NetworkError,
    ImageBuilderClientV3NotFoundError,
    ImageBuilderConfigError,
    ImageBuilderError,
    ImageBuilderNetworkError,
    ImageBuilderV2BuildError,
    ImageBuilderV2Error,
    ImageBuilderV2NetworkError,
)


class TestExceptionHierarchy:
    """Tests for exception inheritance."""

    def test_base_exception(self):
        """Test base ImageBuilderError."""
        exc = ImageBuilderError("test error", request_id="req-123", version="v2")
        assert "test error" in str(exc)
        assert "v2" in str(exc)
        assert "req-123" in str(exc)

    def test_network_error_inheritance(self):
        """Test NetworkError inherits from base."""
        exc = ImageBuilderNetworkError("network error")
        assert isinstance(exc, ImageBuilderError)

    def test_build_error_inheritance(self):
        """Test BuildError inherits from base."""
        exc = ImageBuilderBuildError("build error")
        assert isinstance(exc, ImageBuilderError)

    def test_config_error_inheritance(self):
        """Test ConfigError inherits from base."""
        exc = ImageBuilderConfigError("config error")
        assert isinstance(exc, ImageBuilderError)

    def test_v3_error_inheritance(self):
        """Test V3 errors inherit from unified hierarchy."""
        exc = ImageBuilderClientV3Error("v3 error")
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v3"

    def test_v3_network_error_inheritance(self):
        """Test V3 NetworkError inherits from both V3 and Network."""
        exc = ImageBuilderClientV3NetworkError("v3 network error")
        assert isinstance(exc, ImageBuilderClientV3Error)
        assert isinstance(exc, ImageBuilderNetworkError)
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v3"

    def test_v3_not_found_error_inheritance(self):
        """Test V3 NotFoundError inherits from V3."""
        exc = ImageBuilderClientV3NotFoundError("v3 not found")
        assert isinstance(exc, ImageBuilderClientV3Error)
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v3"

    def test_v3_bad_request_error_inheritance(self):
        """Test V3 BadRequestError inherits from V3."""
        exc = ImageBuilderClientV3BadRequestError("v3 bad request")
        assert isinstance(exc, ImageBuilderClientV3Error)
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v3"

    def test_v3_internal_error_inheritance(self):
        """Test V3 InternalError inherits from V3."""
        exc = ImageBuilderClientV3InternalError("v3 internal error")
        assert isinstance(exc, ImageBuilderClientV3Error)
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v3"

    def test_v2_error_inheritance(self):
        """Test V2 errors inherit from unified hierarchy."""
        exc = ImageBuilderV2Error("v2 error")
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v2"

    def test_v2_network_error_inheritance(self):
        """Test V2 NetworkError inherits from both V2 and Network."""
        exc = ImageBuilderV2NetworkError("v2 network error")
        assert isinstance(exc, ImageBuilderV2Error)
        assert isinstance(exc, ImageBuilderNetworkError)
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v2"

    def test_v2_build_error_inheritance(self):
        """Test V2 BuildError inherits from both V2 and Build."""
        exc = ImageBuilderV2BuildError("v2 build error")
        assert isinstance(exc, ImageBuilderV2Error)
        assert isinstance(exc, ImageBuilderBuildError)
        assert isinstance(exc, ImageBuilderError)
        assert exc.version == "v2"


class TestExceptionVersionTracking:
    """Tests for version tracking in exceptions."""

    def test_base_version_tracking(self):
        """Test version field is tracked."""
        exc = ImageBuilderError("error", version="test-version")
        assert exc.version == "test-version"

    def test_v2_version_automatic(self):
        """Test V2 errors automatically set version."""
        exc = ImageBuilderV2Error("v2 error")
        assert exc.version == "v2"

    def test_v3_version_automatic(self):
        """Test V3 errors automatically set version."""
        exc = ImageBuilderClientV3Error("v3 error")
        assert exc.version == "v3"


class TestExceptionRequestId:
    """Tests for request ID tracking."""

    def test_request_id_tracking(self):
        """Test request_id field is tracked."""
        exc = ImageBuilderError("error", request_id="req-123")
        assert exc.request_id == "req-123"
        assert "req-123" in str(exc)

    def test_request_id_optional(self):
        """Test request_id is optional."""
        exc = ImageBuilderError("error")
        assert exc.request_id is None
        assert "request_id" not in str(exc)


class TestExceptionStringRepresentation:
    """Tests for exception string formatting."""

    def test_message_only(self):
        """Test message-only representation."""
        exc = ImageBuilderError("simple error")
        assert str(exc) == "simple error"

    def test_message_with_version(self):
        """Test message with version."""
        exc = ImageBuilderError("error", version="v2")
        result = str(exc)
        assert "error" in result
        assert "v2" in result

    def test_message_with_request_id(self):
        """Test message with request_id."""
        exc = ImageBuilderError("error", request_id="req-123")
        result = str(exc)
        assert "error" in result
        assert "req-123" in result

    def test_message_with_all_context(self):
        """Test message with version and request_id."""
        exc = ImageBuilderError("error", request_id="req-123", version="v2")
        result = str(exc)
        assert "error" in result
        assert "v2" in result
        assert "req-123" in result
