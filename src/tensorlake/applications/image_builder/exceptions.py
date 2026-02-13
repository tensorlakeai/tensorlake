"""Unified exception hierarchy for ImageBuilder with version tracking."""

from __future__ import annotations


class ImageBuilderError(Exception):
    """Base exception for all ImageBuilder errors with version tracking and request_id support."""

    def __init__(
        self,
        message: str,
        request_id: str | None = None,
        version: str | None = None,
    ) -> None:
        """
        Initialize ImageBuilderError.

        Args:
            message: Human-readable error message
            request_id: Optional request ID for correlation
            version: Optional version identifier ("v2" or "v3")
        """
        self.message = message
        self.request_id = request_id
        self.version = version
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with version and request_id context."""
        parts = [self.message]
        if self.version:
            parts.append(f"(version: {self.version})")
        if self.request_id:
            parts.append(f"(request_id: {self.request_id})")
        return " ".join(parts)


class ImageBuilderNetworkError(ImageBuilderError):
    """Network communication errors during image building."""

    pass


class ImageBuilderBuildError(ImageBuilderError):
    """Build failures with detailed context."""

    pass


class ImageBuilderConfigError(ImageBuilderError):
    """Configuration validation errors."""

    pass


# V3-specific exceptions (inherit from unified hierarchy)


class ImageBuilderClientV3Error(ImageBuilderError):
    """Base exception for ImageBuilder V3 client errors."""

    def __init__(
        self,
        message: str,
        request_id: str | None = None,
    ) -> None:
        """Initialize with version="v3" by default."""
        super().__init__(message, request_id=request_id, version="v3")


class ImageBuilderClientV3NetworkError(ImageBuilderNetworkError, ImageBuilderClientV3Error):
    """Network errors specific to V3 client."""

    pass


class ImageBuilderClientV3NotFoundError(ImageBuilderClientV3Error):
    """Resource not found errors in V3 client."""

    pass


class ImageBuilderClientV3BadRequestError(ImageBuilderClientV3Error):
    """Bad request errors in V3 client."""

    pass


class ImageBuilderClientV3InternalError(ImageBuilderClientV3Error):
    """Internal server errors in V3 client."""

    pass


# V2-specific exceptions (inherit from unified hierarchy)


class ImageBuilderV2Error(ImageBuilderError):
    """Base exception for ImageBuilder V2 errors."""

    def __init__(
        self,
        message: str,
        request_id: str | None = None,
    ) -> None:
        """Initialize with version="v2" by default."""
        super().__init__(message, request_id=request_id, version="v2")


class ImageBuilderV2NetworkError(ImageBuilderNetworkError, ImageBuilderV2Error):
    """Network errors specific to V2 client."""

    pass


class ImageBuilderV2BuildError(ImageBuilderBuildError, ImageBuilderV2Error):
    """Build errors specific to V2 client."""

    pass
