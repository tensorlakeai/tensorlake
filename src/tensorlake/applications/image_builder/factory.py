"""Factory for creating ImageBuilder instances with version detection."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Literal

from .exceptions import ImageBuilderConfigError

if TYPE_CHECKING:
    from .protocol import ImageBuilder

ImageBuilderVersion = Literal["v2", "v3"]


def get_image_builder_version(override: str | None = None) -> ImageBuilderVersion:
    """
    Detect ImageBuilder version from CLI flag > Environment variable > Default (v2).

    Priority order:
    1. CLI flag override (passed as parameter)
    2. TENSORLAKE_IMAGE_BUILDER_VERSION environment variable
    3. Default: "v2"

    Args:
        override: Optional version override from CLI flag

    Returns:
        ImageBuilderVersion: Either "v2" or "v3"

    Raises:
        ImageBuilderConfigError: If version string is invalid
    """
    # Check CLI override first
    version_str = override

    # Fall back to environment variable
    if version_str is None:
        version_str = os.environ.get("TENSORLAKE_IMAGE_BUILDER_VERSION")

    # Default to v2
    if version_str is None:
        return "v2"

    # Normalize and validate
    normalized = version_str.lower().strip()

    # Handle shorthand: "2" -> "v2", "3" -> "v3"
    if normalized == "2":
        normalized = "v2"
    elif normalized == "3":
        normalized = "v3"

    # Validate
    if normalized not in ("v2", "v3"):
        raise ImageBuilderConfigError(
            f"Invalid image builder version: '{version_str}'. "
            f"Valid options: 'v2', 'v3' (default: 'v2')"
        )

    return normalized  # type: ignore[return-value]


def create_image_builder_from_context(
    api_key: str | None = None,
    pat: str | None = None,
    organization_id: str | None = None,
    project_id: str | None = None,
    version: ImageBuilderVersion | None = None,
) -> ImageBuilder:
    """
    Factory function to create ImageBuilder with explicit auth context.

    Creates either v2 adapter or v3 builder based on version detection.

    Args:
        api_key: API key for authentication
        pat: Personal access token for authentication
        organization_id: Organization ID
        project_id: Project ID
        version: Optional version override (defaults to auto-detection)

    Returns:
        ImageBuilder: Instance of v2 adapter or v3 builder

    Raises:
        ImageBuilderConfigError: If version is invalid or configuration is incomplete
    """
    # Detect version if not specified
    detected_version = get_image_builder_version(override=version)

    if detected_version == "v2":
        from .adapter_v2 import ImageBuilderV2Adapter

        return ImageBuilderV2Adapter.from_context(
            api_key=api_key,
            pat=pat,
            organization_id=organization_id,
            project_id=project_id,
        )
    else:  # v3
        # Import from top-level to avoid circular dependency
        # Note: This import is deferred to avoid circular imports at module load time
        from tensorlake.applications import image_builder

        from .client_v3 import ImageBuilderClientV3, ImageBuilderClientV3Options

        # Construct base URL (same logic as from_env)
        server_url = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
        base_url = os.getenv("TENSORLAKE_BUILD_SERVICE", f"{server_url}/images/v3")

        # Build client options
        client_options = ImageBuilderClientV3Options(
            base_url=base_url,
            api_key=api_key,
            pat=pat,
            organization_id=organization_id,
            project_id=project_id,
        )

        # Create client
        client = ImageBuilderClientV3(options=client_options)

        # Create builder (get ImageBuilder from the module)
        ImageBuilder = image_builder.ImageBuilder
        return ImageBuilder(client=client)
