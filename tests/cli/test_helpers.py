"""
Helper utilities for CLI tests.

This module provides utilities to ensure tests respect environment variables
and CLI flags for API URLs, matching the behavior of the actual CLI code.
"""

import os


def get_base_url() -> str:
    """
    Get the base API URL that will be used at runtime.

    This matches the resolution logic in Context.default() and should be used
    in tests for:
    - Creating credentials with the correct endpoint key
    - Mocking HTTP requests to the correct URL
    - Any other test logic that depends on the base URL

    Returns:
        The base API URL from TENSORLAKE_API_URL environment variable,
        or the default "https://api.tensorlake.ai"

    Examples:
        # In test setUp:
        from tests.cli.test_helpers import get_base_url

        def setUp(self):
            self.base_url = get_base_url()

            # Create credentials with the resolved base_url
            config[self.base_url] = {"token": "test_token"}

            # Mock HTTP requests to the resolved base_url
            respx.get(f"{self.base_url}/platform/v1/organizations").mock(...)
    """
    return os.environ.get("TENSORLAKE_API_URL", "https://api.tensorlake.ai")


def get_cloud_url() -> str:
    """
    Get the cloud/dashboard URL that will be used at runtime.

    This matches the resolution logic in Context.default() and should be used
    in tests that check dashboard URLs or cloud-related output.

    Returns:
        The cloud URL from TENSORLAKE_CLOUD_URL environment variable,
        or the default "https://cloud.tensorlake.ai"
    """
    return os.environ.get("TENSORLAKE_CLOUD_URL", "https://cloud.tensorlake.ai")


def make_endpoint_url(path: str) -> str:
    """
    Create a full endpoint URL by combining the base URL with a path.

    Args:
        path: The API path (e.g., "/platform/v1/organizations" or "platform/v1/organizations")

    Returns:
        Full URL combining base_url and path

    Examples:
        make_endpoint_url("/platform/v1/organizations")
        # Returns: "https://api.tensorlake.ai/platform/v1/organizations"
        # Or in CI: "http://127.0.0.1:8900/platform/v1/organizations"
    """
    base_url = get_base_url()
    # Ensure path starts with /
    if not path.startswith("/"):
        path = "/" + path
    return f"{base_url}{path}"
