"""Shared configuration defaults for the sandbox SDK."""

import os

# Environment-derived defaults. These are read once at import time and
# used as parameter defaults throughout the SDK.

API_URL: str = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
API_KEY: str | None = os.getenv("TENSORLAKE_API_KEY")
NAMESPACE: str | None = os.getenv("INDEXIFY_NAMESPACE", "default")
SANDBOX_PROXY_URL: str = os.getenv(
    "TENSORLAKE_SANDBOX_PROXY_URL", "https://sandbox.tensorlake.ai"
)

# Sandbox operations (create, get, list) may take several seconds when the
# server is performing container scheduling or image pulls.
DEFAULT_HTTP_TIMEOUT_SEC: float = 30.0

# Retry configuration for transient errors (connection failures, 429/502/503/504).
MAX_RETRIES: int = 3
RETRY_BACKOFF_SEC: float = 0.5
RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 502, 503, 504})
