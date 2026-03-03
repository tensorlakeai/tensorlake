from typing import Any, Protocol

import httpx


class RequestContextHTTPTransport(Protocol):
    """Interface required by request-context HTTP client wrappers."""

    def build_request(self, method: str, url: str, **kwargs: Any) -> httpx.Request: ...

    def send(self, request: httpx.Request) -> httpx.Response: ...

    def close(self) -> None: ...
