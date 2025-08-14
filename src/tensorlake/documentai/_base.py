from __future__ import annotations

import os
import sys
from typing import Any, Dict

import httpx

from .common import get_doc_ai_base_url_v1, get_doc_ai_base_url_v2
from .models import DocumentAIError, ErrorCode, ErrorResponse, Region


class _BaseClient:
    """
    Handles auth, session objects and raw request helpers.
    All high-level mixins inherit from this class.
    """

    def __init__(
        self, api_key: str | None, server_url: str | None, region: Region = Region.US
    ):
        self.api_key: str = api_key or os.getenv("TENSORLAKE_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "API key is required. Set TENSORLAKE_API_KEY or pass api_key."
            )

        doc_ai_v1 = get_doc_ai_base_url_v1(region=region, server_url=server_url)

        self._client_v1 = httpx.Client(base_url=doc_ai_v1, timeout=None)
        self._aclient_v1 = httpx.AsyncClient(base_url=doc_ai_v1, timeout=None)

        doc_ai_v2 = get_doc_ai_base_url_v2(region=region, server_url=server_url)
        self._client = httpx.Client(base_url=doc_ai_v2, timeout=None)
        self._aclient = httpx.AsyncClient(base_url=doc_ai_v2, timeout=None)

    def close(self):
        """
        Close the HTTP clients.
        """
        self._client_v1.close()
        self._client.close()

    async def _aclose(self):
        """
        Close the asynchronous HTTP clients.
        """
        await self._aclient_v1.aclose()
        await self._aclient.aclose()

    def __enter__(self):
        """
        Context manager entry point.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit point.
        Closes the HTTP clients.
        """
        self.close()

    async def __aenter__(self):
        """
        Asynchronous context manager entry point.
        """
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Asynchronous context manager exit point.
        Closes the asynchronous HTTP clients.
        """
        await self._aclose()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Connection": "close",
        }

    def _request_v1(self, method: str, url: str, **kw: Any) -> httpx.Response:
        resp = self._client_v1.request(method, url, headers=self._headers(), **kw)
        resp.raise_for_status()
        return resp

    def _request(self, method: str, url: str, **kw: Any) -> httpx.Response:
        resp = self._client.request(method, url, headers=self._headers(), **kw)
        if resp.is_success:
            return resp

        error_response = _deserialize_error_response(resp)
        _print_error_line(
            error_response.code, error_response.message, error_response.trace_id
        )

        raise DocumentAIError(
            message=error_response.message,
            code=error_response.code,
        )

    async def _arequest_v1(self, method: str, url: str, **kw: Any) -> httpx.Response:
        resp = await self._aclient_v1.request(
            method, url, headers=self._headers(), **kw
        )
        resp.raise_for_status()
        return resp

    async def _arequest(self, method: str, url: str, **kw: Any) -> httpx.Response:
        resp = await self._aclient.request(method, url, headers=self._headers(), **kw)
        if resp.is_success:
            return resp

        error_response = _deserialize_error_response(resp)
        _print_error_line(
            error_response.code.value, error_response.message, error_response.trace_id
        )

        raise DocumentAIError(
            message=error_response.message,
            code=error_response.code,
        )


def _deserialize_error_response(resp: httpx.Response) -> ErrorResponse:
    """
    Handle error responses and return a structured ErrorResponse.
    """
    try:
        error_response = ErrorResponse.model_validate(resp.json())
        return error_response
    except Exception as e:
        error_response = ErrorResponse(
            message=str(e),
            code=ErrorCode.INTERNAL_ERROR,
            timestamp=int(resp.headers.get("Date", 0)),
            trace_id=resp.headers.get("X-Trace-ID"),
            details=None,
        )

        return error_response


# --- simple color helpers ---
_RESET = "\033[0m"
_BOLD = "\033[1m"
_RED = "\033[31m"
_YELLOW = "\033[33m"


def _use_color() -> bool:
    env = os.getenv("TENSORLAKE_SDK_COLOR")
    if env is not None:
        return env.lower() not in ("0", "false", "no")
    return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()


def _c(s: str, color: str) -> str:
    if not _use_color():
        return s
    return f"{color}{s}{_RESET}"


def _print_error_line(code: Any, message: str, trace_id: str | None = None) -> None:
    prefix = _c("Error:", _BOLD + _RED)
    body = _c(f" {code}", _YELLOW) + f" — {message}"
    suffix = f"  (trace_id={trace_id})" if trace_id else ""
    print(prefix + body + suffix, file=sys.stderr)
