from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from pydantic import ValidationError

from .common import get_doc_ai_base_url
from .models import DocumentAIError, ErrorCode, ErrorResponse, MimeType, Region

try:
    from tensorlake._cloud_sdk import CloudDocumentAIClient as RustCloudDocumentAIClient
    from tensorlake._cloud_sdk import (
        CloudDocumentAIClientError as RustCloudDocumentAIClientError,
    )

    _RUST_DOCUMENT_AI_CLIENT_AVAILABLE = True
except Exception:
    try:
        from _cloud_sdk import CloudDocumentAIClient as RustCloudDocumentAIClient
        from _cloud_sdk import (
            CloudDocumentAIClientError as RustCloudDocumentAIClientError,
        )

        _RUST_DOCUMENT_AI_CLIENT_AVAILABLE = True
    except Exception:
        RustCloudDocumentAIClient = None
        RustCloudDocumentAIClientError = None
        _RUST_DOCUMENT_AI_CLIENT_AVAILABLE = False


class _RustHTTPResponse:
    def __init__(self, status_code: int, headers: dict[str, str], body: str):
        self.status_code = status_code
        self.headers = {k.lower(): v for k, v in headers.items()}
        self._body = body

    @property
    def text(self) -> str:
        return self._body

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict:
        if not self._body:
            return {}
        return json.loads(self._body)


def _parse_rust_client_error_fields(
    e: Exception,
) -> tuple[str | None, int | None, str]:
    kind: str | None = None
    status_code: int | None = None
    message = str(e)

    if len(e.args) == 3:
        kind, status_code, message = e.args
    elif len(e.args) == 1 and isinstance(e.args[0], tuple) and len(e.args[0]) == 3:
        kind, status_code, message = e.args[0]

    return kind, status_code, message


def _raise_as_document_ai_error(e: Exception) -> None:
    if isinstance(e, DocumentAIError):
        raise

    if (
        RustCloudDocumentAIClientError is not None
        and isinstance(e, RustCloudDocumentAIClientError)
        and len(e.args) > 0
    ):
        kind, status_code, message = _parse_rust_client_error_fields(e)
        if kind == "connection":
            raise ConnectionError(message) from None
        if status_code in (401, 403):
            raise DocumentAIError(
                message="Invalid API key or unauthorized access.",
                code="unauthorized",
            ) from None
        if status_code is not None:
            raise DocumentAIError(
                message=message,
                code=ErrorCode.INTERNAL_ERROR.value,
            ) from None
        raise DocumentAIError(
            message=message,
            code=ErrorCode.INTERNAL_ERROR.value,
        ) from None

    raise DocumentAIError(
        message=str(e),
        code=ErrorCode.INTERNAL_ERROR.value,
    ) from e


def _deserialize_rust_response(response_json: str) -> _RustHTTPResponse:
    try:
        payload = json.loads(response_json)
    except Exception as e:
        raise DocumentAIError(
            message=f"Failed to parse Rust response payload: {e}",
            code=ErrorCode.INTERNAL_ERROR.value,
        ) from e

    status_code = int(payload.get("status_code", 500))
    headers = payload.get("headers", {})
    if not isinstance(headers, dict):
        headers = {}
    body = payload.get("body", "")
    if not isinstance(body, str):
        body = str(body)

    return _RustHTTPResponse(status_code=status_code, headers=headers, body=body)


def _append_query_params(url: str, params: dict[str, Any] | None) -> str:
    if not params:
        return url

    query_string = urlencode(params, doseq=True)
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{query_string}"


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

        if not _RUST_DOCUMENT_AI_CLIENT_AVAILABLE:
            raise ValueError(
                "Rust Document AI client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        doc_ai_url = get_doc_ai_base_url(region=region, server_url=server_url)
        try:
            self._rust_client = RustCloudDocumentAIClient(
                api_url=doc_ai_url,
                api_key=self.api_key,
            )
        except Exception as e:
            _raise_as_document_ai_error(e)

    def close(self):
        """
        Close the HTTP clients.
        """
        self._rust_client.close()

    async def _aclose(self):
        """
        Close the asynchronous HTTP clients.
        """
        await asyncio.to_thread(self.close)

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

    def _request(self, method: str, url: str, **kw: Any) -> _RustHTTPResponse:
        params = kw.pop("params", None)
        body_json = kw.pop("json", None)
        if kw:
            unexpected = ", ".join(sorted(kw.keys()))
            raise ValueError(f"Unsupported request kwargs: {unexpected}")

        path = _append_query_params(url, params)

        try:
            response_json = self._rust_client.request_json(
                method=method,
                path=path,
                body_json=(json.dumps(body_json) if body_json is not None else None),
            )
        except Exception as e:
            _raise_as_document_ai_error(e)

        resp = _deserialize_rust_response(response_json)

        if resp.is_success:
            return resp

        if resp.status_code == 401 or resp.status_code == 403:
            raise DocumentAIError(
                message="Invalid API key or unauthorized access.",
                code="unauthorized",
            )

        error_response = _deserialize_error_response(resp)
        _print_error_line(
            error_response.code.value, error_response.message, error_response.trace_id
        )

        raise DocumentAIError(
            message=error_response.message,
            code=error_response.code,
        )

    async def _arequest(self, method: str, url: str, **kw: Any) -> _RustHTTPResponse:
        return await asyncio.to_thread(self._request, method, url, **kw)

    def _parse_events(self, parse_id: str) -> list[dict[str, Any]]:
        try:
            serialized_events = self._rust_client.parse_events_json(parse_id=parse_id)
            return [json.loads(event) for event in serialized_events]
        except Exception as e:
            _raise_as_document_ai_error(e)

    async def _parse_events_async(self, parse_id: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._parse_events, parse_id)


def _deserialize_error_response(resp: _RustHTTPResponse) -> ErrorResponse:
    """
    Handle error responses and return a structured ErrorResponse.
    """
    try:
        error_response = ErrorResponse.model_validate(resp.json())
        return error_response
    except (ValidationError, ValueError) as e:
        print(f"Failed to deserialize error response: {e}", file=sys.stderr)
        return ErrorResponse(
            message=str(resp.text),
            code=ErrorCode.INTERNAL_ERROR,
            trace_id=resp.headers.get("x-trace-id") or resp.headers.get("X-Trace-ID"),
            details=None,
        )


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


def _validate_file_input(
    file_id: Optional[str],
    file_url: Optional[str],
    raw_text: Optional[str],
    mime_type: Optional[MimeType],
):
    if file_id is None and file_url is None and raw_text is None:
        raise ValueError("One of file_id, file_url, or raw_text must be provided.")

    if file_id is not None and file_url is not None and raw_text is not None:
        raise ValueError("Only one of file_id, file_url, or raw_text can be provided.")

    if raw_text is not None and mime_type is None:
        raise ValueError("mime_type must be provided when raw_text is used.")

    if (
        file_id is not None
        and not file_id.startswith("tensorlake-")
        and not file_id.startswith("file_")
    ):
        raise ValueError("file_id must start with 'tensorlake-' or 'file_'.")

    if file_url is not None and not (
        file_url.startswith("http://") or file_url.startswith("https://")
    ):
        raise ValueError("file_url must start with 'http://' or 'https://'.")
