from __future__ import annotations

import os
from typing import Any, Dict

import httpx

from .common import DOC_AI_BASE_URL, DOC_AI_BASE_URL_V2


class _BaseClient:
    """
    Handles auth, session objects and raw request helpers.
    All high-level mixins inherit from this class.
    """

    def __init__(self, api_key: str | None, server_url: str | None):
        self.api_key: str = api_key or os.getenv("TENSORLAKE_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "API key is required. Set TENSORLAKE_API_KEY or pass api_key."
            )

        self._client_v1 = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None)
        self._aclient_v1 = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL_V2, timeout=None)
        self._aclient = httpx.AsyncClient(base_url=DOC_AI_BASE_URL_V2, timeout=None)

        if server_url:
            self._client_v1.base_url = f"{server_url}/documents/v1"
            self._aclient_v1.base_url = f"{server_url}/documents/v1"
            self._client.base_url = f"{server_url}/documents/v2"
            self._aclient.base_url = f"{server_url}/documents/v2"

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
        resp.raise_for_status()
        return resp

    async def _arequest_v1(self, method: str, url: str, **kw: Any) -> httpx.Response:
        resp = await self._aclient_v1.request(
            method, url, headers=self._headers(), **kw
        )
        resp.raise_for_status()
        return resp

    async def _arequest(self, method: str, url: str, **kw: Any) -> httpx.Response:
        resp = await self._aclient.request(method, url, headers=self._headers(), **kw)
        resp.raise_for_status()
        return resp
