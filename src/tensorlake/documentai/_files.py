"""
File-management helpers (upload, list, delete).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from retry import retry

from ._base import _BaseClient
from .common import PaginatedResult
from .files import FileInfo, FileUploader


class _FilesMixin(_BaseClient):
    def __init__(self, api_key: str | None = None, server_url: Optional[str] = None):
        """
        Initialize the FilesMixin with an API key and optional server URL.
        """
        super().__init__(api_key, server_url)
        self._uploader = FileUploader(api_key=self.api_key, server_url=server_url)

    def files(self, cursor: Optional[str] = None) -> PaginatedResult[FileInfo]:
        """
        List files uploaded to Tensorlake.

        Args:
            cursor: Optional cursor for pagination. If not provided, returns the first page.
        """
        resp = self._request_v1(
            "GET", "files", params={"cursor": cursor} if cursor else None
        )
        return PaginatedResult[FileInfo].model_validate(resp.json())

    async def files_async(
        self, cursor: Optional[str] = None
    ) -> PaginatedResult[FileInfo]:
        """
        List files uploaded to Tensorlake asynchronously.

        Args:
            cursor: Optional cursor for pagination. If not provided, returns the first page.
        """
        resp = await self._arequest_v1(
            "GET", "files", params={"cursor": cursor} if cursor else None
        )
        return PaginatedResult[FileInfo].model_validate(resp.json())

    @retry(tries=10, delay=2)
    def upload(self, path: Union[str, Path]) -> str:
        """
        Upload a file to Tensorlake.

        Args:
            file_path: Path to the file to upload
        """
        return self._uploader.upload_file(path)

    @retry(tries=10, delay=2)
    async def upload_async(self, path: Union[str, Path]) -> str:
        """
        Upload a file to Tensorlake asynchronously.
        Args:
            file_path: Path to the file to upload
        """
        return await self._uploader.upload_file_async(path)

    def delete_file(self, file_id: str) -> None:
        """
        Delete a file from Tensorlake.

        Deleting a file will remove it from the system and it cannot be recovered.

        Deleting a file does not delete any results that were generated from it, such as parsed documents.

        Args:
            file_id: The ID of the file to delete. This is the string returned by the upload method.
        """
        self._request_v1("DELETE", f"files/{file_id}")

    async def delete_file_async(self, file_id: str) -> None:
        """
        Delete a file from Tensorlake asynchronously.

        Deleting a file will remove it from the system and it cannot be recovered.

        Deleting a file does not delete any results that were generated from it, such as parsed documents.

        Args:
            file_id: The ID of the file to delete. This is the string returned by the upload method.
        """
        await self._arequest_v1("DELETE", f"files/{file_id}")
