"""
File-management helpers (upload, list, delete).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from retry import retry

from ._base import _BaseClient
from ._utils import _drop_none
from .common import PaginatedResult
from .files import FileInfo, FileUploader
from .models import PaginationDirection, Region


class _FilesMixin(_BaseClient):
    def __init__(
        self,
        api_key: str | None = None,
        server_url: Optional[str] = None,
        region: Optional[Region] = Region.US,
    ):
        """
        Initialize the FilesMixin with an API key and optional server URL.
        """
        super().__init__(api_key=api_key, server_url=server_url, region=region)
        self._uploader = FileUploader(
            api_key=self.api_key, server_url=server_url, region=region
        )

    def files(
        self,
        cursor: Optional[str] = None,
        direction: Optional[PaginationDirection] = None,
        limit: Optional[int] = None,
        filename: Optional[str] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
    ) -> PaginatedResult[FileInfo]:
        """
        List files uploaded to Tensorlake.

        Args:
            cursor: Optional cursor for pagination. If not provided, returns the first page.
            direction: Optional pagination direction (next or prev). Defaults to next.
            limit: Optional limit on the number of results per page. Defaults to 25.
            filename: Optional filter to return only files with a specific name. Name is case-sensitive.
            created_after: Optional filter to return only files created after a specific date (RFC 3339 format).
            created_before: Optional filter to return only files created before a specific date (RFC 3339 format).
        """
        params = _drop_none(
            {
                "cursor": cursor,
                "direction": direction.value if direction else None,
                "limit": limit,
                "filename": filename,
                "createdAfter": created_after,
                "createdBefore": created_before,
            }
        )

        resp = self._request_v1("GET", "files", params=params)
        return PaginatedResult[FileInfo].model_validate(resp.json())

    async def files_async(
        self,
        cursor: Optional[str] = None,
        direction: Optional[PaginationDirection] = None,
        limit: Optional[int] = None,
        filename: Optional[str] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
    ) -> PaginatedResult[FileInfo]:
        """
        List files uploaded to Tensorlake asynchronously.

        Args:
            cursor: Optional cursor for pagination. If not provided, returns the first page.
            direction: Optional pagination direction (next or prev). Defaults to next.
            limit: Optional limit on the number of results per page. Defaults to 25.
            filename: Optional filter to return only files with a specific name. Name is case-sensitive.
            created_after: Optional filter to return only files created after a specific date (RFC 3339 format).
            created_before: Optional filter to return only files created before a specific date (RFC 3339 format).
        """
        params = _drop_none(
            {
                "cursor": cursor,
                "direction": direction.value if direction else None,
                "limit": limit,
                "filename": filename,
                "createdAfter": created_after,
                "createdBefore": created_before,
            }
        )

        resp = await self._arequest_v1("GET", "files", params=params)
        return PaginatedResult[FileInfo].model_validate(resp.json())

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
