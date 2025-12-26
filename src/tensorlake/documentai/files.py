"""
This module contains the FileUploader class, which is used to upload files to the DocumentAI API.
"""

import asyncio
from pathlib import Path
from typing import Optional, Union

import httpx
from pydantic import BaseModel, Field

from .common import get_doc_ai_base_url
from .models import Region


class FileInfo(BaseModel):
    """
    Metadata from a file uploaded to DocumentAI.
    """

    id: str
    name: str
    file_size: int = Field(alias="fileSize")
    mime_type: str = Field(alias="mimeType")
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")


class FileUploader:
    """
    Private class for uploading files to DocumentAI.
    """

    def __init__(
        self, api_key: str, server_url: Optional[str] = None, region: Region = Region.US
    ):
        if not api_key:
            raise ValueError("API key is required for FileUploader.")

        self.api_key = api_key

        doc_ai_base_url = get_doc_ai_base_url(region=region, server_url=server_url)
        self._client = httpx.Client(base_url=doc_ai_base_url, timeout=None)
        self._async_client = httpx.AsyncClient(base_url=doc_ai_base_url, timeout=None)

        if server_url:
            self._client.base_url = f"{server_url}/documents/v2"
            self._async_client.base_url = f"{server_url}/documents/v2"

    def upload_file(self, file_path: Union[str, Path]):
        """
        Upload a file to the Tensorlake

        Args:
            file_path: Path to the file to upload

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            httpx.HTTPError: If the request fails
            FileNotFoundError: If the file doesn't exist
        """

        if file_path is str:
            if file_path.startswith("http://") or file_path.startswith("https://"):
                raise ValueError(
                    "file upload supports only local files. If you want to parse a remote file, please call the parse method with the remote file URL."
                )

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        with open(path, "rb") as f:
            files = {"file": (path.name, f)}
            response = self._client.put(
                url="files",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                },
                files=files,
            )

            if response.status_code >= 400:
                print(f"Error uploading file: {response.text}")
                raise RuntimeError(f"Error uploading file: {response.text}")

            resp = response.json()
            return resp.get("file_id")

    async def upload_file_async(self, path: Union[str, Path]) -> str:
        """
        Upload a file to the Tensorlake asynchronously.

        Args:
            file_path: Path to the file to upload

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            httpx.HTTPError: If the request fails
            FileNotFoundError: If the file doesn't exist
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_content = await asyncio.to_thread(path.read_bytes)
        files = {"file": (path.name, file_content)}
        response = await self._async_client.put(
            url="files",
            headers={
                "Authorization": f"Bearer {self.api_key}",
            },
            files=files,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        resp = response.json()
        return resp.get("file_id")

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
