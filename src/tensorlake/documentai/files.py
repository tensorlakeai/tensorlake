"""
This module contains the FileUploader class, which is used to upload files to the DocumentAI API.
"""

import asyncio
import json
from io import BytesIO
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, Field

from .common import get_doc_ai_base_url
from .models import Region

UploadSource = Union[str, Path, BytesIO]

try:
    from tensorlake._cloud_sdk import CloudDocumentAIClient as RustCloudDocumentAIClient

    _RUST_DOCUMENT_AI_CLIENT_AVAILABLE = True
except Exception:
    try:
        from _cloud_sdk import CloudDocumentAIClient as RustCloudDocumentAIClient

        _RUST_DOCUMENT_AI_CLIENT_AVAILABLE = True
    except Exception:
        RustCloudDocumentAIClient = None
        _RUST_DOCUMENT_AI_CLIENT_AVAILABLE = False


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

        if not _RUST_DOCUMENT_AI_CLIENT_AVAILABLE:
            raise ValueError(
                "Rust Document AI client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        self.api_key = api_key

        doc_ai_base_url = get_doc_ai_base_url(region=region, server_url=server_url)
        self._rust_client = RustCloudDocumentAIClient(
            api_url=doc_ai_base_url,
            api_key=self.api_key,
        )

    def upload_file(self, file_path: UploadSource) -> str:
        """
        Upload a file to Tensorlake.

        Args:
            file_path: Path to the file to upload, or an in-memory BytesIO buffer.
                BytesIO uploads use the buffer's ``name`` attribute as the file
                name when present, otherwise ``upload.bin``.

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        if isinstance(file_path, BytesIO):
            return self._upload_content(
                file_name=self._bytes_io_file_name(file_path),
                content=file_path.getvalue(),
            )

        if isinstance(file_path, str):
            if file_path.startswith("http://") or file_path.startswith("https://"):
                raise ValueError(
                    "file upload supports only local files. If you want to parse a remote file, please call the parse method with the remote file URL."
                )

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        return self._upload_content(file_name=path.name, content=path.read_bytes())

    def _upload_content(self, file_name: str, content: bytes) -> str:
        response_json = self._rust_client.upload_file_json(
            file_name=file_name, content=content
        )
        payload = json.loads(response_json)
        status_code = int(payload.get("status_code", 500))
        body = payload.get("body", "")

        if status_code >= 400:
            print(f"Error uploading file: {body}")
            raise RuntimeError(f"Error uploading file: {body}")

        try:
            return json.loads(body).get("file_id")
        except Exception as e:
            raise RuntimeError(f"Invalid upload response payload: {body}") from e

    @staticmethod
    def _bytes_io_file_name(file_obj: BytesIO) -> str:
        name = getattr(file_obj, "name", None)
        if isinstance(name, (str, Path)):
            file_name = Path(name).name
            if file_name:
                return file_name
        return "upload.bin"

    async def upload_file_async(self, path: UploadSource) -> str:
        """
        Upload a file to Tensorlake asynchronously.

        Args:
            file_path: Path to the file to upload, or an in-memory BytesIO buffer.

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        return await asyncio.to_thread(self.upload_file, path)

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
