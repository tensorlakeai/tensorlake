"""
Module to interact with the Tensorlake Files API.

The Files API allows you to upload files to the Tensorlake platform.

Example:
    >>> from tensorlake.documentai import Files
    >>> files = Files(api_key="YOUR_API_KEY")
    >>> file_id = files.upload("path/to/file.pdf")
    >>> print(file_id)
    "tensorlake-ID"
"""

import os
import hashlib
from pathlib import Path
from typing import AsyncGenerator, Union

import aiofiles
import httpx
import magic
from retry import retry

from tensorlake.documentai.common import DOC_AI_BASE_URL


class Files:
    """
    Class to interact with the Tensorlake Files API.

    Args:
        api_key: API key to use for authentication. If not provided, the value
            will be read from the TENSORLAKE_API_KEY environment variable.
    """

    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

        self._client = httpx.Client(
            base_url=DOC_AI_BASE_URL, timeout=None, headers=self.__headers__()
        )
        self._async_client = httpx.AsyncClient(
            base_url=DOC_AI_BASE_URL, timeout=None, headers=self.__headers__()
        )

    def __headers__(self):
        return {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}

    retry(tries=10, delay=2)
    def upload(self, path: Union[str, Path]) -> str:
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
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        # check if file is longer than 10 mb
        if path.stat().st_size > 10 * 1024 * 1024:
            return self.__upload_large_file__(path)

        with open(path, "rb") as f:
            files = {"file": (f.name, f)}
            response = self._client.post(
                url="files",
                headers=self.__headers__(),
                files=files,
            )
            response.raise_for_status()
            resp = response.json()
            return resp.get("id")

    retry(tries=10, delay=2)
    async def upload_async(self, path: Union[str, Path]) -> str:
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

        if path.stat().st_size > 10 * 1024 * 1024:
            return await self.__upload_large_file_async__(path)

        async with aiofiles.open(path, "rb") as f:
            files = {"file": (path.name, await f.read())}
            response = await self._async_client.post(
                url="files",
                headers=self.__headers__(),
                files=files,
            )
            response.raise_for_status()
            resp = response.json()
            return resp.get("id")

    def __upload_large_file__(self, path: Union[str, Path]) -> str:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_size = path.stat().st_size
        # Initialize upload request
        init_response = self._client.post(
            url="files_large",
            headers=self.__headers__(),
            json={
                "sha256_checksum": self.__calculate_checksum_sha256__(path),
                "file_size": file_size,
                "filename": path.name,
            },
        )
        init_response.raise_for_status()
        init_response_json = init_response.json()

        presign_id = init_response_json.get("id")
        if presign_id.startswith("tensorlake-"):
            return presign_id

        with open(path, "rb") as f:
            with httpx.Client() as upload_client:
                upload_response = upload_client.put(
                    url=init_response_json.get("presigned_url"),
                    data=f.read(),
                    headers={
                        "Content-Type": self.__get_mime_type__(path),
                        "Content-Length": str(file_size),
                    },
                    timeout=httpx.Timeout(None),
                )
                upload_response.raise_for_status()

        print("\nUpload complete!")

        # Finalize the upload
        finalize_response = self._client.post(
            url=f"files_large/{presign_id}", headers=self.__headers__()
        )
        finalize_response.raise_for_status()
        return finalize_response.json().get("id")

    def __calculate_checksum_sha256__(self, path: Union[str, Path]) -> str:
        hasher = hashlib.sha256()
        with open(path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    async def __upload_large_file_async__(self, path: Union[str, Path]) -> str:
        """
        Asynchronously upload large files to Tensorlake
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        checksum_sha256 = await self.__calculate_checksum_sha256_async__(path)
        file_size = path.stat().st_size

        init_response = await self._async_client.post(
            url="files_large",
            headers=self.__headers__(),
            json={
                "sha256_checksum": checksum_sha256,
                "file_size": file_size,
                "filename": path.name,
            },
        )
        init_response.raise_for_status()
        init_response_json = init_response.json()

        presign_id = init_response_json.get("id")
        if presign_id.startswith("tensorlake-"):
            return presign_id

        async with httpx.AsyncClient() as upload_client:
            upload_response = await upload_client.put(
                url=init_response_json.get("presigned_url"),
                data=self.__file_chunk_reader__(path),
                headers={
                    "Content-Type": self.__get_mime_type__(path),
                    "Content-Length": str(file_size),
                },
                timeout=httpx.Timeout(None),
            )
            upload_response.raise_for_status()

        finalize_response = await self._async_client.post(
            url=f"files_large/{presign_id}", headers=self.__headers__()
        )
        finalize_response.raise_for_status()
        finalize_response_json = finalize_response.json()

        return finalize_response_json.get("id")

    async def __file_chunk_reader__(
        self, path: Path, chunk_size: int = 10 * 1024 * 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        Generator that reads a file in chunks asynchronously.
        """
        async with aiofiles.open(path, "rb") as file:
            while chunk := await file.read(chunk_size):
                yield chunk

    async def __calculate_checksum_sha256_async__(self, path: Union[str, Path]) -> str:
        hasher = hashlib.sha256()
        async with aiofiles.open(path, "rb") as file:
            while chunk := await file.read(4096):
                hasher.update(chunk)
        return hasher.hexdigest()

    def __get_mime_type__(self, path: Union[str, Path]) -> str:
        """
        Get the mime type of a file
        """
        mime = magic.Magic(mime=True)
        return mime.from_file(str(path))
