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
from typing import Union

import httpx
from retry import retry

from tensorlake.documentai.common import DOC_AI_BASE_URL


class Files:
    """
    Class to interact with the Tensorlake Files API.

    Args:
        api_key: API key to use for authentication. If not provided, the value
            will be read from the TENSORLAKE_API_KEY environment variable.
    """

    def __init__(self, api_key: str=""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None, headers=self._headers())
        self._async_client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None, headers=self._headers())

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
        }

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
            return self._upload_large_file(path)
        
        with open(path, "rb") as f:
            files = {"file": (f.name, f)}
            response = self._client.post(
                url="files",
                headers=self._headers(),
                files=files,
            )
            response.raise_for_status()
            resp = response.json()
            return resp.get("id")
   
    def _upload_large_file(self, path: Union[str, Path]) -> str:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        checksum_sha256 = self._calculate_checksum_sha256(path)
        with open(path, "rb") as f:
            file_size = path.stat().st_size
            init_response =  self._client.post(
                url="files_large",
                headers=self._headers(),
                json={
                    "sha256_checksum": checksum_sha256,
                    "file_size": file_size,
                    "filename": path.name
                }
            )

            init_response.raise_for_status()
            init_response_json = init_response.json()
        
        presign_id = init_response_json.get("id")

        if presign_id.startswith("tensorlake-"):
            return presign_id

        presigned_url = init_response_json.get("presigned_url")

        upload_headers = {
            "Content-Type": "application/pdf",
            "Content-Length": str(file_size)
        }

        with open(path, "rb") as f:
            with httpx.Client() as upload_client:
                upload_response =  upload_client.put(
                    url=presigned_url,
                    data=f.read(),
                    headers=upload_headers,
                    timeout=httpx.Timeout(None)
                )
                upload_response.raise_for_status()

        finalize_response =  self._client.post(
            url=f"files_large/{presign_id}",
            headers=self._headers()
        )

        finalize_response.raise_for_status()
        finalize_response_json = finalize_response.json()
        return finalize_response_json.get("id")

    def _calculate_checksum_sha256(self, path: Union[str, Path]) -> str:
        hasher = hashlib.sha256()
        with open(path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
