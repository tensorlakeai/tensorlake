import httpx
import os
from typing import Union
from pathlib import Path
from retry import retry
from tensorlake.documentai.common import DOC_AI_BASE_URL


class Files:

    def __init__(self, api_key: str=""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None, headers=self._headers())

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
            Dict containing the API response

        Raises:
            httpx.HTTPError: If the request fails
            FileNotFoundError: If the file doesn't exist
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
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