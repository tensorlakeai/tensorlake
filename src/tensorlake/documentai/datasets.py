"""
DocumentAI datasets module.
"""

from pathlib import Path
from typing import Union

import httpx

from tensorlake.documentai.common import DOC_AI_BASE_URL


class Dataset:
    """DocumentAI dataset class."""

    def __init__(self, dataset_id: str, name: str, api_key: str):
        self.id = dataset_id
        self.name = name
        self.api_key = api_key

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None)
        self._async_client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)

    def add_file(self, file_path: Union[str, Path]):
        """Add a file to the dataset."""
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File {path} not found")

        pass
