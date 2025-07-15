import asyncio
import os
import os.path
from typing import Any


class LocalFSBLOBStore:
    """BLOB store that stores BLOBs in local file system."""

    def get(self, uri: str, offset: int, size: int, logger: Any) -> bytes:
        """Returns binary data stored in file at the supplied URI and offset.

        The URI must be a file URI (starts with "file://"). The path must be absolute.
        Raises Exception on error. Raises KeyError if the file doesn't exist.
        """
        blob_path: str = _blob_path_from_uri(uri)
        if not os.path.isabs(blob_path):
            raise ValueError(f"BLOB file path {blob_path} must be absolute")

        if os.path.exists(blob_path):
            with open(blob_path, mode="rb") as blob_file:
                blob_file.seek(offset)
                return blob_file.read(size)
        else:
            raise KeyError(f"BLOB file at {blob_path} does not exist")

    def put(self, uri: str, offset: int, data: bytes, logger: Any) -> None:
        """Stores the supplied binary data in a file at the supplied URI and offset.

        The URI must be a file URI (starts with "file://"). The path must be absolute.
        Overwrites existing file. Raises Exception on error.
        """
        # Run synchronous code in a thread to not block the event loop.
        blob_path: str = _blob_path_from_uri(uri)
        if not os.path.isabs(blob_path):
            raise ValueError(f"BLOB file path {blob_path} must be absolute")

        os.makedirs(os.path.dirname(blob_path), exist_ok=True)
        with open(blob_path, mode="ab") as blob_file:
            blob_file.seek(offset)
            blob_file.write(data)


def _blob_path_from_uri(uri: str) -> str:
    return uri[7:]  # strip "file://" prefix
