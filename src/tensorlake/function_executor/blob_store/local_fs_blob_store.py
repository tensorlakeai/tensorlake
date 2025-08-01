import hashlib
import os
import os.path
from itertools import chain
from typing import List

from ..logger import FunctionExecutorLogger


class LocalFSBLOBStore:
    """BLOB store that stores BLOBs in local file system."""

    def get(
        self,
        uri: str,
        offset: int,
        destination: memoryview,
        logger: FunctionExecutorLogger,
    ) -> None:
        """Reads binary data stored in file at the supplied URI and offset into the destination memoryview.

        The URI must be a file URI (starts with "file://"). The path must be absolute.
        Raises Exception on error.
        """
        blob_path: str = _blob_path_from_uri(uri)
        if not os.path.isabs(blob_path):
            raise ValueError(f"BLOB file path {blob_path} must be absolute")

        if os.path.exists(blob_path):
            with open(blob_path, mode="rb") as blob_file:
                blob_file.seek(offset)
                # memoryview ensures that the slice we pass points at destination.
                blob_file.readinto(destination)
        else:
            raise KeyError(f"BLOB file at {blob_path} does not exist")

    def put(
        self,
        uri: str,
        offset: int,
        source: List[memoryview],
        logger: FunctionExecutorLogger,
    ) -> str:
        """Stores the supplied memoryviews of binary data in a file at the supplied URI and offset.

        The URI must be a file URI (starts with "file://"). The path must be absolute.
        The file must exist. Raises Exception on error.
        Returns the ETag of the stored data.
        """
        blob_path: str = _blob_path_from_uri(uri)
        if not os.path.isabs(blob_path):
            raise ValueError(f"BLOB file path {blob_path} must be absolute")

        os.makedirs(os.path.dirname(blob_path), exist_ok=True)
        hasher: hashlib.md5 = hashlib.md5()
        with open(blob_path, mode="rb+") as blob_file:
            blob_file.seek(offset)  # Adds zeroes if the file is smaller than offset.
            for source_data in source:
                blob_file.write(source_data)
                hasher.update(source_data)

        return hasher.hexdigest()


def _blob_path_from_uri(uri: str) -> str:
    return uri[7:]  # strip "file://" prefix
