from typing import Any

from .local_fs_blob_store import LocalFSBLOBStore
from .s3_blob_store import S3BLOBStore


class BLOBStore:
    """Dispatches generic BLOB store calls to their real backends."""

    def __init__(self):
        """Creates a BLOB store that uses the supplied BLOB stores."""
        self._local: LocalFSBLOBStore = LocalFSBLOBStore()
        self._s3: S3BLOBStore = S3BLOBStore()

    def get(self, uri: str, offset: int, size: int, logger: Any) -> bytes:
        """Returns binary data stored in BLOB with the supplied URI.

        Raises Exception on error. Raises KeyError if the BLOB doesn't exist.
        """
        if _is_file_uri(uri):
            return self._local.get(uri, offset, size, logger)
        else:
            return self._s3.get(uri, offset, size, logger)

    def put(self, uri: str, offset: int, data: bytes, logger: Any) -> None:
        """Stores the supplied binary data in a BLOB with the supplied URI.

        Overwrites existing BLOB. Raises Exception on error.
        """
        if _is_file_uri(uri):
            self._local.put(uri, offset, data, logger)
        else:
            self._s3.put(uri, offset, data, logger)


def _is_file_uri(uri: str) -> bool:
    return uri.startswith("file://")
