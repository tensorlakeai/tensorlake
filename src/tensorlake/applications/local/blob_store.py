from dataclasses import dataclass
from typing import Dict

from ..metadata import ValueMetadata

# A fake blob store that mimics storing and retrieving blobs locally.


# This class resembles Server DataPayload that we use in remote mode.
@dataclass
class BLOB:
    data: bytes
    metadata: ValueMetadata

    def copy(self) -> "BLOB":
        return BLOB(
            data=bytes(self.data),
            metadata=self.metadata.model_copy(deep=True),
        )


class BLOBStore:
    def __init__(self):
        self._store: Dict[str, BLOB] = {}

    def put(self, blob: BLOB):
        if blob.metadata.id in self._store:
            raise ValueError(
                f"BLOB store put failed: blob with id {blob.metadata.id} already exists."
            )
        self._store[blob.metadata.id] = blob.copy()

    def get(self, blob_id: str) -> BLOB:
        if blob_id not in self._store:
            raise ValueError(
                f"BLOB store get failed: blob with id {blob_id} does not exist."
            )
        return self._store[blob_id].copy()

    def has(self, blob_id: str) -> bool:
        return blob_id in self._store

    def delete(self, blob_id: str):
        if blob_id not in self._store:
            raise ValueError(
                f"BLOB store delete failed: blob with id {blob_id} does not exist."
            )
        del self._store[blob_id]
