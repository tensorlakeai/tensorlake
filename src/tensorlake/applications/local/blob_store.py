from dataclasses import dataclass, replace
from typing import Any, Dict

# A fake blob store that mimics storing and retrieving blobs locally.


# This class voguely resembles Server DataPayload + ValueMetadata
# that we use in remote mode.
@dataclass
class BLOB:
    id: str
    data: bytes
    serializer_name: (
        str | None
    )  # Not None when this is a serialized object, otherwise raw file bytes.
    content_type: str
    cls: Any  # Python class of the serialized value object

    def copy(self) -> "BLOB":
        return replace(self)


class BLOBStore:
    def __init__(self):
        self._store: Dict[str, BLOB] = {}

    def put(self, blob: BLOB):
        if blob.id in self._store:
            raise ValueError(
                f"BLOB store put failed: blob with id {blob.id} already exists."
            )
        self._store[blob.id] = blob.copy()

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
