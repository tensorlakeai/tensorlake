from dataclasses import dataclass
from typing import Dict

# A fake blob store that mimics storing and retrieving blobs locally.


# This class voguely resembles Server DataPayload.
@dataclass
class BLOB:
    id: str
    data: bytes
    serializer_name: (
        str | None
    )  # Not None when this is a serialized object, otherwise raw file bytes.
    content_type: str


class BLOBStore:
    def __init__(self):
        self._store: Dict[str, BLOB] = {}

    def put(self, blob: BLOB):
        if blob.id in self._store:
            raise ValueError(
                f"BLOB store put failed: blob with id {blob.id} already exists."
            )
        self._store[blob.id] = blob

    def get(self, blob_id: str) -> BLOB:
        if blob_id not in self._store:
            raise ValueError(
                f"BLOB store get failed: blob with id {blob_id} does not exist."
            )
        return self._store[blob_id]

    def delete(self, blob_id: str):
        if blob_id not in self._store:
            raise ValueError(
                f"BLOB store delete failed: blob with id {blob_id} does not exist."
            )
        del self._store[blob_id]
