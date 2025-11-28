import os.path
from dataclasses import dataclass
from typing import Dict

from tensorlake.applications.blob_store import BLOB, BLOBChunk, BLOBStore
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.metadata import ValueMetadata


@dataclass
class SerializedValue:
    data: bytes
    metadata: ValueMetadata


class SerializedValueStore:
    """A convenient wrapper over local BLOBStore that allows storing serialized values and their metadata."""

    def __init__(
        self,
        blob_store_dir_path: str,
        blob_store: BLOBStore,
        logger: InternalLogger,
    ):
        self._value_store_dir_path: str = os.path.join(
            blob_store_dir_path, "value_store"
        )
        self._blob_store: BLOBStore = blob_store
        self._logger: InternalLogger = logger.bind(module=__name__)
        # Store metadata in memory and actual data in local file system.
        # Value ID -> ValueMetadata
        self._metadata: Dict[str, ValueMetadata] = {}

    def put(self, value: SerializedValue) -> None:
        if value.metadata.id in self._metadata:
            raise ValueError(
                f"SerializedValueStore put failed: value with id {value.metadata.id} already exists."
            )
        self._metadata[value.metadata.id] = value.metadata.model_copy()
        # Use single chunk BLOBs because local FS operations don't need parallelism.
        # They are much faster than S3 already.
        self._blob_store.put(
            blob=self._value_blob(
                value_id=value.metadata.id, value_size=len(value.data)
            ),
            data=[value.data],
            logger=self._logger,
        )

    def get(self, value_id: str) -> SerializedValue:
        if value_id not in self._metadata:
            raise ValueError(
                f"SerializedValueStore get failed: value with id {value_id} does not exist."
            )
        metadata: ValueMetadata = self._metadata[value_id].model_copy()
        value_size: int = self._stored_value_size(value_id)
        data: bytes = self._blob_store.get(
            blob=self._value_blob(value_id=value_id, value_size=value_size),
            offset=0,
            size=value_size,
            logger=self._logger,
        )
        return SerializedValue(
            data=data,
            metadata=metadata,
        )

    def has(self, value_id: str) -> bool:
        return value_id in self._metadata

    def _stored_value_size(self, value_id: str) -> int:
        return os.path.getsize(self._value_file_path(value_id))

    def _value_file_path(self, value_id: str) -> str:
        return os.path.join(self._value_store_dir_path, value_id)

    def _value_blob_file_uri(self, value_id: str) -> str:
        return "file://" + self._value_file_path(value_id)

    def _value_blob(self, value_id: str, value_size: int) -> BLOB:
        # Use single chunk BLOBs because local FS operations don't need parallelism.
        # They are much faster than S3 already.
        return BLOB(
            id=value_id,
            chunks=[
                BLOBChunk(
                    uri=self._value_blob_file_uri(value_id),
                    size=value_size,
                    etag=None,
                )
            ],
        )
