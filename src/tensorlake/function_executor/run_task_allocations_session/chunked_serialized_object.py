import hashlib
from typing import Union

from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    JsonSerializer,
)

from ..proto.function_executor_pb2 import (
    SerializedObjectEncoding,
    SerializedObjectManifest,
)


class ChunkedSerializedObject:
    """Class to handle chunked serialized objects in a session."""

    def __init__(self, manifest: SerializedObjectManifest):
        self._manifest: SerializedObjectManifest = manifest
        self._data = bytearray()

    def to_tensorlake_data(self) -> TensorlakeData:
        """Converts the serialized object to TensorlakeData.

        Raises ValueError if the conversion is not successful.
        """
        data: Union[str, bytes] = None
        encoder: str = None
        if (
            self._manifest.encoding
            == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
        ):
            data = self._data
            encoder = CloudPickleSerializer.encoding_type
        elif (
            self._manifest.encoding
            == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
        ):
            data = self._data.decode("utf-8")
            encoder = JsonSerializer.encoding_type
        else:
            raise ValueError(
                f"Unsupported serialized object encoding: {SerializedObjectEncoding.Name(self._manifest.encoding)}"
            )

        return TensorlakeData(
            input_id=self._manifest.id.value,
            payload=data,
            encoder=encoder,
        )

    def add_chunk(self, chunk: bytes):
        """Adds a chunk to the serialized object."""
        self._data.extend(chunk)

    def validate(self) -> bool:
        """Checks that this serialized object is complete and valid.

        Raises ValueError if the serialized object is not valid.
        """
        if len(self._data) != self._manifest.size:
            raise ValueError(
                f"Serialized object size mismatch: expected {self._manifest.size}, got {len(self._data)}"
            )
        data_hash: str = _sha256_hash(self._data)
        if data_hash != self._manifest.sha256_hash:
            raise ValueError(
                f"Serialized object hash mismatch: expected {self._manifest.sha256_hash}, got {data_hash}"
            )


def _sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
