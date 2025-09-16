from typing import Any

from .function_executor_pb2 import (
    BLOB,
    BLOBChunk,
    SerializedObjectInsideBLOB,
)


class MessageValidator:
    def __init__(self, message: Any):
        self._message = message

    def required_field(self, field_name: str) -> "MessageValidator":
        if not self._message.HasField(field_name):
            raise ValueError(
                f"Field '{field_name}' is required in {type(self._message).__name__}"
            )
        return self

    def not_set_field(self, field_name: str) -> "MessageValidator":
        """Validates that the field is not set.

        Raises: ValueError: If the field is set."""
        if self._message.HasField(field_name):
            raise ValueError(
                f"Field '{field_name}' should not be set in {type(self._message).__name__}"
            )
        return self

    def required_serialized_object(self, field_name: str) -> "MessageValidator":
        """Validates the SerializedObject.

        Raises: ValueError: If the SerializedObject is invalid or not present."""
        self.required_field(field_name)
        return self.optional_serialized_object(field_name)

    def optional_serialized_object(self, field_name: str) -> "MessageValidator":
        """Validates the SerializedObject.

        Raises: ValueError: If the SerializedObject is invalid."""
        if not self._message.HasField(field_name):
            return self
        (
            MessageValidator(getattr(self._message, field_name))
            .required_serialized_object_manifest("manifest")
            .required_field("data")
        )

        return self

    def required_serialized_object_manifest(
        self, field_name: str
    ) -> "MessageValidator":
        """Validates the SerializedObjectManifest.

        Raises: ValueError: If the SerializedObjectManifest is invalid or not present.
        """
        self.required_field(field_name)
        (
            MessageValidator(getattr(self._message, field_name))
            .required_field("encoding")
            .required_field("encoding_version")
            .required_field("size")
            .required_field("sha256_hash")
        )

        return self

    def required_blob(self, field_name: str) -> "MessageValidator":
        """Validates the BLOB.

        Raises: ValueError: If the BLOB is invalid or not present."""
        self.required_field(field_name)
        blob: BLOB = getattr(self._message, field_name)
        _validate_blob(blob)
        return self

    def required_blobs(self, field_name: str) -> "MessageValidator":
        """Validates the repeated BLOBs.

        Raises: ValueError: If the BLOBs are invalid or not present."""
        blobs = getattr(self._message, field_name)
        is_present: bool = False
        for blob in blobs:
            _validate_blob(blob)
            is_present = True

        if not is_present:
            raise ValueError(
                f"BLOBs '{field_name}' must contain at least one valid BLOB"
            )
        return self

    def optional_blob(self, field_name: str) -> "MessageValidator":
        """Validates the BLOB.

        Raises: ValueError: If the BLOB is invalid."""
        if not self._message.HasField(field_name):
            return self

        return self.required_blob(field_name)

    def required_serialized_objects_inside_blob(
        self, field_name: str
    ) -> "MessageValidator":
        """Validates that at least one SerializaedObjectInsideBLOB is present in the field.

        Raises: ValueError: If the SerializedObjectsInsideBLOB is invalid or not present.
        """
        is_present: True = False
        for so in getattr(self._message, field_name):
            is_present = True
            _validate_serialized_objects_inside_blob(so)

        if not is_present:
            raise ValueError(
                f"Field '{field_name}' must contain at least one SerializedObjectInsideBLOB"
            )

        return self

    def required_serialized_object_inside_blob(
        self, field_name: str
    ) -> "MessageValidator":
        """Validates the SerializedObjectInsideBLOB.

        Raises: ValueError: If the SerializedObjectInsideBLOB is invalid or not present.
        """
        self.required_field(field_name)
        return self.optional_serialized_object_inside_blob(field_name)

    def optional_serialized_object_inside_blob(
        self, field_name: str
    ) -> "MessageValidator":
        """Validates the SerializedObjectInsideBLOB.

        Raises: ValueError: If the SerializedObjectInsideBLOB is invalid."""
        if not self._message.HasField(field_name):
            return self
        _validate_serialized_objects_inside_blob(getattr(self._message, field_name))

        return self


def _validate_serialized_objects_inside_blob(so: SerializedObjectInsideBLOB) -> None:
    MessageValidator(so).required_field("manifest").required_field("offset")


def _validate_blob(blob: BLOB) -> None:
    if len(blob.chunks) < 1:
        raise ValueError("BLOB must have at least one chunk")
    for chunk in blob.chunks:
        _validate_blob_chunk(chunk)


def _validate_blob_chunk(blob_chunk: BLOBChunk) -> None:
    """Validates the BLOB chunk.

    Raises: ValueError: If the BLOB chunk is invalid or not present."""
    (MessageValidator(blob_chunk).required_field("uri").required_field("size"))
