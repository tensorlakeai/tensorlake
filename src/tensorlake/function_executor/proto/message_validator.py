from typing import Any


class MessageValidator:
    def __init__(self, message: Any):
        self._message = message

    def required_field(self, field_name: str) -> "MessageValidator":
        if not self._message.HasField(field_name):
            raise ValueError(
                f"Field '{field_name}' is required in {type(self._message).__name__}"
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
        )

        return self

    def required_blob(self, field_name: str) -> "MessageValidator":
        """Validates the BLOB.

        Raises: ValueError: If the BLOB is invalid or not present."""
        self.required_field(field_name)
        (MessageValidator(getattr(self._message, field_name)).required_field("uri"))

        return self

    def required_serialized_object_blob(self, field_name: str) -> "MessageValidator":
        """Validates the SerializedObjectBLOB.

        Raises: ValueError: If the SerializedObjectBLOB is invalid or not present."""
        self.required_field(field_name)
        return self.optional_serialized_object_blob(field_name)

    def optional_serialized_object_blob(self, field_name: str) -> "MessageValidator":
        """Validates the SerializedObjectBLOB.

        Raises: ValueError: If the SerializedObjectBLOB is invalid."""
        if not self._message.HasField(field_name):
            return self
        (
            MessageValidator(getattr(self._message, field_name))
            .required_serialized_object_manifest("manifest")
            .required_blob("blob")
            .required_field("offset")
        )

        return self
