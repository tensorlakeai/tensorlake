from typing import Any

from .function_executor_pb2 import SerializedObject


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

        so: SerializedObject = getattr(self._message, field_name)
        so_validator: MessageValidator = MessageValidator(so)
        so_validator.required_field("encoding")
        so_validator.required_field("data")
        so_validator.required_field("encoding_version")

        return self
