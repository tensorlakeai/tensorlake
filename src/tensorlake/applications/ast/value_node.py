import pickle
from typing import Any

from pydantic import BaseModel

from ..interface.file import File
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .ast_node import ASTNode


class ValueMetadata(BaseModel):
    # Python class of the serialized value object
    cls: Any
    # Content type for File.
    # Serializer name for non-File.
    extra: str

    def serialize(self) -> bytes:
        # Use pickle because only it can dump a class reference (cls).
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data: bytes) -> "ValueMetadata":
        return pickle.loads(data)

    def deserialize_value(self, value: bytes) -> Any:
        if self.cls is File:
            return File(content=value, content_type=self.extra)
        else:
            return serializer_by_name(self.extra).deserialize(value, [self.cls])

    @property
    def serializer_name(self) -> str | None:
        if self.cls is File:
            return None
        else:
            return self.extra


class ValueNode(ASTNode):
    """A node that represents a value ready to use.

    Children are always an empty list.
    """

    def __init__(
        self,
        value: bytes,
    ):
        super().__init__()
        self._value: bytes = value
        # A workaround to not deserialize metadata to get file content type when generating function output in FE.
        self._content_type: str | None = None

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def content_type(self) -> str | None:
        return self._content_type

    def replace_child(self, old_child: "ASTNode", new_child: "ASTNode") -> None:
        raise ValueError("Value node has no children to replace")

    def to_value(self) -> Any:
        """Converts the value node back to its original value."""
        metadata: ValueMetadata = ValueMetadata.deserialize(self.serialized_metadata)
        return metadata.deserialize_value(self._value)

    @classmethod
    def from_value(cls, value: Any, user_serializer: UserDataSerializer) -> "ValueNode":
        if isinstance(value, File):
            node: ValueNode = ValueNode(value.content)
            node.serialized_metadata = ValueMetadata(
                cls=File, extra=value.content_type
            ).serialize()
            node._content_type = value.content_type
            return node
        else:
            node: ValueNode = ValueNode(user_serializer.serialize(value))
            node.serialized_metadata = ValueMetadata(
                cls=type(value), extra=user_serializer.name
            ).serialize()
            return node

    @classmethod
    def from_serialized(
        cls, node_id: str, value: bytes, metadata: bytes
    ) -> "ValueNode":
        node: ValueNode = ValueNode(value)
        node._id = node_id
        node.serialized_metadata = metadata
        return node
