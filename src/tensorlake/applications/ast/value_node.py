from typing import Any

from ..interface.file import File
from ..interface.futures import request_scoped_id
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .ast_node import ASTNode, ASTNodeMetadata


class ValueMetadata(ASTNodeMetadata):
    # Python class of the serialized value object
    cls: Any
    # Content type for File.
    # Serializer name for non-File.
    extra: str

    @property
    def serializer_name(self) -> str | None:
        if self.cls is File:
            return None
        else:
            return self.extra

    def serialize_value(self, value: Any) -> bytes:
        if self.serializer_name is None:
            return value  # file bytes
        else:
            return serializer_by_name(self.serializer_name).serialize(value)

    def deserialize_value(self, value: bytes) -> Any:
        if self.serializer_name is None:
            return File(content=value, content_type=self.extra)
        else:
            return serializer_by_name(self.serializer_name).deserialize(
                value, [self.cls]
            )


class ValueNode(ASTNode):
    """A node that represents a value ready to use.

    Children are always an empty list.
    """

    def __init__(self, id: str):
        super().__init__(id)
        self._value: None | Any = None
        self._metadata: ValueMetadata | None = None

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, value: Any) -> None:
        self._value = value

    def replace_child(self, old_child: "ASTNode", new_child: "ASTNode") -> None:
        raise ValueError("Value node has no children to replace")

    @classmethod
    def from_value(cls, value: Any, user_serializer: UserDataSerializer) -> "ValueNode":
        if isinstance(value, File):
            node: ValueNode = ValueNode(id=request_scoped_id())
            node.value = value.content
            node.metadata = ValueMetadata(
                nid=node.id, cls=File, extra=value.content_type
            )
            return node
        else:
            node: ValueNode = ValueNode(id=request_scoped_id())
            node.value = value
            node.metadata = ValueMetadata(
                nid=node.id, cls=type(value), extra=user_serializer.name
            )
            return node

    @classmethod
    def from_serialized(
        cls, node_id: str, value: bytes, metadata: bytes
    ) -> "ValueNode":
        node: ValueNode = ValueNode(id=node_id)
        node.metadata = ValueMetadata.deserialize(metadata)
        node.value = node.metadata.deserialize_value(value)
        return node
