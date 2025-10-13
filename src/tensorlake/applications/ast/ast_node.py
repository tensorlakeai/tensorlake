import pickle
from typing import Dict

from pydantic import BaseModel


class ASTNodeMetadata(BaseModel):
    nid: str  # Node ID in the AST

    def serialize(self) -> bytes:
        # Use pickle it dumps a class references and loads
        # original serialized instance type so we don't have
        # to store .node_type enum in metadata.
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data: bytes) -> "ASTNodeMetadata":
        return pickle.loads(data)


class ASTNode:
    """A node in the abstract syntax tree."""

    def __init__(self, id: str):
        self._id: str = id
        self._parent: "ASTNode | None" = None
        # ID -> AST
        # Data dependencies of the node.
        self._children: Dict[str, "ASTNode"] = {}
        self._metadata: ASTNodeMetadata | None = None

    @property
    def metadata(self) -> ASTNodeMetadata | None:
        return self._metadata

    @metadata.setter
    def metadata(self, value: ASTNodeMetadata) -> None:
        self._metadata = value

    @property
    def id(self) -> str:
        return self._id

    @property
    def parent(self) -> "ASTNode | None":
        return self._parent

    @parent.setter
    def parent(self, parent: "ASTNode | None") -> None:
        self._parent = parent

    @property
    def children(self) -> Dict[str, "ASTNode"]:
        return self._children

    def __eq__(self, value):
        if not isinstance(value, ASTNode):
            return False
        return self.id == value.id

    def replace_child(self, old_child: "ASTNode", new_child: "ASTNode") -> None:
        """Replaces an old child node with a new child node.

        The old child node is not valid after the replacement."""
        if old_child.id in self.children:
            self.children[old_child.id] = new_child
            new_child._id = old_child.id
            new_child.parent = self
            old_child.parent = None
            self._children[new_child.id] = new_child
        else:
            raise ValueError(
                f"Old child with id {old_child.id} is not a child of parent node with id {self.id}"
            )

    def add_child(self, child: "ASTNode") -> None:
        self.children[child.id] = child
        child.parent = self
