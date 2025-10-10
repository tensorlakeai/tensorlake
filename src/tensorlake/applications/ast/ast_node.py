from typing import Dict

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate


class ASTNode:
    """A node in the abstract syntax tree."""

    def __init__(self):
        # We need full sized nanoid here because we can run a request
        # for months and we don't want to ever collide these IDs between
        # function calls of the same request.
        self._id: str = nanoid_generate()
        self._serialized_metadata: bytes | None = None
        self._parent: "ASTNode | None" = None
        # ID -> AST
        # Data dependencies of the node.
        self._children: Dict[str, "ASTNode"] = {}

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

    @property
    def serialized_metadata(self) -> bytes | None:
        return self._serialized_metadata

    @serialized_metadata.setter
    def serialized_metadata(self, metadata: bytes) -> None:
        self._serialized_metadata = metadata

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
