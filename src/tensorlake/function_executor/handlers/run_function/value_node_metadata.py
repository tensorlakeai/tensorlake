import pickle

from pydantic import BaseModel


class ValueNodeMetadata(BaseModel):
    """Data required to reconstruct a value AST node."""

    # Node ID of the value node in the AST.
    nid: str
    # Serialized AST ValueMetadata
    metadata: bytes

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @staticmethod
    def deserialize(data: bytes) -> "ValueNodeMetadata":
        return pickle.loads(data)
