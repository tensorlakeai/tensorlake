import pickle
from enum import Enum

from pydantic import BaseModel


class FunctionCallType(Enum):
    REGULAR = 1
    REDUCER = 2


class FunctionCallNodeMetadata(BaseModel):
    """Data required to reconstruct a function call AST node."""

    # Function call node ID in the AST.
    nid: str
    type: FunctionCallType
    # Serialized metadata, either RegularFunctionCallMetadata or None if REDUCER call type.
    metadata: bytes | None

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @staticmethod
    def deserialize(data: bytes) -> "FunctionCallNodeMetadata":
        return pickle.loads(data)
