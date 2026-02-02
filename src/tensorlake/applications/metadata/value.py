from typing import Any

from pydantic import BaseModel


# SDK metadata about a value stored in TensorLake.
# The goal of this metadata is to store all the context required to deserialize a value.
# This decouples code paths that are doing deserialization from serialization code paths
# that depend on a lot of complicated contextual factors.
class ValueMetadata(BaseModel):
    # ID of the value, uniquness guarantees depend on how the field is set.
    # If the value is returned from an awaitable then the ID of the awaitable.
    id: str
    # Type hint of the value, either Type hint from function signature or type(value).
    type_hint: Any
    # None for File, otherwise the name of the serializer used.
    serializer_name: str | None
    # User provided content type for File.
    # Else serializer content type of the serialized value.
    content_type: str
