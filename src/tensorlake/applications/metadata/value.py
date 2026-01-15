from typing import Any

from pydantic import BaseModel


# SDK metadata about a value stored in TensorLake.
class ValueMetadata(BaseModel):
    # ID of the value, uniquness guarantees depend on how the field is set.
    # If the value is returned from an awaitable then the ID of the awaitable.
    id: str
    # Type hint of the value, if no type hint then None and has_type_hint is False.
    # Otherwise, the actual type hint used during serialization and has_type_hint is True.
    type_hint: Any
    has_type_hint: bool
    # None for File, otherwise the name of the serializer used.
    serializer_name: str | None
    # User provided content type for File.
    # None if not File.
    content_type: str | None
