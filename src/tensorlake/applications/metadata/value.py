from typing import Any

from pydantic import BaseModel


# SDK metadata about a value stored in TensorLake.
class ValueMetadata(BaseModel):
    # ID of the value, uniquness guarantees depend on how the field is set.
    # If the value is returned from an awaitable then the ID of the awaitable.
    id: str
    # Python class of the serialized value object
    cls: Any
    # None for File, otherwise the name of the serializer used.
    serializer_name: str | None
    # User provided content type for File.
    # None if not File.
    content_type: str | None
