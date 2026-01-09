from typing import Any

from pydantic import BaseModel


class ReduceOperationMetadata(BaseModel):
    # ID of the reduce operation, uniquness guarantees depend on how the field is set.
    id: str
    # Not None if output serialization format is overridden for reduce function calls.
    # This is used when the output of this reduce operation is used as output of another function call
    # with a different output serializer.
    output_serializer_name_override: str | None
    # This is used when the output of this function call is used as output of another function call.
    # In this case the type hint of the outer function call are applied to the inner function call output.
    output_type_hint_override: Any
    has_output_type_hint_override: bool
