from pydantic import BaseModel


class ReduceOperationMetadata(BaseModel):
    # ID of the reduce operation, uniquness guarantees depend on how the field is set.
    id: str
    # Not None if output serialization format is overridden
    # for reduce function calls.
    output_serializer_name_override: str | None
