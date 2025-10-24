from pydantic import BaseModel


class ReduceOperationMetadata(BaseModel):
    # Request scoped unique identifier of the reduce operation.
    id: str
    # Not None if output serialization format is overridden
    # for reduce function calls.
    output_serializer_name_override: str | None
