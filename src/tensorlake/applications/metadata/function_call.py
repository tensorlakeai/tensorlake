from typing import Any, Dict, List

from pydantic import BaseModel

from .collection import CollectionMetadata


class ArgumentMetadata(BaseModel):
    # ID of awaitable or value from which the argument value is coming from.
    # None if the value is coming from a Collection.
    source_id: str | None
    collection: CollectionMetadata | None


class FunctionCallMetadata(BaseModel):
    # Request scoped unique identifier of the function call.
    id: str
    # Not None if output serialization format is overridden for this function call.
    output_serializer_name_override: str | None
    # Positional arg ix -> Arg metadata.
    args: List[ArgumentMetadata]
    # Keyword Arg name -> Arg metadata.
    kwargs: Dict[str, ArgumentMetadata]
