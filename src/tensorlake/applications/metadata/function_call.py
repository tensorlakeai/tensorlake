from typing import Dict, List

from pydantic import BaseModel

from .collection import CollectionMetadata


class FunctionCallArgumentMetadata(BaseModel):
    # ID of awaitable or value from which the argument value is coming from.
    # None if the value is coming from a Collection embedded into the function
    # call argument.
    value_id: str | None
    collection: CollectionMetadata | None


class FunctionCallMetadata(BaseModel):
    # Request scoped unique identifier of the function call.
    id: str
    # Not None if output serialization format is overridden for this function call.
    # This is used when the output of this function call is consumed by another function call
    # with a different output serializer.
    output_serializer_name_override: str | None
    # Positional arg ix -> Arg metadata.
    args: List[FunctionCallArgumentMetadata]
    # Keyword Arg name -> Arg metadata.
    kwargs: Dict[str, FunctionCallArgumentMetadata]
