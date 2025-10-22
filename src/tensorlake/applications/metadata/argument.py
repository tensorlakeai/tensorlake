from pydantic import BaseModel

from .collection import CollectionMetadata


class ArgumentMetadata(BaseModel):
    # ID of awaitable or value from which the argument value is coming from.
    # None if the value is coming from a Collection embedded into the function
    # call argument.
    value_id: str | None
    collection: CollectionMetadata | None
