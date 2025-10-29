from typing import List, Union

from pydantic import BaseModel


class CollectionItemMetadata(BaseModel):
    # ID of awaitable or value from which the argument value is coming from.
    # None if the value is coming from a Collection.
    value_id: str | None
    collection: Union["CollectionMetadata", None]


class CollectionMetadata(BaseModel):
    # NB. A collection doesn't have its own ID because collections are currently
    # embedded into function call arguments directly, when they are used as function
    # call arguments. This is because they currently have no representation on server
    # side, this is why they can't be returned from a function right now as tail calls.
    # When user awaits a collection manually, the runtime recreates its original list
    # and sends each request to server as a separate awaitable.

    # Ordered IDs of awaitables and values comprising
    # the list of values when all awaitables are resolved.
    items: List[CollectionItemMetadata]
