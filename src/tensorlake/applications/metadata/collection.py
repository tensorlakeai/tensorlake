from typing import List

from pydantic import BaseModel


class CollectionMetadata(BaseModel):
    # NB: Collections are currently embedded into function call arguments directly,
    # so they don't have their own ID yet. They are embedded because they currently
    # have no representation on runtime/server side, they also can't be returned from
    # a function right now.
    #
    # Ordered IDs of awaitables and values comprising
    # the list of values when all awaitables are resolved.
    item_ids: List[str]
