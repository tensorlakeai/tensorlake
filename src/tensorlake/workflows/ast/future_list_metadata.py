from typing import List

from pydantic import BaseModel


class FutureListMetadata(BaseModel):
    # Ordered IDs of child nodes comprising the resolved values list.
    nids: List[str]
