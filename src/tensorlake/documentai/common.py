"""
Common types and constants for the Document AI API.
"""

import os
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel, Field

# Get base URL from environment variable or use default
_server_url = os.getenv("TENSORLAKE_SERVER_URL", "https://api.tensorlake.ai")
DOC_AI_BASE_URL = os.getenv("TENSORLAKE_DOCAI_URL", f"{_server_url}/documents/v1/")

T = TypeVar("T")


class PaginatedResult(BaseModel, Generic[T]):
    """
    A slice from a paginated endpoint.
    """

    items: List[T] = Field(alias="items")
    has_more: bool = Field(alias="hasMore")
    prev_cursor: Optional[str] = Field(alias="prevCursor")
    next_cursor: Optional[str] = Field(alias="nextCursor")
