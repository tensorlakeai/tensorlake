"""
Common types and constants for the Document AI API.
"""

from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel, Field

DOC_AI_BASE_URL = "https://api.tensorlake.ai/documents/v1/"

T = TypeVar("T")


class PaginatedResult(BaseModel, Generic[T]):
    """
    A slice from a paginated endpoint.
    """

    items: List[T] = Field(alias="items")
    total_pages: int = Field(alias="totalPages")
    prev_cursor: Optional[str] = Field(alias="prevCursor")
    next_cursor: Optional[str] = Field(alias="nextCursor")
