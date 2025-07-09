from enum import Enum
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PaginatedResult(BaseModel, Generic[T]):
    """A generic container for paginated list responses."""

    items: List[T]
    has_more: bool
    next_cursor: Optional[str] = None
    prev_cursor: Optional[str] = None


class PaginationDirection(str, Enum):
    next = "next"
    prev = "prev"
