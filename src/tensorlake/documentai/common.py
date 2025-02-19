"""
Common types and constants for the Document AI API.
"""

from enum import Enum
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel, Field

DOC_AI_BASE_URL = "https://api.tensorlake.ai/documents/v1/"


class TableParsingStrategy(str, Enum):
    """
    Algorithm to use for parsing tables in a document.

    TSR: Table Structure Recognition. Great for structured tables.
    VLM: Visual Layout Model. Great for unstructured tables or semi-structured tables.
    """

    TSR = "tsr"
    VLM = "vlm"


class TableOutputMode(str, Enum):
    """
    Output mode for tables in a document.

    JSON: The table is returned in JSON format.
    MARKDOWN: The table is returned in Markdown format.
    HTML: The table is returned in HTML format.
    """

    JSON = "json"
    MARKDOWN = "markdown"
    HTML = "html"


T = TypeVar("T")


class PaginatedResult(BaseModel, Generic[T]):
    """
    A slice from a paginated endpoint.
    """

    items: List[T] = Field(alias="items")
    total_pages: int = Field(alias="totalPages")
    prev_cursor: Optional[str] = Field(alias="prevCursor")
    next_cursor: Optional[str] = Field(alias="nextCursor")
