"""
Common types and constants for the Document AI API.
"""

from enum import Enum
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel

DOC_AI_BASE_URL = "https://api.tensorlake.ai/documents/v1/"


class OutputFormat(str, Enum):
    """
    Output format for parsing a document.

    MARKDOWN: The parsed document is returned in Markdown format. Using Markdown requires setting a chunking strategy.
    JSON: The parsed document is returned in JSON format.
    """

    MARKDOWN = "markdown"
    JSON = "json"


class ChunkingStrategy(str, Enum):
    """
    Chunking strategy for parsing a document.

    NONE: No chunking is applied.
    PAGE: The document is chunked by page.
    SECTION_HEADER: The document is chunked by section headers.
    """

    NONE = "none"
    PAGE = "page"
    SECTION_HEADER = "section_header"


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


class ModelProvider(str, Enum):
    """
    The model provider to use for structured data extraction.

    TENSORLAKE: private models, running on Tensorlake infrastructure.
    SONNET: Claude 3.5 Sonnet model.
    GPT4OMINI: GPT-4o-mini model.
    """

    TENSORLAKE = "tensorlake"
    SONNET = "claude-3-5-sonnet-latest"
    GPT4OMINI = "gpt-4o-mini"


T = TypeVar("T")


class PaginatedResult(BaseModel, Generic[T]):
    """
    A slice from a paginated endpoint.
    """

    items: List[T]
    total_pages: int
    prev_cursor: Optional[str]
    next_cursor: Optional[str]
