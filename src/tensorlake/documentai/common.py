from enum import Enum
from typing import Generic, List, Optional, TypeVar, Union

from pydantic import BaseModel, Field

DOC_AI_BASE_URL = "https://api.tensorlake.ai/documents/v1/"

class PageFragmentType(str, Enum):
    """
    Type of a page fragment.
    """

    SECTION_HEADER = "section_header"

    TEXT = "text"
    TABLE = "table"
    FIGURE = "figure"
    FORMULA = "formula"
    FORM = "form"
    KEY_VALUE_REGION = "key_value_region"
    DOCUMENT_INDEX = "document_index"
    LIST_ITEM = "list_item"

    TABLE_CAPTION = "table_caption"
    FIGURE_CAPTION = "figure_caption"
    FORMULA_CAPTION = "formula_caption"


class Text(BaseModel):
    content: str


class Table(BaseModel):
    content: str
    summary: Optional[str] = None


class Figure(BaseModel):
    content: str
    summary: Optional[str] = None


class PageFragment(BaseModel):
    fragment_type: PageFragmentType
    content: Union[Text, Table, Figure]
    reading_order: Optional[int] = None
    page_number: Optional[int] = None
    bbox: Optional[dict[str, float]] = None


class Page(BaseModel):
    """
    Page in a document.
    """

    page_number: int
    page_fragments: Optional[List[PageFragment]] = []
    layout: Optional[dict] = {}


class Document(BaseModel):
    """
    Document in a document.
    """

    pages: List[Page]


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


class JobStatus(str, Enum):
    """
    Status of a job.
    """

    PROCESSING = "processing"
    SUCCESSFUL = "successful"
    FAILURE = "failure"
    PENDING = "pending"


class JobResult(BaseModel):
    job_id: str = Field(alias="jobId")
    file_id: str = Field(alias="fileId")
    job_type: str = Field(alias="jobType")
    chunks: List[str] = Field(alias="chunks", default_factory=list)
    document: Optional[Document] = Field(alias="document", default=None)
    status: JobStatus = Field(alias="status")


T = TypeVar("T")


class PaginatedResult(BaseModel, Generic[T]):
    """
    A slice from a paginated endpoint.
    """

    items: List[T]
    total_pages: int
    prev_cursor: Optional[str]
    next_cursor: Optional[str]
