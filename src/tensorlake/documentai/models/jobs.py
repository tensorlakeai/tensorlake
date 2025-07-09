"""
DocumentAI job classes.
"""

from enum import Enum
from typing import Any, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from .enums import JobStatus


class JobListItem(BaseModel):
    """
    DocumentAI job item class.
    """

    id: str
    status: JobStatus
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
    dataset_id: Optional[str] = Field(alias="datasetId", default=None)
    file_id: Optional[str] = Field(alias="fileId", default=None)
    file_name: Optional[str] = Field(alias="fileName", default=None)
    finished_at: Optional[str] = Field(alias="finishedAt", default=None)
    message: Optional[str] = Field(default=None)
    pages_parsed: Optional[int] = Field(alias="pagesParsed", default=None)
    trace_id: Optional[str] = Field(alias="traceId", default=None)


class Text(BaseModel):
    """
    Text content of a page fragment.
    """

    content: str


class TableCell(BaseModel):
    """
    Table cell content with text and bounding box information.
    Based on PageFragmentTableCell schema.
    """

    text: str
    bounding_box: dict[str, float]


class Table(BaseModel):
    """
    Table content of a page fragment.
    Based on PageFragmentTable schema with content, cells, and optional formatting fields.
    """

    content: str
    cells: List[TableCell]
    html: Optional[str] = None
    markdown: Optional[str] = None
    table_summary: Optional[str] = None


class Figure(BaseModel):
    """
    Figure content of a page fragment.
    Based on PageFragmentFigure schema with content and optional summary.
    """

    content: str
    summary: Optional[str] = None


class Signature(BaseModel):
    """
    Signature content of a page fragment.
    """

    content: str


class PageFragmentType(str, Enum):
    """
    Type of a page fragment.
    """

    SECTION_HEADER = "section_header"
    TITLE = "title"

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

    PAGE_FOOTER = "page_footer"
    PAGE_HEADER = "page_header"
    PAGE_NUMBER = "page_number"
    SIGNATURE = "signature"
    STRIKETHROUGH = "strikethrough"


class PageFragment(BaseModel):
    """
    Page fragment in a document.
    """

    fragment_type: PageFragmentType
    content: Union[Text, Table, Figure, Signature]
    reading_order: Optional[int] = None
    bbox: Optional[dict[str, float]] = None


class Page(BaseModel):
    """
    Page in a document.
    """

    dimensions: Optional[List[int]] = None
    layout: Optional[dict] = None
    page_fragments: Optional[List[PageFragment]] = None
    page_number: int


class Document(BaseModel):
    """
    Document in a document.
    """

    pages: List[Page]


class StructuredData(BaseModel):
    """
    DocumentAI structured data class.
    Can contain either a single data item or a list of data items.
    """

    data: Any = Field()
    page_numbers: Union[int, List[int]] = Field()
    schema_name: Optional[str] = Field(default=None)


class Chunk(BaseModel):
    """
    Chunk of a Page in a Document.
    """

    page_number: int
    content: str


class Output(BaseModel):
    """
    Output of a job.
    """

    chunks: List[Chunk] = Field(alias="chunks", default_factory=list)
    document: Optional[Document] = None
    num_pages: Optional[int] = 0
    structured_data: Optional[StructuredData] = None
    error_message: Optional[str] = Field(alias="errorMessage", default="")


class Job(BaseModel):
    """
    DocumentAI job class.
    """

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="jobId")
    status: JobStatus = Field(alias="status")
    file_name: str = Field(alias="fileName")
    file_id: str = Field(alias="fileId")
    trace_id: Optional[str] = Field(alias="traceId", default=None)
    createdAt: Optional[str] = Field(alias="createdAt", default=None)
    updatedAt: Optional[str] = Field(alias="updatedAt", default=None)
    outputs: Optional[Output] = Field(alias="outputs", default=None)
