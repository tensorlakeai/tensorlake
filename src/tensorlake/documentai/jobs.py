"""
DocumentAI job classes.
"""

from enum import Enum
from typing import List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field


class JobStatus(str, Enum):
    """
    Status of a job.
    """

    PROCESSING = "processing"
    SUCCESSFUL = "successful"
    FAILURE = "failure"
    PENDING = "pending"


class JobListItem(BaseModel):
    """
    DocumentAI job item class.

    """

    id: str
    file_id: str = Field(alias="fileId")
    file_name: str = Field(alias="fileName")
    status: JobStatus
    job_type: str = Field(alias="jobType")
    error_message: Optional[str] = Field(alias="errorMessage")
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")


class Text(BaseModel):
    """
    Text content of a page fragment.
    """

    content: str


class TableCell(BaseModel):
    text: str
    bounding_box: Tuple[float, float, float, float]


class Table(BaseModel):
    """
    Table content of a page fragment.
    """

    content: str
    table_summary: Optional[str] = None
    cells: List[TableCell]


class Figure(BaseModel):
    """
    Figure content of a page fragment.
    """

    content: str
    figure_summary: Optional[str] = None


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


class PageFragment(BaseModel):
    """
    Page fragment in a document.
    """

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


class StructuredDataPage(BaseModel):
    """
    DocumentAI structured data class.
    """

    page_number: int
    data: dict = Field(alias="json_result", default_factory=dict)


class StructuredData(BaseModel):
    """
    DocumentAI structured data class.
    """

    pages: List[StructuredDataPage] = Field(alias="pages", default_factory=list)


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
