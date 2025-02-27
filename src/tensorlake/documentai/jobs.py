"""
DocumentAI job classes.
"""

from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, Field


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


class Table(BaseModel):
    """
    Table content of a page fragment.
    """

    content: str
    summary: Optional[str] = None


class Figure(BaseModel):
    """
    Figure content of a page fragment.
    """

    content: str
    summary: Optional[str] = None


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


class Output(BaseModel):
    """
    Output of a job.
    """

    chunks: List[str] = Field(alias="chunks", default_factory=list)
    document: Optional[Document]
    num_pages: Optional[int]
    structured_data: Optional[StructuredData] = None
    # error_message: Optional[str] = Field(alias="errorMessage")


class Job(BaseModel):
    """
    DocumentAI job class.
    """

    job_id: str = Field(alias="jobId")
    file_id: str = Field(alias="fileId")
    status: JobStatus = Field(alias="status")
    outputs: Optional[Output] = None
