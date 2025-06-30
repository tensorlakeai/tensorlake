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

    FAILURE = "failure"
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESSFUL = "successful"


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
    outputs: Optional[Output] = Field(alias="outputs", default=None)


class ParseStatus(str, Enum):
    """
    Status of a parse job in the v2 API.
    """

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class TypedOrUntypedParsedDocument(BaseModel):
    """
    Parsed document output containing chunks, document layout, and structured data.
    """

    chunks: List[Chunk] = Field(
        description="Chunks of layout text extracted from the document. This is a vector of Chunk objects, each containing a piece of text extracted from the document."
    )
    document: Optional[Document] = Field(
        None,
        description="The layout of the document. This is a JSON object that contains the layout information of the document.",
    )
    structured_data: Optional[StructuredData] = Field(
        None,
        description="Structured data extracted from the document. The structured data will be organized according to the JSON schema defined in the parse request.",
    )


class ParseRequestOptions(BaseModel):
    """
    The options used for scheduling the parse job.
    """

    configuration: dict = Field(description="The configuration used for the parse job.")
    file_id: Optional[str] = Field(None, description="The tensorlake file ID.")
    file_name: Optional[str] = Field(
        None, description="The name of the file used for the parse job."
    )
    file_url: Optional[str] = Field(
        None, description="The URL of the file used for the parse job."
    )
    page_range: Optional[str] = Field(
        None, description="The page range that was requested for parsing."
    )
    raw_text: Optional[str] = Field(None, description="The raw_text for the parse job.")
    trace_id: Optional[str] = Field(None, description="The trace ID for the parse job.")


class ParseResult(BaseModel):
    """
    Result of a parse operation in the v2 API.
    """

    # Inherited from TypedOrUntypedParsedDocument
    chunks: Optional[List[Chunk]] = Field(
        None, description="Chunks of layout text extracted from the document."
    )
    document: Optional[Document] = Field(
        None, description="The layout of the document."
    )
    structured_data: Optional[StructuredData] = Field(
        None, description="Structured data extracted from the document."
    )

    # ParseResult specific fields
    parse_id: str = Field(description="The unique identifier for the parse job")
    parsed_pages_count: int = Field(
        description="The number of pages that were parsed successfully.", ge=0
    )
    status: ParseStatus = Field(description="The status of the parse job.")
    created_at: str = Field(
        description="The date and time when the parse job was created in RFC 3339 format."
    )
    options: ParseRequestOptions = Field(
        description="The options used for scheduling the parse job."
    )

    # Optional fields
    errors: Optional[dict] = Field(
        None, description="Error occurred during any part of the parse execution."
    )
    finished_at: Optional[str] = Field(
        None,
        description="The date and time when the parse job was finished in RFC 3339 format.",
    )
    labels: Optional[dict] = Field(
        None, description="Labels associated with the parse job."
    )
    tasks_completed_count: Optional[int] = Field(
        None,
        description="The number of tasks that have been completed for the parse job.",
        ge=0,
    )
    tasks_total_count: Optional[int] = Field(
        None,
        description="The total number of tasks that are expected to be completed for the parse job.",
        ge=0,
    )
