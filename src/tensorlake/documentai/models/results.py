"""
This module contains the data models for the parsing results of a document.
"""

from typing import List, Optional, Union, Any
from enum import Enum

from pydantic import BaseModel, Field

from .enums import ParseStatus
from .options import Options


class PageClass(BaseModel):
    """
    Page class information containing the class name and page numbers.
    """

    page_class: str = Field(description="The name of the page class")
    page_numbers: List[int] = Field(
        description="List of page numbers (1-indexed) where this page class appears"
    )


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
    page_fragments: Optional[List[PageFragment]] = None
    page_number: int


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


class ParseRequestOptions(BaseModel):
    """
    The options used for scheduling the parse job.
    """

    configuration: Options = Field(
        description="The configuration used for the parse job. This is derived from the configuration settings submitted with the parse request. It can be used to understand how the parse job was configured, such as the parsing strategy, extraction methods, etc. Values not provided in the request will be set to their default values."
    )
    file_id: Optional[str] = Field(
        None,
        description="The tensorlake file ID. This is the ID of the file used for the parse job. It has `tensorlake_` prefix. It can be undefined if the parse operation was created with a `file_url` or `raw_text` field instead of a file ID.",
    )
    file_name: Optional[str] = Field(
        None,
        description="The name of the file used for the parse job. This is only populated if the parse operation was created with a `file_id`.",
    )
    file_url: Optional[str] = Field(
        None,
        description="The URL of the file used for the parse job. It can be undefined if the parse operation was created with a `file_id` or `raw_text` field instead of a file URL.",
    )
    page_range: Optional[str] = Field(
        None,
        description="The page range that was requested for parsing. This is the same as the value provided in the `pages` field of the request. It can be undefined if the parse operation was created without a specific page range. Meaning the whole document was parsed.",
    )
    raw_text: Optional[str] = Field(
        None,
        description="The raw_text for the parse job. This is only populated if the parse operation was created with a `raw_text` field. And the mime type is of a text-based format (e.g., plain text, HTML). It can be undefined if the parse operation was created with a `file_id` or `file_url` field instead of raw_text.",
    )
    trace_id: Optional[str] = Field(
        None,
        description="The trace ID for the parse job. It can be undefined if the operation is still in pending state. This is used for debugging purposes.",
    )


class ParseResult(BaseModel):
    """
    Result of a parse operation in the v2 API.
    """

    # Parsed document specific fields
    chunks: Optional[List[Chunk]] = Field(
        default=None,
        description="Chunks of layout text extracted from the document. This is a vector of `Chunk` objects, each containing a piece of text extracted from the document. The chunks are typically used for further processing, such as indexing or searching. The value will vary depending on the chunking strategy used during parsing.",
    )
    pages: Optional[List[Page]] = Field(
        default=None,
        description="The layout of the document. This is a JSON object that contains the layout information of the document. It can be used to understand the structure of the document, such as the position of text, tables, figures, etc.",
    )
    page_classes: Optional[List[PageClass]] = Field(
        default=None,
        description="Page classes extracted from the document. This is a list of `PageClass` objects containing the class name and page numbers where each page class appears.",
    )
    structured_data: Optional[List[StructuredData]] = Field(
        default=None,
        description="Structured data extracted from the document. The structured data is a list of `StructuredData` objects containing the structured data extracted from the document; formatted according to the schema. This is used to extract structured information from the document, such as tables, forms, or other structured content.",
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
