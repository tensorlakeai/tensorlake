"""
This module contains the data models for the parsing results of a document.
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from ._enums import PageFragmentType, ParseStatus


class PageClass(BaseModel):
    """
    Page class information containing the class name and page numbers.
    """

    page_class: str = Field(description="The name of the page class")
    page_numbers: List[int] = Field(
        description="List of page numbers (1-indexed) where this page class appears"
    )
    classification_reasons: Optional[Dict[int, str]] = Field(
        default=None,
        description="Optional mapping of page numbers to classification reasons",
    )


class Text(BaseModel):
    """
    Text content of a page fragment.
    """

    content: str


class Header(BaseModel):
    """
    Header type content of a page fragment.
    """

    level: int
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
    summary: Optional[str] = None


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


class PageFragment(BaseModel):
    """
    Page fragment in a document.
    """

    fragment_type: PageFragmentType
    content: Union[Text, Header, Table, Figure, Signature]
    reading_order: Optional[int] = None
    bbox: Optional[dict[str, float]] = None


class PageDimensions(BaseModel):
    """
    Page dimensions containing width and height information.
    """

    width: int = Field(description="Width of the page")
    height: int = Field(description="Height of the page")


class Page(BaseModel):
    """
    Page in a document.
    """

    dimensions: Optional[List[int]] = None
    page_dimensions: Optional[PageDimensions] = None
    page_fragments: Optional[List[PageFragment]] = None
    page_number: int
    classification_reason: Optional[str] = None


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
    error: Optional[str] = Field(
        default=None,
        description="Error occurred during any part of the parse execution.",
    )
    finished_at: Optional[str] = Field(
        default=None,
        description="The date and time when the parse job was finished in RFC 3339 format.",
    )
    labels: Optional[dict] = Field(
        default=None,
        description="Labels associated with the parse job.",
    )

    total_pages: Optional[int] = Field(
        default=None,
        description="The total number of pages in the document that was parsed.",
    )
