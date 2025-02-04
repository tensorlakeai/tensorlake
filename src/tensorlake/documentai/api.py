from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel


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


class ParsedDocument(BaseModel):
    num_pages: Optional[int] = None
    document: Optional[Document] = None
    chunks: List[str]
