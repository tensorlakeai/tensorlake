"""
Enums for document parsing.
"""

from enum import Enum


class ChunkingStrategy(str, Enum):
    """
    Chunking strategy for parsing a document.

    FRAGMENT: Each page element is converted into markdown form.
    NONE: No chunking is applied.
    PAGE: The document is chunked by page.
    SECTION: The document is chunked into sections. Title and section headers are used as chunking markers.
    """

    FRAGMENT = "fragment"
    NONE = "none"
    PAGE = "page"
    SECTION = "section"


class MimeType(str, Enum):
    """
    Supported MIME types for document parsing.

    CSV: Comma-separated values files.
    DOCX: Microsoft Word documents.
    HTML: HTML files.
    JPEG: JPEG image files.
    KEYNOTE: Apple Keynote presentations.
    PDF: Portable Document Format files.
    PNG: PNG image files.
    PPTX: Microsoft PowerPoint presentations.
    TEXT: Plain text files.
    XLS: Microsoft Excel spreadsheets (legacy format).
    XLSM: Microsoft Excel spreadsheets (macros enabled).
    XLSX: Microsoft Excel spreadsheets.
    """

    CSV = "text/csv"
    DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    HTML = "text/html"
    JPEG = "image/jpeg"
    KEYNOTE = "application/vnd.apple.keynote"
    PDF = "application/pdf"
    PNG = "image/png"
    PPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    TEXT = "text/plain"
    XLS = "application/vnd.ms-excel"
    XLSM = "application/vnd.ms-excel.sheet.macroEnabled.12"
    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class ModelProvider(str, Enum):
    """
    The model provider to use for structured data extraction.

    TENSORLAKE: Private models, running on Tensorlake infrastructure.
    SONNET: Latest release of Claude 3.5 Sonnet model from Anthropic
    GPT4OMINI: GPT-4o-mini model from OpenAI.
    """

    TENSORLAKE = "tensorlake"
    SONNET = "sonnet"
    GPT4OMINI = "gpt_4o_mini"


class ParseStatus(str, Enum):
    """
    Status of a parse job in the v2 API.
    """

    FAILURE = "failure"
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESSFUL = "successful"


class PartitionStrategy(str, Enum):
    """
    Partition strategy for parsing a document.

    NONE: No partitioning is applied.
    PAGE: Partition the document into pages.
    """

    NONE = "none"
    PAGE = "page"


class TableOutputMode(str, Enum):
    """
    Output mode for tables in a document.

    MARKDOWN: The table is returned in Markdown format.
    HTML: The table is returned in HTML format.
    """

    MARKDOWN = "markdown"
    HTML = "html"


class TableParsingFormat(str, Enum):
    """
    Determines how the system identifies and extracts tables from the document.

    TSR: Better suited for clean, grid-like tables.
    VLM: Help for tables with merged cells or irregular structures.
    """

    TSR = "tsr"
    VLM = "vlm"


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
