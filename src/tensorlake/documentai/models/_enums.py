"""
Enums for document parsing.
"""

from enum import Enum
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class Region(str, Enum):
    EU = "eu"
    US = "us"


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


class OcrPipelineProvider(str, Enum):
    """
    The different models for OCR (Optical Character Recognition).

    Tensorlake01: It's fast but could have lower accuracy on complex tables. It's good for legal documents with footnotes.
    Tensorlake02: It's slower but could have higher accuracy on complex tables. It's good for financial documents with merged cells.
    Tensorlake03: A compact model that we deliver to on-premise users. It takes about 2 minutes to startup on Tensorlake's Cloud because it's meant for testing for users who are eventually going to deploy this model on dedicated hardware in their own datacenter.
    """

    TENSORLAKE01 = "model01"
    TENSORLAKE02 = "model02"
    TENSORLAKE03 = "model03"


class ParseStatus(str, Enum):
    """
    Status of a parse job in the v2 API.
    """

    FAILURE = "failure"
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESSFUL = "successful"
    DETECTING_LAYOUT = "detecting_layout"
    LAYOUT_DETECTED = "detected_layout"
    EXTRACTING_DATA = "extracting_data"
    EXTRACTED_DATA = "extracted_data"
    FORMATTING_OUTPUT = "formatting_output"
    FORMATTED_OUTPUT = "formatted_output"


class PartitionStrategy(str, Enum):
    """
    Strategy for partitioning a document before structured data extraction.

    NONE: No partitioning is applied. The entire document is treated as a single unit for extraction.
    PAGE: The document is partitioned by individual pages. Each page is treated as a separate unit for extraction.
    SECTION: The document is partitioned into sections based on detected section headers. Each section is treated as a separate unit for extraction.
    FRAGMENT: The document is partitioned by individual page elements (fragments). Each fragment is treated as a separate unit for extraction.
    PATTERNS: The document is partitioned based on user-defined start and end patterns.
    """

    NONE = "none"
    PAGE = "page"
    SECTION = "section"
    FRAGMENT = "fragment"


class SimplePartitionStrategy(BaseModel):
    """
    Variant of PartitionStrategy for simple strategies.
    """

    strategy: Literal["none", "page", "section", "fragment"]


class PatternConfig(BaseModel):
    """
    Configuration for pattern-based partitioning.
    """

    start_patterns: Optional[List[str]] = None
    end_patterns: Optional[List[str]] = None

    def __init__(self, **data):
        super().__init__(**data)
        if not self.start_patterns and not self.end_patterns:
            raise ValueError("At least one start or end pattern must be provided.")


class PatternPartitionStrategy(BaseModel):
    """
    Partition strategy based on start and end patterns.
    """

    strategy: Literal["patterns"] = Field(default="patterns")
    patterns: PatternConfig


PartitionConfig = Annotated[
    Union[SimplePartitionStrategy, PatternPartitionStrategy],
    Field(discriminator="strategy"),
]


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
