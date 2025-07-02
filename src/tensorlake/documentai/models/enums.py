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


class JobStatus(str, Enum):
    """
    Status of a job.
    """

    FAILURE = "failure"
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESSFUL = "successful"


class MimeType(str, Enum):
    """
    Supported MIME types for document parsing.

    PDF: Portable Document Format files.
    DOCX: Microsoft Word documents.
    PPTX: Microsoft PowerPoint presentations.
    KEYNOTE: Apple Keynote presentations.
    JPEG: JPEG image files.
    TEXT: Plain text files.
    HTML: HTML files.
    XLSX: Microsoft Excel spreadsheets.
    """

    PDF = "application/pdf"
    DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    PPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    KEYNOTE = "application/vnd.apple.keynote"
    JPEG = "image/jpeg"
    TEXT = "text/plain"
    HTML = "text/html"
    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class ModelProvider(str, Enum):
    """
    The model provider to use for structured data extraction.

    TENSORLAKE: Private models, running on Tensorlake infrastructure.
    SONNET: Latest release of Claude 3.5 Sonnet model from Anthropic
    GPT4OMINI: GPT-4o-mini model from OpenAI.
    """

    TENSORLAKE = "tensorlake"
    SONNET = "claude-3-5-sonnet-latest"
    GPT4OMINI = "gpt-4o-mini"


class ParseStatus(str, Enum):
    """
    Status of a parse job in the v2 API.
    """

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


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
