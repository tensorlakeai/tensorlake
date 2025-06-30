"""
This module contains the data models for parsing a document.
"""

from enum import Enum
from typing import Optional, Type, Union

from pydantic import BaseModel, Field, Json


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


class FormDetectionMode(str, Enum):
    """
    Algorithm to use for detecting forms in a document.

    VLM: Uses a VLM to identify questions and answers in a form.
         Does not provide bounding boxes and is prone to hallucinations.
    OBJECT_DETECTION: Uses a layout detector to identify questions and answers.
                      Does not work well with very complex forms.
    """

    VLM = "vlm"
    OBJECT_DETECTION = "object_detection"


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

    TENSORLAKE: private models, running on Tensorlake infrastructure.
    SONNET: Claude 3.7 Sonnet model.
    GPT4OMINI: GPT-4o-mini model.
    """

    TENSORLAKE = "tensorlake"
    SONNET = "claude-3-5-sonnet-latest"
    GPT4OMINI = "gpt-4o-mini"


class TableParsingFormat(str, Enum):
    """
    Determines how the system identifies and extracts tables from the document.

    TSR: Better suited for clean, grid-like tables.
    VLM: Help for tables with merged cells or irregular structures.
    """

    TSR = "tsr"
    VLM = "vlm"


class TableOutputMode(str, Enum):
    """
    Output mode for tables in a document.

    MARKDOWN: The table is returned in Markdown format.
    HTML: The table is returned in HTML format.
    """

    MARKDOWN = "markdown"
    HTML = "html"


class EnrichmentOptions(BaseModel):
    """
    Options for enriching a document with additional information.

    This object helps to extend the output of the document parsing process with additional information.
    This includes summarization of tables and figures, which can help to provide a more comprehensive understanding of the document.
    """

    figure_summarization: bool = Field(
        False,
        description="Boolean flag to enable figure summarization. The default is `false`.",
    )
    figure_summarization_prompt: Optional[str] = Field(
        None,
        description="The prompt to guide the figure summarization. If not provided, a default prompt will be used. It is not required to provide a prompt. The prompt only has effect if `figure_summarization` is set to `true`.",
    )
    table_summarization: bool = Field(
        False,
        description="Boolean flag to enable summary generation for parsed tables. The default is `false`.",
    )
    table_summarization_prompt: Optional[str] = Field(
        None,
        description="The prompt to guide the table summarization. If not provided, a default prompt will be used. It is not required to provide a prompt. The prompt only has effect if `table_summarization` is set to `true`.",
    )


class ParsingOptions(BaseModel):
    """
    Options for parsing a document.
    """

    chunking_strategy: Optional[ChunkingStrategy] = None
    disable_layout_detection: Optional[bool] = False
    form_detection_mode: Optional[FormDetectionMode] = (
        FormDetectionMode.OBJECT_DETECTION
    )
    remove_strikethrough: bool = False
    signature_detection: Optional[bool] = False
    skew_detection: bool = False
    table_output_mode: TableOutputMode = TableOutputMode.MARKDOWN
    table_parsing_format: TableParsingFormat = TableParsingFormat.TSR


class StructuredExtractionOptions(BaseModel):
    """
    Options for structured data extraction from a document.
    """

    chunking_strategy: Optional[ChunkingStrategy] = None
    json_schema: Union[Type[BaseModel], Json] = Field(..., alias="schema")
    model_provider: ModelProvider = ModelProvider.TENSORLAKE
    page_class: Optional[str] = None
    page_class_definition: Optional[str] = None
    prompt: Optional[str] = None
    schema_name: str = "document_schema"
    skip_ocr: bool = False

    class Config:
        validate_by_name = True  # Enables usage of 'schema=' as well


class ParseRequest(BaseModel):
    """
    Request model for parsing a document.

    A file ID, a file URL, or raw text must be provided.
    """

    file_id: Optional[str] = Field(
        None, description="ID of the file previously uploaded to Tensorlake."
    )
    file_url: Optional[str] = Field(
        None, description="External URL of the file to parse."
    )
    raw_text: Optional[str] = Field(None, description="The raw text to parse.")

    parsing_options: Optional[ParsingOptions] = Field(
        None,
        description="Additional options for tailoring the document parsing process.",
    )
    structured_extraction_options: Optional[list[StructuredExtractionOptions]] = Field(
        None, description="List of structured extraction options for extraction."
    )
    enrichment_options: Optional[EnrichmentOptions] = Field(
        None,
        description="Options for enriching a document with additional information.",
    )

    page_range: Optional[str] = Field(
        None,
        description='The range of pages to parse in the document. This should be a comma-separated list of page numbers or ranges (e.g., "1,2,3-5"). Ranges are inclusive, meaning "1-3" will parse pages 1, 2, and 3. If not provided, all pages will be parsed.',
    )
    labels: Optional[dict] = Field(
        None,
        description='Labels to attach to the parse operation. These labels can be used to store metadata about the parse operation. The format should be a JSON object, e.g. {"key1": "value1", "key2": "value2"}.',
    )
    mime_type: Optional[MimeType] = Field(
        None,
        description="The MIME type of the document being parsed. It is optional if `file_id` or `file_url` are provided, as the MIME type will be inferred from the file extension or content.",
    )
