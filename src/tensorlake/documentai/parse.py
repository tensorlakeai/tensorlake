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

    TSR = "table_structure_recognition"
    VLM = "vlm"


class TableParsingStrategy(str, Enum):
    """
    Algorithm to use for parsing tables in a document.

    TSR: Table Structure Recognition. Great for structured tables.
    VLM: Visual Layout Model. Great for unstructured tables or semi-structured tables.
    """

    TSR = "tsr"
    VLM = "vlm"
    UNKNOWN = "unknown"


class TableOutputMode(str, Enum):
    """
    Output mode for tables in a document.

    MARKDOWN: The table is returned in Markdown format.
    HTML: The table is returned in HTML format.
    """

    MARKDOWN = "markdown"
    HTML = "html"


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


class ParseRequest(BaseModel):
    """
    Request model for parsing a document.
    """

    file_id: Optional[str] = None
    file_url: Optional[str] = None
    page_range: Optional[str] = None
    raw_text: Optional[str] = None
    enrichment_options: Optional["EnrichmentOptions"] = None
    parsing_options: Optional["ParsingOptions"] = None
    structured_extraction_options: Optional["StructuredExtractionOptions"] = None

    class Config:
        arbitrary_types_allowed = (
            True  # Allows the use of custom types like ParsingOptions
        )
        json_encoders = {
            ParsingOptions: lambda v: v.dict()
        }  # Custom JSON encoder for ParsingOptions


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
        allow_population_by_field_name = True  # Enables usage of 'schema=' as well


class EnrichmentOptions(BaseModel):
    """
    Options for enriching a document with additional information.
    """

    figure_summarization: Optional[bool] = False
    figure_summarization_prompt: Optional[str] = None
    table_summarization: Optional[bool] = False
    table_summarization_prompt: Optional[str] = None
