"""
This module contains the data models for parsing a document.
"""

from enum import Enum
from typing import Optional, Type, Union

from pydantic import BaseModel, Json


class ChunkingStrategy(str, Enum):
    """
    Chunking strategy for parsing a document.

    NONE: No chunking is applied.
    PAGE: The document is chunked by page.
    SECTION_HEADER: The document is chunked by section headers.
    """

    NONE = "none"
    PAGE = "page"
    SECTION_HEADER = "section_header"


class TableParsingStrategy(str, Enum):
    """
    Algorithm to use for parsing tables in a document.

    TSR: Table Structure Recognition. Great for structured tables.
    VLM: Visual Layout Model. Great for unstructured tables or semi-structured tables.
    """

    TSR = "tsr"
    VLM = "vlm"


class TableOutputMode(str, Enum):
    """
    Output mode for tables in a document.

    JSON: The table is returned in JSON format.
    MARKDOWN: The table is returned in Markdown format.
    HTML: The table is returned in HTML format.
    """

    JSON = "json"
    MARKDOWN = "markdown"
    HTML = "html"


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


class ExtractionOptions(BaseModel):
    """
    Options for structured data extraction.
    """

    schema: Union[Type[BaseModel], Json]
    prompt: Optional[str] = None
    provider: ModelProvider = ModelProvider.TENSORLAKE


class ParsingOptions(BaseModel):
    """
    Options for parsing a document.
    """

    chunking_strategy: Optional[ChunkingStrategy] = None
    skew_correction: bool = False
    table_parsing_strategy: TableParsingStrategy = TableParsingStrategy.VLM
    table_parsing_prompt: Optional[str] = None
    figure_summarization_prompt: Optional[str] = None
    table_output_mode: TableOutputMode = TableOutputMode.MARKDOWN
    page_range: Optional[str] = None
    extraction_options: Optional[ExtractionOptions] = None
    deliver_webhook: bool = False
    detect_signature: Optional[bool] = False
    table_summary: Optional[bool] = False
    figure_summary: Optional[bool] = False
