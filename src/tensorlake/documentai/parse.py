"""
This module contains the data models for parsing a document.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel

from .common import TableOutputMode, TableParsingStrategy


class OutputFormat(str, Enum):
    """
    Output format for parsing a document.

    MARKDOWN: The parsed document is returned in Markdown format. Using Markdown requires setting a chunking strategy.
    JSON: The parsed document is returned in JSON format.
    """

    MARKDOWN = "markdown"
    JSON = "json"


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


class ParsingOptions(BaseModel):
    """
    Options for parsing a document.
    """

    format: OutputFormat = OutputFormat.MARKDOWN
    chunking_strategy: Optional[ChunkingStrategy] = None
    table_parsing_strategy: TableParsingStrategy = TableParsingStrategy.TSR
    table_parsing_prompt: Optional[str] = None
    figure_summarization_prompt: Optional[str] = None
    table_output_mode: TableOutputMode = TableOutputMode.MARKDOWN
    summarize_table: bool = False
    summarize_figure: bool = False
    page_range: Optional[str] = None
    deliver_webhook: bool = False
