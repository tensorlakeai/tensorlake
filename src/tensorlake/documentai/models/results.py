"""
This module contains the data models for the parsing results of a document.
"""

from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

from .enums import ParseStatus
from .jobs import Chunk, Document, StructuredData
from .options import Options


class PageClass(BaseModel):
    """
    Page class classification result.

    Contains page numbers (1-indexed) where a specific page class appears in the document.
    """

    pages: List[int] = Field(
        description="Vector of page numbers (1-indexed) where this page class appears.",
        default=[],
    )


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
    document: Optional[Document] = Field(
        default=None,
        description="The layout of the document. This is a JSON object that contains the layout information of the document. It can be used to understand the structure of the document, such as the position of text, tables, figures, etc.",
    )
    page_classes: Optional[Dict[str, PageClass]] = Field(
        default=None,
        description="Page classes extracted from the document. This is a map where the keys are page class names provided in the parse request under the `page_classification_options` field, and the values are vectors of page numbers (1-indexed) where each page class appears. This is used to categorize pages in the document based on the classification options provided.",
    )
    structured_data: Optional[
        Dict[str, Union[StructuredData, List[StructuredData]]]
    ] = Field(
        default=None,
        description="Structured data extracted from the document. The structured data is a map where the keys are the names of the json schema provided in the parse request, and the values are `StructuredData` objects containing the structured data extracted from the document; formatted according to the schema. When the `structured_extraction` option uses a `chunking_strategy` of `None`, the structured data will be extracted from the entire document, and it will be represented as a single entry in the map with the schema name as the key. When the `structured_extraction` option uses a `chunking_strategy`, the structured data will be extracted from each chunk of text, and it will be represented as multiple entries in the map, with the schema name as the key and a vector of `StructuredData` objects as the value. This is used to extract structured information from the document, such as tables, forms, or other structured content.",
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
    options: ParseRequestOptions = Field(
        description="The options used for scheduling the parse job."
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
    tasks_completed_count: Optional[int] = Field(
        None,
        description="The number of tasks that have been completed for the parse job.",
        ge=0,
    )
    tasks_total_count: Optional[int] = Field(
        None,
        description="The total number of tasks that are expected to be completed for the parse job.",
        ge=0,
    )
