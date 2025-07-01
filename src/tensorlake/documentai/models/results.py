"""
This module contains the data models for the parsing results of a document.
"""

from typing import List, Optional

from pydantic import BaseModel, Field

from .enums import ParseStatus
from .jobs import Chunk, Document, StructuredData
from .options import Options


class ParseRequestOptions(BaseModel):
    """
    The options used for scheduling the parse job.
    """

    configuration: Options = Field(
        description="The configuration used for the parse job. This is derived from the configuration settings submitted with the parse request. It can be used to understand how the parse job was configured, such as the parsing strategy, extraction methods, etc. Values not provided in the request will be set to their default values."
    )
    file_id: Optional[str] = Field(None, description="The tensorlake file ID.")
    file_name: Optional[str] = Field(
        None, description="The name of the file used for the parse job."
    )
    file_url: Optional[str] = Field(
        None, description="The URL of the file used for the parse job."
    )
    page_range: Optional[str] = Field(
        None, description="The page range that was requested for parsing."
    )
    raw_text: Optional[str] = Field(None, description="The raw_text for the parse job.")
    trace_id: Optional[str] = Field(None, description="The trace ID for the parse job.")


class ParseResult(BaseModel):
    """
    Result of a parse operation in the v2 API.
    """

    chunks: Optional[List[Chunk]] = Field(
        None, description="Chunks of layout text extracted from the document."
    )
    document: Optional[Document] = Field(
        None, description="The layout of the document."
    )
    structured_data: Optional[StructuredData] = Field(
        None, description="Structured data extracted from the document."
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
