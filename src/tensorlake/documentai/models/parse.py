"""
This module contains the data models for parsing a document.
"""

from typing import List, Optional

from pydantic import BaseModel, Field

from .enums import MimeType
from .options import (
    EnrichmentOptions,
    PageClassConfig,
    ParsingOptions,
    StructuredExtractionOptions,
)


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
    page_classifications: Optional[List[PageClassConfig]] = Field(
        None,
        description="The properties of this object define the settings for page classification. If this object is present, the API will perform page classification on the document.",
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
