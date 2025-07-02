"""
This module contains the data models for parsing a document.
"""

from typing import Optional

from pydantic import Field

from .enums import MimeType
from .options import Options


class ParseRequest(Options):
    """
    Request model for parsing a document.
    This object defines the request body for the parse endpoint.

    This class inherits from Options:
     - enrichment_options
     - parsing_options
     - structured_extraction_options
     - page_classifications

    A file ID, a file URL, or raw text must be provided.
    """

    file_id: Optional[str] = Field(
        None,
        description="ID of the file previously uploaded to Tensorlake. This is the ID of the file in Tensorlake's storage system. It has a tensorlake- prefix. This field must be provided if `file_url` and `raw_text` are not provided.",
    )
    file_url: Optional[str] = Field(
        None,
        description="External URL of the file to parse. This URL should point to a publicly accessible file that can be downloaded. This field must be provided if `file_id` and `raw_text` are not provided.",
    )
    raw_text: Optional[str] = Field(
        None,
        description="The raw text to parse. This should be a free-text representation of the document. This field must be provided if `file_id` and `file_url` are not provided.",
    )
    labels: Optional[dict] = Field(
        None,
        description='Labels to attach to the parse operation. These labels can be used to store metadata about the parse operation. The format should be a JSON object, e.g. {"key1": "value1", "key2": "value2"}.',
    )
    mime_type: Optional[MimeType] = Field(
        None,
        description="The MIME type of the content provided. This is required if `content` is provided. For content provided as a string, the MIME type should be of subtype `text` i.e. `text/plain` or `text/html`. It is optional if `file_id` or `file_url` are provided, as the MIME type will be inferred from the file. When using `file_id` or `file_url`, the MIME type will be inferred from the file. If provided, the value will have precedence over the inferred MIME type.",
    )
    page_range: Optional[str] = Field(
        None,
        description='The range of pages to parse in the document. This should be a comma-separated list of page numbers or ranges (e.g., "1,2,3-5"). If not provided, all pages will be parsed. This field is optional and can be used to limit the parsing to specific pages of the document. This field is only applicable if the document is a multi-page document i.e., a PDF or a PowerPoint file.',
    )
