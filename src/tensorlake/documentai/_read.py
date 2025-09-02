from typing import Optional, overload

from ._base import _BaseClient, _validate_file_input
from ._utils import _drop_none
from .models import (
    EnrichmentOptions,
    MimeType,
    ParsingOptions,
)


class _ReadMixin(_BaseClient):

    # Sync method overloads
    @overload
    def read(
        self,
        *,
        file_id: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """Read document by file ID."""

    @overload
    def read(
        self,
        *,
        file_url: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """Read document from URL."""

    @overload
    def read(
        self,
        *,
        raw_text: str,
        mime_type: MimeType,  # Required when using raw_text
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """Read from raw text. MIME type is required."""

    def read(
        self,
        file_id: Optional[str] = None,
        file_url: Optional[str] = None,
        raw_text: Optional[str] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """
        Create a new read operation.

        This operation will extract text from the specified document.

        This method is asynchronous. It will return an identifier for the operation, which can be used
        to retrieve the results with the wait_for_completion, or the get_parsed_result methods.

        Args:
            file_id: The Tensorlake file ID. This is the unique identifier returned by the SDK after
                uploading a file. Either `file_id`, `file_url`, or `raw_text` must be provided.

            file_url: A publicly accessible URL of the file to read. Either `file_id`, `file_url`, or `raw_text` must be provided.

            raw_text: The raw text content to read. Either `file_id`, `file_url`, or `raw_text` must be provided.
                If provided, the MIME type must also be specified.

            page_range: The range of pages to read (e.g., "1-5"). If not specified, all pages will be read.

            labels: Optional labels to apply to the read operation. These labels will be included in the result
                metadata.

            mime_type: The MIME type of the file. This is used to determine how to process the file.

            parsing_options: Options for parsing the document. Tensorlake already provides sane defaults for most use cases.

            enrichment_options:  Options for enriching a document with additional information.

                This object helps to extend the output of the document parsing process with additional information.

                This includes summarization of tables and figures, which can help to provide a more comprehensive understanding of the document.
        """
        _validate_file_input(
            file_id=file_id, file_url=file_url, raw_text=raw_text, mime_type=mime_type
        )

        payload = _drop_none(
            {
                "file_id": file_id,
                "file_url": file_url,
                "raw_text": raw_text,
                "page_range": page_range,
                "labels": labels,
                "mime_type": mime_type.value if mime_type else None,
                "parsing_options": (
                    parsing_options.model_dump(exclude_none=True)
                    if parsing_options
                    else None
                ),
                "enrichment_options": (
                    enrichment_options.model_dump(exclude_none=True)
                    if enrichment_options
                    else None
                ),
            }
        )

        response = self._request("POST", "read", json=payload)
        json_response = response.json()
        return json_response["parse_id"]

    # Async method overloads
    @overload
    async def read_async(
        self,
        *,
        file_id: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """Read document by file ID asynchronously."""

    @overload
    async def read_async(
        self,
        *,
        file_url: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """Read document from URL asynchronously."""

    @overload
    async def read_async(
        self,
        *,
        raw_text: str,
        mime_type: MimeType,  # Required when using raw_text
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """Read from raw text asynchronously. MIME type is required."""

    async def read_async(
        self,
        file_id: Optional[str] = None,
        file_url: Optional[str] = None,
        raw_text: Optional[str] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """
        Create a new read operation asynchronously.

        This operation will extract text from the specified document.

        This method is asynchronous. It will return an identifier for the operation, which can be used
        to retrieve the results with the wait_for_completion, or the get_parsed_result methods.

        Args:
            file_id: The Tensorlake file ID. This is the unique identifier returned by the SDK after
                uploading a file. Either `file_id`, `file_url`, or `raw_text` must be provided.

            file_url: A publicly accessible URL of the file to read. Either `file_id`, `file_url`, or `raw_text` must be provided.

            raw_text: The raw text content to read. Either `file_id`, `file_url`, or `raw_text` must be provided.
                If provided, the MIME type must also be specified.

            page_range: The range of pages to read (e.g., "1-5"). If not specified, all pages will be read.

            labels: Optional labels to apply to the read operation. These labels will be included in the result
                metadata.

            mime_type: The MIME type of the file. This is used to determine how to process the file.

            parsing_options: Options for parsing the document. Tensorlake already provides sane defaults for most use cases.

            enrichment_options:  Options for enriching a document with additional information.

                This object helps to extend the output of the document parsing process with additional information.

                This includes summarization of tables and figures, which can help to provide a more comprehensive understanding of the document.
        """
        _validate_file_input(
            file_id=file_id, file_url=file_url, raw_text=raw_text, mime_type=mime_type
        )

        payload = _drop_none(
            {
                "file_id": file_id,
                "file_url": file_url,
                "raw_text": raw_text,
                "page_range": page_range,
                "labels": labels,
                "mime_type": mime_type.value if mime_type else None,
                "parsing_options": (
                    parsing_options.model_dump(exclude_none=True)
                    if parsing_options
                    else None
                ),
                "enrichment_options": (
                    enrichment_options.model_dump(exclude_none=True)
                    if enrichment_options
                    else None
                ),
            }
        )

        response = await self._arequest("POST", "read", json=payload)
        json_response = response.json()
        return json_response["parse_id"]
