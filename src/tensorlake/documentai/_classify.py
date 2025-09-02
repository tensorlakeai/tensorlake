from typing import List, Optional, overload

from ._base import _BaseClient, _validate_file_input
from ._utils import _drop_none
from .models import (
    MimeType,
    PageClassConfig,
)


class _ClassifyMixin(_BaseClient):

    # Sync method overloads
    @overload
    def classify(
        self,
        page_classifications: List[PageClassConfig],
        *,
        file_id: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Classify document by file ID."""

    @overload
    def classify(
        self,
        page_classifications: List[PageClassConfig],
        *,
        file_url: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Classify document from URL."""

    @overload
    def classify(
        self,
        page_classifications: List[PageClassConfig],
        *,
        raw_text: str,
        mime_type: MimeType,  # Required when using raw_text
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
    ) -> str:
        """Classify from raw text. MIME type is required."""

    def classify(
        self,
        page_classifications: List[PageClassConfig],
        file_id: Optional[str] = None,
        file_url: Optional[str] = None,
        raw_text: Optional[str] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """
        Create a new classify operation.

        This operation will classify pages from the specified document based on the provided
        page classification configurations.

        This method is asynchronous. It will return an identifier for the operation, which can be used
        to retrieve the results with the wait_for_completion, or the get_parsed_result methods.

        Args:
            page_classifications: A list of page classification configurations to apply to the document.
                At least one page classification must be provided.

            file_id: The Tensorlake file ID. This is the unique identifier returned by the SDK after
                uploading a file. Either `file_id`, `file_url`, or `raw_text` must be provided.

            file_url: A publicly accessible URL of the file to read. Either `file_id`, `file_url`, or `raw_text` must be provided.

            raw_text: The raw text content to read. Either `file_id`, `file_url`, or `raw_text` must be provided.
                If provided, the MIME type must also be specified.

            page_range: The range of pages to read (e.g., "1-5"). If not specified, all pages will be read.

            labels: Optional labels to apply to the read operation. These labels will be included in the result
                metadata.

            mime_type: The MIME type of the file. This is used to determine how to process the file.
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
                "page_classifications": [
                    pc.model_dump(exclude_none=True) for pc in page_classifications
                ],
            }
        )

        response = self._request("POST", "classify", json=payload)
        json_response = response.json()
        return json_response["parse_id"]

    # Async method overloads
    @overload
    async def classify_async(
        self,
        page_classifications: List[PageClassConfig],
        *,
        file_id: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Classify document by file ID asynchronously."""

    @overload
    async def classify_async(
        self,
        page_classifications: List[PageClassConfig],
        *,
        file_url: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Classify document from URL asynchronously."""

    @overload
    async def classify_async(
        self,
        page_classifications: List[PageClassConfig],
        *,
        raw_text: str,
        mime_type: MimeType,  # Required when using raw_text
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
    ) -> str:
        """Classify from raw text asynchronously. MIME type is required."""

    async def classify_async(
        self,
        page_classifications: List[PageClassConfig],
        file_id: Optional[str] = None,
        file_url: Optional[str] = None,
        raw_text: Optional[str] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """
        Create a new classify operation asynchronously.

        This operation will classify pages from the specified document based on the provided
        page classification configurations.

        This method is asynchronous. It will return an identifier for the operation, which can be used
        to retrieve the results with the wait_for_completion, or the get_parsed_result methods.

        Args:
            page_classifications: A list of page classification configurations to apply to the document.
                At least one page classification must be provided.

            file_id: The Tensorlake file ID. This is the unique identifier returned by the SDK after
                uploading a file. Either `file_id`, `file_url`, or `raw_text` must be provided.

            file_url: A publicly accessible URL of the file to read. Either `file_id`, `file_url`, or `raw_text` must be provided.

            raw_text: The raw text content to read. Either `file_id`, `file_url`, or `raw_text` must be provided.
                If provided, the MIME type must also be specified.

            page_range: The range of pages to read (e.g., "1-5"). If not specified, all pages will be read.

            labels: Optional labels to apply to the read operation. These labels will be included in the result
                metadata.

            mime_type: The MIME type of the file. This is used to determine how to process the file.
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
                "page_classifications": [
                    pc.model_dump(exclude_none=True) for pc in page_classifications
                ],
            }
        )

        response = await self._arequest("POST", "classify", json=payload)
        json_response = response.json()
        return json_response["parse_id"]
