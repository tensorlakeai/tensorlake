from typing import Optional, List, Union

from ._base import _BaseClient, _validate_file_input
from .models import MimeType, PageClassConfig, StructuredExtractionOptions
from ._utils import _drop_none

from ._parse import _convert_seo


class _ExtractMixin(_BaseClient):

    def extract(
        self,
        structured_extraction_options: Union[
            StructuredExtractionOptions, List[StructuredExtractionOptions]
        ],
        file_id: Optional[str],
        file_url: Optional[str],
        raw_text: Optional[str],
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
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

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.
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
            }
        )

        if isinstance(structured_extraction_options, StructuredExtractionOptions):
            payload["structured_extraction_options"] = [
                _convert_seo(structured_extraction_options)
            ]
        else:
            payload["structured_extraction_options"] = [
                _convert_seo(opt) for opt in structured_extraction_options
            ]

        response = self._request("POST", "extract", json=payload)
        json_response = response.json()
        return json_response["parse_id"]

    async def extract_async(
        self,
        structured_extraction_options: Union[
            StructuredExtractionOptions, List[StructuredExtractionOptions]
        ],
        file_id: Optional[str],
        file_url: Optional[str],
        raw_text: Optional[str],
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
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

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.
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
            }
        )

        if isinstance(structured_extraction_options, StructuredExtractionOptions):
            payload["structured_extraction_options"] = [
                _convert_seo(structured_extraction_options)
            ]
        else:
            payload["structured_extraction_options"] = [
                _convert_seo(opt) for opt in structured_extraction_options
            ]

        response = await self._arequest("POST", "extract", json=payload)
        json_response = response.json()
        return json_response["parse_id"]
