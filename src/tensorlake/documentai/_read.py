from typing import Optional

from ._base import _BaseClient, _validate_file_input
from ._completion_waiter import WaitableOperation, _CompletionWaiter
from .models import (
    MimeType,
    ParsingOptions,
    EnrichmentOptions,
    ReadResult,
    PaginationDirection,
    ParseStatus,
    PaginatedResult,
)
from ._utils import _drop_none


class _ReadMixin(_CompletionWaiter, _BaseClient):

    def read(
        self,
        file_id: Optional[str],
        file_url: Optional[str],
        raw_text: Optional[str],
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> str:
        """
        Create a new read operation.

        This operation will extract text from the specified document.

        This method is asynchronous. It will return an identifier for the operation, which you can
        use to check the status of the operation or retrieve the results once it's complete.

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

        payload = {
            "file_id": file_id,
            "file_url": file_url,
            "raw_text": raw_text,
            "page_range": page_range,
            "labels": labels,
            "mime_type": mime_type.value if mime_type else None,
            "parsing_options": parsing_options.model_dump(exclude_none=True),
            "enrichment_options": enrichment_options.model_dump(exclude_none=True),
        }

        response = self._request("POST", "read", json=payload)
        json_response = response.json()
        return json_response["read_id"]

    def get_read_result(self, read_id: str) -> ReadResult:
        """
        Get the result of a read operation.

        If the read operation is still in progress, the ReadResult will not contain the parsed data.

        Args:
            read_id: The ID of the read operation to retrieve. This is the string returned by the read method.
        """
        response = self._request("GET", f"read/{read_id}")
        json_response = response.json()
        return ReadResult.model_validate(json_response)

    def wait_for_read_completion(self, read_id: str) -> ReadResult:
        """
        Wait for the completion of a read operation.

        This methods establishes a connection to the server-sent events (SSE) endpoint for the specified read ID
        and listens for updates until the read operation is complete.

        Args:
            read_id: The ID of the read operation to wait for. This is the string returned by the read method.
        """
        return self._base_wait_for_completion(
            read_id, WaitableOperation.READ, ReadResult
        )

    def _base_get_result(self, entity_id):
        return self.get_read_result(entity_id)

    def read_and_wait(
        self,
        file_id: Optional[str],
        file_url: Optional[str],
        raw_text: Optional[str],
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ) -> ReadResult:
        """
        Read a document and wait for the operation to complete.

        This method combines the read and wait_for_read_completion methods into a single call.

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
        read_id = self.read(
            file_id=file_id,
            file_url=file_url,
            raw_text=raw_text,
            page_range=page_range,
            labels=labels,
            mime_type=mime_type,
            parsing_options=parsing_options,
            enrichment_options=enrichment_options,
        )
        return self.wait_for_read_completion(read_id)

    def delete_read(self, read_id: str) -> None:
        """
        Delete a read operation.

        This will remove the read and its associated data from the system and it cannot be recovered.

        If the read operation is still pending or processing, this method will raise an error.

        Args:
            read_id: The ID of the read operation to delete. This is the string returned by the read method.
        """
        self._request("DELETE", f"read/{read_id}")

    def list_read_results(
        self,
        cursor: Optional[str] = None,
        direction: Optional[PaginationDirection] = None,
        dataset_name: Optional[str] = None,
        limit: Optional[int] = None,
        filename: Optional[str] = None,
        status: Optional[ParseStatus] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        finished_after: Optional[str] = None,
        finished_before: Optional[str] = None,
    ) -> PaginatedResult[ReadResult]:
        """
        List every read result in the Tensorlake project.

        Args:
            cursor: Optional cursor for pagination. If provided, the method will return the next page of
                results starting from this cursor. If not provided, it will return the first page of results.

            direction: Optional pagination direction. If provided, it can be "next" or "prev" to navigate through the pages.

            dataset_name: Optional name of the dataset to filter the results by. If provided, only parse results
                associated with this dataset will be returned.
            limit: Optional limit on the number of results to return. If not provided, a default limit will be used.

            filename: Optional filename to filter the results by. If provided, only parse results associated with this filename will be returned.

            status: Optional status to filter the results by. If provided, only parse results with this status will be returned.

            created_after: Optional timestamp to filter the results by creation time. If provided, only parse results created after this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

            created_before: Optional timestamp to filter the results by creation time. If provided, only parse results created before this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

            finished_after: Optional timestamp to filter the results by finish time. If provided, only parse results finished after this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

            finished_before: Optional timestamp to filter the results by finish time. If provided, only parse results finished before this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").
        """
        params = _drop_none(
            {
                "cursor": cursor,
                "direction": direction.value if direction else None,
                "dataset_name": dataset_name,
                "limit": limit,
                "filename": filename,
                "status": status.value if status else None,
                "created_after": created_after,
                "created_before": created_before,
                "finished_after": finished_after,
                "finished_before": finished_before,
            }
        )

        response = self._request("GET", "read", params=params)
        return PaginatedResult[ReadResult].model_validate(
            response.json(), from_attributes=True
        )

    # Async API
    async def read_async(
        self,
        file_id: Optional[str],
        file_url: Optional[str],
        raw_text: Optional[str],
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ):
        """
        Create a new read operation asynchronously.

        This operation will extract text from the specified document.

        This method is asynchronous. It will return an identifier for the operation, which you can
        use to check the status of the operation or retrieve the results once it's complete.

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

        payload = {
            "file_id": file_id,
            "file_url": file_url,
            "raw_text": raw_text,
            "page_range": page_range,
            "labels": labels,
            "mime_type": mime_type.value if mime_type else None,
            "parsing_options": parsing_options.model_dump(exclude_none=True),
            "enrichment_options": enrichment_options.model_dump(exclude_none=True),
        }

        response = await self._arequest("POST", "read", json=payload)
        json_response = response.json()
        return json_response["read_id"]

    async def get_read_result_async(self, read_id: str) -> ReadResult:
        """
        Get the result of a read operation.

        If the read operation is still in progress, the ReadResult will not contain the parsed data.

        Args:
            read_id: The ID of the read operation to retrieve. This is the string returned by the read method.
        """
        response = await self._arequest("GET", f"read/{read_id}")
        json_response = response.json()
        return ReadResult.model_validate(json_response)

    async def wait_for_completion_async(self, read_id: str) -> ReadResult:
        """
        Wait for the completion of a read operation asynchronously.

        This methods establishes a connection to the server-sent events (SSE) endpoint for the specified read ID
        and listens for updates until the read operation is complete.

        Args:
            read_id: The ID of the read operation to wait for. This is the string returned by the read method.
        """
        return await self._base_wait_for_completion_async(
            read_id, WaitableOperation.READ, ReadResult
        )

    async def read_and_wait_async(
        self,
        file_id: Optional[str],
        file_url: Optional[str],
        raw_text: Optional[str],
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        parsing_options: Optional[ParsingOptions] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
    ):
        """
        Read a document and wait for the operation to complete asynchronously.

        This method combines the read and wait_for_read_completion methods into a single call.

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
        read_id = await self.read_async(
            file_id=file_id,
            file_url=file_url,
            raw_text=raw_text,
            page_range=page_range,
            labels=labels,
            mime_type=mime_type,
            parsing_options=parsing_options,
            enrichment_options=enrichment_options,
        )
        return await self.wait_for_completion_async(read_id)

    async def delete_read_async(self, read_id: str) -> None:
        """
        Delete a read operation.

        Args:
            read_id: The ID of the read operation to delete.
        """
        await self._arequest("DELETE", f"read/{read_id}")

    async def list_read_results_async(
        self,
        cursor: Optional[str] = None,
        direction: Optional[PaginationDirection] = None,
        dataset_name: Optional[str] = None,
        limit: Optional[int] = None,
        filename: Optional[str] = None,
        status: Optional[ParseStatus] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        finished_after: Optional[str] = None,
        finished_before: Optional[str] = None,
    ) -> list[ReadResult]:
        """
        List every read result in the Tensorlake project.

        Args:
            cursor: Optional cursor for pagination. If provided, the method will return the next page of
                results starting from this cursor. If not provided, it will return the first page of results.

            direction: Optional pagination direction. If provided, it can be "next" or "prev" to navigate through the pages.

            dataset_name: Optional name of the dataset to filter the results by. If provided, only parse results
                associated with this dataset will be returned.
            limit: Optional limit on the number of results to return. If not provided, a default limit will be used.

            filename: Optional filename to filter the results by. If provided, only parse results associated with this filename will be returned.

            status: Optional status to filter the results by. If provided, only parse results with this status will be returned.

            created_after: Optional timestamp to filter the results by creation time. If provided, only parse results created after this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

            created_before: Optional timestamp to filter the results by creation time. If provided, only parse results created before this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

            finished_after: Optional timestamp to filter the results by finish time. If provided, only parse results finished after this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

            finished_before: Optional timestamp to filter the results by finish time. If provided, only parse results finished before this timestamp will be
                returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").
        """
        params = _drop_none(
            {
                "cursor": cursor,
                "direction": direction.value if direction else None,
                "dataset_name": dataset_name,
                "limit": limit,
                "filename": filename,
                "status": status.value if status else None,
                "created_after": created_after,
                "created_before": created_before,
                "finished_after": finished_after,
                "finished_before": finished_before,
            }
        )

        response = await self._arequest("GET", "read", params=params)
        return PaginatedResult[ReadResult].model_validate(
            response.json(), from_attributes=True
        )

    async def _base_get_result_async(self, _entity_id):
        return await self.get_read_result_async(_entity_id)
