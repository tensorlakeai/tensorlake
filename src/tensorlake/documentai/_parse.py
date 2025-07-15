from __future__ import annotations

import asyncio
import inspect
import json
import time
from typing import Any, Dict, List, Mapping, Optional

from pydantic import BaseModel

from ._base import _BaseClient
from ._utils import _drop_none
from .models import (
    EnrichmentOptions,
    MimeType,
    PageClassConfig,
    PaginatedResult,
    PaginationDirection,
    ParseResult,
    ParseStatus,
    ParsingOptions,
    StructuredExtractionOptions,
)


class _ParseMixin(_BaseClient):

    def parse(
        self,
        file: str,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """
        Parse a document.

        This method allows you to parse a file using the default parsing options provided by Tensorlake,
        or to specify custom parsing options, structured extraction options, enrichment options, and page classifications.

        This method returns a parse_id, which can be used to track the status of the parsing operation. The SDK
        provides methods to check the status of the parsing operation and retrieve the parsed result.

        Args:
            file: The file to parse. This can be a URL, a file ID (from Tensorlake), or raw text.

             parsing_options: Optional parsing options to customize how documents in the dataset are parsed. Tensorlake
                provides default parsing options, but you can specify custom options to tailor the parsing process.

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.

            enrichment_options: Optional enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures.

            page_classifications: Optional list of page classification configurations. If provided, the API will perform
                page classification on the documents in the dataset. This can help in organizing and understanding the
                content of the documents based on their page types.

            page_range: Optional page range to parse. This can be a string like "1,2,3-5" to specify specific pages or ranges.

            labels: Optional labels to attach to the parsed document. This can be a dictionary of key-value pairs.

            mime_type: Optional MIME type of the file. This can be used to specify the type of content being parsed, such as "application/pdf" or "text/plain".
        """

        body = _create_parse_req(
            file,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
            page_range,
            labels,
            mime_type,
        )
        return self._request("POST", "/parse", json=body).json()["parse_id"]

    async def parse_async(self, *args, **kw) -> str:
        """
        Parse a document asynchronously.

        This method allows you to parse a file using the default parsing options provided by Tensorlake,
        or to specify custom parsing options, structured extraction options, enrichment options, and page classifications.

        This method returns a parse_id, which can be used to track the status of the parsing operation. The SDK
        provides methods to check the status of the parsing operation and retrieve the parsed result.

        Args:
            file: The file to parse. This can be a URL, a file ID (from Tensorlake), or raw text.

             parsing_options: Optional parsing options to customize how documents in the dataset are parsed. Tensorlake
                provides default parsing options, but you can specify custom options to tailor the parsing process.

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.

            enrichment_options: Optional enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures.

            page_classifications: Optional list of page classification configurations. If provided, the API will perform
                page classification on the documents in the dataset. This can help in organizing and understanding the
                content of the documents based on their page types.

            page_range: Optional page range to parse. This can be a string like "1,2,3-5" to specify specific pages or ranges.

            labels: Optional labels to attach to the parsed document. This can be a dictionary of key-value pairs.

            mime_type: Optional MIME type of the file. This can be used to specify the type of content being parsed, such as "application/pdf" or "text/plain".
        """
        body = _create_parse_req(*args, **kw)
        resp = await self._arequest("POST", "/parse", json=body)
        return resp.json()["parse_id"]

    def wait_for_completion(self, parse_id: str) -> ParseResult:
        """
        Wait for the completion of a parse operation.

        This method polls the status of a parse operation until it is complete. It checks the status every 5 seconds
        and returns the final ParseResult once the operation is no longer pending or processing.

        Args:
            parse_id: The ID of the parse operation to wait for. This is the string returned by the parse method.
        """
        parse = self.get_parsed_result(parse_id)
        while parse.status in {ParseStatus.PENDING, ParseStatus.PROCESSING}:
            print("waiting 5 s…")
            time.sleep(5)
            parse = self.get_parsed_result(parse_id)
            print(f"parse status: {parse.status.name.lower()}")
        return parse

    async def wait_for_completion_async(self, parse_id: str) -> ParseResult:
        """
        Wait for the completion of a parse operation asynchronously.

        This method polls the status of a parse operation until it is complete. It checks the status every 5 seconds
        and returns the final ParseResult once the operation is no longer pending or processing.

        Args:
            parse_id: The ID of the parse operation to wait for. This is the string returned by the parse method.
        """

        parse = await self.get_parsed_result_async(parse_id)
        while parse.status in {ParseStatus.PENDING, ParseStatus.PROCESSING}:
            print("waiting 5 s…")
            await asyncio.sleep(5)
            parse = await self.get_parsed_result_async(parse_id)
            print(f"parse status: {parse.status}")
        return parse

    def parse_and_wait(
        self,
        file: str,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> ParseResult:
        """
        Parse a document and wait for the result.

        This method combines the parse and wait_for_completion methods to parse a document and return the final
        ParseResult once the parsing operation is complete.

        Args:
            file: The file to parse. This can be a URL, a file ID (from Tensorlake), or raw text.

             parsing_options: Optional parsing options to customize how documents in the dataset are parsed. Tensorlake
                provides default parsing options, but you can specify custom options to tailor the parsing process.

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.

            enrichment_options: Optional enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures.

            page_classifications: Optional list of page classification configurations. If provided, the API will perform
                page classification on the documents in the dataset. This can help in organizing and understanding the
                content of the documents based on their page types.

            page_range: Optional page range to parse. This can be a string like "1,2,3-5" to specify specific pages or ranges.

            labels: Optional labels to attach to the parsed document. This can be a dictionary of key-value pairs.

            mime_type: Optional MIME type of the file. This can be used to specify the type of content being parsed, such as "application/pdf" or "text/plain".
        """
        parse_id = self.parse(
            file,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
            page_range,
            labels,
            mime_type,
        )
        return self.wait_for_completion(parse_id)

    async def parse_and_wait_async(
        self,
        file: str,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> ParseResult:
        """
        Parse a document and wait for the result asynchronously.

        This method combines the parse_async and wait_for_completion_async methods to parse a document and return the final
        ParseResult once the parsing operation is complete.

        Args:
            file: The file to parse. This can be a URL, a file ID (from Tensorlake), or raw text.
            parsing_options: Optional parsing options to customize how documents in the dataset are parsed.
            structured_extraction_options: Optional structured extraction options to guide the extraction of structured data.
            enrichment_options: Optional enrichment options to extend the output of the document parsing process.
            page_classificat            file: The file to parse. This can be a URL, a file ID (from Tensorlake), or raw text.

             parsing_options: Optional parsing options to customize how documents in the dataset are parsed. Tensorlake
                provides default parsing options, but you can specify custom options to tailor the parsing process.

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.

            enrichment_options: Optional enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures.

            page_classifications: Optional list of page classification configurations. If provided, the API will perform
                page classification on the documents in the dataset. This can help in organizing and understanding the
                content of the documents based on their page types.

            page_range: Optional page range to parse. This can be a string like "1,2,3-5" to specify specific pages or ranges.

            labels: Optional labels to attach to the parsed document. This can be a dictionary of key-value pairs.

            mime_type: Optional MIME type of the file. This can be used to specify the type of content being parsed, such as "application/pdf" or "text/plain".
        """
        parse_id = await self.parse_async(
            file,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
            page_range,
            labels,
            mime_type,
        )
        return await self.wait_for_completion_async(parse_id)

    def get_parsed_result(self, parse_id: str) -> ParseResult:
        """
        Get the result of a parse operation.

        If the parse operation is still in progress, the ParseResult will not contain the parsed data.

        Args:
            parse_id: The ID of the parse operation to retrieve. This is the string returned by the parse method.
        """
        return ParseResult.model_validate(
            self._request("GET", f"parse/{parse_id}").json()
        )

    async def get_parsed_result_async(self, parse_id: str) -> ParseResult:
        """
        Get the result of a parse operation asynchronously.

        If the parse operation is still in progress, the ParseResult will not contain the parsed data.

        Args:
            parse_id: The ID of the parse operation to retrieve. This is the string returned by the parse method.
        """
        resp = await self._arequest("GET", f"parse/{parse_id}")
        return ParseResult.model_validate(resp.json())

    def delete_parse(self, parse_id: str) -> None:
        """
        Delete a parse operation.

        This will remove the parse and its associated data from the system and it cannot be recovered.

        If the parse operation is still pending or processing, this method will raise an error.

        Args:
            parse_id: The ID of the parse operation to delete. This is the string returned by the parse method.
        """
        parse = self.get_parsed_result(parse_id)

        if parse.status in {ParseStatus.PENDING, ParseStatus.PROCESSING}:
            raise ValueError(
                "Cannot delete a parse operation that is still pending or processing. "
                "Please wait for the operation to complete before deleting."
            )

        self._request("DELETE", f"parse/{parse.parse_id}")

    def delete_parse_async(self, parse_id: str) -> None:
        """
        Delete a parse operation asynchronously.

        This will remove the parse and its associated data from the system and it cannot be recovered.

        If the parse operation is still pending or processing, this method will raise an error.

        Args:
            parse_id: The ID of the parse operation to delete. This is the string returned by the parse method.
        """
        parse = self.get_parsed_result(parse_id)

        if parse.status in {ParseStatus.PENDING, ParseStatus.PROCESSING}:
            raise ValueError(
                "Cannot delete a parse operation that is still pending or processing. "
                "Please wait for the operation to complete before deleting."
            )

        return self._arequest("DELETE", f"parse/{parse.parse_id}")

    def list_parse_results(
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
    ) -> PaginatedResult[ParseResult]:
        """
        List every parse result in the Tensorlake project.

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

        params: Dict[str, Any] = _drop_none(
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

        resp = self._request("GET", "parse", params=params)
        return PaginatedResult[ParseResult].model_validate(
            resp.json(), from_attributes=True
        )

    async def list_parse_results_async(
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
    ) -> PaginatedResult[ParseResult]:
        """
        List every parse result in the Tensorlake project asynchronously.

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
        params: Dict[str, Any] = _drop_none(
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

        resp = await self._arequest("GET", "parse", params=params)
        return PaginatedResult[ParseResult].model_validate(
            resp.json(), from_attributes=True
        )


def _create_parse_req(
    file: str,
    parsing_options: Optional[ParsingOptions] = None,
    structured_extraction_options: Optional[List[StructuredExtractionOptions]] = None,
    enrichment_options: Optional[EnrichmentOptions] = None,
    page_classifications: Optional[List[PageClassConfig]] = None,
    page_range: Optional[str] = None,
    labels: Optional[dict] = None,
    mime_type: Optional[MimeType] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}

    if file.startswith(("http://", "https://")):
        payload["file_url"] = file
    elif file.startswith("tensorlake-"):
        payload["file_id"] = file
    else:
        payload["raw_text"] = file

    if labels:
        payload["labels"] = labels
    if page_range:
        payload["page_range"] = page_range
    if mime_type:
        payload["mime_type"] = mime_type.value

    if parsing_options:
        payload["parsing_options"] = parsing_options.model_dump(exclude_none=True)
    if enrichment_options:
        payload["enrichment_options"] = enrichment_options.model_dump(exclude_none=True)
    if page_classifications:
        payload["page_classifications"] = [
            pc.model_dump(exclude_none=True) for pc in page_classifications
        ]

    if structured_extraction_options:
        payload["structured_extraction_options"] = [
            _convert_seo(opt) for opt in structured_extraction_options
        ]

    return payload


def _convert_seo(opt) -> Dict[str, Any]:
    """Convert StructuredExtractionOptions to plain dict with JSON schema resolved."""
    d = opt.model_dump(exclude_none=True)

    if hasattr(opt, "json_schema"):
        schema = opt.json_schema

        if inspect.isclass(schema) and issubclass(schema, BaseModel):
            d["json_schema"] = schema.model_json_schema()

        elif isinstance(schema, BaseModel):
            d["json_schema"] = schema.model_json_schema()

        elif isinstance(schema, Mapping):
            d["json_schema"] = dict(schema)

        else:
            try:
                d["json_schema"] = json.loads(schema)
            except (json.JSONDecodeError, TypeError):
                d["json_schema"] = schema

    return d
