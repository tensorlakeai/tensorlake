from __future__ import annotations

import asyncio
import inspect
import json
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Union

from httpx_sse import ServerSentEvent, aconnect_sse, connect_sse
from pydantic import BaseModel, ValidationError
from rich.live import Live
from rich.text import Text

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
            Union[StructuredExtractionOptions, List[StructuredExtractionOptions]]
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

    def wait_for_completion(self, parse_id: str):
        """
        Wait for the completion of a parse operation.

        This methods establishes a connection to the server-sent events (SSE) endpoint for the specified parse ID
        and listens for updates until the parse operation is complete.

        Args:
            parse_id: The ID of the parse operation to wait for. This is the string returned by the parse method.
        """
        status_text = Text("Waiting for completion of parse job.", style="bold")
        retry_count = 0

        with Live(
            status_text,
            refresh_per_second=4,
            transient=True,
            redirect_stdout=True,
            redirect_stderr=True,
        ) as live:
            # Print static info above the live line
            live.console.print(f"Parse ID: {parse_id}")

            def set_status(message: str, style: Optional[str] = None) -> None:
                live.update(Text(message, style=style), refresh=True)

            def print_line(message: str) -> None:
                live.console.print(message)

            while retry_count < 5:
                try:
                    with connect_sse(
                        client=self._client,
                        method="GET",
                        url=f"parse/{parse_id}",
                        headers=self._headers(),
                    ) as sse:
                        for sse_event in sse.iter_sse():
                            parse_result = self._handle_sse_event(
                                sse_event, set_status, print_line
                            )
                            if parse_result:
                                # Final status has already been rendered via set_status
                                return parse_result

                        live.console.print(
                            "[yellow]SSE connection ended without completion event[/yellow]"
                        )

                except (ConnectionError, TimeoutError) as e:
                    retry_count += 1
                    live.console.print(
                        f"[yellow]Connection issue (attempt {retry_count} / 5): {e}[/yellow]"
                    )
                    if retry_count < 5:
                        wait_time = min(2**retry_count, 30)
                        live.console.print(
                            f"[yellow]Retrying in {wait_time} seconds...[/yellow]"
                        )
                        time.sleep(wait_time)

            live.console.print(
                "[yellow]Max retries reached. Checking final status...[/yellow]"
            )
            # Fetch final status and render a final line
            final_result = self.get_parsed_result(parse_id)
            if final_result.status:
                # Best-effort styling
                style = (
                    "green"
                    if str(final_result.status).lower()
                    in {"done", "success", "completed"}
                    else (
                        "red"
                        if str(final_result.status).lower() in {"failed", "error"}
                        else "magenta"
                    )
                )
                set_status(f"Status: {final_result.status}", style)
            return final_result

    async def wait_for_completion_async(self, parse_id: str) -> ParseResult:
        """
        Wait for the completion of a parse operation asynchronously.

        This methods establishes a connection to the server-sent events (SSE) endpoint for the specified parse ID
        and listens for updates until the parse operation is complete.

        Args:
            parse_id: The ID of the parse operation to wait for. This is the string returned by the parse method.
        """
        status_text = Text("Waiting for completion of parse job.", style="bold")
        retry_count = 0

        with Live(
            status_text,
            refresh_per_second=4,
            transient=True,
            redirect_stdout=True,
            redirect_stderr=True,
        ) as live:
            live.console.print(f"Parse ID: {parse_id}")

            def set_status(message: str, style: Optional[str] = None) -> None:
                live.update(Text(message, style=style), refresh=True)

            def print_line(message: str) -> None:
                live.console.print(message)

            while retry_count < 5:
                try:
                    async with aconnect_sse(
                        client=self._aclient,
                        method="GET",
                        url=f"parse/{parse_id}",
                        headers=self._headers(),
                    ) as sse:
                        async for sse_event in sse.aiter_sse():
                            parse_result = self._handle_sse_event(
                                sse_event, set_status, print_line
                            )
                            if parse_result:
                                return parse_result

                            # Always yield after processing each event
                            await asyncio.sleep(0)

                        live.console.print(
                            "[yellow]SSE connection ended without completion event[/yellow]"
                        )
                except (ConnectionError, TimeoutError) as e:
                    retry_count += 1
                    live.console.print(
                        f"[yellow]Connection issue (attempt {retry_count} / 5): {e}[/yellow]"
                    )
                    if retry_count < 5:
                        wait_time = min(2**retry_count, 30)
                        live.console.print(
                            f"[yellow]Retrying in {wait_time} seconds...[/yellow]"
                        )
                        await asyncio.sleep(wait_time)

            live.console.print(
                "[yellow]Max retries reached. Checking final status...[/yellow]"
            )
            final_result = await self.get_parsed_result_async(parse_id)
            if final_result.status:
                style = (
                    "green"
                    if str(final_result.status).lower()
                    in {"done", "success", "completed"}
                    else (
                        "red"
                        if str(final_result.status).lower() in {"failed", "error"}
                        else "magenta"
                    )
                )
                set_status(f"Status: {final_result.status}", style)
            return final_result

    def _handle_sse_event(
        self,
        sse_event: ServerSentEvent,
        set_status: Callable[[str, Optional[str]], None],
        print_line: Callable[[str], None],
    ) -> Optional[ParseResult]:
        """
        Handle SSE event and return True if parse is complete (success or failure).
        """
        match sse_event.event:
            case "parse_update":
                try:
                    parse_result = ParseResult.model_validate_json(sse_event.data)
                    set_status(f"Status: {parse_result.status.value}", "magenta")
                except ValidationError:
                    set_status(f"Parse update received: {sse_event.data}", "blue")

                return None
            case "parse_done":
                parse_result = ParseResult.model_validate_json(sse_event.data)
                set_status(f"Parse ID: {parse_result.parse_id} done", "green")
                return parse_result
            case "parse_failed":
                parse_result = ParseResult.model_validate_json(sse_event.data)
                message = (
                    f"Parse failed ({parse_result.parse_id}): {parse_result.error}"
                    if parse_result.error
                    else f"Parse failed ({parse_result.parse_id})"
                )
                set_status(message, "red")
                # Also print a persistent line above the live display so it remains after exit
                print_line(f"[red]{message}[/red]")
                return parse_result
            case "parse_queued":
                set_status("Parse job waiting in queue.", "yellow")
                return None
            case _:
                set_status(f"Unknown SSE event: {sse_event.event}", "cyan")
                return None

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
        self._request("DELETE", f"parse/{parse_id}")

    async def delete_parse_async(self, parse_id: str) -> None:
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

        await self._arequest("DELETE", f"parse/{parse.parse_id}")

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
    structured_extraction_options: Optional[
        Union[StructuredExtractionOptions, List[StructuredExtractionOptions]]
    ] = None,
    enrichment_options: Optional[EnrichmentOptions] = None,
    page_classifications: Optional[List[PageClassConfig]] = None,
    page_range: Optional[str] = None,
    labels: Optional[dict] = None,
    mime_type: Optional[MimeType] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}

    if file.startswith(("http://", "https://")):
        payload["file_url"] = file
    elif file.startswith("tensorlake-") or file.startswith("file_"):
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
        if isinstance(structured_extraction_options, StructuredExtractionOptions):
            payload["structured_extraction_options"] = [
                _convert_seo(structured_extraction_options)
            ]
        else:
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
