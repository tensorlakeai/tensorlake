from __future__ import annotations

import asyncio
import inspect
import json
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from .models import (
    EnrichmentOptions,
    MimeType,
    PageClassConfig,
    ParseResult,
    ParseStatus,
    ParsingOptions,
    StructuredExtractionOptions,
)
from ._base import _BaseClient


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
        Parse
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
        Parse async
        """
        body = _create_parse_req(*args, **kw)
        resp = await self._arequest("POST", "/parse", json=body)
        return resp.json()["parse_id"]

    def wait_for_completion(self, parse_id: str) -> ParseResult:
        """
        Given a parse_id, poll for status until its completion.
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
        Wait async
        """

        parse = await self.get_parsed_result_async(parse_id)
        while parse.status in {ParseStatus.PENDING, ParseStatus.PROCESSING}:
            print("waiting 5 s…")
            await asyncio.sleep(5)
            parse = await self.get_parsed_result_async(parse_id)
            print(f"parse status: {parse.status}")
        return parse

    def get_parsed_result(self, parse_id: str) -> ParseResult:
        """
        get
        """
        return ParseResult.model_validate(
            self._request("GET", f"parse/{parse_id}").json()
        )

    async def get_parsed_result_async(self, parse_id: str) -> ParseResult:
        """
        Get async
        """
        resp = await self._arequest("GET", f"parse/{parse_id}")
        return ParseResult.model_validate(resp.json())


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


def _convert_seo(opt: StructuredExtractionOptions) -> Dict[str, Any]:
    """Convert StructuredExtractionOptions to plain dict with JSON schema resolved."""
    d = opt.model_dump(exclude_none=True)
    if hasattr(opt, "json_schema"):
        schema = opt.json_schema
        if inspect.isclass(schema) and issubclass(schema, BaseModel):
            d["json_schema"] = schema.model_json_schema()
        elif isinstance(schema, BaseModel):
            d["json_schema"] = schema.model_json_schema()
        else:  # assume str
            try:
                d["json_schema"] = json.loads(schema)
            except json.JSONDecodeError:
                d["json_schema"] = schema
    return d
