import os
from enum import Enum
from typing import Optional, Type

import httpx
from pydantic import BaseModel, Json

from tensorlake.documentai.api import Document
from tensorlake.documentai.common import DOC_AI_BASE_URL


class OutputFormat(str, Enum):
    MARKDOWN = "markdown"
    JSON = "json"

class ChunkingStrategy(str, Enum):
    NONE = "none"
    PAGE = "page"
    SECTION_HEADER = "section_header"


class TableParsingStrategy(str, Enum):
    TSR = "tsr"
    VLM = "vlm"

class ParsingOptions(BaseModel):
    """
    Options for parsing a document.
    """
    format: OutputFormat = OutputFormat.MARKDOWN
    chunking_strategy: Optional[ChunkingStrategy] = None
    table_parsing_strategy: TableParsingStrategy = TableParsingStrategy.TSR
    table_parsing_prompt: Optional[str] = None
    summarize_table: bool = False
    summarize_figure: bool = False
    page_range: Optional[str] = None
    deliver_webhook: bool = False

class ExtractionOptions(BaseModel):
    """
    Options for parsing a document.
    """
    json_schema: Optional[Json]
    model: Type[BaseModel]
    deliver_webhook: bool = False


class DocumentParser:

    def __init__(self, api_key: str=""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None, headers=self._headers())

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _create_parse_req(self, file: str, options: ParsingOptions) -> dict:
        payload = {
            "file": file,
            "outputMode": options.format.value,
            "deliverWebhook": options.deliver_webhook 
        }
        if options.chunking_strategy:
            payload["chunkStrategy"] = options.chunking_strategy.value
    
        if options.page_range:
            payload["pages"] = options.page_range 
        return payload
    
    def _create_extract_req(self, file: str, options: ExtractionOptions) -> dict:
        payload = {
            "file": file,
            "schema": options.schema,
            "deliverWebhook": options.deliver_webhook 
        }
        return payload
 

    def parse(self, file: str, options: ParsingOptions, timeout: int=5) -> str:
        """
        Parse a document.
        """
        response = self._client.post(
            url="/parse_async",
            headers=self._headers(),
            json=self._create_parse_req(file, options),
            timeout=2,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        resp = response.json()
        return resp.get("jobId")

    def extract(self, file: str, options: ExtractionOptions, timeout: int=5) -> str:
        """
        Parse a document.
        """
        response = self._client.post(
            url="/extract_async",
            headers=self._headers(),
            json=self._create_extract_req(file, options),
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        resp = response.json()
        return resp.get("jobId")
