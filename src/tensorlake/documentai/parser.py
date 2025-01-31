import os
import httpx
from enum import Enum
from typing import Optional
from pydantic import BaseModel

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


class DocumentParser:

    def __init__(self, api_key: str=""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

    def _headers(self) -> dict:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        return headers

    def _create_req(self, file: str, options: ParsingOptions) -> dict:
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
 

    def parse_document(self, file: str, options: ParsingOptions, timeout: int=5) -> str:
        """
        Parse a document.
        """
        with httpx.Client() as client:
            response = client.post(
                url=DOC_AI_BASE_URL,
                headers=self._headers(),
                json=self._create_req(file, options),
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                print(e.response.text)
                raise e
            resp = response.json()
            return resp.get("jobId")

    
    async def parse_document_async(self, path: str, options: ParsingOptions) -> Document:
        """
        Parse a document asynchronously.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url=DOC_AI_BASE_URL,
                headers=self._headers(),
                json=self._create_req()
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                print(e.response)
                raise e
            resp = response.json()
            return resp.get("jobId")