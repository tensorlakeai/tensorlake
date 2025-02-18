"""
Tensorlake Document AI client
"""

import json
import os
from pathlib import Path
from typing import Optional, Union

import httpx
from pydantic import BaseModel, Json
from retry import retry

from tensorlake.documentai.common import (
    DOC_AI_BASE_URL,
    ChunkingStrategy,
    FileUploader,
    JobResult,
    ModelProvider,
    OutputFormat,
    TableOutputMode,
    TableParsingStrategy,
)
from tensorlake.documentai.datasets import Dataset


class ParsingOptions(BaseModel):
    """
    Options for parsing a document.
    """

    format: OutputFormat = OutputFormat.MARKDOWN
    chunking_strategy: Optional[ChunkingStrategy] = None
    table_parsing_strategy: TableParsingStrategy = TableParsingStrategy.TSR
    table_parsing_prompt: Optional[str] = None
    figure_summarization_prompt: Optional[str] = None
    table_output_mode: TableOutputMode = TableOutputMode.MARKDOWN
    summarize_table: bool = False
    summarize_figure: bool = False
    page_range: Optional[str] = None
    deliver_webhook: bool = False


class ExtractionOptions(BaseModel):
    """
    Options for structured data extraction.

    Args:
        json_schema: The JSON schema to guide structured data extraction from the file.
        model: The model provider to use for structured data extraction.. Defaults to ModelProvider.TENSORLAKE.
        deliver_webhook: Whether to deliver the result to a webhook. Defaults to False.
        prompt: Override the prompt to customize structured extractions. Use this if you want to extract data froma file using a different prompt than the one we use to extract.
        table_parsing_strategy: The algorithm to use for parsing tables in the document. Defaults to TableParsingStrategy.TSR.
    """

    json_schema: Optional[Json]
    model: ModelProvider = ModelProvider.TENSORLAKE
    deliver_webhook: bool = False
    prompt: Optional[str] = None
    table_parsing_strategy: TableParsingStrategy = TableParsingStrategy.TSR


class DatasetOptions(BaseModel):
    """DocumentAI create dataset request class."""

    name: str
    description: Optional[str] = None
    parsing_options: Optional[ParsingOptions] = None
    extraction_options: Optional[ExtractionOptions] = None


class DocumentAI:
    """
    Document AI client for Tensorlake.
    """

    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None)
        self._async_client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        self.__file_uploader__ = FileUploader(api_key=api_key)

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def get_job(self, job_id: str) -> JobResult:
        """
        Get the result of a job by its ID.
        """
        response = self._client.get(
            url=f"jobs/{job_id}",
            headers=self._headers(),
        )
        response.raise_for_status()
        resp = response.json()
        job_result = JobResult.model_validate(resp)
        return job_result

    def __create_parse_settings__(self, options: ParsingOptions) -> dict:
        return {
            "outputMode": options.format.value,
            "figureSummarization": options.summarize_figure,
            "tableSummarization": options.summarize_table,
            "tableOutputMode": options.table_output_mode.value,
            "tableParsingStrategy": options.table_parsing_strategy.value,
            "tableSummarizationPrompt": options.table_parsing_prompt,
            "figureSummarizationPrompt": options.figure_summarization_prompt,
        }

    def __create_parse_req__(self, file: str, options: ParsingOptions) -> dict:
        payload = {
            "file": file,
            "deliverWebhook": options.deliver_webhook,
            "settings": self.__create_parse_settings__(options),
        }
        if options.chunking_strategy:
            payload["chunkStrategy"] = options.chunking_strategy.value

        if options.page_range:
            payload["pages"] = options.page_range

        return payload

    def __create_extract_settings__(self, options: ExtractionOptions) -> dict:
        return {
            "jsonSchema": json.dumps(options.json_schema),
            "prompt": options.prompt,
            "modelProvider": options.model.value,
            "tableParsingStrategy": options.table_parsing_strategy.value,
        }

    def _create_extract_req(self, file: str, options: ExtractionOptions) -> dict:
        payload = {
            "file": file,
            "deliverWebhook": options.deliver_webhook,
            "settings": self.__create_extract_settings__(options),
        }

        return payload

    def parse(self, file: str, options: ParsingOptions, timeout: int = 5) -> str:
        """
        Parse a document.
        """
        response = self._client.post(
            url="/parse_async",
            headers=self._headers(),
            json=self.__create_parse_req__(file, options),
            timeout=2,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        resp = response.json()
        return resp.get("jobId")

    async def parse_async(
        self, file: str, options: ParsingOptions, timeout: int = 5
    ) -> str:
        """
        Parse a document asynchronously.
        """
        response = await self._async_client.post(
            url="/parse_async",
            headers=self._headers(),
            json=self.__create_parse_req__(file, options),
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        resp = response.json()
        return resp.get("jobId")

    def extract(self, file: str, options: ExtractionOptions, timeout: int = 5) -> str:
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

    async def extract_async(
        self, file: str, options: ExtractionOptions, timeout: int = 5
    ) -> str:
        """
        Parse a document asynchronously.
        """
        response = await self._async_client.post(
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

    retry(tries=10, delay=2)

    def upload(self, path: Union[str, Path]) -> str:
        """
        Upload a file to the Tensorlake

        Args:
            file_path: Path to the file to upload

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            httpx.HTTPError: If the request fails
            FileNotFoundError: If the file doesn't exist
        """
        return self.__file_uploader__.upload_file(path)

    retry(tries=10, delay=2)

    async def upload_async(self, path: Union[str, Path]) -> str:
        """
        Upload a file to the Tensorlake asynchronously.

        Args:
            file_path: Path to the file to upload

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            httpx.HTTPError: If the request fails
            FileNotFoundError: If the file doesn't exist
        """
        return await self.__file_uploader__.upload_file_async(path)

    def create_dataset(self, dataset: DatasetOptions) -> Dataset:
        """
        Create a new dataset.

        Args:
            dataset: The dataset to create.

        Returns:
            str: The ID of the created dataset.
        """

        if dataset.parsing_options and dataset.extraction_options:
            raise ValueError("Dataset cannot have both parsing and extraction options.")

        response = self._client.post(
            url="datasets",
            headers=self._headers(),
            json={
                "name": dataset.name,
                "description": dataset.description,
                "parseSettings": (
                    self.__create_parse_settings__(dataset.parsing_options)
                    if dataset.parsing_options
                    else None
                ),
                "extractSettings": (
                    self.__create_extract_settings__(dataset.extraction_options)
                    if dataset.extraction_options
                    else None
                ),
            },
        )

        response.raise_for_status()
        resp = response.json()
        return Dataset(
            dataset_id=resp.get("id"), name=dataset.name, api_key=self.api_key
        )
