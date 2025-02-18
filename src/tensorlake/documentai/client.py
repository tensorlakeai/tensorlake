"""
Tensorlake Document AI client
"""

import json
import os
from pathlib import Path
from typing import Union

import httpx
from retry import retry

from tensorlake.documentai.common import (
    DOC_AI_BASE_URL,
)
from tensorlake.documentai.datasets import Dataset, DatasetOptions
from tensorlake.documentai.extract import ExtractionOptions
from tensorlake.documentai.files import FileUploader
from tensorlake.documentai.jobs import Job
from tensorlake.documentai.parse import ParsingOptions


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

    def get_job(self, job_id: str) -> Job:
        """
        Get the result of a job by its ID.
        """
        response = self._client.get(
            url=f"jobs/{job_id}",
            headers=self._headers(),
        )
        response.raise_for_status()
        resp = response.json()
        job_result = Job.model_validate(resp)
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

    async def create_dataset_async(self, dataset: DatasetOptions) -> Dataset:
        """
        Create a new dataset asynchronously.

        Args:
            dataset: The dataset to create.

        Returns:
            str: The ID of the created dataset.
        """

        if dataset.parsing_options and dataset.extraction_options:
            raise ValueError("Dataset cannot have both parsing and extraction options.")

        response = await self._async_client.post(
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

    def get_dataset(self, dataset_id: str) -> Dataset:
        """
        Get a dataset by its ID.

        Args:
            dataset_id: The ID of the dataset.

        Returns:
            Dataset: The dataset.
        """
        response = self._client.get(
            url=f"datasets/{dataset_id}",
            headers=self._headers(),
        )
        response.raise_for_status()
        resp = response.json()
        return Dataset(
            dataset_id=resp.get("id"), name=resp.get("name"), api_key=self.api_key
        )

    async def get_dataset_async(self, dataset_id: str) -> Dataset:
        """
        Get a dataset by its ID asynchronously.
        """
        response = await self._async_client.get(
            url=f"datasets/{dataset_id}",
            headers=self._headers(),
        )
        response.raise_for_status()
        resp = response.json()
        return Dataset(
            dataset_id=resp.get("id"), name=resp.get("name"), api_key=self.api_key
        )

    def delete_dataset(self, dataset_id: str):
        """
        Delete a dataset by its ID.

        Args:
            dataset_id: The ID of the dataset.
        """
        response = self._client.delete(
            url=f"datasets/{dataset_id}",
            headers=self._headers(),
        )
        response.raise_for_status()

    async def delete_dataset_async(self, dataset_id: str):
        """
        Delete a dataset by its ID asynchronously.
        """
        response = await self._async_client.delete(
            url=f"datasets/{dataset_id}",
            headers=self._headers(),
        )
        response.raise_for_status()
