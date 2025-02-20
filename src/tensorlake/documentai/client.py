"""
Tensorlake Document AI client
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Optional, Union

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

    def __headers__(self):
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
            headers=self.__headers__(),
        )
        response.raise_for_status()
        return Job.model_validate(response.json())

    async def get_job_async(self, job_id: str) -> Job:
        """
        Get the result of a job by its ID asynchronously.
        """
        response = await self._async_client.get(
            url=f"jobs/{job_id}",
            headers=self.__headers__(),
        )
        response.raise_for_status()
        return Job.model_validate(response.json())

    def wait_for_completion(self, job_id) -> Job:
        """
        Wait for a job to complete.
        """
        job = self.get_job(job_id)
        finished_job = job
        while finished_job.status in ["pending", "processing"]:
            print("waiting 5s...")
            time.sleep(5)
            finished_job = self.get_job(job.job_id)
            print(f"job status: {finished_job.status}")

        return finished_job

    async def wait_for_completion_async(self, job_id: str) -> Job:
        """
        Wait for a job to complete asynchronously.
        """
        job = await self.get_job_async(job_id)
        finished_job = job
        while finished_job.status in ["pending", "processing"]:
            print("waiting 5s...")
            await asyncio.sleep(5)
            finished_job = await self.get_job_async(job.job_id)
            print(f"job_id: {job_id}, job status: {finished_job.status}")

        return finished_job

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
        return asyncio.run(self.parse_async(file, options, timeout))

    async def parse_async(
        self, file: str, options: ParsingOptions, timeout: int = 5
    ) -> str:
        """
        Parse a document asynchronously.
        """
        response = await self._async_client.post(
            url="/parse_async",
            headers=self.__headers__(),
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
        return asyncio.run(self.extract_async(file, options, timeout))

    async def extract_async(
        self, file: str, options: ExtractionOptions, timeout: int = 5
    ) -> str:
        """
        Parse a document asynchronously.
        """
        response = await self._async_client.post(
            url="/extract_async",
            headers=self.__headers__(),
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

    def create_dataset(
        self, dataset: DatasetOptions, ignore_if_exists=False
    ) -> Dataset:
        """
        Create a new dataset.

        Args:
            dataset: The dataset to create.
        Returns:
            str: The ID of the created dataset.
        """
        return asyncio.run(self.create_dataset_async(dataset, ignore_if_exists))

    async def create_dataset_async(
        self, dataset: DatasetOptions, ignore_if_exists=False
    ) -> Dataset:
        """
        Create a new dataset asynchronously.

        Args:
            dataset: The dataset to create.

        Returns:
            str: The ID of the created dataset.
        """

        if ignore_if_exists:
            existing_dataset = await self.get_dataset_async(dataset.name)
            if existing_dataset:
                return existing_dataset

        if dataset.parsing_options and dataset.extraction_options:
            raise ValueError("Dataset cannot have both parsing and extraction options.")

        response = await self._async_client.post(
            url="datasets",
            headers=self.__headers__(),
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
        return self.__dataset_from_response__(response)

    def get_dataset(self, name: str) -> Optional[Dataset]:
        """
        Get a dataset by its ID.

        Args:
            dataset_id: The ID of the dataset.

        Returns:
            Dataset: The dataset.
        """

        async def asyncfunc():
            dataset = await self.get_dataset_async(name)
            return dataset

        loop = asyncio.get_event_loop()
        dataset = loop.run_until_complete(asyncfunc())
        return dataset

    async def get_dataset_async(self, name: str) -> Optional[Dataset]:
        """
        Get a dataset by its ID asynchronously.
        """
        response = await self._async_client.get(
            url=f"datasets/{name}",
            headers=self.__headers__(),
        )

        return self.__dataset_from_response__(response)

    def __dataset_from_response__(self, response: httpx.Response) -> Dataset:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
        resp = response.json()
        settings = None
        if resp.get("extractSettings") is not None:
            settings = ExtractionOptions.model_validate(resp.get("extractSettings"))
        elif resp.get("parseSettings") is not None:
            settings = ParsingOptions.model_validate(resp.get("parseSettings"))
        else:
            raise ValueError("Dataset does not have any settings.")
        return Dataset(
            dataset_id=resp.get("id"),
            name=resp.get("name"),
            api_key=self.api_key,
            settings=settings,
            status=resp.get("status"),
        )

    def delete_dataset(self, name: str):
        """
        Delete a dataset by its ID.

        Args:
            dataset_id: The ID of the dataset.
        """
        asyncio.run(self.delete_dataset_async(name))

    async def delete_dataset_async(self, name: str):
        """
        Delete a dataset by its ID asynchronously.
        """
        response = await self._async_client.delete(
            url=f"datasets/{name}",
            headers=self.__headers__(),
        )
        response.raise_for_status()
