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
from pydantic import Json
from retry import retry

from tensorlake.documentai.common import DOC_AI_BASE_URL, PaginatedResult
from tensorlake.documentai.datasets import Dataset, DatasetOptions
from tensorlake.documentai.files import FileInfo, FileUploader
from tensorlake.documentai.jobs import Job
from tensorlake.documentai.parse import ParsingOptions


class DocumentAI:
    """
    Document AI client for Tensorlake.
    """

    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY").strip()

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

    def delete_job(self, job_id: str):
        """
        Delete a job by its ID.
        """
        asyncio.run(self.delete_job_async(job_id))

    async def delete_job_async(self, job_id: str):
        """
        Delete a job by its ID asynchronously.
        """
        response = await self._async_client.delete(
            url=f"jobs/{job_id}",
            headers=self.__headers__(),
        )
        response.raise_for_status()

    def jobs(self, cursor: Optional[str] = None) -> PaginatedResult[Job]:
        """
        Get a list of jobs.
        """
        return asyncio.run(self.jobs_async(cursor))

    async def jobs_async(self, cursor: Optional[str] = None) -> PaginatedResult[Job]:
        """
        Get a list of jobs asynchronously.
        """
        response = await self._async_client.get(
            url="/jobs",
            headers=self.__headers__(),
            params={"cursor": cursor} if cursor else None,
        )
        response.raise_for_status()
        result = PaginatedResult[Job].model_validate(response.json())
        return result

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
        json_schema = None
        if options.extraction_options:
            if isinstance(options.extraction_options.schema, Json):
                json_schema = json.loads(options.extraction_options.schema)
            else:
                json_schema = options.extraction_options.schema.model_json_schema()

        return {
            "chunkStrategy": (
                options.chunking_strategy.value if options.chunking_strategy else None
            ),
            "tableOutputMode": options.table_output_mode.value,
            "tableParsingMode": options.table_parsing_strategy.value,
            "tableSummarizationPrompt": options.table_parsing_prompt,
            "figureSummarizationPrompt": options.figure_summarization_prompt,
            "jsonSchema": json_schema,
            "structuredExtractionPrompt": (
                options.extraction_options.prompt
                if options.extraction_options
                else None
            ),
            "modelProvider": (
                options.extraction_options.provider.value
                if options.extraction_options
                else None
            ),
        }

    def __create_parse_req__(
        self, file: str, options: ParsingOptions, deliver_webhook: bool
    ) -> dict:
        payload = {
            "file": file,
            "pages": options.page_range,
            "deliverWebhook": deliver_webhook,
            "settings": self.__create_parse_settings__(options),
        }

        return payload

    def files(self, cursor: Optional[str] = None) -> PaginatedResult[FileInfo]:
        """
        Get a list of files.
        """
        return asyncio.run(self.files_async(cursor))

    async def files_async(
        self, cursor: Optional[str] = None
    ) -> PaginatedResult[FileInfo]:
        """
        Get a list of files asynchronously.
        """
        response = await self._async_client.get(
            url="/files",
            headers=self.__headers__(),
            params={"cursor": cursor} if cursor else None,
        )
        response.raise_for_status()
        result = PaginatedResult[FileInfo].model_validate(response.json())
        return result

    def parse(
        self,
        file: str,
        options: ParsingOptions,
        timeout: int = 5,
        deliver_webhook: bool = False,
    ) -> str:
        """
        Parse a document.
        """
        response = self._client.post(
            url="/parse",
            headers=self.__headers__(),
            json=self.__create_parse_req__(file, options, deliver_webhook),
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        resp = response.json()
        return resp.get("jobId")

    def parse_and_wait(
        self,
        file: str,
        options: ParsingOptions,
        timeout: int = 5,
        deliver_webhook: bool = False,
    ) -> Job:
        """
        Parse a document and wait for completion.
        """
        job_id = self.parse(file, options, timeout, deliver_webhook)
        return self.wait_for_completion(job_id)

    async def parse_async(
        self,
        file: str,
        options: ParsingOptions,
        timeout: int = 5,
        deliver_webhook: bool = False,
    ) -> str:
        """
        Parse a document asynchronously.
        """
        response = await self._async_client.post(
            url="/parse",
            headers=self.__headers__(),
            json=self.__create_parse_req__(file, options, deliver_webhook),
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

    def delete_file(self, file_id: str):
        """
        Delete a file by its ID.
        """
        asyncio.run(self.delete_file_async(file_id))

    async def delete_file_async(self, file_id: str):
        """
        Delete a file by its ID asynchronously.
        """
        response = await self._async_client.delete(
            url=f"files/{file_id}",
            headers=self.__headers__(),
        )
        response.raise_for_status()

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

        response = await self._async_client.post(
            url="datasets",
            headers=self.__headers__(),
            json={
                "name": dataset.name,
                "description": dataset.description,
                "settings": self.__create_parse_settings__(dataset.options),
            },
        )
        return await self.get_dataset_async(response.json().get("id"))

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

        print(response)
        print(response.json())
        resp = response.json()

        settings = ParsingOptions.model_validate(resp.get("settings"))
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
