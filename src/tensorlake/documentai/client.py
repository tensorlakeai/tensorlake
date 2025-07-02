"""
Tensorlake Document AI client
"""

import asyncio
import inspect
import json
import os
import time
from pathlib import Path
from typing import Optional, Union

import httpx
from pydantic import BaseModel, Json
from retry import retry

from tensorlake.documentai.common import (
    DOC_AI_BASE_URL,
    DOC_AI_BASE_URL_V2,
    PaginatedResult,
)
from tensorlake.documentai.datasets import Dataset, DatasetOptions
from tensorlake.documentai.files import FileInfo, FileUploader
from tensorlake.documentai.models import (
    EnrichmentOptions,
    Job,
    MimeType,
    PageClassConfig,
    ParseResult,
    ParseStatus,
    ParsingOptions,
    StructuredExtractionOptions,
)


class DocumentAI:
    """
    Document AI client for Tensorlake.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("TENSORLAKE_API_KEY")

        if not self.api_key:
            raise ValueError(
                "API key is required. Set the TENSORLAKE_API_KEY environment variable or pass it as an argument."
            )

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None)
        self.__file_uploader__ = FileUploader(api_key=self.api_key)

    def __headers__(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Connection": "close",
        }

    def __create_parse_req__(
        self,
        file: str,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            list[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[list[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> dict:
        payload = {}

        # file check
        if file.startswith("http://") or file.startswith("https://"):
            payload["file_url"] = file
        elif file.startswith("tensorlake-"):
            payload["file_id"] = file
        else:
            payload["raw_text"] = file

        # optional field check
        if labels:
            payload["labels"] = labels

        if page_range:
            payload["page_range"] = page_range

        if mime_type:
            payload["mime_type"] = mime_type.value

        # other parsing options
        if parsing_options:
            payload["parsing_options"] = parsing_options.model_dump(exclude_none=True)

        if enrichment_options:
            payload["enrichment_options"] = enrichment_options.model_dump(
                exclude_none=True
            )

        if page_classifications:
            payload["page_classifications"] = [
                page_classification.model_dump(exclude_none=True)
                for page_classification in page_classifications
            ]

        if structured_extraction_options:
            converted_options = []
            for structured_extraction_option in structured_extraction_options:
                option_dict = structured_extraction_option.model_dump(exclude_none=True)

                # Handle json_schema conversion
                if hasattr(structured_extraction_option, "json_schema"):
                    json_schema = structured_extraction_option.json_schema
                    if inspect.isclass(json_schema) and issubclass(
                        json_schema, BaseModel
                    ):
                        option_dict["json_schema"] = json_schema.model_json_schema()
                    elif isinstance(json_schema, BaseModel):
                        option_dict["json_schema"] = json_schema.model_json_schema()
                    elif isinstance(json_schema, str):
                        try:
                            option_dict["json_schema"] = json.loads(json_schema)
                        except json.JSONDecodeError:
                            option_dict["json_schema"] = json_schema

                converted_options.append(option_dict)

            payload["structured_extraction_options"] = converted_options

        return payload

    def parse(
        self,
        file: str,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            list[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[list[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """
        Parse a document using the v2 API endpoint.
        """
        client = httpx.Client(base_url=DOC_AI_BASE_URL_V2, timeout=None)

        response = client.post(
            url="/parse",
            headers=self.__headers__(),
            json=self.__create_parse_req__(
                file,
                parsing_options,
                structured_extraction_options,
                enrichment_options,
                page_classifications,
                page_range,
                labels,
                mime_type,
            ),
        )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        finally:
            client.close()

        resp = response.json()
        return resp.get("parse_id")

    async def parse_async(
        self,
        file: str,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            list[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[list[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """
        Parse a document asynchronously using the v2 API endpoint.
        """
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL_V2, timeout=None)
        try:
            response = await client.post(
                url="/parse",
                headers=self.__headers__(),
                json=self.__create_parse_req__(
                    file,
                    parsing_options,
                    structured_extraction_options,
                    enrichment_options,
                    page_classifications,
                    page_range,
                    labels,
                    mime_type,
                ),
            )
            response.raise_for_status()
            resp = response.json()
            return resp.get("parse_id")
        except httpx.HTTPStatusError as e:
            print(e.response.text)
            raise e
        finally:
            await client.aclose()

    def parse_and_wait(
        self,
        file: str,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            list[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[list[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> ParseResult:
        """
        Parse a document and wait for completion.
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
            list[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[list[PageClassConfig]] = None,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> ParseResult:
        """
        Parse a document and wait for completion asynchronously.
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

    def get_parse(self, parse_id: str) -> ParseResult:
        """
        Get the result of a parse job by its parse ID.
        """
        client = httpx.Client(base_url=DOC_AI_BASE_URL_V2, timeout=None)
        try:
            response = client.get(
                url=f"parse/{parse_id}",
                headers=self.__headers__(),
            )
            response.raise_for_status()
            return response.json()
        finally:
            client.close()

    async def get_parse_async(self, parse_id: str) -> ParseResult:
        """
        Get the result of a parse job by its parse ID asynchronously.
        """
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL_V2, timeout=None)
        try:
            response = await client.get(
                url=f"parse/{parse_id}",
                headers=self.__headers__(),
            )
            response.raise_for_status()
            return response.json()
        finally:
            await client.aclose()

    def wait_for_completion(self, parse_id) -> ParseResult:
        """
        Wait for a job to complete.
        """
        parse = self.get_parse(parse_id)
        finished_parse = parse
        print(f"Waiting for job {parse} to complete...")
        while finished_parse["status"] in [ParseStatus.PENDING, ParseStatus.PROCESSING]:
            print("waiting 5s...")
            time.sleep(5)
            finished_parse = self.get_parse(parse_id)
            print(f"parse status: {finished_parse['status']}")

        return finished_parse

    async def wait_for_completion_async(self, parse_id: str) -> ParseResult:
        """
        Wait for a job to complete asynchronously.
        """
        parse = await self.get_parse_async(parse_id)
        finished_parse = parse
        while finished_parse["status"] in [ParseStatus.PENDING, ParseStatus.PROCESSING]:
            print("waiting 5s...")
            await asyncio.sleep(5)
            finished_parse = await self.get_parse_async(parse_id)
            print(f"parse_id: {parse_id}, job status: {finished_parse['status']}")

        return finished_parse

    # -------------------------------------------------------------------------
    # Files Management
    # -------------------------------------------------------------------------

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
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        response = await client.get(
            url="/files",
            headers=self.__headers__(),
            params={"cursor": cursor} if cursor else None,
        )
        response.raise_for_status()
        result = PaginatedResult[FileInfo].model_validate(response.json())
        return result

    @retry(tries=10, delay=2)
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

    @retry(tries=10, delay=2)
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
        uploader = FileUploader(api_key=self.api_key)
        return await uploader.upload_file_async(path)

    def delete_file(self, file_id: str):
        """
        Delete a file by its ID.
        """
        asyncio.run(self.delete_file_async(file_id))

    async def delete_file_async(self, file_id: str):
        """
        Delete a file by its ID asynchronously.
        """
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        response = await client.delete(
            url=f"files/{file_id}",
            headers=self.__headers__(),
        )
        response.raise_for_status()

    # -------------------------------------------------------------------------
    # Dataset Management, will be removed in this module in the future
    # -------------------------------------------------------------------------

    def __create_parse_settings__(self, options: ParsingOptions) -> dict:
        json_schema = None
        if options.extraction_options:
            if isinstance(options.extraction_options.json_schema, str):
                json_schema = json.loads(options.extraction_options.json_schema)
            elif isinstance(options.extraction_options.json_schema, dict):
                json_schema = options.extraction_options.json_schema
            elif isinstance(options.extraction_options.json_schema, Json):
                json_schema = json.loads(options.extraction_options.json_schema)
            elif inspect.isclass(options.extraction_options.json_schema) and issubclass(
                options.extraction_options.json_schema, BaseModel
            ):
                json_schema = options.extraction_options.json_schema.model_json_schema()
            elif isinstance(options.extraction_options.json_schema, BaseModel):
                json_schema = options.extraction_options.json_schema.model_json_schema()

        return {
            "chunkStrategy": (
                options.chunking_strategy.value if options.chunking_strategy else None
            ),
            "tableOutputMode": options.table_output_mode.value,
            "tableParsingMode": options.table_parsing_strategy.value,
            "tableSummarizationPrompt": options.table_parsing_prompt,
            "figureSummarizationPrompt": options.figure_summarization_prompt,
            "deliverWebhook": options.deliver_webhook,
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
            "skewCorrection": (
                options.skew_correction
                if options.skew_correction is not None
                else False
            ),
            "detectSignature": (
                options.detect_signature
                if options.detect_signature is not None
                else False
            ),
            "structuredExtractionSkipOcr": (
                options.extraction_options.skip_ocr
                if options.extraction_options is not None
                and options.extraction_options.skip_ocr is not None
                else False
            ),
            "disableLayoutDetection": (
                options.disable_layout_detection
                if options.disable_layout_detection is not None
                else False
            ),
            "formDetectionMode": (
                options.form_detection_mode.value
                if options.form_detection_mode is not None
                else "object_detection"
            ),
        }

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

        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        await client.post(
            url="datasets",
            headers=self.__headers__(),
            json={
                "name": dataset.name,
                "description": dataset.description,
                "settings": self.__create_parse_settings__(dataset.options),
            },
        )

        return await self.get_dataset_async(dataset.name)

    def get_dataset(self, name: str) -> Optional[Dataset]:
        """
        Get a dataset by its ID.

        Args:
            dataset_id: The ID of the dataset.

        Returns:
            Dataset: The dataset.
        """

        return asyncio.run(self.get_dataset_async(name))

    async def get_dataset_async(self, name: str) -> Optional[Dataset]:
        """
        Get a dataset by its ID asynchronously.
        """
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        response = await client.get(
            url=f"datasets/{name}",
            headers=self.__headers__(),
        )

        return self.__dataset_from_response__(response)

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
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        response = await client.delete(
            url=f"datasets/{name}",
            headers=self.__headers__(),
        )
        response.raise_for_status()

    # -------------------------------------------------------------------------
    # Job Management, will be deprecated in the future
    # -------------------------------------------------------------------------

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
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        response = await client.get(
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
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        response = await client.delete(
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
        client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)
        response = await client.get(
            url="/jobs",
            headers=self.__headers__(),
            params={"cursor": cursor} if cursor else None,
        )
        response.raise_for_status()
        result = PaginatedResult[Job].model_validate(response.json())
        return result
