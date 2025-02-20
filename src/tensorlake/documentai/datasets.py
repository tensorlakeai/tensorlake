"""
DocumentAI datasets module.
"""

import asyncio
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

import httpx
from pydantic import BaseModel, Field

from tensorlake.documentai.common import (
    DOC_AI_BASE_URL,
    PaginatedResult,
)
from tensorlake.documentai.extract import ExtractionOptions
from tensorlake.documentai.files import FileUploader
from tensorlake.documentai.jobs import Document, Job, JobStatus
from tensorlake.documentai.parse import ParsingOptions


class DatasetOptions(BaseModel):
    """DocumentAI create dataset request class."""

    name: str
    description: Optional[str] = None
    parsing_options: Optional[ParsingOptions] = None
    extraction_options: Optional[ExtractionOptions] = None


class IngestArgs(BaseModel):
    """
    DocumentAI create dataset request class.

    Args:
        file_url: The public URL of the file to upload. Only one of file_url, file_path, or file_id should be provided.
        file_path: The local filesystem path to the file to upload. Only one of file_url, file_path, or file_id should be provided.
        file_id: The Tensorlake ID of the file to upload; starts with tensorlake-. Only one of file_url, file_path, or file_id should be provided.

        deliver_webhook: Whether to deliver the result to a webhook. Defaults to False.
        pages: The pages to process in the document. Defaults to None.
    """

    file_url: Optional[str] = None
    file_path: Optional[str] = None
    file_id: Optional[str] = None

    deliver_webhook: bool = False
    pages: Optional[str] = None


class DatasetStatus(str, Enum):
    """
    Dataset status enum.
    """

    IDLE = "idle"
    PROCESSING = "processing"


class DownloadableJobOutput(BaseModel):
    """
    DocumentAI dataset job item class. This class is used to download the output of a job.
    """

    id: str
    file_id: str = Field(alias="fileId")
    outputs_url: Optional[str] = Field(alias="outputsUrl")
    status: JobStatus
    error_message: Optional[str] = Field(alias="errorMessage", default=None)
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")


class DatasetAnalytics(BaseModel):
    """
    DocumentAI dataset output analytics class.
    """

    total_jobs: int = Field(alias="totalJobs")
    total_processing_jobs: int = Field(alias="totalProcessingJobs")
    total_error_jobs: int = Field(alias="totalErrorJobs")
    total_successful_jobs: int = Field(alias="totalSuccessfulJobs")


class DatasetInfo(BaseModel):
    """
    DocumentAI dataset output class.
    """

    id: str
    name: str
    description: Optional[str]
    parse_settings: Optional[ParsingOptions] = Field(
        alias="parseSettings", default=None
    )
    extract_settings: Optional[ExtractionOptions] = Field(
        alias="extractSettings", default=None
    )
    status: DatasetStatus
    jobs: PaginatedResult[DownloadableJobOutput]
    analytics: DatasetAnalytics
    created_at: str = Field(alias="createdAt")


class StructuredDataPage(BaseModel):
    """
    DocumentAI structured data class.
    """

    page_number: int
    parsed_page: str
    data: dict = Field(alias="json_result", default_factory=dict)


class StructuredData(BaseModel):
    """
    DocumentAI structured data class.
    """

    pages: List[StructuredDataPage] = Field(alias="pages", default_factory=list)


class DatasetItem(BaseModel):
    """
    DocumentAI downloaded job output class.
    """

    chunks: List[str] = Field(alias="chunks", default_factory=list)
    document: Optional[Document] = Field(alias="document", default=None)
    structured_data: Optional[StructuredData] = Field(default=None)
    error_message: Optional[str] = Field(alias="errorMessage", default=None)


class DatasetItems(BaseModel):
    """
    DocumentAI dataset output cursor class.
    """

    cursor: Optional[str] = None
    total_pages: int = 0
    items: dict[str, DatasetItem] = {}


class DatasetOutputFormat(str, Enum):
    """
    Dataset output format enum.
    """

    CSV = "csv"


class Dataset:
    """
    DocumentAI dataset class.
    """

    def __init__(
        self,
        dataset_id: str,
        name: str,
        api_key: str,
        settings: Union[ExtractionOptions, ParsingOptions],
        status: DatasetStatus,
    ):
        self.id = dataset_id
        self.name = name
        self.api_key = api_key
        if isinstance(settings, ExtractionOptions):
            self.dataset_type = "extract"
        elif isinstance(settings, ParsingOptions):
            self.dataset_type = "parse"
        else:
            raise ValueError(
                "settings must be either ExtractionOptions or ParsingOptions"
            )
        self.status = status

        self.__file_uploader__ = FileUploader(api_key)
        self._async_client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)

    def __headers__(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def ingest(self, ingest_args: IngestArgs) -> Job:
        """
        Submit a new job to extend the dataset with a new file.

        Args:
            options: The options for extending the dataset.

        Returns:
            The job result. It contains the job ID, the Tensorlake file ID, and the status of the job.
        """
        return asyncio.run(self.ingest_async(ingest_args))

    async def ingest_async(self, ingest_args: IngestArgs) -> str:
        """
        Submit a new job to extend the dataset with a new file asynchronously.

        Args:
            ingest_args: The URL or path, and the pages to parse of a file.

        Returns:
            The job ID.
        """
        if (
            ingest_args.file_url is not None
            and ingest_args.file_path is not None
            and ingest_args.file_id is not None
        ):
            raise ValueError(
                "Only one of file_url, file_path, or file_id should be provided"
            )

        file_id = None
        if ingest_args.file_url is not None:
            file_id = ingest_args.file_url
        elif ingest_args.file_id is not None:
            file_id = ingest_args.file_id
        elif ingest_args.file_path is not None:
            path = Path(ingest_args.file_path)
            if not path.exists():
                raise FileNotFoundError(f"File {path} not found")
            file_id = await self.__file_uploader__.upload_file_async(
                ingest_args.file_path
            )

        if file_id is None:
            raise ValueError("file_url, file_path, or file_id should be provided")

        data = {
            "file_id": (None, file_id),
        }

        if ingest_args.deliver_webhook:
            data["deliver_webhook"] = (None, f"{ingest_args.deliver_webhook}")

        if ingest_args.pages:
            data["pages"] = (None, f"{ingest_args.pages}")

        try:
            response = await self._async_client.post(
                url=f"/datasets/{self.name}",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                },
                files=data,
            )
            response.raise_for_status()

            response_json = response.json()
            return response_json["jobId"]
        except httpx.HTTPStatusError as e:
            print(f"error: {e.response.text}")
            raise e

    def items(self, cursor: Optional[str] = None) -> DatasetItems:
        """
        Get the outputs of the dataset.

        Returns:
            The outputs of the dataset.
        """

        return asyncio.run(self.items_async(cursor))

    async def items_async(self, cursor: Optional[str] = None) -> DatasetItems:
        """
        Get the outputs of the dataset asynchronously.

        Returns:
            The outputs of the dataset.
        """
        url = f"datasets/{self.name}/jobs"
        if cursor:
            url += f"?cursor={cursor}"

        resp = await self._async_client.get(
            url=url,
            headers=self.__headers__(),
        )

        resp.raise_for_status()

        raw_outputs = DatasetInfo.model_validate(resp.json())
        outputs = {}

        for job in raw_outputs.jobs.items:
            if job.status == JobStatus.SUCCESSFUL:
                resp = await self._async_client.get(job.outputs_url)
                resp.raise_for_status()

                resp_json = resp.json()
                downloaded_output = DatasetItem(
                    chunks=resp_json["chunks"] if "chunks" in resp_json else [],
                    document=(
                        Document.model_validate(resp_json["document"])
                        if "document" in resp_json
                        else None
                    ),
                    structured_data=(
                        StructuredData.model_validate(resp_json)
                        if self.dataset_type == "extract"
                        else None
                    ),
                )
                outputs[job.id] = downloaded_output

            if job.status == JobStatus.FAILURE:
                outputs[job.id] = DatasetItem(error_message=job.error_message)

        return DatasetItems(
            cursor=raw_outputs.jobs.next_cursor,
            total_pages=raw_outputs.jobs.total_pages,
            items=outputs,
        )
