"""
DocumentAI datasets module.
"""

from enum import Enum
from pathlib import Path
from typing import List, Optional

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


class DatasetExtendOptions(BaseModel):
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
    ERROR = "error"
    DONE = "done"


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


class DatasetOutputAnalytics(BaseModel):
    """
    DocumentAI dataset output analytics class.
    """

    total_jobs: int = Field(alias="totalJobs")
    total_processing_jobs: int = Field(alias="totalProcessingJobs")
    total_error_jobs: int = Field(alias="totalErrorJobs")
    total_done_jobs: int = Field(alias="totalDoneJobs")


class DatasetOutput(BaseModel):
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
    analytics: DatasetOutputAnalytics
    created_at: str = Field(alias="createdAt")


class DownloadedJobOutput(BaseModel):
    """
    DocumentAI downloaded job output class.
    """

    chunks: List[str] = Field(alias="chunks", default_factory=list)
    document: Optional[Document] = Field(alias="document", default=None)
    error_message: Optional[str] = Field(alias="errorMessage", default=None)


class DatasetOutputCursor(BaseModel):
    """
    DocumentAI dataset output cursor class.
    """

    cursor: Optional[str] = None
    total_pages: int = 0
    outputs: dict[str, DownloadedJobOutput] = {}


class DatasetOutputFormat(str, Enum):
    """
    Dataset output format enum.
    """

    CSV = "csv"


class Dataset:
    """
    DocumentAI dataset class.
    """

    def __init__(self, dataset_id: str, name: str, api_key: str):
        self.id = dataset_id
        self.name = name
        self.api_key = api_key

        self.__file_uploader__ = FileUploader(api_key)
        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None)
        self._async_client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)

    def __headers__(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def extend(self, options: DatasetExtendOptions) -> Job:
        """
        Submit a new job to extend the dataset with a new file.

        Args:
            options: The options for extending the dataset.

        Returns:
            The job result. It contains the job ID, the Tensorlake file ID, and the status of the job.
        """
        if (
            options.file_url is not None
            and options.file_path is not None
            and options.file_id is not None
        ):
            raise ValueError(
                "Only one of file_url, file_path, or file_id should be provided"
            )

        file_id = None
        if options.file_url is not None:
            file_id = options.file_url
        elif options.file_id is not None:
            file_id = options.file_id
        elif options.file_path is not None:
            path = Path(options.file_path)
            if not path.exists():
                raise FileNotFoundError(f"File {path} not found")

            file_id = self.__file_uploader__.upload_file(options.file_path)

        if file_id is None:
            raise ValueError("file_url, file_path, or file_id should be provided")

        data = {
            "file_id": (None, file_id),
            "deliver_webhook": (None, f"{options.deliver_webhook}"),
            "pages": (None, f"{options.pages}"),
        }

        response = self._client.post(
            url=f"datasets/{self.id}",
            headers={
                "Authorization": f"Bearer {self.api_key}",
            },
            files=data,
        )
        response.raise_for_status()
        return Job.model_validate(response.json())

    async def extend_async(self, options: DatasetExtendOptions) -> str:
        """
        Submit a new job to extend the dataset with a new file asynchronously.

        Args:
            options: The options for extending the dataset.

        Returns:
            The job result. It contains the job ID, the Tensorlake file ID, and the status of the job.
        """
        if (
            options.file_url is not None
            and options.file_path is not None
            and options.file_id is not None
        ):
            raise ValueError(
                "Only one of file_url, file_path, or file_id should be provided"
            )

        file_id = None
        if options.file_url is not None:
            file_id = options.file_url
        elif options.file_id is not None:
            file_id = options.file_id
        elif options.file_path is not None:
            path = Path(options.file_path)
            if not path.exists():
                raise FileNotFoundError(f"File {path} not found")
            file_id = await self.__file_uploader__.upload_file_async(options.file_path)

        if file_id is None:
            raise ValueError("file_url, file_path, or file_id should be provided")

        data = {
            "file_id": (None, file_id),
        }

        if options.deliver_webhook:
            data["deliver_webhook"] = (None, f"{options.deliver_webhook}")

        if options.pages:
            data["pages"] = (None, f"{options.pages}")

        try:
            response = await self._async_client.post(
                url=f"/datasets/{self.id}",
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

    def outputs(self, cursor: Optional[str] = None) -> DatasetOutputCursor:
        """
        Get the outputs of the dataset.

        Returns:
            The outputs of the dataset.
        """

        url = f"datasets/{self.id}"
        if cursor:
            url += f"?cursor={cursor}"

        resp = self._client.get(
            url=url,
            headers=self.__headers__(),
        )

        resp.raise_for_status()
        raw_outputs = DatasetOutput.model_validate(resp.json())

        outputs = {}
        for job in raw_outputs.jobs.items:
            if job.status == JobStatus.SUCCESSFUL:
                resp = self._client.get(job.outputs_url)
                resp.raise_for_status()

                resp_json = resp.json()
                downloaded_output = DownloadedJobOutput(
                    chunks=resp_json["chunks"] if "chunks" in resp_json else [],
                    document=(
                        Document.model_validate(resp_json["document"])
                        if "document" in resp_json
                        else None
                    ),
                )
                outputs[job.id] = downloaded_output

            if job.status == JobStatus.FAILURE:
                outputs[job.id] = DownloadedJobOutput(error_message=job.error_message)

        return DatasetOutputCursor(
            cursor=raw_outputs.jobs.next_cursor,
            total_pages=raw_outputs.jobs.total_pages,
            outputs=outputs,
        )

    async def outputs_async(self, cursor: Optional[str] = None) -> DatasetOutputCursor:
        """
        Get the outputs of the dataset asynchronously.

        Returns:
            The outputs of the dataset.
        """
        url = f"datasets/{self.id}"
        if cursor:
            url += f"?cursor={cursor}"

        resp = await self._async_client.get(
            url=url,
            headers=self.__headers__(),
        )

        resp.raise_for_status()

        raw_outputs = DatasetOutput.model_validate(resp.json())
        outputs = {}

        for job in raw_outputs.jobs.items:
            if job.status == JobStatus.SUCCESSFUL:
                resp = await self._async_client.get(job.outputs_url)
                resp.raise_for_status()

                resp_json = resp.json()

                downloaded_output = DownloadedJobOutput(
                    chunks=resp_json["chunks"] if "chunks" in resp_json else [],
                    document=(
                        Document.model_validate(resp_json["document"])
                        if "document" in resp_json
                        else None
                    ),
                )
                outputs[job.id] = downloaded_output

            if job.status == JobStatus.FAILURE:
                outputs[job.id] = DownloadedJobOutput(error_message=job.error_message)

        return DatasetOutputCursor(
            cursor=raw_outputs.jobs.next_cursor,
            total_pages=raw_outputs.jobs.total_pages,
            outputs=outputs,
        )
