"""
DocumentAI datasets module.
"""

from pathlib import Path
from typing import Optional, Union

import httpx
from pydantic import BaseModel

from tensorlake.documentai.common import DOC_AI_BASE_URL, FileUploader, JobResult


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


class Dataset:
    """DocumentAI dataset class."""

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

    def extend(self, options: DatasetExtendOptions) -> JobResult:
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

        if options.file_url is not None:
            data = {
                "file_id": options.file_url,
                "deliver_webhook": options.deliver_webhook,
                "pages": options.pages,
            }

            resp = self._client.post(
                url=f"datasets/{self.id}",
                headers=self.__headers__(),
                data=data,
            )
            resp.raise_for_status()
            return JobResult.model_validate(resp.json())

        if options.file_path is not None:
            path = Path(options.file_path)
            if not path.exists():
                raise FileNotFoundError(f"File {path} not found")

            file_id = self.__file_uploader__.upload_file(options.file_path)

            data = {
                "file_id": file_id,
                "deliver_webhook": options.deliver_webhook,
                "pages": options.pages,
            }

            resp = self._client.post(
                url=f"datasets/{self.id}",
                headers=self.__headers__(),
                data=data,
            )
            resp.raise_for_status()
            return JobResult.model_validate(resp.json())

        data = {
            "file_id": options.file_id,
            "deliver_webhook": options.deliver_webhook,
            "pages": options.pages,
        }

        resp = self._client.post(
            url=f"datasets/{self.id}",
            headers=self.__headers__(),
            data=data,
        )

        resp.raise_for_status()
        return JobResult.model_validate(resp.json())

    async def extend_async(self, options: DatasetExtendOptions) -> JobResult:
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

        if options.file_url is not None:
            data = {
                "file_id": options.file_url,
                "deliver_webhook": options.deliver_webhook,
                "pages": options.pages,
            }

            resp = await self._async_client.post(
                url=f"datasets/{self.id}",
                headers=self.__headers__(),
                data=data,
            )
            resp.raise_for_status()
            return JobResult.model_validate(resp.json())

        if options.file_path is not None:
            path = Path(options.file_path)
            if not path.exists():
                raise FileNotFoundError(f"File {path} not found")

            file_id = await self.__file_uploader__.upload_file_async(options.file_path)

            data = {
                "file_id": file_id,
                "deliver_webhook": options.deliver_webhook,
                "pages": options.pages,
            }

            resp = await self._async_client.post(
                url=f"datasets/{self.id}",
                headers=self.__headers__(),
                data=data,
            )
            resp.raise_for_status()
            return JobResult.model_validate(resp.json())

        data = {
            "file_id": options.file_id,
            "deliver_webhook": options.deliver_webhook,
            "pages": options.pages,
        }

        resp = await self._async_client.post(
            url=f"datasets/{self.id}",
            headers=self.__headers__(),
            data=data,
        )

        resp.raise_for_status()
        return JobResult.model_validate(resp.json())
