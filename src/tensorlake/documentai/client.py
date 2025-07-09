"""
Tensorlake Document AI client
"""

import asyncio
import inspect
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Union

import anyio
import httpx
from pydantic import BaseModel
from retry import retry

from tensorlake.documentai.common import (
    DOC_AI_BASE_URL,
    DOC_AI_BASE_URL_V2,
    PaginatedResult,
)
from tensorlake.documentai.files import FileInfo, FileUploader
from tensorlake.documentai.models import (
    Dataset,
    EnrichmentOptions,
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

    def __init__(self, api_key: Optional[str] = None, server_url: Optional[str] = None):
        self.api_key = api_key or os.getenv("TENSORLAKE_API_KEY")

        if not self.api_key:
            raise ValueError(
                "API key is required. Set the TENSORLAKE_API_KEY environment variable or pass it as an argument."
            )

        self._client_v1 = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None)
        self._aclient_v1: httpx.AsyncClient = httpx.AsyncClient(
            base_url=DOC_AI_BASE_URL, timeout=None
        )
        self._client = httpx.Client(base_url=DOC_AI_BASE_URL_V2, timeout=None)
        self._aclient: httpx.AsyncClient = httpx.AsyncClient(
            base_url=DOC_AI_BASE_URL_V2, timeout=None
        )

        if server_url:
            self._client_v1.base_url = f"{server_url}/documents/v1"
            self._aclient_v1.base_url = f"{server_url}/documents/v1"
            self._client.base_url = f"{server_url}/documents/v2"
            self._aclient.base_url = f"{server_url}/documents/v2"

        self._file_uploader = FileUploader(api_key=self.api_key, server_url=server_url)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Connection": "close",
        }

    def _request_v1(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """
        Make a request to the v1 API endpoint.
        """
        resp = self._client_v1.request(method, url, headers=self._headers(), **kwargs)

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(f"Error in request to {url}: {e}")
            print(e.response.text)
            raise e

        return resp

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        resp = self._client.request(method, url, headers=self._headers(), **kwargs)

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(f"Error in request to {url}: {e}")
            print(e.response.text)
            raise e

        return resp

    async def _arequest(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        resp = await self._aclient.request(
            method, url, headers=self._headers(), **kwargs
        )
        resp.raise_for_status()
        return resp

    async def _arequest_v1(
        self, method: str, url: str, **kwargs: Any
    ) -> httpx.Response:
        resp = await self._aclient_v1.request(
            method, url, headers=self._headers(), **kwargs
        )
        resp.raise_for_status()
        return resp

    def _create_parse_req(
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
        body = self._create_parse_req(
            file,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
            page_range,
            labels,
            mime_type,
        )

        response = self._request(
            "POST",
            "/parse",
            json=body,
        )

        return response.json().get("parse_id")

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
        body = (
            self._create_parse_req(
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

        response = await self._arequest(
            "POST",
            "/parse",
            json=body,
        )

        return response.json().get("parse_id")

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

    def get_parsed_result(self, parse_id: str) -> ParseResult:
        """
        Get the result of a parse job by its parse ID.
        """
        response = self._request("GET", f"parse/{parse_id}")
        return ParseResult.model_validate(response.json())

    async def get_parsed_result_async(self, parse_id: str) -> ParseResult:
        """
        Get the result of a parse job by its parse ID asynchronously.
        """
        response = await self._arequest("GET", f"parse/{parse_id}")
        return ParseResult.model_validate(response.json())

    def wait_for_completion(self, parse_id) -> ParseResult:
        """
        Wait for a job to complete.
        """
        parse = self.get_parsed_result(parse_id)
        finished_parse = parse
        while finished_parse.status in [ParseStatus.PENDING, ParseStatus.PROCESSING]:
            print("waiting 5s...")
            time.sleep(5)
            finished_parse = self.get_parsed_result(parse_id)
            print(f"parse status: {finished_parse.status.name.lower()}")
        return finished_parse

    async def wait_for_completion_async(self, parse_id: str) -> ParseResult:
        """
        Wait for a job to complete asynchronously.
        """
        parse = await self.get_parsed_result_async(parse_id)
        finished_parse = parse
        while finished_parse.status in [ParseStatus.PENDING, ParseStatus.PROCESSING]:
            print("waiting 5s...")
            await asyncio.sleep(5)
            finished_parse = await self.get_parsed_result_async(parse_id)
            print(f"parse status: {finished_parse.status}")
        return finished_parse

    # -------------------------------------------------------------------------
    # Files Management
    # -------------------------------------------------------------------------

    def files(self, cursor: Optional[str] = None) -> PaginatedResult[FileInfo]:
        """
        Get a list of files.
        """
        response = self._request_v1(
            "GET",
            "files",
            params={"cursor": cursor} if cursor else None,
        )

        response.raise_for_status()

        return PaginatedResult[FileInfo].model_validate(response.json())

    async def files_async(
        self, cursor: Optional[str] = None
    ) -> PaginatedResult[FileInfo]:
        """
        Get a list of files asynchronously.
        """
        response = await self._arequest_v1(
            "GET",
            "files",
            params={"cursor": cursor} if cursor else None,
        )

        response.raise_for_status()

        return PaginatedResult[FileInfo].model_validate(response.json())

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
        return self._file_uploader.upload_file(path)

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
        response = self._request_v1(
            "DELETE",
            f"files/{file_id}",
        )
        response.raise_for_status()

    async def delete_file_async(self, file_id: str):
        """
        Delete a file by its ID asynchronously.
        """
        response = await self._arequest_v1(
            "DELETE",
            f"files/{file_id}",
        )
        response.raise_for_status()

    # -------------------------------------------------------------------------
    # Dataset Management, will be removed in this module in the future
    # -------------------------------------------------------------------------

    def create_dataset(
        self,
        name: str,
        description: Optional[str] = None,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
    ) -> Dataset:
        body = self._create_dataset_req(
            name,
            description,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
        )

        response = self._request("POST", "/datasets", json=body).json()

        return self.get_dataset(response["slug"])

    async def create_dataset_async(
        self,
        name: str,
        description: Optional[str] = None,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
    ) -> Dataset:
        body = self._create_dataset_req(
            name,
            description,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
        )

        response = await self._arequest("POST", "/datasets", json=body)
        response_json = response.json()

        return await self.get_dataset_async(response_json["slug"])

    def _create_dataset_req(
        self,
        name: str,
        description: Optional[str] = None,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
    ) -> Dict[str, Any]:
        """
        Create a request body for creating a dataset.
        """
        body: Dict[str, Any] = _drop_none({"name": name, "description": description})
        if parsing_options:
            body["parsing_options"] = parsing_options.model_dump(exclude_none=True)

        if enrichment_options:
            body["enrichment_options"] = enrichment_options.model_dump(
                exclude_none=True
            )
        if page_classifications:
            body["page_classifications"] = [
                pc.model_dump(exclude_none=True) for pc in page_classifications
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

            body["structured_extraction_options"] = converted_options

        print(f"Creating dataset with body: {body}")
        return body

    def get_dataset(
        self,
        slug: str,
    ) -> Dataset:
        data = self._request("GET", f"/datasets/{slug}").json()
        return Dataset.model_validate(data)

    async def get_dataset_async(self, slug: str) -> Dataset:
        data = (await self._arequest("GET", f"/datasets/{slug}")).json()
        return Dataset.model_validate(data)

    def delete_dataset(self, dataset: Dataset) -> None:
        self._request("DELETE", f"/datasets/{dataset.slug}")

    async def delete_dataset_async(self, slug: str) -> None:
        await self._arequest("DELETE", f"/datasets/{slug}")

    def parse_dataset_file(
        self,
        dataset: Dataset,
        file: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        wait_for_completion: bool = False,
    ) -> Union[str, ParseResult]:

        dataset_parse_req = self._create_dataset_parse_req(
            file,
            page_range,
            labels,
            mime_type,
        )

        response = self._request(
            "POST",
            f"/datasets/{dataset.slug}/parse",
            json=dataset_parse_req,
        ).json()

        if not wait_for_completion:
            return response["parse_id"]

        return self.wait_for_completion(response["parse_id"])

    async def parse_dataset_file_async(
        self,
        dataset: Dataset,
        file: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        wait_for_completion: bool = False,
    ) -> Union[str, ParseResult]:
        dataset_parse_req = self._create_dataset_parse_req(
            file,
            page_range,
            labels,
            mime_type,
        )

        response = await self._arequest(
            "POST",
            f"/datasets/{dataset.slug}/parse",
            json=dataset_parse_req,
        )
        response_json = response.json()

        if not wait_for_completion:
            return response_json["parse_id"]

        return await self.wait_for_completion_async(response_json["parse_id"])

    def _create_dataset_parse_req(
        self,
        file: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> Dict[str, Any]:
        """
        Create a request body for parsing a file in a dataset.
        """
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

        return payload


def _drop_none(mapping: MutableMapping[str, Any]) -> Dict[str, Any]:
    """Return a copy of *mapping* without keys whose values are ``None``."""

    return {k: v for k, v in mapping.items() if v is not None}
