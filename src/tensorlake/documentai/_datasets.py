"""
Dataset creation, parsing and deletion helpers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from ._base import _BaseClient
from ._parse import _convert_seo
from ._utils import _drop_none
from .models import (
    Dataset,
    DatasetDataFilter,
    DatasetStatus,
    EnrichmentOptions,
    MimeType,
    PageClassConfig,
    PaginatedResult,
    PaginationDirection,
    ParseResult,
    ParsingOptions,
    StructuredExtractionOptions,
)


class _DatasetMixin(_BaseClient):
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
        """
        Create a new dataset in Tensorlake.

        Creating a dataset allows you to group related documents together for parsing and analysis.

        Args:
            name: The name of the dataset. This is used to identify the dataset in the UI and API.
            description: Optional description of the dataset. This can be used to provide additional context
                about the dataset.
            parsing_options: Optional parsing options to customize how documents in the dataset are parsed. Tensorlake
                provides default parsing options, but you can specify custom options to tailor the parsing process.

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.

            enrichment_options: Optional enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures.

            page_classifications: Optional list of page classification configurations. If provided, the API will perform
                page classification on the documents in the dataset. This can help in organizing and understanding the
                content of the documents based on their page types.
        """
        body = _create_dataset_req(
            name,
            description,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
        )
        dataset_id = self._request("POST", "/datasets", json=body).json()["dataset_id"]
        return self.get_dataset(dataset_id)

    async def create_dataset_async(self, *args, **kw) -> Dataset:  # type: ignore[override]
        """
        Create a new dataset in Tensorlake asynchronously.

        Creating a dataset allows you to group related documents together for parsing and analysis.

        Args:
            name: The name of the dataset. This is used to identify the dataset in the UI and API.
            description: Optional description of the dataset. This can be used to provide additional context

                about the dataset.

            parsing_options: Optional parsing options to customize how documents in the dataset are parsed. Tensorlake
                provides default parsing options, but you can specify custom options to tailor the parsing process.

            structured_extraction_options: Optional structured extraction options to guide the extraction of structured
                data from documents in the dataset. This allows you to define schemas and extraction strategies for
                structured data.

            enrichment_options: Optional enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures.

            page_classifications: Optional list of page classification configurations. If provided, the API will perform
                page classification on the documents in the dataset. This can help in organizing and understanding the
                content of the documents based on their page types.
        """
        body = _create_dataset_req(*args, **kw)
        resp = await self._arequest("POST", "/datasets", json=body)
        dataset_id = resp.json()["dataset_id"]
        return await self.get_dataset_async(dataset_id)

    def get_dataset(self, dataset_id: str) -> Dataset:
        """
        Get a dataset by its ID.

        This method retrieves information about a specific dataset, including its name, status, description,
        and creation date.

        Args:
            dataset_id: The ID of the dataset to retrieve. This is the string returned by the
                create_dataset method.
        """

        return Dataset.model_validate(
            self._request("GET", f"/datasets/{dataset_id}").json()
        )

    async def get_dataset_async(self, dataset_id: str) -> Dataset:
        """
        Get a dataset by its ID asynchronously.

        This method retrieves information about a specific dataset, including its name, status, description,
        and creation date.

        Args:
            dataset_id: The ID of the dataset to retrieve. This is the string returned by the
                create_dataset method.
        """

        resp = await self._arequest("GET", f"/datasets/{dataset_id}")
        return Dataset.model_validate(resp.json())

    def delete_dataset(self, dataset_id: str | Dataset) -> None:
        """
        Delete a dataset by its ID.

        Deleting a dataset will remove it from the system and it cannot be recovered.

        Deleting a dataset does not delete any files used in it, but it will remove any parsed results associated with the dataset.

        Args:
            dataset_id: The ID of the dataset to delete. This is the string returned by the
                create_dataset method, or a Dataset object.
        """
        did = dataset_id.dataset_id if isinstance(dataset_id, Dataset) else dataset_id
        self._request("DELETE", f"/datasets/{did}")

    async def delete_dataset_async(self, dataset_id: str | Dataset) -> None:
        """
        Delete a dataset by its ID asynchronously.

        Deleting a dataset will remove it from the system and it cannot be recovered.

        Deleting a dataset does not delete any files used in it, but it will remove any parsed results associated with the dataset.

        Args:
            dataset_id: The ID of the dataset to delete. This is the string returned by the
                create_dataset method, or a Dataset object.
        """
        did = dataset_id.dataset_id if isinstance(dataset_id, Dataset) else dataset_id
        await self._arequest("DELETE", f"/datasets/{did}")

    def parse_dataset_file(
        self,
        dataset: Dataset | str,
        file: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        wait_for_completion: bool = False,
    ) -> Union[str, ParseResult]:
        """
        Parse a file using the dataset's configuration.

        This method allows you to parse a file using the parsing options and structured extraction options defined in the dataset.
        It returns the parse ID if `wait_for_completion` is False, or the full ParseResult if `wait_for_completion` is True.

        The file can be provided as a URL, a file ID (from Tensorlake), or as raw text.

        Args:
            dataset: The Dataset object or dataset ID to use for parsing. This should be the dataset created with create_dataset, or
              the result of get_dataset.

            file: The file to parse. This can be a URL, a file ID (from Tensorlake), or raw text.

            page_range: Optional page range to parse. This can be a string like "1,2,3-5" to specify specific pages or ranges.

            labels: Optional labels to attach to the parsed document. This can be a dictionary of key-value pairs.

            mime_type: Optional MIME type of the file. This can be used to specify the type of content being parsed, such as "application/pdf" or "text/plain".

            wait_for_completion: If True, the method will wait for the parsing to complete and return the full ParseResult.
                If False, it will return the parse ID immediately.
        """

        body = _create_dataset_parse_req(file, page_range, labels, mime_type)

        did = dataset.dataset_id if isinstance(dataset, Dataset) else dataset
        parse_id = self._request("POST", f"/datasets/{did}/parse", json=body).json()[
            "parse_id"
        ]
        if wait_for_completion:
            return self.wait_for_completion(parse_id)  # from _ParseMixin
        return parse_id

    async def parse_dataset_file_async(
        self,
        dataset: Dataset | str,
        file: str,
        page_range: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
        wait_for_completion: bool = False,
    ) -> Union[str, ParseResult]:
        """
        Parse a file using the dataset's configuration asynchronously.

        This method allows you to parse a file using the parsing options and structured extraction options defined in the dataset.
        It returns the parse ID if `wait_for_completion` is False, or the full ParseResult if `wait_for_completion` is True.

        The file can be provided as a URL, a file ID (from Tensorlake), or as raw text.

        Args:
            dataset: The Dataset object or dataset ID to use for parsing. This should be the dataset created with create_dataset, or
              the result of get_dataset.
            file: The file to parse. This can be a URL, a file ID (from Tensorlake), or raw text.
            page_range: Optional page range to parse. This can be a string like "1,2,3-5" to specify specific pages or ranges.
            labels: Optional labels to attach to the parsed document. This can be a dictionary of key-value pairs.
            mime_type: Optional MIME type of the file. This can be used to specify
                the type of content being parsed, such as "application/pdf" or "text/plain".
            wait_for_completion: If True, the method will wait for the parsing to complete and return the full ParseResult.
                If False, it will return the parse ID immediately.
        """
        body = _create_dataset_parse_req(file, page_range, labels, mime_type)
        did = dataset.dataset_id if isinstance(dataset, Dataset) else dataset
        resp = await self._arequest("POST", f"/datasets/{did}/parse", json=body)
        parse_id = resp.json()["parse_id"]
        if wait_for_completion:
            # wait_for_completion_async lives in _ParseMixin
            return await self.wait_for_completion_async(parse_id)
        return parse_id

    def list_datasets(
        self,
        cursor: Optional[str] = None,
        direction: Optional[PaginationDirection] = None,
        limit: Optional[int] = None,
        status: Optional[DatasetStatus] = None,
        name: Optional[str] = None,
    ) -> PaginatedResult[Dataset]:
        """
        List datasets in your Tensorlake project.

        This method retrieves a paginated list of datasets, allowing you to filter by status and name.

        Args:
            cursor: Optional cursor for pagination. If provided, the method will return the next page of
                results starting from this cursor. If not provided, it will return the first page of results.
            direction: Optional pagination direction. If provided, it can be "next" or "prev" to navigate through the pages.
            status: Optional status to filter datasets by. If provided, only datasets with this status will
                be returned. If not provided, all datasets will be returned.
            name: Optional name to filter datasets by. If provided, only datasets that resemble this name will be returned.
                If not provided, all datasets will be returned.
        Returns:
            A PaginatedResult object containing a list of Dataset objects and pagination information.
        """
        params: Dict[str, Any] = _drop_none(
            {
                "cursor": cursor,
                "direction": direction.value if direction else None,
                "limit": limit,
                "status": status.value if status else None,
                "name": name,
            }
        )

        response = self._request("GET", "/datasets", params=params)
        data = response.json()
        datasets = [Dataset.model_validate(d) for d in data["items"]]
        return PaginatedResult[Dataset](
            items=datasets,
            has_more=data.get("has_more", False),
            next_cursor=data.get("next_cursor"),
            prev_cursor=data.get("prev_cursor"),
        )

    async def list_datasets_async(
        self,
        cursor: Optional[str] = None,
        direction: Optional[PaginationDirection] = None,
        limit: Optional[int] = None,
        status: Optional[DatasetStatus] = None,
        name: Optional[str] = None,
    ) -> PaginatedResult[Dataset]:
        """
        List datasets in your Tensorlake project asynchronously.

        This method retrieves a paginated list of datasets, allowing you to filter by status and name.

        Args:
            cursor: Optional cursor for pagination. If provided, the method will return the next page of
                results starting from this cursor. If not provided, it will return the first page of results.
            direction: Optional pagination direction. If provided, it can be "next" or "prev" to navigate through the pages.
            status: Optional status to filter datasets by. If provided, only datasets with this status will
                be returned. If not provided, all datasets will be returned.
            name: Optional name to filter datasets by. If provided, only datasets that resemble this name will be returned.
                If not provided, all datasets will be returned.
        Returns:
            A PaginatedResult object containing a list of Dataset objects and pagination information.
        """
        params: Dict[str, Any] = _drop_none(
            {
                "cursor": cursor,
                "direction": direction.value if direction else None,
                "limit": limit,
                "status": status.value if status else None,
                "name": name,
            }
        )

        resp = await self._arequest("GET", "/datasets", params=params)
        data = resp.json()
        return PaginatedResult[Dataset].model_validate(data, from_attributes=True)

    def update_dataset(
        self,
        dataset: Dataset | str,
        description: Optional[str] = None,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
    ) -> Dataset:
        """
        Update an existing dataset.

        This method allows you to modify the properties of an existing dataset, such as its name, description,
        parsing options, structured extraction options, enrichment options, and page classifications.

        Updating a dataset does not change previously parsed files or their results, but it will affect future parsing operations.

        Args:
            dataset: The Dataset object or dataset ID to update. This should be the dataset created with create_dataset,
                or the result of get_dataset.
            description: Optional new description for the dataset. If provided, this will update the dataset's description.
            parsing_options: Optional new parsing options to customize how documents in the dataset are parsed.
                If provided, this will update the dataset's parsing options. The previous parsing options will be kept
                if not provided.
            structured_extraction_options: Optional new structured extraction options to guide the extraction of structured
                data from documents in the dataset. If provided, this will update the dataset's structured extraction
                options. The previous structured extraction options will be kept if not provided.
            enrichment_options: Optional new enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures. If provided, this will update
                the dataset's enrichment options. The previous enrichment options will be kept if not provided.
            page_classifications: Optional new list of page classification configurations. If provided, this will update
                the dataset's page classifications. The previous page classifications will be kept if not provided.
        """
        body = _create_dataset_req(
            None,
            description,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
        )

        did = dataset.dataset_id if isinstance(dataset, Dataset) else dataset
        response = self._request("PUT", f"/datasets/{did}", json=body)
        return Dataset.model_validate(response.json())

    async def update_dataset_async(
        self,
        dataset: Dataset | str,
        description: Optional[str] = None,
        parsing_options: Optional[ParsingOptions] = None,
        structured_extraction_options: Optional[
            List[StructuredExtractionOptions]
        ] = None,
        enrichment_options: Optional[EnrichmentOptions] = None,
        page_classifications: Optional[List[PageClassConfig]] = None,
    ) -> Dataset:
        """
        Update an existing dataset asynchronously.

        This method allows you to modify the properties of an existing dataset, such as its name, description,
        parsing options, structured extraction options, enrichment options, and page classifications.

        Updating a dataset does not change previously parsed files or their results, but it will affect future parsing operations.

        Args:
            dataset: The Dataset object or dataset ID to update. This should be the dataset created with create_dataset,
                or the result of get_dataset.
            description: Optional new description for the dataset. If provided, this will update the dataset's description.
            parsing_options: Optional new parsing options to customize how documents in the dataset are parsed.
                If provided, this will update the dataset's parsing options. The previous parsing options will be kept
                if not provided.
            structured_extraction_options: Optional new structured extraction options to guide the extraction of structured
                data from documents in the dataset. If provided, this will update the dataset's structured extraction
                options. The previous structured extraction options will be kept if not provided.
            enrichment_options: Optional new enrichment options to extend the output of the document parsing process with
                additional information, such as summarization of tables and figures. If provided, this will update
                the dataset's enrichment options. The previous enrichment options will be kept if not provided.
            page_classifications: Optional new list of page classification configurations. If provided, this will update
                the dataset's page classifications. The previous page classifications will be kept if not provided.
        """
        body = _create_dataset_req(
            None,
            description,
            parsing_options,
            structured_extraction_options,
            enrichment_options,
            page_classifications,
        )

        did = dataset.dataset_id if isinstance(dataset, Dataset) else dataset
        resp = await self._arequest("PUT", f"/datasets/{did}", json=body)
        return Dataset.model_validate(resp.json())

    def get_dataset_data(
        self,
        dataset: Dataset | str,
        filters: DatasetDataFilter | None = None,
    ) -> PaginatedResult[ParseResult]:
        """
        List every parse result in the Tensorlake project.

        Args:
            dataset: The Dataset object or dataset ID to filter the results by. This should be the dataset created with create_dataset,
                or the result of get_dataset.

            filters: Optional set of query filters wrapped in a `DatasetDataFilter`
                    object. Omit or leave attributes as `None` for default behaviour.
        """
        filters = filters or DatasetDataFilter()
        did = dataset.dataset_id if isinstance(dataset, Dataset) else dataset
        response = self._request(
            "GET",
            f"/datasets/{did}/data",
            params=filters.to_query_params(),
        )
        return PaginatedResult[ParseResult].model_validate(response.json())

    async def get_dataset_data_async(
        self, dataset: Dataset | str, filters: DatasetDataFilter | None = None
    ) -> PaginatedResult[ParseResult]:
        """
        List every parse result in the Tensorlake project asynchronously.

        Args:
            dataset: The Dataset object or dataset ID to filter the results by. This should be the dataset created with create_dataset,
                or the result of get_dataset.

            filters: Optional set of query filters wrapped in a `DatasetDataFilter`
                    object. Omit or leave attributes as `None` for default behaviour.
        """
        filters = filters or DatasetDataFilter()
        did = dataset.dataset_id if isinstance(dataset, Dataset) else dataset
        response = await self._arequest(
            "GET",
            f"/datasets/{did}/data",
            params=filters.to_query_params(),
        )
        return PaginatedResult[ParseResult].model_validate(response.json())


def _create_dataset_parse_req(
    file: str,
    page_range: Optional[str],
    labels: Optional[dict],
    mime_type: Optional[MimeType],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}

    if file.startswith(("http://", "https://")):
        payload["file_url"] = file
    elif file.startswith("tensorlake-"):
        payload["file_id"] = file
    else:
        payload["raw_text"] = file

    if labels:
        payload["labels"] = labels
    if page_range:
        payload["page_range"] = page_range
    if mime_type:
        payload["mime_type"] = mime_type.value
    return payload


def _create_dataset_req(
    name: Optional[str],
    description: Optional[str],
    parsing_options: Optional[ParsingOptions],
    structured_extraction_options: Optional[List[StructuredExtractionOptions]],
    enrichment_options: Optional[EnrichmentOptions],
    page_classifications: Optional[List[PageClassConfig]],
) -> Dict[str, Any]:
    body: Dict[str, Any] = _drop_none({"name": name, "description": description})

    if parsing_options:
        body["parsing_options"] = parsing_options.model_dump(exclude_none=True)
    if enrichment_options:
        body["enrichment_options"] = enrichment_options.model_dump(exclude_none=True)
    if page_classifications:
        body["page_classifications"] = [
            pc.model_dump(exclude_none=True) for pc in page_classifications
        ]
    if structured_extraction_options:
        body["structured_extraction_options"] = [
            _convert_seo(opt) for opt in structured_extraction_options
        ]
    return body
