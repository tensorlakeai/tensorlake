from typing import Optional, overload

from ._base import _BaseClient, _validate_file_input
from ._utils import _drop_none
from .models import FormFillingOptions, MimeType


class _EditMixin(_BaseClient):

    # Sync method overloads
    @overload
    def edit(
        self,
        form_filling_options: FormFillingOptions,
        *,
        file_id: str,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Edit document by file ID."""

    @overload
    def edit(
        self,
        form_filling_options: FormFillingOptions,
        *,
        file_url: str,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Edit document from URL."""

    @overload
    def edit(
        self,
        form_filling_options: FormFillingOptions,
        *,
        raw_text: str,
        mime_type: MimeType,  # Required when using raw_text
        labels: Optional[dict] = None,
    ) -> str:
        """Edit from raw text. MIME type is required."""

    def edit(
        self,
        form_filling_options: FormFillingOptions,
        file_id: Optional[str] = None,
        file_url: Optional[str] = None,
        raw_text: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """
        Create a new edit operation.

        This operation will edit the specified document using the provided options.

        This method is asynchronous. It will return an identifier for the operation, which can be used
        to retrieve the results with the wait_for_completion, or the get_parsed_result methods.
        """
        _validate_file_input(
            file_id=file_id, file_url=file_url, raw_text=raw_text, mime_type=mime_type
        )
        payload = _drop_none(
            {
                "file_id": file_id,
                "file_url": file_url,
                "raw_text": raw_text,
                "labels": labels,
                "mime_type": mime_type.value if mime_type else None,
                "form_filling": form_filling_options.model_dump(
                    exclude_none=True
                ),
            }
        )

        response = self._request("POST", "edit", json=payload)
        json_response = response.json()
        return json_response["parse_id"]

    # Async method overloads
    @overload
    async def edit_async(
        self,
        form_filling_options: FormFillingOptions,
        *,
        file_id: str,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Edit document by file ID asynchronously."""

    @overload
    async def edit_async(
        self,
        form_filling_options: FormFillingOptions,
        *,
        file_url: str,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """Edit document from URL asynchronously."""

    @overload
    async def edit_async(
        self,
        form_filling_options: FormFillingOptions,
        *,
        raw_text: str,
        mime_type: MimeType,  # Required when using raw_text
        labels: Optional[dict] = None,
    ) -> str:
        """Edit from raw text asynchronously. MIME type is required."""

    async def edit_async(
        self,
        form_filling_options: FormFillingOptions,
        file_id: Optional[str] = None,
        file_url: Optional[str] = None,
        raw_text: Optional[str] = None,
        labels: Optional[dict] = None,
        mime_type: Optional[MimeType] = None,
    ) -> str:
        """
        Create a new edit operation asynchronously.

        This operation will edit the specified document using the provided options.

        This method is asynchronous. It will return an identifier for the operation, which can be used
        to retrieve the results with the wait_for_completion, or the get_parsed_result methods.
        """
        _validate_file_input(
            file_id=file_id, file_url=file_url, raw_text=raw_text, mime_type=mime_type
        )

        payload = _drop_none(
            {
                "file_id": file_id,
                "file_url": file_url,
                "raw_text": raw_text,
                "labels": labels,
                "mime_type": mime_type.value if mime_type else None,
                "form_filling": form_filling_options.model_dump(
                    exclude_none=True
                ),
            }
        )

        response = await self._arequest("POST", "edit", json=payload)
        json_response = response.json()
        return json_response["parse_id"]