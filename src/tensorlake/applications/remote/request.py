import base64
from typing import Any, List

from ..function.type_hints import deserialize_type_hints
from ..interface.file import File
from ..interface.request import Request
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .api_client import APIClient
from .manifests.application import ApplicationManifest


class RemoteRequest(Request):
    def __init__(
        self,
        application_name: str,
        application_manifest: ApplicationManifest,
        request_id: str,
        client: APIClient,
    ):
        self._application_name: str = application_name
        self._application_manifest: ApplicationManifest = application_manifest
        self._request_id: str = request_id
        self._client: APIClient = client

    @property
    def id(self) -> str:
        return self._request_id

    def output(self) -> Any:
        self._client.wait_on_request_completion(
            application_name=self._application_name, request_id=self._request_id
        )
        serialized_output: bytes
        output_content_type: str
        serialized_output, output_content_type = self._client.request_output(
            application_name=self._application_name,
            request_id=self._request_id,
        )
        # When deserializing API function inputs we use its payload type hints to
        # deserialize the output correctly. Here we're doing a symmetric operation.
        # We use API function return value type hint. This is a consistent UX for API functions.
        output_type_hints_base64: str = (
            self._application_manifest.entrypoint.output_type_hints_base64
        )
        serialized_output_type_hints: bytes = base64.decodebytes(
            output_type_hints_base64.encode("utf-8")
        )
        try:
            output_type_hints: List[Any] = deserialize_type_hints(
                serialized_output_type_hints
            )
        except Exception:
            # If we can't deserialize type hints, we just assume no type hints.
            # This usually happens when the application function return types are not loaded into current process.
            output_type_hints: List[Any] = []

        is_file_output: bool = False
        for type_hint in output_type_hints:
            if type_hint is File:
                is_file_output = True

        if is_file_output:
            return File(content=serialized_output, content_type=output_content_type)
        else:
            api_output_serializer: UserDataSerializer = serializer_by_name(
                self._application_manifest.entrypoint.output_serializer
            )
            return api_output_serializer.deserialize(
                serialized_output, output_type_hints
            )
