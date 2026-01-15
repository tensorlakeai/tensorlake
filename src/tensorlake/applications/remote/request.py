import base64
from typing import Any

from ..function.type_hints import deserialize_type_hints
from ..function.user_data_serializer import deserialize_value
from ..interface import DeserializationError, Request
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .api_client import APIClient, RequestOutput
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

        request_output: RequestOutput = self._client.request_output(
            application_name=self._application_name,
            request_id=self._request_id,
        )

        try:
            output_type_hints_base64: str = (
                self._application_manifest.entrypoint.output_type_hints_base64
            )
            serialized_output_type_hints: bytes = base64.decodebytes(
                output_type_hints_base64.encode("utf-8")
            )
            return_type_hints: list[Any] = deserialize_type_hints(
                serialized_output_type_hints
            )
        except Exception:
            # If we can't deserialize type hints, we just assume no type hints.
            # This usually happens when the application function return types are
            # not loaded into current process.
            return_type_hints: list[Any] = []

        if len(return_type_hints) == 0:
            raise DeserializationError(
                "Can't deserialize request output due to empty return type hints."
            )

        output_deserializer: UserDataSerializer = serializer_by_name(
            self._application_manifest.entrypoint.output_serializer
        )
        return deserialize_value(
            serialized_value=request_output.serialized_value,
            serializer=output_deserializer,
            content_type=request_output.content_type,
            type_hints=return_type_hints,
        )
