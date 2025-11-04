import base64
from typing import Any, List

from ..function.type_hints import deserialize_type_hints
from ..function.user_data_serializer import deserialize_value
from ..interface import ApplicationValidationError, Request
from ..metadata import ValueMetadata
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
            return_type_hints: List[Any] = deserialize_type_hints(
                serialized_output_type_hints
            )
        except Exception:
            # If we can't deserialize type hints, we just assume no type hints.
            # This usually happens when the application function return types are not loaded into current process.
            return_type_hints: List[Any] = []

        last_exception: BaseException | None = None

        if len(return_type_hints) == 0:
            raise ApplicationValidationError(
                "Can't deserialize request output. Please add a return type hint to the application function "
                f"{self._application_name} to enable deserialization of the request output."
            )

        for type_hint in return_type_hints:
            try:
                return deserialize_value(
                    serialized_value=serialized_output,
                    metadata=ValueMetadata(
                        id="fake_id",
                        cls=type_hint,
                        serializer_name=self._application_manifest.entrypoint.output_serializer,
                        content_type=output_content_type,
                    ),
                )
            except BaseException as e:
                last_exception = e

        # last_exception is Never None here if return_type_hints is not empty.
        raise last_exception
