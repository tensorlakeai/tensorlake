from typing import Any, List

from ..function.type_hints import function_return_type_hint
from ..function.user_data_serializer import function_output_serializer
from ..interface.file import File
from ..interface.function import Function
from ..interface.request import Request
from ..user_data_serializer import UserDataSerializer
from .api_client import APIClient


class RemoteRequest(Request):
    def __init__(
        self,
        application: Function,
        request_id: str,
        client: APIClient,
    ):
        self._application: Function = application
        self._request_id: str = request_id
        self._client: APIClient = client

    @property
    def id(self) -> str:
        return self._request_id

    def output(self) -> Any:
        app_name: str = self._application.function_config.function_name
        self._client.wait_on_request_completion(
            application_name=app_name, request_id=self._request_id
        )
        serialized_output: bytes
        output_content_type: str
        serialized_output, output_content_type = self._client.request_output(
            application_name=app_name,
            request_id=self._request_id,
        )
        # When deserializing API function inputs we use its payload type hints to
        # deserialize the output correctly. Here we're doing a symmetric operation.
        # We use API function return value type hint. This is a consistent UX for API functions.
        api_return_type_hint: List[Any] = function_return_type_hint(self._application)
        is_file_output: bool = False
        for type_hint in api_return_type_hint:
            if type_hint is File:
                is_file_output = True

        if is_file_output:
            return File(content=serialized_output, content_type=output_content_type)
        else:
            # API function serializer is always statically set in its api_config.
            api_output_serializer: UserDataSerializer = function_output_serializer(
                self._application, None
            )
            return api_output_serializer.deserialize(
                serialized_output, api_return_type_hint
            )
