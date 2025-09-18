from typing import Any

from ..function.user_data_serializer import function_input_serializer
from ..interface.application import Application
from ..interface.function import Function
from ..interface.request import Request
from ..user_data_serializer import UserDataSerializer
from .api_client import APIClient
from .request import RemoteRequest


class RemoteRunner:
    def __init__(self, application: Application, api: Function, payload: Any):
        self._application: Application = application
        self._api: Function = api
        self._payload: Any = payload
        self._client: APIClient = APIClient()

    def run(self) -> Request:
        payload_serializer: UserDataSerializer = function_input_serializer(self._api)
        serialized_payload: bytes = payload_serializer.serialize(self._payload)
        request_id: str = self._client.call(
            application_name=self._application.name,
            api_function_name=self._api.function_config.function_name,
            payload=serialized_payload,
            payload_content_type=payload_serializer.content_type,
            block_until_done=False,
        )
        return RemoteRequest(
            application_name=self._application.name,
            api_function_name=self._api.function_config.function_name,
            request_id=request_id,
            client=self._client,
        )
