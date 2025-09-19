from ..interface.application import Application
from ..interface.function import Function
from ..interface.request import Request
from .api_client import APIClient
from .request import RemoteRequest


class RemoteRunner:
    def __init__(
        self,
        application: Application,
        api: Function,
        payload: bytes,
        payload_content_type: str,
    ):
        self._application: Application = application
        self._api: Function = api
        self._payload: bytes = payload
        self._payload_content_type: str = payload_content_type
        self._client: APIClient = APIClient()

    def run(self) -> Request:
        request_id: str = self._client.call(
            application_name=self._application.name,
            api_function_name=self._api.function_config.function_name,
            payload=self._payload,
            payload_content_type=self._payload_content_type,
            block_until_done=False,
        )
        return RemoteRequest(
            application_name=self._application.name,
            api_function=self._api,
            request_id=request_id,
            client=self._client,
        )
