from ..interface.function import Function
from ..interface.request import Request
from .api_client import APIClient
from .request import RemoteRequest


class RemoteRunner:
    def __init__(
        self,
        application: Function,
        payload: bytes,
        payload_content_type: str,
    ):
        self._application: Function = application
        self._payload: bytes = payload
        self._payload_content_type: str = payload_content_type
        self._client: APIClient = APIClient()

    def run(self) -> Request:
        app_name: str = self._application.function_config.function_name
        request_id: str = self._client.call(
            application_name=app_name,
            api_function_name=app_name,
            payload=self._payload,
            payload_content_type=self._payload_content_type,
            block_until_done=False,
        )
        return RemoteRequest(
            application=self._application,
            request_id=request_id,
            client=self._client,
        )
