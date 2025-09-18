from typing import Any

from tensorlake.workflows.interface.request import Request

from .api_client import APIClient


class RemoteRequest(Request):
    def __init__(
        self,
        application_name: str,
        api_function_name: str,
        request_id: str,
        client: APIClient,
    ):
        self._application_name: str = application_name
        self._api_function_name: str = api_function_name
        self._request_id: str = request_id
        self._client: APIClient = client

    @property
    def id(self) -> str:
        return self._request_id

    def output(self) -> Any:
        self._client.wait_on_request_completion(
            application_name=self._application_name, request_id=self._request_id
        )
        return self._client.function_outputs(
            application_name=self._application_name,
            request_id=self._request_id,
            function_name=self._api_function_name,
        )
