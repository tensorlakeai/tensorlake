import json

import httpx

from ...interface.exceptions import InternalError, SDKUsageError
from ...interface.request_context import FunctionProgress
from ..http_server.handlers.progress_update import (
    PROGRESS_UPDATE_PATH,
    PROGRESS_UPDATE_VERB,
    FunctionProgressUpdateRequest,
)


class FunctionProgressHTTPClient(FunctionProgress):
    """HTTP client for accessing function progress in subprocesses and child threads.

    Thread-safe for use in multiple threaded applications.
    """

    def __init__(
        self,
        request_id: str,
        allocation_id: str,
        function_name: str,
        http_client: httpx.Client,
    ):
        self._request_id: str = request_id
        self._allocation_id: str = allocation_id
        self._function_name: str = function_name
        self._http_client: httpx.Client = http_client

    def update(
        self,
        current: int | float,
        total: int | float,
        message: str | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        # If we don't validate user supplied inputs here then there will be a Pydantic validation error
        # below which will raise an InternalError instead of SDKUsageError.
        if not isinstance(current, (int, float)):
            raise SDKUsageError(f"'current' needs to be a number, got: {current}")
        if not isinstance(total, (int, float)):
            raise SDKUsageError(f"'total' needs to be a number, got: {total}")
        if message is not None and not isinstance(message, str):
            raise SDKUsageError(f"'message' needs to be a string, got: {message}")

        if attributes is not None:
            if not isinstance(attributes, dict):
                raise SDKUsageError(
                    f"'attributes' needs to be a dictionary of string key/value pairs, got: {attributes}"
                )
            for key, value in attributes.items():
                if not isinstance(key, str):
                    raise SDKUsageError(f"'attributes' key {key} needs to be a string")
                if not isinstance(value, str):
                    raise SDKUsageError(
                        f"'attributes' value {value} for key '{key}' needs to be a string"
                    )

        try:
            request_payload: FunctionProgressUpdateRequest = (
                FunctionProgressUpdateRequest(
                    request_id=self._request_id,
                    allocation_id=self._allocation_id,
                    function_name=self._function_name,
                    current=current,
                    total=total,
                    message=message,
                    attributes=attributes,
                )
            )
            request: httpx.Request = self._http_client.build_request(
                PROGRESS_UPDATE_VERB,
                url=PROGRESS_UPDATE_PATH,
                json=request_payload.model_dump(),
            )
            response: httpx.Response = self._http_client.send(request)
            response.raise_for_status()
        except Exception as e:
            raise InternalError(
                f"Failed to update function progress via HTTP: {e}"
            ) from e

    def __getstate__(self):
        raise SDKUsageError("Pickling of FunctionProgressHTTPClient is not supported.")

    def __setstate__(self, state):
        raise SDKUsageError(
            "Unpickling of FunctionProgressHTTPClient is not supported."
        )
