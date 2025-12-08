import json

import httpx

from ...interface.exceptions import InternalError, SDKUsageError, SerializationError
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

    def __init__(self, request_id: str, allocation_id: str, http_client: httpx.Client):
        self._request_id: str = request_id
        self._allocation_id: str = allocation_id
        self._http_client: httpx.Client = http_client

    def update(
        self,
        current: float,
        total: float,
        message: str | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        # Instead of handling serialization errors on the Server, just validate attributes on client side.
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
