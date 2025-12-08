from typing import Any

import httpx

from tensorlake.applications.blob_store import BLOB, BLOBStore
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.request_context.request_state import (
    REQUEST_STATE_USER_DATA_SERIALIZER,
)

from ...interface.exceptions import InternalError, SDKUsageError
from ...interface.request_context import RequestState
from ..http_server.handlers.request_state.commit_write import (
    COMMIT_WRITE_PATH,
    COMMIT_WRITE_VERB,
    CommitWriteRequest,
)
from ..http_server.handlers.request_state.prepare_read import (
    PREPARE_READ_PATH,
    PREPARE_READ_VERB,
    PrepareReadRequest,
    PrepareReadResponse,
)
from ..http_server.handlers.request_state.prepare_write import (
    PREPARE_WRITE_PATH,
    PREPARE_WRITE_VERB,
    PrepareWriteRequest,
    PrepareWriteResponse,
)


class RequestStateHTTPClient(RequestState):
    """HTTP client for accessing request state in subprocesses and child threads.

    Thread-safe.
    """

    def __init__(
        self,
        request_id: str,
        allocation_id: str,
        http_client: httpx.Client,
        blob_store: BLOBStore,
        logger: InternalLogger,
    ):
        self._request_id: str = request_id
        self._allocation_id: str = allocation_id
        self._http_client: httpx.Client = http_client
        self._blob_store: BLOBStore = blob_store
        self._logger: InternalLogger = logger.bind(module=__name__)

    def set(self, key: str, value: Any) -> None:
        # NB: This is called from user code, user code is blocked.
        # Any exception raised here goes directly to user code.

        # If we don't validate user supplied inputs here then there will be a Pydantic validation error
        # below which will raise an InternalError instead of SDKUsageError.
        if not isinstance(key, str):
            raise SDKUsageError(f"State key must be a string, got: {key}")
        # Raises SerializationError to customer code on failure.
        serialized_value: bytes = REQUEST_STATE_USER_DATA_SERIALIZER.serialize(value)

        try:
            blob: BLOB = self._get_writeable_blob(key=key, size=len(serialized_value))
            uploaded_blob: BLOB = self._blob_store.put(
                blob=blob,
                data=[serialized_value],
                logger=self._logger,
            )
            self._commit_writeable_blob(key=key, blob=uploaded_blob)
        except Exception as e:
            self._logger.error(
                "Failed to set request state",
                exc_info=e,
                key=key,
            )
            raise InternalError(f"Failed to set request state for key '{key}'.")

    def get(self, key: str, default: Any | None = None) -> Any | None:
        # NB: This is called from user code, user code is blocked.
        # Any exception raised here goes directly to user code.

        # If we don't validate user supplied inputs here then there will be a Pydantic validation error
        # below which will raise an InternalError instead of SDKUsageError.
        if not isinstance(key, str):
            raise SDKUsageError(f"State key must be a string, got: {key}")

        try:
            blob: BLOB | None = self._get_read_only_blob(key=key)
            if blob is None:
                return default

            size: int = sum(chunk.size for chunk in blob.chunks)
            serialized_value: bytes = self._blob_store.get(
                blob=blob, offset=0, size=size, logger=self._logger
            )
        except Exception as e:
            self._logger.error(
                "Failed to get request state",
                exc_info=e,
                key=key,
            )
            raise InternalError(f"Failed to get request state for key '{key}'.")

        # Raises SerializationError to customer code on failure.
        # possible_types=[] because pickle deserializer knows the target type already.
        return REQUEST_STATE_USER_DATA_SERIALIZER.deserialize(
            serialized_value, possible_types=[]
        )

    def _get_read_only_blob(self, key: str) -> BLOB | None:
        request_payload: PrepareReadRequest = PrepareReadRequest(
            request_id=self._request_id,
            allocation_id=self._allocation_id,
            state_key=key,
        )
        request: httpx.Request = self._http_client.build_request(
            PREPARE_READ_VERB,
            url=PREPARE_READ_PATH,
            json=request_payload.model_dump(),
        )
        response: httpx.Response = self._http_client.send(request)
        response.raise_for_status()
        response_payload: PrepareReadResponse = PrepareReadResponse.model_validate_json(
            response.content
        )
        return response_payload.blob

    def _get_writeable_blob(self, key: str, size: int) -> BLOB:
        request_payload: PrepareWriteRequest = PrepareWriteRequest(
            request_id=self._request_id,
            allocation_id=self._allocation_id,
            state_key=key,
            size=size,
        )
        request: httpx.Request = self._http_client.build_request(
            PREPARE_WRITE_VERB,
            url=PREPARE_WRITE_PATH,
            json=request_payload.model_dump(),
        )
        response: httpx.Response = self._http_client.send(request)
        response.raise_for_status()
        response_payload: PrepareWriteResponse = (
            PrepareWriteResponse.model_validate_json(response.content)
        )
        return response_payload.blob

    def _commit_writeable_blob(self, key: str, blob: BLOB) -> None:
        request_payload: CommitWriteRequest = CommitWriteRequest(
            request_id=self._request_id,
            allocation_id=self._allocation_id,
            state_key=key,
            blob=blob,
        )
        request: httpx.Request = self._http_client.build_request(
            COMMIT_WRITE_VERB,
            url=COMMIT_WRITE_PATH,
            json=request_payload.model_dump(),
        )
        response: httpx.Response = self._http_client.send(request)
        response.raise_for_status()

    def __getstate__(self):
        raise SDKUsageError("Pickling of RequestStateHTTPClient is not supported.")

    def __setstate__(self, state):
        raise SDKUsageError("Unpickling of RequestStateHTTPClient is not supported.")
