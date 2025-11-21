import threading
from dataclasses import dataclass
from typing import Any, Dict

import grpc

from tensorlake.applications import InternalError, RequestState, TensorlakeError
from tensorlake.applications.interface.awaitables import (
    _request_scoped_id,
)
from tensorlake.applications.request_context.request_state import (
    REQUEST_STATE_USER_DATA_SERIALIZER,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    AllocationRequestStateOperationResult,
)
from tensorlake.function_executor.proto.status_pb2 import Status

from ..blob_store.blob_store import BLOBStore
from ..logger import FunctionExecutorLogger
from .allocation_state_wrapper import AllocationStateWrapper


@dataclass
class _RequestStateOperationInfo:
    result: AllocationRequestStateOperationResult | None
    # Set only once after the BLOB is set.
    result_available: threading.Event


class AllocationRequestState(RequestState):
    def __init__(
        self,
        allocation_state: AllocationStateWrapper,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ) -> None:
        self._allocation_state: AllocationStateWrapper = allocation_state
        self._blob_store: BLOBStore = blob_store
        self._logger: FunctionExecutorLogger = logger.bind(module=__name__)

        # Operation ID -> _RequestStateOperationInfo.
        self._request_state_operations: Dict[str, _RequestStateOperationInfo] = {}

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair."""
        # NB: This is called from user code, user code is blocked.
        # Any exception raised here goes directly to user code.
        try:
            serialized_value: bytes = REQUEST_STATE_USER_DATA_SERIALIZER.serialize(
                value
            )
            blob: BLOB = self._get_writeable_blob(key=key, size=len(serialized_value))
            self._blob_store.put(
                blob=blob, data=[serialized_value], logger=self._logger
            )
            self._commit_writeable_blob(key=key, blob=blob)
        except TensorlakeError:
            raise
        except Exception as e:
            self._logger.error(
                "Failed to set request state",
                exc_info=e,
                key=key,
            )
            raise InternalError(f"Failed to set request state for key '{key}'.")

    def get(self, key: str, default: Any | None = None) -> Any | None:
        """Get a value by key. If the key does not exist, return the default value."""
        # NB: This is called from user code, user code is blocked.
        # Any exception raised here goes directly to user code.
        try:
            blob: BLOB | None = self._get_read_only_blob(key=key)
            if blob is None:
                return default

            size: int = sum(chunk.size for chunk in blob.chunks)
            serialized_value: bytes = self._blob_store.get(
                blob=blob, offset=0, size=size, logger=self._logger
            )
            # possible_types=[] because pickle deserializer knows the target type already.
            deserialized_value: Any = REQUEST_STATE_USER_DATA_SERIALIZER.deserialize(
                serialized_value, possible_types=[]
            )
            return deserialized_value
        except TensorlakeError:
            raise
        except Exception as e:
            self._logger.error(
                "Failed to get request state",
                exc_info=e,
                key=key,
            )
            raise InternalError(f"Failed to get request state for key '{key}'.")

    def deliver_result(self, result: AllocationRequestStateOperationResult) -> None:
        """Deliver the result of a request state operation.

        Doesn't raise any exceptions.
        """
        if result.operation_id not in self._request_state_operations:
            self._logger.error(
                "received result for unknown request state operation",
                operation_id=result.operation_id,
            )
            return

        operation_info = self._request_state_operations[result.operation_id]
        operation_info.result = result
        operation_info.result_available.set()

    def _get_read_only_blob(self, key: str) -> BLOB | None:
        """Gets a read-only BLOB for the given key.

        Returns None if the key does not exist.
        """
        # TODO: Implement this method.
        pass

    def _get_writeable_blob(self, key: str, size: int) -> BLOB:
        # TODO: Implement this method.
        pass

    def _commit_writeable_blob(self, key: str, blob: BLOB) -> None:
        # TODO: Implement this method.
        pass
