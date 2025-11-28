import threading
from dataclasses import dataclass
from typing import Any, Dict

import grpc

from tensorlake.applications import InternalError, RequestState, TensorlakeError
from tensorlake.applications.blob_store import BLOB, BLOBStore
from tensorlake.applications.interface.awaitables import (
    _request_scoped_id,
)
from tensorlake.applications.request_context.request_state import (
    REQUEST_STATE_USER_DATA_SERIALIZER,
)
from tensorlake.function_executor.proto.function_executor_pb2 import BLOB as BLOBProto
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationRequestStateCommitWriteOperation,
    AllocationRequestStateOperation,
    AllocationRequestStateOperationResult,
    AllocationRequestStatePrepareReadOperation,
    AllocationRequestStatePrepareWriteOperation,
)

from ...applications.internal_logger import InternalLogger
from .allocation_state_wrapper import AllocationStateWrapper
from .blob_utils import blob_proto_to_blob, blob_to_blob_proto


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
        logger: InternalLogger,
    ) -> None:
        self._allocation_state: AllocationStateWrapper = allocation_state
        self._blob_store: BLOBStore = blob_store
        self._logger: InternalLogger = logger.bind(module=__name__)

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
            blob: BLOBProto = self._get_writeable_blob(
                key=key, size=len(serialized_value)
            )
            uploaded_blob: BLOB = self._blob_store.put(
                blob=blob_proto_to_blob(blob),
                data=[serialized_value],
                logger=self._logger,
            )
            self._commit_writeable_blob(key=key, blob=blob_to_blob_proto(uploaded_blob))
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
            blob: BLOBProto | None = self._get_read_only_blob(key=key)
            if blob is None:
                return default

            size: int = sum(chunk.size for chunk in blob.chunks)
            serialized_value: bytes = self._blob_store.get(
                blob=blob_proto_to_blob(blob), offset=0, size=size, logger=self._logger
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

    def deliver_operation_result(
        self, result: AllocationRequestStateOperationResult
    ) -> None:
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

    def _get_read_only_blob(self, key: str) -> BLOBProto | None:
        """Gets a read-only BLOB for the given key.

        Returns None if the key does not exist.
        """
        operation: AllocationRequestStateOperation = AllocationRequestStateOperation(
            operation_id=_request_scoped_id(),
            state_key=key,
            prepare_read=AllocationRequestStatePrepareReadOperation(),
        )
        operation_info: _RequestStateOperationInfo = _RequestStateOperationInfo(
            result=None,
            result_available=threading.Event(),
        )
        self._request_state_operations[operation.operation_id] = operation_info
        self._allocation_state.add_request_state_operation(operation)

        operation_info.result_available.wait()

        self._allocation_state.remove_request_state_operation(id=operation.operation_id)
        del self._request_state_operations[operation.operation_id]

        if operation_info.result.status.code == grpc.StatusCode.NOT_FOUND.value[0]:
            return None
        elif operation_info.result.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "prepare request state read operation Executor call failed",
                operation_id=operation.operation_id,
                status=operation_info.result.status,
            )
            raise InternalError(f"Request state get operation failed for key '{key}'.")

        return operation_info.result.prepare_read.blob

    def _get_writeable_blob(self, key: str, size: int) -> BLOBProto:
        """Gets a write-only BLOB for the given key."""
        operation: AllocationRequestStateOperation = AllocationRequestStateOperation(
            operation_id=_request_scoped_id(),
            state_key=key,
            prepare_write=AllocationRequestStatePrepareWriteOperation(
                size=size,
            ),
        )
        operation_info: _RequestStateOperationInfo = _RequestStateOperationInfo(
            result=None,
            result_available=threading.Event(),
        )
        self._request_state_operations[operation.operation_id] = operation_info
        self._allocation_state.add_request_state_operation(operation)

        operation_info.result_available.wait()

        self._allocation_state.remove_request_state_operation(id=operation.operation_id)
        del self._request_state_operations[operation.operation_id]

        if operation_info.result.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "prepare request state write operation Executor call failed",
                operation_id=operation.operation_id,
                status=operation_info.result.status,
            )
            raise InternalError(f"Request state set operation failed for key '{key}'.")

        return operation_info.result.prepare_write.blob

    def _commit_writeable_blob(self, key: str, blob: BLOBProto) -> None:
        """Commits writes to a previously obtained write-only BLOB for the given key."""
        operation: AllocationRequestStateOperation = AllocationRequestStateOperation(
            operation_id=_request_scoped_id(),
            state_key=key,
            commit_write=AllocationRequestStateCommitWriteOperation(
                blob=blob,
            ),
        )
        operation_info: _RequestStateOperationInfo = _RequestStateOperationInfo(
            result=None,
            result_available=threading.Event(),
        )
        self._request_state_operations[operation.operation_id] = operation_info
        self._allocation_state.add_request_state_operation(operation)

        operation_info.result_available.wait()

        self._allocation_state.remove_request_state_operation(id=operation.operation_id)
        del self._request_state_operations[operation.operation_id]

        if operation_info.result.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "failed to commit BLOB write for request state set operation",
                operation_id=operation.operation_id,
                status=operation_info.result.status,
            )
            raise InternalError(f"Request state set operation failed for key '{key}'.")
