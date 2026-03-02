import threading
from dataclasses import dataclass

import grpc

from tensorlake.applications.interface.futures import (
    _request_scoped_id,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.request_context.http_server.handlers.app_state.commit_write import (
    CommitWriteRequest,
    CommitWriteResponse,
)
from tensorlake.applications.request_context.http_server.handlers.app_state.prepare_read import (
    PrepareReadRequest,
    PrepareReadResponse,
)
from tensorlake.applications.request_context.http_server.handlers.app_state.prepare_write import (
    PrepareWriteRequest,
    PrepareWriteResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2 import BLOB as BLOBProto
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationAppStateCommitWriteOperation,
    AllocationAppStateOperation,
    AllocationAppStateOperationResult,
    AllocationAppStatePrepareReadOperation,
    AllocationAppStatePrepareWriteOperation,
)

from ..allocation_state_wrapper import AllocationStateWrapper
from ..blob_utils import blob_proto_to_blob, blob_to_blob_proto


@dataclass
class _AppStateOperationInfo:
    result: AllocationAppStateOperationResult | None
    # Set only once after the result is available.
    result_available: threading.Event


class AllocationAppState:
    def __init__(
        self,
        allocation_state: AllocationStateWrapper,
        logger: InternalLogger,
    ) -> None:
        self._allocation_state: AllocationStateWrapper = allocation_state
        self._logger: InternalLogger = logger.bind(module=__name__)
        # Operation ID -> _AppStateOperationInfo.
        self._app_state_operations: dict[str, _AppStateOperationInfo] = {}

    def prepare_read(self, request: PrepareReadRequest) -> PrepareReadResponse:
        blob: BLOBProto | None = self._get_read_only_blob(key=request.state_key)
        return PrepareReadResponse(
            blob=None if blob is None else blob_proto_to_blob(blob)
        )

    def prepare_write(self, request: PrepareWriteRequest) -> PrepareWriteResponse:
        blob: BLOBProto = self._get_writeable_blob(
            key=request.state_key, size=request.size
        )
        return PrepareWriteResponse(blob=blob_proto_to_blob(blob))

    def commit_write(self, request: CommitWriteRequest) -> CommitWriteResponse:
        self._commit_writeable_blob(
            key=request.state_key, blob=blob_to_blob_proto(request.blob)
        )
        return CommitWriteResponse()

    def deliver_operation_result(
        self, result: AllocationAppStateOperationResult
    ) -> None:
        """Deliver the result of an app state operation.

        Doesn't raise any exceptions.
        """
        if result.operation_id not in self._app_state_operations:
            self._logger.error(
                "received result for unknown app state operation",
                operation_id=result.operation_id,
            )
            return

        operation_info = self._app_state_operations[result.operation_id]
        operation_info.result = result
        operation_info.result_available.set()

    def _get_read_only_blob(self, key: str) -> BLOBProto | None:
        """Gets a read-only BLOB for the given key.

        Returns None if the key does not exist.
        """
        operation: AllocationAppStateOperation = AllocationAppStateOperation(
            operation_id=_request_scoped_id(),
            state_key=key,
            prepare_read=AllocationAppStatePrepareReadOperation(),
        )
        operation_info: _AppStateOperationInfo = _AppStateOperationInfo(
            result=None,
            result_available=threading.Event(),
        )
        self._app_state_operations[operation.operation_id] = operation_info
        self._allocation_state.add_app_state_operation(operation)

        operation_info.result_available.wait()

        self._allocation_state.remove_app_state_operation(id=operation.operation_id)
        del self._app_state_operations[operation.operation_id]

        if operation_info.result.status.code == grpc.StatusCode.NOT_FOUND.value[0]:
            return None
        elif operation_info.result.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "prepare app state read operation Executor call failed",
                operation_id=operation.operation_id,
                status=operation_info.result.status,
            )
            raise RuntimeError(f"App state get operation failed for key '{key}'.")

        return operation_info.result.prepare_read.blob

    def _get_writeable_blob(self, key: str, size: int) -> BLOBProto:
        """Gets a write-only BLOB for the given key."""
        operation: AllocationAppStateOperation = AllocationAppStateOperation(
            operation_id=_request_scoped_id(),
            state_key=key,
            prepare_write=AllocationAppStatePrepareWriteOperation(
                size=size,
            ),
        )
        operation_info: _AppStateOperationInfo = _AppStateOperationInfo(
            result=None,
            result_available=threading.Event(),
        )
        self._app_state_operations[operation.operation_id] = operation_info
        self._allocation_state.add_app_state_operation(operation)

        operation_info.result_available.wait()

        self._allocation_state.remove_app_state_operation(id=operation.operation_id)
        del self._app_state_operations[operation.operation_id]

        if operation_info.result.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "prepare app state write operation Executor call failed",
                operation_id=operation.operation_id,
                status=operation_info.result.status,
            )
            raise RuntimeError(f"App state set operation failed for key '{key}'.")

        return operation_info.result.prepare_write.blob

    def _commit_writeable_blob(self, key: str, blob: BLOBProto) -> None:
        """Commits writes to a previously obtained write-only BLOB for the given key."""
        operation: AllocationAppStateOperation = AllocationAppStateOperation(
            operation_id=_request_scoped_id(),
            state_key=key,
            commit_write=AllocationAppStateCommitWriteOperation(
                blob=blob,
            ),
        )
        operation_info: _AppStateOperationInfo = _AppStateOperationInfo(
            result=None,
            result_available=threading.Event(),
        )
        self._app_state_operations[operation.operation_id] = operation_info
        self._allocation_state.add_app_state_operation(operation)

        operation_info.result_available.wait()

        self._allocation_state.remove_app_state_operation(id=operation.operation_id)
        del self._app_state_operations[operation.operation_id]

        if operation_info.result.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "failed to commit BLOB write for app state set operation",
                operation_id=operation.operation_id,
                status=operation_info.result.status,
            )
            raise RuntimeError(f"App state set operation failed for key '{key}'.")
