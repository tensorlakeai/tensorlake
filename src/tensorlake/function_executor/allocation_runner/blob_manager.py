import threading
from dataclasses import dataclass

import grpc

from tensorlake.applications.interface.futures import (
    _request_scoped_id,
)
from tensorlake.applications.internal_logger import InternalLogger

from ..proto.function_executor_pb2 import (
    BLOB,
    AllocationOutputBLOB,
    AllocationUpdate,
)
from .allocation_state_wrapper import AllocationStateWrapper


@dataclass
class _OutputBLOBRequestInfo:
    # Not None once the BLOB is ready to be used.
    blob: AllocationOutputBLOB | None
    # Set only once after the BLOB is set.
    blob_available: threading.Event


class AllocationBLOBManager:
    def __init__(
        self,
        allocation_state: AllocationStateWrapper,
        logger: InternalLogger,
    ) -> None:
        self._allocation_state: AllocationStateWrapper = allocation_state
        self._logger: InternalLogger = logger.bind(module=__name__)
        # BLOB ID -> _OutputBLOBRequestInfo.
        self._output_blob_requests: dict[str, _OutputBLOBRequestInfo] = {}

    def deliver_update(self, update: AllocationUpdate) -> None:
        # No need for any locks because we never block here so we hold GIL non stop.
        if update.HasField("output_blob"):
            blob: AllocationOutputBLOB = update.output_blob
            blob_id: str = blob.blob.id

            if blob_id not in self._output_blob_requests:
                self._logger.error(
                    "received output blob update for unknown blob request",
                    blob_id=blob_id,
                )
                return

            blob_request_info: _OutputBLOBRequestInfo = self._output_blob_requests[
                blob_id
            ]
            blob_request_info.blob = blob
            blob_request_info.blob_available.set()
        else:
            self._logger.error(
                "received unexpected allocation update",
                update=str(update),
            )

    def get_new_output_blob(self, size: int) -> BLOB:
        """Returns new BLOB to upload function outputs to.

        Raises exception on error.
        """
        blob_id: str = _request_scoped_id()
        blob_request_info: _OutputBLOBRequestInfo = _OutputBLOBRequestInfo(
            blob=None,
            blob_available=threading.Event(),
        )
        self._output_blob_requests[blob_id] = blob_request_info
        self._allocation_state.add_output_blob_request(id=blob_id, size=size)

        blob_request_info.blob_available.wait()

        self._allocation_state.remove_output_blob_request(id=blob_id)
        del self._output_blob_requests[blob_id]

        if isinstance(blob_request_info.blob, AllocationOutputBLOB):
            if blob_request_info.blob.status.code != grpc.StatusCode.OK.value[0]:
                self._logger.error(
                    "received output blob with error status",
                    blob_id=blob_request_info.blob.blob.id,
                    status=blob_request_info.blob.status,
                )
                raise RuntimeError(
                    f"Failed to create output BLOB: {blob_request_info.blob.status}"
                )
            return blob_request_info.blob.blob
        else:
            return blob_request_info.blob
