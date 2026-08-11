import threading
from dataclasses import dataclass

import grpc

from tensorlake.applications.interface.futures import (
    _request_scoped_id,
)
from tensorlake.applications.internal_logger import InternalLogger

from ..proto.function_executor_pb2 import (
    BLOB,
    AllocationOutputBlob,
)
from .allocation_state_wrapper import AllocationStateWrapper


@dataclass
class _OutputBLOBRequestInfo:
    # Not None once the BLOB is ready to be used.
    blob: AllocationOutputBlob | None
    # Internal reconciliation failure which must be surfaced to the waiter.
    error: BaseException | None
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
        # Keeps dict insertion order identical to AllocationState request order,
        # which is the only correlation available for BLOB-less error replies.
        self._output_blob_requests_lock = threading.Lock()

    def deliver_output_blob(self, output_blob: AllocationOutputBlob) -> None:
        """Delivers an output blob response to the pending get_new_output_blob() call."""
        blob_id: str = output_blob.blob.id
        matched_by_order: bool = False
        blob_request_info: _OutputBLOBRequestInfo | None = None
        reconciliation_error: BaseException | None = None
        with self._output_blob_requests_lock:
            if blob_id == "" and self._output_blob_requests:
                # The protocol omits `blob` when the status is not OK, so an
                # error response has no BLOB ID. Allocation state responses are
                # delivered in request order; associate a BLOB-less response
                # with the oldest outstanding request.
                blob_id = next(iter(self._output_blob_requests))
                matched_by_order = True

            blob_request_info = self._output_blob_requests.pop(blob_id, None)
            if blob_request_info is not None:
                # Reconcile the request before waking its waiter. A second
                # BLOB-less failure can arrive before that waiter is scheduled
                # and must match the next request, not this one a second time.
                try:
                    self._allocation_state.remove_output_blob_request(id=blob_id)
                except BaseException as error:
                    # The request has already been matched and removed from the
                    # correlation map. Always wake its waiter: otherwise an
                    # internal state invariant failure becomes a permanent
                    # allocation hang.
                    reconciliation_error = error

        if blob_request_info is None:
            self._logger.error(
                "received output blob update for unknown blob request",
                blob_id=blob_id or None,
            )
            return

        blob_request_info.error = reconciliation_error
        blob_request_info.blob = output_blob
        blob_request_info.blob_available.set()
        if reconciliation_error is not None:
            self._logger.error(
                "failed to reconcile delivered output blob request",
                blob_id=blob_id,
                exc_info=reconciliation_error,
            )
            return

        self._logger.debug(
            "delivering output blob update",
            blob_id=blob_id,
            status_code=output_blob.status.code,
            matched_by_order=matched_by_order,
        )

    def get_new_output_blob(self, size: int) -> BLOB:
        """Returns new BLOB to upload function outputs to.

        Raises exception on error.
        """
        blob_id: str = _request_scoped_id()
        blob_request_info: _OutputBLOBRequestInfo = _OutputBLOBRequestInfo(
            blob=None,
            error=None,
            blob_available=threading.Event(),
        )
        with self._output_blob_requests_lock:
            self._output_blob_requests[blob_id] = blob_request_info
            try:
                self._allocation_state.add_output_blob_request(id=blob_id, size=size)
            except BaseException:
                # Publishing and correlation are one logical operation. Do not
                # leave an unpublished request at the head of the map, where it
                # would steal the next ordered BLOB-less error response.
                del self._output_blob_requests[blob_id]
                raise

        blob_request_info.blob_available.wait()

        if blob_request_info.error is not None:
            raise RuntimeError(
                "Failed to reconcile output BLOB request state"
            ) from blob_request_info.error
        blob: AllocationOutputBlob = blob_request_info.blob
        if blob.status.code != grpc.StatusCode.OK.value[0]:
            self._logger.error(
                "received output blob with error status",
                blob_id=blob.blob.id,
                status=blob.status,
            )
            raise RuntimeError(f"Failed to create output BLOB: {blob.status}")
        return blob.blob
