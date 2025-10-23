import hashlib
import threading

from ..proto.function_executor_pb2 import (
    BLOB,
    AllocationFunctionCall,
    AllocationFunctionCallWatcher,
    AllocationOutputBLOBRequest,
    AllocationProgress,
    AllocationResult,
    AllocationState,
    ExecutionPlanUpdates,
)


class AllocationStateWrapper:
    def __init__(self) -> None:
        self._allocation_state: AllocationState = AllocationState(
            function_calls=[],
        )
        self._allocation_state_update_lock: threading.Condition = threading.Condition()
        self._update_hash()

    def update_progress(self, current: float, total: float) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.progress = AllocationProgress(
                current=current, total=total
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def set_result(self, result: AllocationResult) -> None:
        # This method is expected to be called only once.
        with self._allocation_state_update_lock:
            self._allocation_state.result = result
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def add_function_call(
        self, execution_plan_updates: ExecutionPlanUpdates, args_blob: BLOB
    ) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.function_calls.append(
                AllocationFunctionCall(
                    execution_plan_updates=execution_plan_updates,
                    args_blob=args_blob,
                )
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def delete_function_call(self, function_call_id: str) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.function_calls[:] = [
                fc
                for fc in self._allocation_state.function_calls
                if fc.execution_plan_updates.root_function_call_id != function_call_id
            ]
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def add_function_call_watcher(self, function_call_id: str) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.function_call_watchers.append(
                AllocationFunctionCallWatcher(
                    function_call_id=function_call_id,
                )
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def delete_function_call_watcher(self, function_call_id: str) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.function_call_watchers[:] = [
                watcher
                for watcher in self._allocation_state.function_call_watchers
                if watcher.function_call_id != function_call_id
            ]
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def add_output_blob_request(self, id: str, size: int) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.output_blob_requests.append(
                AllocationOutputBLOBRequest(
                    id=id,
                    size=size,
                )
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def remove_output_blob_request(self, id: str) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.output_blob_requests[:] = [
                request
                for request in self._allocation_state.output_blob_requests
                if request.id != id
            ]
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def wait_for_update(self, last_seen_hash: str | None) -> AllocationState:
        """Returns copy of the current allocation state when it's updated."""
        with self._allocation_state_update_lock:
            # No more state updates will happen if the result field is set.
            # Return to avoid deadlock here.
            if self._allocation_state.HasField("result"):
                return AllocationState().CopyFrom(self._allocation_state)

            while True:
                if last_seen_hash != self._allocation_state.sha256_hash:
                    return AllocationState().CopyFrom(self._allocation_state)
                self._allocation_state_update_lock.wait()

    def _update_hash(self) -> None:
        self._allocation_state.ClearField("sha256_hash")
        self._allocation_state.sha256_hash = hashlib.sha256(
            self._allocation_state.SerializeToString(deterministic=True)
        ).hexdigest()
