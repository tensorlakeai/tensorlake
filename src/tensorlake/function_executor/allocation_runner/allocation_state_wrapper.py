import hashlib
import threading
from typing import Any, Callable, Iterable

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
        self._allocation_state_update_lock: threading.Condition = threading.Condition()
        self._allocation_state: AllocationState = AllocationState(
            function_calls=[],
            function_call_watchers=[],
            output_blob_requests=[],
        )
        self._update_hash()

    def update_progress(self, current: float, total: float) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.progress.CopyFrom(
                AllocationProgress(current=current, total=total)
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def set_result(self, result: AllocationResult) -> None:
        # This method is expected to be called only once.
        with self._allocation_state_update_lock:
            self._allocation_state.result.CopyFrom(result)
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def has_result(self) -> bool:
        with self._allocation_state_update_lock:
            return self._allocation_state.HasField("result")

    def add_function_call(
        self, execution_plan_updates: ExecutionPlanUpdates, args_blob: BLOB | None
    ) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.function_calls.append(
                AllocationFunctionCall(
                    updates=execution_plan_updates,
                    args_blob=args_blob,
                )
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def delete_function_call(self, function_call_id: str) -> None:
        with self._allocation_state_update_lock:
            _remove_repeated_field_item(
                lambda fc: fc.updates.root_function_call_id == function_call_id,
                self._allocation_state.function_calls,
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def add_function_call_watcher(self, watcher_id: str, function_call_id: str) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.function_call_watchers.append(
                AllocationFunctionCallWatcher(
                    watcher_id=watcher_id,
                    function_call_id=function_call_id,
                )
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def delete_function_call_watcher(self, watcher_id: str) -> None:
        with self._allocation_state_update_lock:
            _remove_repeated_field_item(
                lambda fcw: fcw.watcher_id == watcher_id,
                self._allocation_state.function_call_watchers,
            )
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
            _remove_repeated_field_item(
                lambda req: req.id == id,
                self._allocation_state.output_blob_requests,
            )
            self._update_hash()
            self._allocation_state_update_lock.notify_all()

    def wait_for_update(self, last_seen_hash: str | None) -> AllocationState:
        """Returns copy of the current allocation state when it's updated."""
        with self._allocation_state_update_lock:
            while True:
                if last_seen_hash != self._allocation_state.sha256_hash:
                    return self._copy_state_locked()
                if self._allocation_state.HasField("result"):
                    # No more state updates will happen if the result field is set.
                    # Return to avoid deadlock in wait() below.
                    return self._copy_state_locked()
                self._allocation_state_update_lock.wait()

    def _copy_state_locked(self) -> AllocationState:
        allocation_state_copy = AllocationState()
        allocation_state_copy.CopyFrom(self._allocation_state)
        return allocation_state_copy

    def _update_hash(self) -> None:
        self._allocation_state.ClearField("sha256_hash")
        self._allocation_state.sha256_hash = hashlib.sha256(
            self._allocation_state.SerializeToString(deterministic=True)
        ).hexdigest()


def _remove_repeated_field_item(
    predicate: Callable[[Any], bool], repeated_field: Iterable[Any]
) -> None:
    """Removes the first item matching the predicate in the repeated proto field.

    Raises ValueError if no item matches the predicate.
    """
    for index, item in enumerate(repeated_field):
        if predicate(item):
            del repeated_field[index]
            return
    raise ValueError("No item found matching the predicate.")
