import hashlib
import threading
import time
from typing import Any

from tensorlake.applications import Function, FunctionProgress
from tensorlake.applications.request_context.request_context_base import (
    RequestContextBase,
)
from tensorlake.applications.request_context.request_metrics_recorder import (
    RequestMetricsRecorder,
)

from ..blob_store.blob_store import BLOBStore
from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import (
    Allocation,
    AllocationFunctionCallResult,
    AllocationProgress,
    AllocationResult,
    AllocationState,
    FunctionRef,
)
from ..request_state.proxied_request_state import ProxiedRequestState
from ..request_state.request_state_proxy_server import RequestStateProxyServer
from .result_helper import ResultHelper


class AllocationRunner:
    """Runs a single allocation in a separate thread, allowing to track its state.

    Sets allocation.result when finished.
    """

    def __init__(
        self,
        allocation: Allocation,
        request_state_proxy_server: RequestStateProxyServer,
        function_ref: FunctionRef,
        function: Function,
        function_instance_arg: Any | None,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._allocation: Allocation = allocation
        self._request_state_proxy_server: RequestStateProxyServer = (
            request_state_proxy_server
        )
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._function_instance_arg: Any | None = function_instance_arg
        self._blob_store: BLOBStore = blob_store
        self._logger = logger.bind(module=__name__)

        self._finished: bool = False
        self._request_context: RequestContextBase = RequestContextBase(
            request_id=self._allocation.request_id,
            state=ProxiedRequestState(
                allocation_id=self._allocation.allocation_id,
                proxy_server=self._request_state_proxy_server,
            ),
            progress=ProxiedAllocationProgress(self),
            metrics=RequestMetricsRecorder(),
        )
        self._result_helper: ResultHelper = ResultHelper(self._request_context.metrics)
        self._allocation_state: AllocationState = AllocationState(
            function_calls=[],
        )
        _update_allocation_state_hash(self._allocation_state)
        self._allocation_state_update_lock: threading.Condition = threading.Condition()
        self._allocation_thread: threading.Thread = threading.Thread(
            target=self._run_allocation_thread,
            daemon=True,
        )

    def wait_allocation_state_update(
        self, last_seen_hash: str | None
    ) -> AllocationState:
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

    def run(self) -> None:
        """Runs the allocation in a separate thread.

        When the allocation is finished, sets it .result field.
        """
        self._allocation_thread.start()

    @property
    def finished(self) -> bool:
        return self._finished

    def deliver_function_call_result(
        self, result: AllocationFunctionCallResult
    ) -> None:
        """Delivers function call result to the allocation."""
        # TODO: Implement.
        pass

    def _update_allocation_state_progress(self, current: float, total: float) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.progress = AllocationProgress(
                current=current, total=total
            )
            _update_allocation_state_hash(self._allocation_state)
            self._allocation_state_update_lock.notify_all()

    def _update_allocation_state_result(self, result: AllocationResult) -> None:
        # This method is expected to be called only once.
        self._allocation.result = result
        with self._allocation_state_update_lock:
            self._allocation_state.result = result
            _update_allocation_state_hash(self._allocation_state)
            self._allocation_state_update_lock.notify_all()

    def _run_allocation_thread(self) -> None:
        try:
            result: AllocationResult = self._run()
            self._update_allocation_state_result(result)
        except BaseException as e:
            self._logger.error(
                "allocation failed due to exception in function executor code",
                exc_info=e,
            )
            self._update_allocation_state_result(
                self._result_helper.internal_error_result()
            )
        finally:
            self._finished = True

    def _run(self) -> AllocationResult:
        pass


def _update_allocation_state_hash(allocation_state: AllocationState) -> None:
    allocation_state.ClearField("sha256_hash")
    allocation_state.sha256_hash = hashlib.sha256(
        allocation_state.SerializeToString(deterministic=True)
    ).hexdigest()


class ProxiedAllocationProgress(FunctionProgress):
    def __init__(self, allocation_runner: AllocationRunner):
        self._allocation_runner: AllocationRunner = allocation_runner

    def update(self, current: float, total: float) -> None:
        self._allocation_runner._update_allocation_state_progress(current, total)
        # sleep(0) here momentarily releases the GIL, giving other
        # FE threads a chance to run before returning back to customer code that
        # might never return GIL. i.e. allowing the FE to handle incoming RPCs,
        # report back allocation state updates, etc.
        # NB: this was never tested to fix anything in practice but nice to have
        # this just in case.
        time.sleep(0)
