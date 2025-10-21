from typing import Any

from tensorlake.applications import Function
from tensorlake.applications.request_context.request_context_base import (
    RequestContextBase,
)

from ..blob_store.blob_store import BLOBStore
from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import (
    Allocation,
    AllocationResult,
    FunctionRef,
    SerializedObjectInsideBLOB,
)


class AllocationRunner:
    def __init__(
        self,
        allocation: Allocation,
        request_context: RequestContextBase,
        function_ref: FunctionRef,
        function: Function,
        function_instance_arg: Any | None,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._allocation: Allocation = allocation
        self._request_context: RequestContextBase = request_context
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._function_instance_arg: Any | None = function_instance_arg
        self._blob_store: BLOBStore = blob_store
        self._logger = logger.bind(module=__name__)

    # TODO: Figure out inputs and outputs, we'd need some logic for publishing Allocation state.
    def run(self) -> None:
        pass
