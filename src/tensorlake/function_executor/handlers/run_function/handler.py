import pickle
import time
from typing import Any, List

from tensorlake.workflows.ast.function_call_node import (
    RegularFunctionCallMetadata,
    RegularFunctionCallNode,
)
from tensorlake.workflows.ast.reducer_call_node import ReducerFunctionCallMetadata
from tensorlake.workflows.ast.value_node import ValueNode
from tensorlake.workflows.function.api_call import (
    api_function_call_with_serialized_payload,
)
from tensorlake.workflows.function.reducer_call import reducer_function_call
from tensorlake.workflows.interface.function import Function
from tensorlake.workflows.interface.function_call import RegularFunctionCall
from tensorlake.workflows.interface.request_context import RequestContext

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import (
    Allocation,
    AllocationResult,
    FunctionRef,
)
from ...user_events import (
    AllocationEventDetails,
    log_user_event_allocations_finished,
    log_user_event_allocations_started,
)
from .download import download_api_function_payload_bytes, download_function_arguments
from .response_helper import ResponseHelper


class Handler:
    def __init__(
        self,
        allocation: Allocation,
        request_context: RequestContext,
        function_ref: FunctionRef,
        function: Function,
        function_instance_arg: Any | None,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._allocation: Allocation = allocation
        self._request_context: RequestContext = request_context
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._function_instance_arg: Any | None = function_instance_arg
        self._blob_store: BLOBStore = blob_store
        self._logger = logger.bind(module=__name__)
        self._response_helper = ResponseHelper(
            function=function,
            inputs=allocation.inputs,
            request_state=request_context.state,
            blob_store=blob_store,
            logger=self._logger,
        )

    def run(self) -> AllocationResult:
        """Runs the allocation.

        Raises an exception if our own code failed, customer function failure doesn't result in any exception.
        """
        event_details: List[AllocationEventDetails] = [
            AllocationEventDetails(
                namespace=self._function_ref.namespace,
                application_name=self._function_ref.application_name,
                application_version=self._function_ref.application_version,
                function_name=self._function_ref.function_name,
                request_id=self._allocation.request_id,
                task_id=self._allocation.task_id,
                allocation_id=self._allocation.allocation_id,
            )
        ]
        log_user_event_allocations_started(event_details)
        try:
            return self._run()
        finally:
            log_user_event_allocations_finished(event_details)

    def _run(self) -> AllocationResult:
        fe_log_start: int = self._logger.end()
        function_call: RegularFunctionCall = self._reconstruct_function_call()

        try:
            output: Any = self._call(function_call)
        except BaseException as e:
            return self._response_helper.from_function_exception(
                exception=e,
                fe_log_start=fe_log_start,
                metrics=None,
            )

        return self._response_helper.from_function_output(
            output=output, fe_log_start=fe_log_start
        )

    def _reconstruct_function_call(self) -> RegularFunctionCall:
        if self._allocation.inputs.HasField("function_call_metadata"):
            downloaded_args: List[ValueNode] = download_function_arguments(
                self._allocation, self._blob_store, self._logger
            )
            metadata: RegularFunctionCallMetadata | ReducerFunctionCallMetadata = (
                pickle.loads(self._allocation.inputs.function_call_metadata)
            )
            if isinstance(metadata, RegularFunctionCallMetadata):
                # FIXME: We're deserializing metadata twice here because RegularFunctionCallNode
                # doesn't have an option to get created from serialized metadata.
                return RegularFunctionCallNode.from_serialized(
                    self._function_ref.function_name,
                    self._allocation.inputs.function_call_metadata,
                    downloaded_args,
                ).to_regular_function_call()
            elif isinstance(metadata, ReducerFunctionCallMetadata):
                if len(downloaded_args) != 2:
                    raise ValueError(
                        f"Expected 2 arguments for reducer function call, got {len(downloaded_args)}"
                    )
                accumulator: Any = downloaded_args[0].to_value()
                item: Any = downloaded_args[1].to_value()
                return reducer_function_call(self._function, accumulator, item)
            else:
                raise ValueError(
                    f"Received function call with unexpected metadata type: {metadata}"
                )
        else:
            if self._function.api_config is None:
                raise ValueError("Received non API function call without SDK metadata")

            payload: bytes = download_api_function_payload_bytes(
                self._allocation, self._blob_store, self._logger
            )
            return api_function_call_with_serialized_payload(self._function, payload)

    def _call(self, function_call: RegularFunctionCall) -> Any:
        self._logger.info("running function")
        start_time = time.monotonic()

        try:
            return self._function.original_function(
                *function_call.args, **function_call.kwargs
            )
        finally:
            self._logger.info(
                "function finished",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )
