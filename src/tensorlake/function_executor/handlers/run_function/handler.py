import contextvars
import time
from typing import Any, List

from tensorlake.applications.ast import (
    ReducerFunctionCallMetadata,
    RegularFunctionCallMetadata,
    RegularFunctionCallNode,
    ValueNode,
)
from tensorlake.applications.function.application_call import (
    application_function_call_with_serialized_payload,
)
from tensorlake.applications.function.function_call import (
    set_self_arg,
)
from tensorlake.applications.function.reducer_call import reducer_function_call
from tensorlake.applications.interface.function import Function
from tensorlake.applications.interface.function_call import RegularFunctionCall
from tensorlake.applications.request_context.contextvar import (
    set_current_request_context,
)
from tensorlake.applications.request_context.request_context_base import (
    RequestContextBase,
)

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import (
    Allocation,
    AllocationResult,
    FunctionRef,
    SerializedObjectInsideBLOB,
)
from ...user_events import (
    AllocationEventDetails,
    log_user_event_allocations_finished,
    log_user_event_allocations_started,
)
from .download import (
    download_application_function_payload_bytes,
    download_function_arguments,
)
from .function_call_node_metadata import FunctionCallNodeMetadata, FunctionCallType
from .response_helper import ResponseHelper


class Handler:
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
        self._response_helper = ResponseHelper(
            function_ref=function_ref,
            function=function,
            inputs=allocation.inputs,
            request_metrics=request_context.metrics,
            blob_store=blob_store,
            logger=self._logger,
        )
        # Extracted from function call metadata later.
        self._function_output_serializer_override: str | None = None

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
        function_call: RegularFunctionCall = self._reconstruct_function_call()
        if self._function_instance_arg is not None:
            set_self_arg(function_call, self._function_instance_arg)

        context: contextvars.Context = contextvars.Context()
        try:
            output: Any = context.run(self._call_with_context, function_call)
        except BaseException as e:
            return self._response_helper.from_function_exception(e)

        return self._response_helper.from_function_output(
            output=output,
            output_serializer_override=self._function_output_serializer_override,
        )

    def _reconstruct_function_call(self) -> RegularFunctionCall:
        if len(self._allocation.inputs.function_call_metadata) > 0:
            downloaded_args: List[ValueNode] = download_function_arguments(
                self._allocation, self._blob_store, self._logger
            )
            node_metadata: FunctionCallNodeMetadata = (
                FunctionCallNodeMetadata.deserialize(
                    self._allocation.inputs.function_call_metadata
                )
            )
            if node_metadata.type == FunctionCallType.REGULAR:
                call_metadata: RegularFunctionCallMetadata = (
                    RegularFunctionCallMetadata.deserialize(node_metadata.metadata)
                )
                self._function_output_serializer_override = call_metadata.oso
                return RegularFunctionCallNode.from_serialized(
                    node_metadata.nid,
                    self._function_ref.function_name,
                    node_metadata.metadata,
                    downloaded_args,
                ).to_regular_function_call()
            elif node_metadata.type == FunctionCallType.REDUCER:
                if len(downloaded_args) != 2:
                    raise ValueError(
                        f"Expected 2 arguments for reducer function call, got {len(downloaded_args)}"
                    )
                call_metadata: ReducerFunctionCallMetadata = (
                    ReducerFunctionCallMetadata.deserialize(node_metadata.metadata)
                )
                self._function_output_serializer_override = call_metadata.oso
                accumulator: Any = downloaded_args[0].to_value()
                item: Any = downloaded_args[1].to_value()
                return reducer_function_call(self._function, accumulator, item)
            else:
                raise ValueError(
                    f"Received function call with unexpected function call node metadata type: {node_metadata.type}"
                )
        else:
            if self._function.application_config is None:
                raise ValueError(
                    "Non-application function was called without SDK metadata"
                )

            payload: bytes = download_application_function_payload_bytes(
                self._allocation, self._blob_store, self._logger
            )
            payload_arg_so: SerializedObjectInsideBLOB = self._allocation.inputs.args[0]
            return application_function_call_with_serialized_payload(
                application=self._function,
                payload=payload,
                payload_content_type=payload_arg_so.manifest.content_type or "",
            )

    def _call_with_context(self, function_call: RegularFunctionCall) -> Any:
        # This function is executed in contextvars.Context of the Tensorlake Function call.
        set_current_request_context(self._request_context)
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
