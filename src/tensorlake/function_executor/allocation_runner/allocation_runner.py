import contextvars
import hashlib
import threading
import time
from typing import Any, Dict, List

from tensorlake.applications import Function, FunctionProgress
from tensorlake.applications.function.application_call import (
    application_function_call,
    deserialize_application_function_call_payload,
)
from tensorlake.applications.function.function_call import (
    set_self_arg,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value,
    function_input_serializer,
    function_output_serializer,
    serialize_value,
)
from tensorlake.applications.interface.awaitables import FunctionCallAwaitable
from tensorlake.applications.metadata import (
    FunctionCallMetadata,
    ReduceOperationMetadata,
    deserialize_metadata,
)
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
from ..user_events import (
    AllocationEventDetails,
    log_user_event_allocations_finished,
    log_user_event_allocations_started,
)
from .download import download_function_arguments
from .result_helper import ResultHelper
from .value import SerializedValue, Value


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

        self._allocation_event_details: AllocationEventDetails = AllocationEventDetails(
            namespace=self._function_ref.namespace,
            application_name=self._function_ref.application_name,
            application_version=self._function_ref.application_version,
            function_name=self._function_ref.function_name,
            request_id=self._allocation.request_id,
            function_call_id=self._allocation.function_call_id,
            allocation_id=self._allocation.allocation_id,
        )

        # Extracted from function call metadata later.
        self._function_output_serializer_override: str | None = None
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
        """Delivers function call result to the allocation.

        Caller should ensure that the function call belongs to this allocation
        and that the allocation is not finished.
        """
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
            log_user_event_allocations_started([self._allocation_event_details])
            result: AllocationResult = self._run_allocation()
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
            log_user_event_allocations_finished([self._allocation_event_details])
            self._finished = True

    def _run_allocation(self) -> AllocationResult:
        # We need to be very careful who's code we're running here. Exceptions raised in customer
        # code should be caught here and converted into proper AllocationResult indicating customer code failure.
        # Exceptions in our internal FE code are just raised here and handled by caller.

        # This is internal FE code.
        serialized_args: List[SerializedValue] = download_function_arguments(
            self._allocation, self._blob_store, self._logger
        )
        function_call_metadata: (
            FunctionCallMetadata | ReduceOperationMetadata | None
        ) = self._fetch_validated_function_call_metadata(serialized_args)

        # This is user code.
        try:
            args: Dict[str, Value] = _deserialize_function_arguments(
                self._function, serialized_args
            )
        except BaseException as e:
            # TODO: Log this using print exception to show the error to user.
            #
            # TODO: Implement serialization of function exception as customer code execution.
            # Handle any exceptions raised in customer code and convert them into proper AllocationResult.
            return self._response_helper.from_function_exception(e)

        # This is internal FE code.
        function_call: FunctionCallAwaitable = self._reconstruct_function_call(
            function_call_metadata=function_call_metadata,
            args=args,
        )

        # This is user code
        try:
            self._run_user_function(
                function_call_metadata=function_call_metadata,
                serialized_args=serialized_args,
            )
        except BaseException as e:
            # TODO: Log this using print exception to show the error to user.
            #
            # TODO: Implement serialization of function exception as customer code execution.
            # Handle any exceptions raised in customer code and convert them into proper AllocationResult.
            return self._response_helper.from_function_exception(e)

        return self._handle_allocation_output(output)

    def _fetch_validated_function_call_metadata(
        self, serialized_args: List[SerializedValue]
    ) -> FunctionCallMetadata | ReduceOperationMetadata | None:
        if len(self._allocation.inputs.function_call_metadata) > 0:
            # Function call created by SDK.
            for serialized_arg in serialized_args:
                if serialized_arg.metadata is None:
                    self._logger.error(
                        "function argument is missing metadata",
                    )
                    raise ValueError("Function argument is missing metadata.")

            function_call_metadata = deserialize_metadata(
                self._allocation.inputs.function_call_metadata
            )
            if not isinstance(
                function_call_metadata, (FunctionCallMetadata, ReduceOperationMetadata)
            ):
                self._logger.error(
                    "unsupported function call metadata type",
                    metadata_type=type(function_call_metadata).__name__,
                )
                raise ValueError(
                    f"Unsupported function call metadata type: {type(function_call_metadata).__name__}"
                )

            if (
                isinstance(function_call_metadata, ReduceOperationMetadata)
                and len(serialized_args) != 2
            ):
                raise ValueError(
                    f"Expected 2 arguments for reducer function call, got {len(serialized_args)}"
                )

            self._function_output_serializer_override = (
                function_call_metadata.output_serializer_name_override
            )
        else:
            # Application function call created by Server.
            if len(serialized_args) != 1:
                self._logger.error(
                    "expected exactly one function argument for server-created application function call",
                    num_args=len(serialized_args),
                )
                raise ValueError(
                    f"Expected exactly one function argument for server-created application "
                    f"function call, got {len(serialized_args)}."
                )

            if self._function._application_config is None:
                raise ValueError(
                    "Non-application function was called without SDK metadata"
                )

    def _reconstruct_function_call(
        self,
        function_call_metadata: FunctionCallMetadata | ReduceOperationMetadata | None,
        args: Dict[str, Value],
    ) -> FunctionCallAwaitable:
        if function_call_metadata is None:
            # Application function call created by Server.
            function_call: FunctionCallAwaitable = (
                _reconstruct_application_function_call(
                    function=self._function,
                    args=args,
                )
            )
        else:
            # SDK-created function call.
            function_call: FunctionCallAwaitable = _reconstruct_sdk_function_call(
                function=self._function,
                function_call_metadata=function_call_metadata,
                args=args,
            )

        if self._function_instance_arg is not None:
            set_self_arg(function_call.args, self._function_instance_arg)

    def _run_user_function(self, function_call: FunctionCallAwaitable) -> Any:
        """Runs user function and returns its output."""

        context: contextvars.Context = contextvars.Context()
        # TODO: Serialize output in customer code context.
        # TODO: Figure out what to return.
        output: Any = context.run(self._run_user_function_in_new_context, function_call)

    def _run_user_function_in_new_context(
        self, function_call: FunctionCallAwaitable
    ) -> Any:
        pass

    def _handle_allocation_output(self, output: Any) -> AllocationResult:
        # This is internal FE code.
        # TODO: upload the output and etc.
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


def _deserialize_function_arguments(
    function: Function, serialized_args: List[SerializedValue]
) -> Dict[str, Value]:
    args: Dict[str, Value] = {}
    for serialized_arg in serialized_args:
        if serialized_arg.metadata is None:
            # Application payload argument. It's allready validated to be only one argument.
            args["application_payload"] = deserialize_application_function_call_payload(
                application=function,
                payload=serialized_arg.data,
                payload_content_type=serialized_arg.content_type,
            )
        else:
            args[serialized_arg.metadata.id] = deserialize_value(
                serialized_arg.data, serialized_arg.metadata
            )

    return args


def _reconstruct_application_function_call(
    application: Function, args: Dict[str, Value]
) -> FunctionCallAwaitable:
    return application_function_call(
        application=application,
        payload=args["application_payload"],
    )


def _reconstruct_sdk_function_call(
    function: Function,
    function_call_metadata: FunctionCallMetadata | ReduceOperationMetadata,
    args: Dict[str, Value],
) -> FunctionCallAwaitable:
    if isinstance(function_call_metadata, FunctionCallMetadata):
        return RegularFunctionCallNode.from_serialized(
            self._function_ref.function_name,
            node_metadata.metadata,
            downloaded_args,
        ).to_regular_function_call()
    elif isinstance(function_call_metadata, ReduceOperationMetadata):
        accumulator: Any = serialized_args[0].to_value()
        item: Any = serialized_args[1].to_value()
        return function.awaitable(accumulator, item)


def _reconstruct_function_arg_value(
    self, arg_metadata: FunctionCallArgumentMetadata
) -> Any:
    """Reconstructs the original value from function arg metadata."""
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    if arg_metadata.collection is None:
        return _deserialize_blob_value(self._blob_store.get(arg_metadata.value_id))
    else:
        return self._reconstruct_collection_value(arg_metadata.collection)


def _reconstruct_collection_value(
    self, collection_metadata: CollectionMetadata
) -> List[Any]:
    """Reconstructs the original values from the supplied collection metadata."""
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    values: List[Any] = []
    for item in collection_metadata.items:
        if item.collection is None:
            values.append(_deserialize_blob_value(self._blob_store.get(item.value_id)))
        else:
            values.append(self._reconstruct_collection_value(item.collection))
    return values
