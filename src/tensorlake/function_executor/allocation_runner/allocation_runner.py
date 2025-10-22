import contextvars
import hashlib
import threading
import time
from typing import Any, Dict, List, Set

from tensorlake.applications import (
    ApplicationValidationError,
    Function,
    FunctionProgress,
    RequestError,
)
from tensorlake.applications.function.application_call import (
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
from tensorlake.applications.interface.awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    Future,
    ReduceOperationAwaitable,
)
from tensorlake.applications.metadata import (
    CollectionMetadata,
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ReduceOperationMetadata,
    deserialize_metadata,
)
from tensorlake.applications.request_context.contextvar import (
    set_current_request_context,
)
from tensorlake.applications.request_context.request_context_base import (
    RequestContextBase,
)
from tensorlake.applications.request_context.request_metrics_recorder import (
    RequestMetricsRecorder,
)
from tensorlake.applications.user_data_serializer import (
    UserDataSerializer,
)

from ..blob_store.blob_store import BLOBStore
from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationFunctionCallResult,
    AllocationProgress,
    AllocationResult,
    AllocationState,
    FunctionRef,
    SerializedObjectInsideBLOB,
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
from .upload import upload_request_error, upload_serialized_values
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
        # TODO: Add function call IDs created by this allocation here.
        self._function_call_ids: Set[str] = set()

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
        ) = _validate_and_deserialize_function_call_metadata(
            serialized_function_call_metadata=self._allocation.inputs.function_call_metadata,
            serialized_args=serialized_args,
            function=self._function,
            logger=self._logger,
        )
        output_serializer_override: str | None = None
        if function_call_metadata is not None:
            output_serializer_override = (
                function_call_metadata.output_serializer_name_override
            )
        output_serializer: UserDataSerializer = function_output_serializer(
            self._function,
            output_serializer_override=output_serializer_override,
        )

        # This is user code.
        try:
            arg_values: Dict[str, Value] = _deserialize_function_arguments(
                self._function, serialized_args
            )
        except BaseException as e:
            # TODO: Log this using print exception to show the error to user.
            #
            # TODO: Implement serialization of function exception as customer code execution.
            # Handle any exceptions raised in customer code and convert them into proper AllocationResult.
            return self._response_helper.from_generic_function_exception(e)

        # This is internal FE code.
        args, kwargs = self._reconstruct_function_call_args(
            function_call_metadata=function_call_metadata,
            arg_values=arg_values,
        )

        # This is user code.
        try:
            output: Any = self._call_user_function(args, kwargs)
            serialized_values: Dict[str, SerializedValue] = {}
            output: SerializedValue | Awaitable = _process_function_output(
                output=output,
                function_name=self._function_ref.function_name,
                function_output_serializer=output_serializer,
                function_call_ids=self._function_call_ids,
                serialized_values=serialized_values,
            )
        except RequestError as e:
            # This is internal FE code.
            # TODO: Log this using print exception to show the error to user.
            #
            # TODO: Implement serialization of function exception as customer code execution.
            # Handle any exceptions raised in customer code and convert them into proper AllocationResult.
            request_error_so, uploaded_blob = upload_request_error(
                message=e.message,
                destination_blob=self._allocation.inputs.request_error_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )
            request_error_so: SerializedObjectInsideBLOB
            uploaded_blob: BLOB
            return self._response_helper.from_request_error(
                request_error_so, uploaded_blob
            )
        except BaseException as e:
            # This is internal FE code.
            # TODO: Log this using print exception to show the error to user.
            #
            # TODO: Implement serialization of function exception as customer code execution.
            # Handle any exceptions raised in customer code and convert them into proper AllocationResult.
            return self._response_helper.from_generic_function_exception(e)

        # This is internal FE code.
        uploaded_serialized_objects, uploaded_blob = upload_serialized_values(
            serialized_values=serialized_values,
            destination_blob=self._allocation.inputs.function_outputs_blob,
            blob_store=self._blob_store,
            logger=self._logger,
        )
        uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB]
        uploaded_blob: BLOB

        return self._response_helper.from_function_output(
            output=output, uploaded_serialized_objects=uploaded_serialized_objects
        )

    def _reconstruct_function_call_args(
        self,
        function_call_metadata: FunctionCallMetadata | ReduceOperationMetadata | None,
        arg_values: Dict[str, Value],
    ) -> tuple[List[Any], Dict[str, Any]]:
        if function_call_metadata is None:
            # Application function call created by Server.
            args: List[Any] = [arg_values["application_payload"]]
            kwargs: Dict[str, Any] = {}
        else:
            # SDK-created function call.
            args, kwargs = _reconstruct_sdk_function_call_args(
                function_call_metadata=function_call_metadata,
                arg_values=arg_values,
            )

        if self._function_instance_arg is not None:
            set_self_arg(args, self._function_instance_arg)

        return args, kwargs

    def _call_user_function(self, args: List[Any], kwargs: Dict[str, Any]) -> Any:
        """Runs user function and returns its output."""
        context: contextvars.Context = contextvars.Context()
        return context.run(self._call_user_function_in_new_context, args, kwargs)

    def _call_user_function_in_new_context(
        self, args: List[Any], kwargs: Dict[str, Any]
    ) -> Any:
        # This function is executed in contextvars.Context of the Tensorlake Function call.
        set_current_request_context(self._request_context)

        self._logger.info("running function")
        start_time = time.monotonic()

        try:
            return self._function._original_function(*args, **kwargs)
        finally:
            self._logger.info(
                "function finished",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )


def _process_function_output(
    output: Any,
    function_name: str,
    serializer: UserDataSerializer,
    function_call_ids: Set[str],
    serialized_values: Dict[str, SerializedValue],
) -> SerializedValue | Awaitable:
    """Validates the function output and replaces each value with a SerializedValue.

    This results in Awaitables tree being returned with each value being SerializedValue.
    Updates serialized_values with each SerializedValue created from concrete values.
    serialized_values is mapping from value ID to SerializedValue.
    """
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    if not isinstance(output, (Awaitable, Future)):
        data, metadata = serialize_value(output, serializer=serializer)
        serialized_values[metadata.id] = SerializedValue(
            metadata=metadata,
            data=data,
            content_type=metadata.content_type,
        )
        return serialized_values[metadata.id]

    if isinstance(output, Future):
        raise ApplicationValidationError(
            f"Invalid argument: cannot run Future {repr(output)}, "
            "please pass an Awaitable or a concrete value."
        )

    awaitable: Awaitable
    if awaitable.id in function_call_ids:
        raise ApplicationValidationError(
            f"Invalid argument: {repr(awaitable)} is an Awaitable with already running Future, "
            "only not running Awaitable can be passed as function argument or returned from a function."
        )

    # This is a very important check for our UX. We can await for AwaitableList
    # in user code but we cannot return it from a function as a tail call because
    # there's no Python code to reassemble the list from individual resolved awaitables.
    if isinstance(output, AwaitableList):
        raise ApplicationValidationError(
            f"Function '{function_name}' returned an AwaitableList {repr(output)}. "
            "An AwaitableList can only be used as a function argument, not returned from it."
        )
    elif isinstance(awaitable, ReduceOperationAwaitable):
        awaitable: ReduceOperationAwaitable
        for index, item in enumerate(list(awaitable.inputs)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.inputs[index] = _process_function_output(
                output=item,
                function_name=function_name,
                serializer=serializer,
                function_call_ids=function_call_ids,
            )
        return awaitable
    elif isinstance(awaitable, FunctionCallAwaitable):
        awaitable: FunctionCallAwaitable
        for index, arg in enumerate(list(awaitable.args)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.args[index] = _process_function_output(
                output=arg,
                function_name=function_name,
                serializer=serializer,
                function_call_ids=function_call_ids,
            )
        for kwarg_name, kwarg_value in dict(awaitable.kwargs).items():
            # Iterating over dict copy to allow modifying the original list.
            awaitable.kwargs[kwarg_name] = _process_function_output(
                output=kwarg_value,
                function_name=function_name,
                serializer=serializer,
                function_call_ids=function_call_ids,
            )
        return awaitable
    else:
        raise ApplicationValidationError(
            f"Unexpected type of awaitable returned from function: {type(awaitable)}"
        )


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
    for ix, serialized_arg in enumerate(serialized_args):
        if serialized_arg.metadata is None:
            # Application payload argument. It's allready validated to be only one argument.
            args["application_payload"] = Value(
                metadata=None,
                object=deserialize_application_function_call_payload(
                    application=function,
                    payload=serialized_arg.data,
                    payload_content_type=serialized_arg.content_type,
                ),
                input_ix=ix,
            )
        else:
            args[serialized_arg.metadata.id] = Value(
                metadata=serialized_arg.metadata,
                object=deserialize_value(
                    serialized_value=serialized_arg.data,
                    metadata=serialized_arg.metadata,
                ),
                input_ix=ix,
            )

    return args


def _validate_and_deserialize_function_call_metadata(
    serialized_function_call_metadata: bytes,
    serialized_args: List[SerializedValue],
    function: Function,
    logger: FunctionExecutorLogger,
) -> FunctionCallMetadata | ReduceOperationMetadata | None:
    if len(serialized_function_call_metadata) > 0:
        # Function call created by SDK.
        for serialized_arg in serialized_args:
            if serialized_arg.metadata is None:
                logger.error(
                    "function argument is missing metadata",
                )
                raise ValueError("Function argument is missing metadata.")

        function_call_metadata = deserialize_metadata(serialized_function_call_metadata)
        if not isinstance(
            function_call_metadata, (FunctionCallMetadata, ReduceOperationMetadata)
        ):
            logger.error(
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
    else:
        # Application function call created by Server.
        if len(serialized_args) != 1:
            logger.error(
                "expected exactly one function argument for server-created application function call",
                num_args=len(serialized_args),
            )
            raise ValueError(
                f"Expected exactly one function argument for server-created application "
                f"function call, got {len(serialized_args)}."
            )

        if function._application_config is None:
            raise ValueError("Non-application function was called without SDK metadata")


def _reconstruct_sdk_function_call_args(
    function_call_metadata: FunctionCallMetadata | ReduceOperationMetadata,
    arg_values: Dict[str, Value],
) -> tuple[List[Any], Dict[str, Any]]:
    if isinstance(function_call_metadata, FunctionCallMetadata):
        args: List[Any] = []
        kwargs: Dict[str, Any] = {}

        for arg_metadata in function_call_metadata.args:
            args.append(_reconstruct_function_arg_value(arg_metadata, arg_values))
        for kwarg_key, kwarg_metadata in function_call_metadata.kwargs.items():
            kwargs[kwarg_key] = _reconstruct_function_arg_value(
                kwarg_metadata, arg_values
            )
        return args, kwargs
    elif isinstance(function_call_metadata, ReduceOperationMetadata):
        args: List[Value] = list(arg_values.values())
        # Server provides accumulator first, item second
        args.sort(key=lambda arg: arg.input_ix)
        return args, {}


def _reconstruct_function_arg_value(
    arg_metadata: FunctionCallArgumentMetadata, arg_values: Dict[str, Value]
) -> Any:
    """Reconstructs the original value from function arg metadata."""
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    if arg_metadata.collection is None:
        return arg_values[arg_metadata.value_id].object
    else:
        return _reconstruct_collection_value(arg_metadata.collection, arg_values)


def _reconstruct_collection_value(
    collection_metadata: CollectionMetadata, arg_values: Dict[str, Value]
) -> List[Any]:
    """Reconstructs the original values from the supplied collection metadata."""
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    values: List[Any] = []
    for item in collection_metadata.items:
        if item.collection is None:
            values.append(arg_values[item.value_id].object)
        else:
            values.append(_reconstruct_collection_value(item.collection, arg_values))
    return values
