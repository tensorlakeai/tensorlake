import contextvars
import hashlib
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from tensorlake.applications import (
    RETURN_WHEN,
    ApplicationValidationError,
    Function,
    FunctionProgress,
    Future,
    RequestError,
)
from tensorlake.applications.function.user_data_serializer import (
    function_output_serializer,
)
from tensorlake.applications.interface.awaitables import (
    Awaitable,
    AwaitableList,
)
from tensorlake.applications.metadata import (
    FunctionCallMetadata,
    ReduceOperationMetadata,
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
    PickleUserDataSerializer,
    UserDataSerializer,
)

from ..blob_store.blob_store import BLOBStore
from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationFunctionCall,
    AllocationFunctionCallResult,
    AllocationProgress,
    AllocationResult,
    AllocationState,
    ExecutionPlanUpdates,
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
from .contextvars import set_allocation_id_context_variable
from .download import download_function_arguments
from .result_helper import ResultHelper
from .sdk_algorithms import (
    awaitable_to_execution_plan_updates,
    deserialize_function_arguments,
    reconstruct_function_call_args,
    serialize_values_in_awaitable_tree,
    validate_and_deserialize_function_call_metadata,
    validate_user_object,
)
from .upload import (
    serialized_values_to_serialized_objects,
    upload_request_error,
    upload_serialized_objects_to_blob,
)
from .value import SerializedValue, Value


@dataclass
class _UserFutureInfo:
    # Original Future created by user code.
    user_future: Future
    # Not None if this user future is for an AwaitableList.
    collection: List[Future | Any] | None


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
        self._result_helper: ResultHelper = ResultHelper(
            function_ref=function_ref,
            function=function,
            metrics=self._request_context.metrics,
            logger=self._logger,
        )
        self._allocation_state: AllocationState = AllocationState(
            function_calls=[],
        )
        _update_allocation_state_hash(self._allocation_state)
        self._allocation_state_update_lock: threading.Condition = threading.Condition()
        self._allocation_thread: threading.Thread = threading.Thread(
            target=self._run_allocation_thread,
            daemon=True,
        )
        # Futures that were created by user code during this allocation.
        # Future ID -> _UserFutureInfo.
        self._user_futures: Dict[str, _UserFutureInfo] = {}
        # TODO: Figure out when to remove entries from _user_futures and function call entries
        # from allocation state. They should not take much memory cause they don't contain
        # actual values, just metadata, but still we don't want this to grow unboundedly.

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

    def run_futures_runtime_hook(
        self, futures: List[Future], start_delay: float | None
    ) -> None:
        # NB: This code is called from user function thread. User function code is blocked.
        # Right now we can only be called from the function thread, not any child threads that user
        # code might have created because contextvars are not propagated to child threads.
        # All exceptions raise here will be propagated to user code by design.
        for future in futures:
            future_info: _UserFutureInfo = _UserFutureInfo(
                user_future=future,
                collection=None,
            )
            if isinstance(future.awaitable, AwaitableList):
                future_info.collection = []
                for item in future.awaitable.items:
                    validate_user_object(
                        user_object=future.awaitable,
                        function_call_ids=self._user_futures.keys(),
                    )
                    if isinstance(item, Awaitable):
                        # Calls our hook recursively.
                        if start_delay is None:
                            future_info.collection.append(item.run())
                        else:
                            future_info.collection.append(
                                item.run_later(start_delay=start_delay)
                            )
                    else:
                        future_info.collection.append(item)
            else:
                validate_user_object(
                    user_object=future.awaitable,
                    function_call_ids=self._user_futures.keys(),
                )
                self._run_user_future(future)

            self._user_futures[future.awaitable.id] = future_info

    def _run_user_future(self, future: Future) -> None:
        serialized_values: Dict[str, SerializedValue] = {}
        awaitable_with_serialized_values: Awaitable = (
            serialize_values_in_awaitable_tree(
                user_object=future.awaitable,
                # value serializer is not going to be used because we're serializing a call tree here, not a value.
                value_serializer=PickleUserDataSerializer(),
                serialized_values=serialized_values,
            )
        )
        serialized_objects, blob_data = serialized_values_to_serialized_objects(
            serialized_values=serialized_values
        )
        args_blob: BLOB = self._get_new_output_blob(
            size=sum(len(data) for data in blob_data)
        )
        uploaded_serialized_objects, uploaded_args_blob = (
            upload_serialized_objects_to_blob(
                serialized_objects=serialized_objects,
                blob_data=blob_data,
                destination_blob=args_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        )
        awaitable_execution_plan_pb: ExecutionPlanUpdates
        awaitable_execution_plan_pb = awaitable_to_execution_plan_updates(
            awaitable=awaitable_with_serialized_values,
            uploaded_serialized_objects=uploaded_serialized_objects,
            # Output serializer name override is only applicable to tail calls.
            output_serializer_name_override=None,
            function_ref=self._function_ref,
            logger=self._logger,
        )

        self._add_function_call_to_allocation_state(
            execution_plan_updates=awaitable_execution_plan_pb,
            args_blob=uploaded_args_blob,
        )

    def wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> None:
        # NB: This code is called from user function thread. User function code is blocked.
        # Right now we can only be called from the function thread, not any child threads that user
        # code might have created because contextvars are not propagated to child threads.
        # All exceptions raise here will be propagated to user code by design.
        # TODO: Implement.
        pass

    def _get_new_output_blob(self, size: int) -> BLOB:
        """Returns new BLOB to upload function outputs to."""
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

    def _add_function_call_to_allocation_state(
        self, execution_plan_updates: ExecutionPlanUpdates, args_blob: BLOB
    ) -> None:
        with self._allocation_state_update_lock:
            self._allocation_state.function_calls.append(
                AllocationFunctionCall(
                    execution_plan_updates=execution_plan_updates,
                    args_blob=args_blob,
                )
            )
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
            self._update_allocation_state_result(self._result_helper.internal_error())
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
        ) = validate_and_deserialize_function_call_metadata(
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
            arg_values: Dict[str, Value] = deserialize_function_arguments(
                self._function, serialized_args
            )
        except BaseException as e:
            # This is internal FE code.
            return self._result_helper.from_user_exception(e)

        # This is internal FE code.
        args, kwargs = reconstruct_function_call_args(
            function_call_metadata=function_call_metadata,
            arg_values=arg_values,
            function_instance_arg=self._function_instance_arg,
        )

        # This is user code.
        try:
            output: Any = self._call_user_function(args, kwargs)
            # This is a very important check for our UX. We can await for AwaitableList
            # in user code but we cannot return it from a function as a tail call because
            # there's no Python code to reassemble the list from individual resolved awaitables.
            if isinstance(output, AwaitableList):
                raise ApplicationValidationError(
                    f"Function '{self._function_ref.function_name}' returned an AwaitableList {repr(output)}. "
                    "An AwaitableList can only be used as a function argument, not returned from it."
                )
        except RequestError as e:
            # This is user code.
            try:
                utf8_message: bytes = e.message.encode("utf-8")
            except BaseException:
                return self._result_helper.from_user_exception(e)

            # This is internal FE code.
            request_error_so, uploaded_outputs_blob = upload_request_error(
                utf8_message=utf8_message,
                destination_blob=self._allocation.inputs.request_error_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )
            return self._result_helper.from_request_error(
                request_error=e,
                request_error_output=request_error_so,
                uploaded_request_error_blob=uploaded_outputs_blob,
            )
        except BaseException as e:
            # This is internal FE code.
            return self._result_helper.from_user_exception(e)

        # This is user code.
        try:
            validate_user_object(
                user_object=output,
                function_call_ids=self._user_futures.keys(),
            )
            serialized_values: Dict[str, SerializedValue] = {}
            output: SerializedValue | Awaitable = serialize_values_in_awaitable_tree(
                user_object=output,
                value_serializer=output_serializer,
                serialized_values=serialized_values,
            )
        except BaseException as e:
            # This is internal FE code.
            return self._result_helper.from_user_exception(e)

        # This is internal FE code.
        serialized_objects, blob_data = serialized_values_to_serialized_objects(
            serialized_values=serialized_values
        )
        outputs_blob: BLOB = self._get_new_output_blob(
            size=sum(len(data) for data in blob_data)
        )
        uploaded_serialized_objects, uploaded_outputs_blob = (
            upload_serialized_objects_to_blob(
                serialized_objects=serialized_objects,
                blob_data=blob_data,
                destination_blob=outputs_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        )

        output_pb: SerializedObjectInsideBLOB | ExecutionPlanUpdates
        # This is user code.
        try:
            if isinstance(output, Awaitable):
                output_pb = awaitable_to_execution_plan_updates(
                    awaitable=output,
                    uploaded_serialized_objects=uploaded_serialized_objects,
                    output_serializer_name_override=output_serializer.name,
                    function_ref=self._function_ref,
                    logger=self._logger,
                )
            else:
                output_pb = uploaded_serialized_objects[output.metadata.id]
        except BaseException as e:
            return self._result_helper.from_user_exception(e)

        return self._result_helper.from_function_output(
            output=output_pb, uploaded_outputs_blob=uploaded_outputs_blob
        )

    def _call_user_function(self, args: List[Any], kwargs: Dict[str, Any]) -> Any:
        """Runs user function and returns its output."""
        context: contextvars.Context = contextvars.Context()
        return context.run(self._call_user_function_in_new_context, args, kwargs)

    def _call_user_function_in_new_context(
        self, args: List[Any], kwargs: Dict[str, Any]
    ) -> Any:
        # This function is executed in contextvars.Context of the Tensorlake Function call.
        set_current_request_context(self._request_context)
        set_allocation_id_context_variable(self._allocation.allocation_id)

        self._logger.info("running function")
        start_time = time.monotonic()

        try:
            return self._function._original_function(*args, **kwargs)
        finally:
            self._logger.info(
                "function finished",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
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
