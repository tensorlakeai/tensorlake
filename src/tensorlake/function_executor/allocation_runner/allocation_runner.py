import contextvars
import datetime
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from google.protobuf.timestamp_pb2 import Timestamp

from tensorlake.applications import (
    RETURN_WHEN,
    ApplicationValidationError,
    Function,
    FunctionCallFailure,
    FunctionProgress,
    Future,
    RequestError,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value,
    function_output_serializer,
)
from tensorlake.applications.interface.awaitables import (
    Awaitable,
    AwaitableList,
    request_scoped_id,
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
    AllocationFunctionCallResult,
    AllocationOutcomeCode,
    AllocationResult,
    AllocationState,
    AllocationUpdate,
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
from .allocation_state_wrapper import AllocationStateWrapper
from .contextvars import set_allocation_id_context_variable
from .download import download_function_arguments, download_serialized_objects
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
    # Not None when the future is completed.
    # Requires a watcher setup.
    result: AllocationFunctionCallResult | None
    # Set only once after the result is set.
    result_available: threading.Event


@dataclass
class _OutputBLOBRequestInfo:
    # Not None once the BLOB is ready to be used.
    blob: BLOB | None
    # Set only once after the BLOB is set.
    blob_available: threading.Event


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
        self._allocation_state: AllocationStateWrapper = AllocationStateWrapper()
        self._allocation_thread: threading.Thread = threading.Thread(
            target=self._run_allocation_thread,
            daemon=True,
        )
        # Futures that were created by user code during this allocation.
        # Future ID -> _UserFutureInfo.
        self._user_futures: Dict[str, _UserFutureInfo] = {}
        # BLOB ID -> _OutputBLOBRequestInfo.
        self._output_blob_requests: Dict[str, _OutputBLOBRequestInfo] = {}

    def wait_allocation_state_update(
        self, last_seen_hash: str | None
    ) -> AllocationState:
        """Returns copy of the current allocation state when it's updated."""
        return self._allocation_state.wait_for_update(last_seen_hash)

    def run(self) -> None:
        """Runs the allocation in a separate thread.

        When the allocation is finished, sets it .result field.
        """
        self._allocation_thread.start()

    # finished() and is_terminal_state() need to be consistent with each other.
    # So once we return a terminal state to client and it calls delete_allocation,
    # finished() must return True.
    @property
    def finished(self) -> bool:
        return self._allocation_state.has_result()

    @classmethod
    def is_terminal_state(cls, state: AllocationState) -> bool:
        return state.HasField("result")

    def deliver_allocation_update(self, update: AllocationUpdate) -> None:
        # No need for any locks because we never block here so we hold GIL non stop.
        if update.HasField("function_call_result"):
            function_call_id: str = update.function_call_result.function_call_id
            if function_call_id not in self._user_futures:
                self._logger.error(
                    "received function call result for unknown future",
                    function_call_id=function_call_id,
                )
                return
            future_info: _UserFutureInfo = self._user_futures[function_call_id]
            future_info.result = update.function_call_result
            future_info.result_available.set()
        elif update.HasField("output_blob"):
            blob_id: str = update.output_blob.id
            if blob_id not in self._output_blob_requests:
                self._logger.error(
                    "received output blob update for unknown blob request",
                    blob_id=blob_id,
                )
                return

            blob_request_info: _OutputBLOBRequestInfo = self._output_blob_requests[
                blob_id
            ]
            blob_request_info.blob = update.output_blob
            blob_request_info.blob_available.set()
        else:
            self._logger.error(
                "received empty allocation update",
                update=str(update),
            )

    def run_futures_runtime_hook(
        self, futures: List[Future], start_delay: float | None
    ) -> None:
        # NB: This code is called from user function thread. User function code is blocked.
        # Right now we can only be called from the function thread, not any child threads that user
        # code might have created because contextvars are not propagated to child threads.
        # All exceptions raised here will be propagated to user code by design.
        for future in futures:
            future_info: _UserFutureInfo = _UserFutureInfo(
                user_future=future,
                collection=None,
                result=None,
                result_available=threading.Event(),
            )
            if isinstance(future.awaitable, AwaitableList):
                future_info.collection = []
                for item in future.awaitable.items:
                    validate_user_object(
                        user_object=item,
                        function_call_ids=self._user_futures.keys(),
                    )
                    if isinstance(item, Awaitable):
                        # Calls our hook recursively.
                        # Also creates _FutureInfo entries for nested futures.
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
                self._run_user_future(future, start_delay)

            self._user_futures[future.awaitable.id] = future_info

    def wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        """Doesn't raise any exceptions. All exceptions are set on individual futures."""
        # NB: This code is called from user function thread. User function code is blocked.
        # Right now we can only be called from the function thread, not any child threads that user
        # code might have created because contextvars are not propagated to child threads.
        # All exceptions raise here will be propagated to user code by design.
        deadline: float | None = (
            time.monotonic() + timeout if timeout is not None else None
        )
        # NB: The futures order in these lists should be the original order (like stable sort).
        done: List[Future] = []
        not_done: List[Future] = []

        # FIXME: When FIRST_COMPLETED or FIRST_FAILURE is used we have to wait for all the
        # future in parallel instead of serially. Without this, if the first future takes
        # a long time to complete, but the second one completes quickly, we still wait
        # for the first one to complete before checking the second one. This is not what customers expect.
        for future in futures:
            remaining_timeout: float | None = (
                deadline - time.monotonic() if deadline is not None else None
            )
            if remaining_timeout is not None and remaining_timeout <= 0:
                break

            try:
                self._wait_future_completion(future, remaining_timeout)
            except BaseException as e:
                # Something went wrong while waiting for the future.
                self._logger.error(
                    "Unexpected error while waiting for child future completion",
                    child_future_id=future.awaitable.id,
                    exc_info=e,
                )
                future.set_exception(e)

            if future.done():
                done.append(future)
            else:
                not_done.append(future)

            if return_when == RETURN_WHEN.FIRST_COMPLETED:
                if len(done) > 0:
                    break
            elif return_when == RETURN_WHEN.FIRST_FAILURE:
                if future.exception is not None:
                    break
            # else ALL_COMPLETED

        for future in futures:
            if future not in done and future not in not_done:
                not_done.append(future)

        return done, not_done

    def _wait_future_completion(self, future: Future, timeout: float | None) -> None:
        """Wait for the completion of the future and sets its result or exception if didn't timeout.

        Raises Exception if something unexpected went wrong while waiting for the future.
        Normally all exceptions are set on the future itself.
        """
        start_time: float = time.monotonic()
        self._logger.info(
            "waiting for child future completion",
            child_future_id=future.awaitable.id,
        )

        deadline: float | None = (
            time.monotonic() + timeout if timeout is not None else None
        )
        future_info: _UserFutureInfo = self._user_futures[future.awaitable.id]
        if future_info.collection is not None:
            self._wait_future_list_completion(future_info, deadline)
            return

        function_call_id: str = future_info.user_future.awaitable.id
        function_call_watcher_id: str = request_scoped_id()
        self._allocation_state.add_function_call_watcher(
            watcher_id=function_call_watcher_id, function_call_id=function_call_id
        )

        result_wait_timeout: float | None = (
            deadline - time.monotonic() if deadline is not None else None
        )
        if future_info.result_available.wait(timeout=result_wait_timeout):
            result: AllocationFunctionCallResult = future_info.result
            if (
                result.outcome_code
                == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS
            ):
                serialized_output: SerializedValue = download_serialized_objects(
                    serialized_objects=[result.value_output],
                    serialized_object_blobs=[result.value_blob],
                    blob_store=self._blob_store,
                    logger=self._logger,
                )[0]
                if serialized_output.metadata is None:
                    raise ValueError(
                        "Function Call output SerializedValue is missing metadata."
                    )
                output: Any = deserialize_value(
                    serialized_output.data, serialized_output.metadata
                )
                future.set_result(output)
            elif (
                result.outcome_code
                == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
            ):
                if result.HasField("request_error_output"):
                    serialized_request_error: SerializedValue = (
                        download_serialized_objects(
                            serialized_objects=[result.request_error_output],
                            serialized_object_blobs=[result.request_error_blob],
                            blob_store=self._blob_store,
                            logger=self._logger,
                        )[0]
                    )
                    future.set_exception(
                        RequestError(
                            message=serialized_request_error.data.decode("utf-8")
                        )
                    )
                else:
                    # FIXME: Function call arguments can be huge, we should limit the amount of characters here.
                    future.set_exception(
                        FunctionCallFailure(
                            f"Function call {repr(future.awaitable)} failed"
                        )
                    )
            else:
                # Unknown outcome code.
                # FIXME: Function call arguments can be huge, we should limit the amount of characters here.
                future.set_exception(
                    FunctionCallFailure(
                        f"Function call {repr(future.awaitable)} failed"
                    )
                )
        # else timeout, no result or error

        self._allocation_state.delete_function_call_watcher(
            watcher_id=function_call_watcher_id
        )
        self._allocation_state.delete_function_call(function_call_id=function_call_id)
        self._logger.info(
            "child future completed",
            child_future_id=future.awaitable.id,
            duration_sec=f"{time.monotonic() - start_time:.3f}",
            success=future.exception is None,
        )

    def _wait_future_list_completion(
        self, future_info: _UserFutureInfo, deadline: float | None
    ) -> None:
        """Wait for the completion of the future representing an AwaitableList and sets its result or exception if didn't timeout."""
        # Reconstruct the original collection out of individual futures.
        future: Future = future_info.user_future
        collection: List[Any] = []
        exception: BaseException | None = None
        for item in future_info.collection:
            item_timeout: float | None = (
                deadline - time.monotonic() if deadline is not None else None
            )
            if item_timeout is not None and item_timeout <= 0:
                break
            self._wait_future_completion(item, timeout=item_timeout)
            if item.exception is None:
                collection.append(item.result())
            else:
                exception = item.exception
                break

        if exception is None:
            future.set_result(collection)
        else:
            future.set_exception(exception)

    def _run_user_future(self, future: Future, start_delay: float | None) -> None:
        self._logger.info(
            "starting child future",
            child_future_id=future.awaitable.id,
        )
        serialized_values: Dict[str, SerializedValue] = {}
        awaitable_with_serialized_values: Awaitable = (
            serialize_values_in_awaitable_tree(
                user_object=future.awaitable,
                # value serializer is not going to be used because we're serializing a call tree here, not a value.
                value_serializer=PickleUserDataSerializer(),
                serialized_values=serialized_values,
            )
        )

        serialized_objects: Dict[str, SerializedObjectInsideBLOB] = {}
        uploaded_args_blob: BLOB | None = None
        # Only request args blob and upload to it if there are any actual function arguments in the call tree.
        if len(serialized_values) > 0:
            serialized_objects, blob_data = serialized_values_to_serialized_objects(
                serialized_values=serialized_values
            )
            args_blob: BLOB | None = self._get_new_output_blob(
                size=sum(len(data) for data in blob_data)
            )
            uploaded_args_blob = upload_serialized_objects_to_blob(
                serialized_objects=serialized_objects,
                blob_data=blob_data,
                destination_blob=args_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )

        awaitable_execution_plan_pb: ExecutionPlanUpdates
        awaitable_execution_plan_pb = awaitable_to_execution_plan_updates(
            awaitable=awaitable_with_serialized_values,
            uploaded_serialized_objects=serialized_objects,
            # Output serializer name override is only applicable to tail calls.
            output_serializer_name_override=None,
            function_ref=self._function_ref,
            logger=self._logger,
        )
        if start_delay is not None:
            start_at: Timestamp = Timestamp()
            start_at.FromDatetime(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=start_delay)
            )
            awaitable_execution_plan_pb.start_at.CopyFrom(start_at)

        self._allocation_state.add_function_call(
            execution_plan_updates=awaitable_execution_plan_pb,
            args_blob=uploaded_args_blob,
        )

    def _get_new_output_blob(self, size: int) -> BLOB:
        """Returns new BLOB to upload function outputs to."""
        blob_id: str = request_scoped_id()
        blob_request_info: _OutputBLOBRequestInfo = _OutputBLOBRequestInfo(
            blob=None,
            blob_available=threading.Event(),
        )
        self._output_blob_requests[blob_id] = blob_request_info
        self._allocation_state.add_output_blob_request(id=blob_id, size=size)

        blob_request_info.blob_available.wait()
        self._allocation_state.remove_output_blob_request(id=blob_id)
        del self._output_blob_requests[blob_id]
        return blob_request_info.blob

    def _run_allocation_thread(self) -> None:
        alloc_result: AllocationResult | None = None
        try:
            log_user_event_allocations_started([self._allocation_event_details])
            alloc_result = self._run_allocation()
        except BaseException as e:
            self._logger.error(
                "allocation failed due to exception in function executor code",
                exc_info=e,
            )
        finally:
            log_user_event_allocations_finished([self._allocation_event_details])
            if alloc_result is None:
                # This can only happen if the exception was raised and logged above.
                alloc_result = self._result_helper.internal_error()

            self._allocation.result.CopyFrom(alloc_result)
            # This must be the last thing we do. Immeditately after this the allocation can be deleted.
            self._allocation_state.set_result(alloc_result)

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
            return self._result_helper.from_user_exception(
                self._allocation_event_details, e
            )

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
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )

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
            return self._result_helper.from_user_exception(
                self._allocation_event_details, e
            )

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
            return self._result_helper.from_user_exception(
                self._allocation_event_details, e
            )

        # This is internal FE code.
        serialized_objects: Dict[str, SerializedObjectInsideBLOB] = {}
        uploaded_outputs_blob: BLOB | None = None
        # Only request output blob and upload to it if there are any actual function arguments in the call tree.
        if len(serialized_values) > 0:
            serialized_objects, blob_data = serialized_values_to_serialized_objects(
                serialized_values=serialized_values
            )
            outputs_blob: BLOB = self._get_new_output_blob(
                size=sum(len(data) for data in blob_data)
            )
            uploaded_outputs_blob = upload_serialized_objects_to_blob(
                serialized_objects=serialized_objects,
                blob_data=blob_data,
                destination_blob=outputs_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )

        output_pb: SerializedObjectInsideBLOB | ExecutionPlanUpdates
        # This is user code.
        try:
            if isinstance(output, Awaitable):
                output_pb = awaitable_to_execution_plan_updates(
                    awaitable=output,
                    uploaded_serialized_objects=serialized_objects,
                    output_serializer_name_override=output_serializer.name,
                    function_ref=self._function_ref,
                    logger=self._logger,
                )
            else:
                output_pb = serialized_objects[output.metadata.id]
        except BaseException as e:
            return self._result_helper.from_user_exception(
                self._allocation_event_details, e
            )

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


class ProxiedAllocationProgress(FunctionProgress):
    def __init__(self, allocation_runner: AllocationRunner):
        self._allocation_runner: AllocationRunner = allocation_runner

    def update(self, current: float, total: float) -> None:
        self._allocation_runner._allocation_state.update_progress(current, total)
        # sleep(0) here momentarily releases the GIL, giving other
        # FE threads a chance to run before returning back to customer code that
        # might never return GIL. i.e. allowing the FE to handle incoming RPCs,
        # report back allocation state updates, etc.
        # NB: this was never tested to fix anything in practice but nice to have
        # this just in case.
        time.sleep(0)
