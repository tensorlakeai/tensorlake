import asyncio
import contextvars
import datetime
import inspect
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Dict, List

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from tensorlake.applications import (
    RETURN_WHEN,
    DeserializationError,
    Function,
    Future,
    InternalError,
    RequestContext,
    RequestError,
    SDKUsageError,
    TensorlakeError,
    TimeoutError,
)
from tensorlake.applications.algorithms import (
    derived_function_call_future,
    validate_tail_call_user_future,
)
from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.function.function_call import create_function_error
from tensorlake.applications.function.type_hints import (
    function_signature,
    return_type_hint,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value_with_metadata,
    function_output_serializer,
)
from tensorlake.applications.interface.futures import (
    FunctionCallFuture,
    ListFuture,
    ReduceOperationFuture,
    _FutureListKind,
    _InitialMissing,
    _request_scoped_id,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.metadata import (
    FunctionCallMetadata,
)
from tensorlake.applications.registry import get_function
from tensorlake.applications.request_context.contextvar import (
    set_current_request_context,
)
from tensorlake.applications.request_context.http_server.handlers.progress_update import (
    FunctionProgressUpdateRequest,
    FunctionProgressUpdateResponse,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.commit_write import (
    CommitWriteRequest,
    CommitWriteResponse,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.prepare_read import (
    PrepareReadRequest,
    PrepareReadResponse,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.prepare_write import (
    PrepareWriteRequest,
    PrepareWriteResponse,
)
from tensorlake.applications.user_data_serializer import (
    UserDataSerializer,
)

from ..proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    AllocationFunctionCallCreationResult,
    AllocationFunctionCallResult,
    AllocationOutcomeCode,
    AllocationOutputBLOB,
    AllocationResult,
    AllocationState,
    AllocationUpdate,
    ExecutionPlanUpdates,
    FunctionRef,
    SerializedObjectInsideBLOB,
)
from ..user_events import (
    AllocationEventDetails,
    log_user_event_allocations_finished,
    log_user_event_allocations_started,
)
from .allocation_state_wrapper import AllocationStateWrapper
from .contextvars import set_allocation_id_context_variable
from .download import download_function_arguments, download_serialized_objects
from .request_context.progress import AllocationProgress
from .request_context.request_state import AllocationRequestState
from .result_helper import ResultHelper
from .sdk_algorithms import (
    FutureInfo,
    deserialize_application_function_call_args,
    deserialize_sdk_function_call_args,
    future_durable_id,
    reconstruct_sdk_function_call_args,
    replace_user_values_with_serialized_values,
    serialize_user_value,
    to_execution_plan_updates,
    validate_and_deserialize_function_call_metadata,
)
from .upload import (
    serialized_values_to_serialized_objects,
    upload_request_error,
    upload_serialized_objects_to_blob,
)
from .value import SerializedValue, Value


@dataclass
class FunctionCallCreationInfo:
    # Not None when the function call creation is completed by Executor.
    result: AllocationFunctionCallCreationResult | None
    # Set only once after the function call creation result is set.
    result_available: threading.Event


@dataclass
class FunctionCallWatcherInfo:
    # Not None when the function call result is delivered by Executor.
    result: AllocationFunctionCallResult | None
    # Set only once after the function call creation result is set.
    result_available: threading.Event


@dataclass
class _OutputBLOBRequestInfo:
    # Not None once the BLOB is ready to be used.
    # BLOB type use here is deprecated.
    blob: BLOB | AllocationOutputBLOB | None
    # Set only once after the BLOB is set.
    blob_available: threading.Event


class AllocationRunner:
    """Runs a single allocation in a separate thread, allowing to track its state.

    Sets allocation.result when finished.
    """

    def __init__(
        self,
        allocation: Allocation,
        function_ref: FunctionRef,
        function: Function,
        function_instance_arg: Any | None,
        blob_store: BLOBStore,
        request_context: RequestContext,
        logger: InternalLogger,
    ):
        self._allocation: Allocation = allocation
        self._function_ref: FunctionRef = function_ref
        self._function: Function = function
        self._function_instance_arg: Any | None = function_instance_arg
        self._blob_store: BLOBStore = blob_store
        self._request_context: RequestContext = request_context
        self._logger: InternalLogger = logger.bind(module=__name__)

        self._allocation_event_details: AllocationEventDetails = AllocationEventDetails(
            namespace=self._function_ref.namespace,
            application_name=self._function_ref.application_name,
            application_version=self._function_ref.application_version,
            function_name=self._function_ref.function_name,
            request_id=self._allocation.request_id,
            function_call_id=self._allocation.function_call_id,
            allocation_id=self._allocation.allocation_id,
        )

        self._allocation_state: AllocationStateWrapper = AllocationStateWrapper()
        self._request_state: AllocationRequestState = AllocationRequestState(
            allocation_state=self._allocation_state,
            logger=logger,
        )
        self._allocation_progress: AllocationProgress = AllocationProgress(
            allocation_state=self._allocation_state,
            logger=logger,
        )
        self._request_context: RequestContext = request_context
        self._result_helper: ResultHelper = ResultHelper(
            function_ref=function_ref,
            function=function,
            logger=self._logger,
        )
        self._allocation_thread: threading.Thread = threading.Thread(
            target=self._run_allocation_thread,
            daemon=True,
        )
        # Allocation function output related overrides.
        self._output_value_serializer_name_override: str | None = None
        self._has_output_value_type_hint_override: bool = False
        self._output_value_type_hint_override: Any = None

        # Allocation Execution state.
        #
        # Futures that were created (started) during this allocation.
        # Future ID -> FutureInfo.
        self._future_infos: Dict[str, FutureInfo] = {}
        # Durable ID of the previous future started by this allocation.
        self._previous_future_durable_id: str = allocation.function_call_id
        # Allocation Function Call ID -> FunctionCallCreationInfo.
        # Allocation Function Call ID is different from Function Call ID in ExecutionPlanUpdates.
        self._function_call_creations: Dict[str, FunctionCallCreationInfo] = {}
        # Watcher ID -> FunctionCallWatcherInfo.
        self._function_call_watchers: Dict[str, FunctionCallWatcherInfo] = {}
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
        if update.HasField("function_call_creation_result"):
            alloc_function_call_id: str = (
                update.function_call_creation_result.allocation_function_call_id
                if update.function_call_creation_result.HasField(
                    "allocation_function_call_id"
                )
                else update.function_call_creation_result.function_call_id
            )
            if alloc_function_call_id not in self._function_call_creations:
                self._logger.error(
                    "received function call creation result for unknown allocation function call",
                    alloc_fn_call_id=alloc_function_call_id,
                )
                return
            function_call_creation_info: FunctionCallCreationInfo = (
                self._function_call_creations[alloc_function_call_id]
            )
            function_call_creation_info.result = update.function_call_creation_result
            function_call_creation_info.result_available.set()
        elif update.HasField("function_call_result"):
            watcher_id: str = (
                update.function_call_result.watcher_id
                if update.function_call_result.HasField("watcher_id")
                else update.function_call_result.function_call_id
            )
            if watcher_id not in self._function_call_watchers:
                self._logger.error(
                    "received function call result for unknown watcher",
                    watcher_id=watcher_id,
                )
                return
            watcher_info: FunctionCallWatcherInfo = self._function_call_watchers[
                watcher_id
            ]
            watcher_info.result = update.function_call_result
            watcher_info.result_available.set()
        elif update.HasField("output_blob_deprecated") or update.HasField(
            "output_blob"
        ):
            blob: BLOB | AllocationOutputBLOB | None = None
            blob_id: str | None = None
            if update.HasField("output_blob_deprecated"):
                blob = update.output_blob_deprecated
                blob_id = blob.id
            else:
                blob = update.output_blob
                blob_id = blob.blob.id

            if blob_id not in self._output_blob_requests:
                self._logger.error(
                    "received output blob update for unknown blob request",
                    blob_id=blob_id,
                )
                return

            blob_request_info: _OutputBLOBRequestInfo = self._output_blob_requests[
                blob_id
            ]
            blob_request_info.blob = blob
            blob_request_info.blob_available.set()
        elif update.HasField("request_state_operation_result"):
            self._request_state.deliver_operation_result(
                update.request_state_operation_result
            )
        else:
            self._logger.error(
                "received unexpected allocation update",
                update=str(update),
            )

    def run_request_context_operation(
        self,
        operation: (
            PrepareWriteRequest
            | PrepareReadRequest
            | CommitWriteRequest
            | FunctionProgressUpdateRequest
        ),
    ) -> (
        PrepareWriteResponse
        | PrepareReadResponse
        | CommitWriteResponse
        | FunctionProgressUpdateResponse
    ):
        """Runs the given request context operation and returns its result.

        Blocks until the operation completes.
        Raises exception on error.
        """
        if isinstance(operation, PrepareReadRequest):
            return self._request_state.prepare_read(operation)
        elif isinstance(operation, PrepareWriteRequest):
            return self._request_state.prepare_write(operation)
        elif isinstance(operation, CommitWriteRequest):
            return self._request_state.commit_write(operation)
        elif isinstance(operation, FunctionProgressUpdateRequest):
            return self._allocation_progress.update(operation)
        else:
            raise RuntimeError(
                f"Unknown request context operation type: {type(operation)}"
            )

    def run_futures_runtime_hook(self, futures: List[Future]) -> None:
        # NB: This code is called from user function thread. User function code is blocked.
        # Right now we can only be called from the function thread, not any child threads that user
        # code might have created because contextvars are not propagated to child threads.
        # All exceptions raised here will be propagated to user code by design.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        try:
            return self._run_futures_runtime_hook(futures)
        except TensorlakeError:
            raise
        except Exception as e:
            self._logger.error(
                "Unexpected exception in run_futures_runtime_hook",
                exc_info=e,
            )
            raise InternalError(f"Unexpected error while running futures")

    def _run_futures_runtime_hook(self, futures: List[Future]) -> None:
        # NB: To support durability, ordering of running the Futures must be deterministic.
        for user_future in futures:
            if not isinstance(user_future, Future):
                raise SDKUsageError(f"Cannot run a non-Future object {user_future}.")
            if user_future._id in self._future_infos:
                raise InternalError(
                    f"Future with ID {user_future._id} is already running, this should never happen."
                )

            durable_id: str = future_durable_id(
                future=user_future,
                parent_function_call_id=self._allocation.function_call_id,
                previous_future_durable_id=self._previous_future_durable_id,
                future_infos=self._future_infos,
            )
            self._previous_future_durable_id = durable_id
            future_info: FutureInfo = FutureInfo(
                future=user_future,
                durable_id=durable_id,
                map_future_output=None,
                reduce_future_output=None,
            )
            self._future_infos[user_future._id] = future_info

            if isinstance(user_future, ListFuture):
                self._run_list_future(future_info)
            elif isinstance(user_future, ReduceOperationFuture):
                self._run_reduce_opearation_future(future_info)
            elif isinstance(user_future, FunctionCallFuture):
                self._run_function_call_future(future_info)
            else:
                raise InternalError(
                    f"Unsupported Future type: {type(user_future)} with ID {user_future._id}"
                )

    def _run_list_future(self, future_info: FutureInfo) -> None:
        # Server can't run ListFuture, we need to run each list item separately as a new
        # internal (not user visible) Future. All child Futures of user_future are already running.
        # FIXME: This is not efficient. When we change to log oriented protocol, all the function calls need
        # to come in a single batch of ordered events instead sending one function call to Server at a time.
        user_future: ListFuture = future_info.future

        if user_future._metadata.kind != _FutureListKind.MAP_OPERATION:
            raise InternalError(
                f"Unsupported ListFuture kind: {user_future._metadata.kind}"
            )
        function: Function = get_function(user_future._metadata.function_name)

        map_inputs: list[Future | Any]
        if isinstance(user_future._items, ListFuture):
            inputs_future_info: FutureInfo = self._future_infos[user_future._items._id]
            map_inputs = inputs_future_info.map_future_output
        else:
            map_inputs = user_future._items

        map_outputs: list[FunctionCallFuture] = []
        for input in map_inputs:
            # Calling SDK recursively here. The depth of recursion is strictly one.
            # This is because the input is an already running Future, we won't decend into it.
            mapped_input: FunctionCallFuture = derived_function_call_future(
                user_future, function, input
            )
            map_outputs.append(mapped_input)

        future_info.map_future_output = map_outputs

    def _run_reduce_opearation_future(self, future_info: FutureInfo) -> None:
        # Server can't run ReduceOperationFuture, we need to run each list item separately as a new
        # internal (not user visible) Future. All child Futures of user_future are already running.
        # FIXME: This is not efficient. When we change to log oriented protocol, all the function calls need
        # to come in a single batch of ordered events instead sending one function call to Server at a time.
        user_future: ReduceOperationFuture = future_info.future

        # We do recursive calls into the SDK but with only one level, so it's okay.
        function: Function = get_function(user_future._function_name)

        inputs: list[Future | Any] = []
        if user_future._initial is not _InitialMissing:
            inputs.append(user_future._initial)

        if isinstance(user_future._items, ListFuture):
            inputs_future_info: FutureInfo = self._future_infos[user_future._items._id]
            inputs.extend(inputs_future_info.map_future_output)
        else:
            inputs.extend(user_future._items)

        if len(inputs) == 0:
            raise SDKUsageError("reduce of empty iterable with no initial value")

        if len(inputs) == 1:
            # This is UX corner case. If this is a Future and user didn't do tail_call on it
            # then they can get a value serialized in a wrong way here. We'll need to explain
            # it in user docs.
            future_info.reduce_future_output = inputs[0]
            return

        # Create a chain of function calls to reduce all args one by one.
        # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
        # using string concat function into "abcd".

        # inputs now contain at least two items.
        last_future: Future = derived_function_call_future(
            user_future, function, inputs[0], inputs[1]
        )
        for input in inputs[2:]:
            # Calling SDK recursively here. The depth of recursion is strictly one.
            # This is because the input is an already running Future, we won't descend into it.
            last_future = derived_function_call_future(
                user_future, function, last_future, input
            )

        future_info.reduce_future_output = last_future

    def wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        # NB: This code is called from user function thread. User function code is blocked.
        # Right now we can only be called from the function thread, not any child threads that user
        # code might have created because contextvars are not propagated to child threads.
        # All exceptions raise here will be propagated to user code by design.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        try:
            return self._wait_futures_runtime_hook(futures, timeout, return_when)
        except TensorlakeError:
            raise
        except Exception as e:
            self._logger.error(
                "Unexpected exception in wait_futures_runtime_hook",
                exc_info=e,
            )
            raise InternalError(f"Unexpected error while waiting for futures")

    def _wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        if return_when not in (
            RETURN_WHEN.ALL_COMPLETED,
            RETURN_WHEN.FIRST_COMPLETED,
            RETURN_WHEN.FIRST_FAILURE,
        ):
            raise SDKUsageError(f"Not supported return_when value: '{return_when}'")

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
            try:
                self._wait_future_completion(future=future, deadline=deadline)
            except BaseException as e:
                # Something went wrong while waiting for the future.
                self._logger.error(
                    "Unexpected error while waiting for child future completion",
                    future_id=future._id,
                    exc_info=e,
                )
                future.set_exception(
                    InternalError(
                        f"Unexpected error while waiting for child future completion: {e}"
                    )
                )

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

    def await_future_runtime_hook(self, future: Future) -> Generator[None, None, Any]:
        # NB: This code is called from user async function thread.
        # All exceptions raised here will be propagated to user code by design.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        try:
            return self._await_future_runtime_hook(future)
        except TensorlakeError:
            raise
        except Exception as e:
            self._logger.error(
                "Unexpected exception in await_future_runtime_hook",
                exc_info=e,
            )
            raise InternalError("Unexpected error while awaiting future")

    def _await_future_runtime_hook(self, future: Future) -> Generator[None, None, Any]:
        user_aio_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        # Result is set to None because the actual result is stored in the
        # Tensorlake SDK Future. This asyncio Future is only used for waiting
        # in user code, not for getting the result.
        user_aio_loop_future: asyncio.Future = user_aio_loop.create_future()

        def background_wait():
            try:
                self._wait_future_completion(future=future, deadline=None)
            except BaseException as e:
                self._logger.error(
                    "Unexpected error while waiting for child future completion",
                    future_id=future._id,
                    exc_info=e,
                )
                future.set_exception(
                    InternalError(
                        f"Unexpected error while waiting for child future completion: {e}"
                    )
                )
            user_aio_loop.call_soon_threadsafe(user_aio_loop_future.set_result, None)

        threading.Thread(target=background_wait, daemon=True).start()
        yield from user_aio_loop_future.__await__()

    def _wait_future_completion(self, future: Future, deadline: float | None) -> None:
        """Waits for the completion of the future and sets its result or exception if didn't timeout.

        Raises Exception on unexpected internal error. Normally all exceptions are set on the future itself.
        """
        if future.done():
            # Short circuit just for performance optimization
            # so we don't call Server to get the result again.
            return

        future_info: FutureInfo = self._future_infos.get(future._id)
        if future_info is None:
            raise InternalError(
                f"Unknown Future with ID {future._id} is not tracked in AllocationRunner."
            )

        if isinstance(future, ListFuture):
            self._wait_future_list_completion(future_info, deadline)
        elif isinstance(future, ReduceOperationFuture):
            self._wait_reduce_operation_future_completion(future_info, deadline)
        elif isinstance(future, FunctionCallFuture):
            self._wait_function_call_future_completion(future_info, deadline)
        else:
            raise InternalError(
                f"Unsupported Future type: {type(future)} with ID {future._id}"
            )

    def _wait_function_call_future_completion(
        self, future_info: FutureInfo, deadline: float | None
    ) -> None:
        future: FunctionCallFuture = future_info.future
        start_time: float = time.monotonic()

        if future_info.durable_id is None:
            self._logger.error(
                "Durable Future ID is not set for FutureInfo.",
                future_id=future_info.future._id,
            )
            future.set_exception(
                InternalError("Durable Future ID is not set for FutureInfo.")
            )
            return

        function_call_watcher_id: str = _request_scoped_id()
        self._logger.info(
            "waiting for child future completion",
            future_id=future._id,
            future_fn_call_id=future_info.durable_id,
            future_watcher_id=function_call_watcher_id,
        )
        watcher_info: FunctionCallWatcherInfo = FunctionCallWatcherInfo(
            result=None,
            result_available=threading.Event(),
        )
        self._function_call_watchers[function_call_watcher_id] = watcher_info
        # FIXME: Temorary workaround for missing watcher_id coming from Executor.
        # Remove once Executor is updated.
        self._function_call_watchers[future_info.durable_id] = watcher_info
        self._allocation_state.add_function_call_watcher(
            id=function_call_watcher_id,
            root_function_call_id=future_info.durable_id,
        )

        result_wait_timeout: float | None = (
            deadline - time.monotonic() if deadline is not None else None
        )
        result_available: bool = watcher_info.result_available.wait(
            timeout=result_wait_timeout
        )

        self._allocation_state.delete_function_call_watcher(id=function_call_watcher_id)
        del self._function_call_watchers[function_call_watcher_id]
        # FIXME: Temorary workaround for missing watcher_id coming from Executor.
        # Remove once Executor is updated.
        del self._function_call_watchers[future_info.durable_id]

        if result_available:
            result: AllocationFunctionCallResult = watcher_info.result
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
                    future.set_exception(
                        InternalError(
                            "Function Call output SerializedValue is missing metadata."
                        )
                    )
                    return
                output: Any = deserialize_value_with_metadata(
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
                    # We don't have a user visible cause of failure.
                    future.set_exception(create_function_error(future, cause=None))
            else:
                self._logger.error(
                    f"Unexpected outcome code in function call result: {result.outcome_code}"
                )
                future.set_exception(
                    InternalError(
                        f"Unexpected outcome code in function call result: {result.outcome_code}"
                    )
                )
        else:
            # timeout and no result or error are available.
            future.set_exception(TimeoutError())

        # Future result is set, we can remove the Future from tracking.
        del self._future_infos[future._id]
        self._logger.info(
            "child future completed",
            future_id=future._id,
            future_fn_call_id=future_info.durable_id,
            future_watcher_id=function_call_watcher_id,
            duration_sec=f"{time.monotonic() - start_time:.3f}",
            success=future.exception is None,
        )

    def _wait_future_list_completion(
        self, future_info: FutureInfo, deadline: float | None
    ) -> None:
        """Wait for the completion of the future representing a ListFuture and sets its result or exception.

        Raises Exception on unexpected internal error. Normally all exceptions are set on the future itself.
        """
        # Reconstruct the original collection out of individual futures.
        future: ListFuture = future_info.future
        collection: List[Any] = []
        exception: TensorlakeError | None = None
        is_timeout: bool = False

        for item in future_info.map_future_output:
            if deadline is not None and deadline - time.monotonic() <= 0:
                is_timeout = True
                break

            self._wait_future_completion(future=item, deadline=deadline)
            if item.exception is None:
                collection.append(item.result())
            else:
                exception = item.exception
                break

        if is_timeout:
            future.set_exception(TimeoutError())
        elif exception is not None:
            future.set_exception(exception)
        else:
            future.set_result(collection)

    def _wait_reduce_operation_future_completion(
        self, future_info: FutureInfo, deadline: float | None
    ) -> None:
        """Wait for the completion of the future representing a ReduceOperationFuture and sets its result or exception.

        Raises Exception on unexpected internal error. Normally all exceptions are set on the future itself.
        """
        future: ReduceOperationFuture = future_info.future
        reduce_future_output: Future | Any | None = future_info.reduce_future_output

        if reduce_future_output is None:
            future.set_exception(
                InternalError("Reduce operation future is missing the output future.")
            )
            return

        if isinstance(reduce_future_output, Future):
            # FIXME: Recursive call. Max 1000 recursion depth is allowed in Python by default.
            self._wait_future_completion(future=reduce_future_output, deadline=deadline)
            if reduce_future_output.exception is not None:
                future.set_exception(reduce_future_output.exception)
            else:
                future.set_result(reduce_future_output.result())
        else:
            # This can happen when we have only one item to reduce, in that case we shortcut and set the output directly without creating a Future for it.
            future.set_result(reduce_future_output)

    def _run_function_call_future(self, future_info: FutureInfo) -> None:
        future: FunctionCallFuture = future_info.future

        # Copy the user's future to not pollute user Future by the modifications coming next.
        future_copy: FunctionCallFuture = FunctionCallFuture(
            id=future_info.durable_id,
            function_name=future._function_name,
            args=list(future._args),
            kwargs=dict(future._kwargs),
        )

        serialized_values: Dict[str, SerializedValue] = (
            replace_user_values_with_serialized_values(
                future=future_copy,
                future_infos=self._future_infos,
            )
        )
        serialized_objects: Dict[str, SerializedObjectInsideBLOB] = {}
        uploaded_args_blob: BLOB | None = None
        # Only request args blob and upload to it if there are any actual function arguments in the call tree.
        if len(serialized_values) > 0:
            serialized_objects, blob_data = serialized_values_to_serialized_objects(
                serialized_values=serialized_values
            )
            args_blob: BLOB = self._get_new_output_blob(
                size=sum(len(data) for data in blob_data)
            )
            uploaded_args_blob = upload_serialized_objects_to_blob(
                serialized_objects=serialized_objects,
                blob_data=blob_data,
                destination_blob=args_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )

        execution_plan_pb: ExecutionPlanUpdates = to_execution_plan_updates(
            future=future_copy,
            uploaded_serialized_objects=serialized_objects,
            output_serializer_name_override=(
                self._output_value_serializer_name_override
                if future._tail_call
                else None
            ),
            output_type_hint_override=(
                self._output_value_type_hint_override if future._tail_call else None
            ),
            has_output_type_hint_override=(
                self._has_output_value_type_hint_override
                if future._tail_call
                else False
            ),
            function_ref=self._function_ref,
            future_infos=self._future_infos,
        )
        if future._start_delay is not None:
            start_at: Timestamp = Timestamp()
            start_at.FromDatetime(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=future._start_delay)
            )
            execution_plan_pb.start_at.CopyFrom(start_at)

        function_call_creation_info: FunctionCallCreationInfo = (
            FunctionCallCreationInfo(
                result=None,
                result_available=threading.Event(),
            )
        )
        alloc_function_call_id: str = _request_scoped_id()
        self._logger.info(
            "starting child future",
            future_id=future_info.future._id,
            future_fn_call_id=future_info.durable_id,
            future_alloc_fn_call_id=alloc_function_call_id,
        )
        self._function_call_creations[alloc_function_call_id] = (
            function_call_creation_info
        )
        # Temporary workaround for missing allocation_function_call_id coming from Executor.
        # TODO: Remove once Executor is updated.
        self._function_call_creations[future_info.durable_id] = (
            function_call_creation_info
        )
        self._allocation_state.add_function_call(
            id=alloc_function_call_id,
            execution_plan_updates=execution_plan_pb,
            args_blob=uploaded_args_blob,
        )

        function_call_creation_info.result_available.wait()

        del self._function_call_creations[alloc_function_call_id]
        # TODO: Remove once Executor is updated.
        del self._function_call_creations[future_info.durable_id]
        self._allocation_state.delete_function_call(id=alloc_function_call_id)

        if (
            function_call_creation_info.result.status.code
            != grpc.StatusCode.OK.value[0]
        ):
            self._logger.error(
                "child future function call creation failed",
                future_id=future_info.future._id,
                future_fn_call_id=future_info.durable_id,
                future_alloc_fn_call_id=alloc_function_call_id,
                status=function_call_creation_info.result.status,
            )
            exception: InternalError = InternalError("Failed to start function call")
            future_info.future.set_exception(exception)
            raise exception
        else:
            self._logger.info(
                "started child function call future",
                future_id=future_info.future._id,
                future_fn_call_id=future_info.durable_id,
                future_alloc_fn_call_id=alloc_function_call_id,
            )

    def _get_new_output_blob(self, size: int) -> BLOB:
        """Returns new BLOB to upload function outputs to.

        Raises exception on error.
        """
        blob_id: str = _request_scoped_id()
        blob_request_info: _OutputBLOBRequestInfo = _OutputBLOBRequestInfo(
            blob=None,
            blob_available=threading.Event(),
        )
        self._output_blob_requests[blob_id] = blob_request_info
        self._allocation_state.add_output_blob_request(id=blob_id, size=size)

        blob_request_info.blob_available.wait()

        self._allocation_state.remove_output_blob_request(id=blob_id)
        del self._output_blob_requests[blob_id]

        if isinstance(blob_request_info.blob, AllocationOutputBLOB):
            if blob_request_info.blob.status.code != grpc.StatusCode.OK.value[0]:
                self._logger.error(
                    "received output blob with error status",
                    blob_id=blob_request_info.blob.blob.id,
                    status=blob_request_info.blob.status,
                )
                raise RuntimeError(
                    f"Failed to create output BLOB: {blob_request_info.blob.status}"
                )
            return blob_request_info.blob.blob
        else:
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
                # alloc_result is None only if an exception was raised.
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
        function_call_metadata: FunctionCallMetadata | None = (
            validate_and_deserialize_function_call_metadata(
                serialized_function_call_metadata=self._allocation.inputs.function_call_metadata,
                serialized_args=serialized_args,
                function=self._function,
                logger=self._logger,
            )
        )

        if function_call_metadata is None:
            # Application function call created by Server.
            # This is our code.
            # Application function call doesn't have a parent call that can override output serializer.
            # Application function overrides output type hint and serializer for all its child tail call futures.
            self._output_value_serializer_name_override = function_output_serializer(
                function=self._function,
                output_serializer_override=None,
            ).name
            self._output_value_type_hint_override = return_type_hint(
                function_signature(self._function).return_annotation
            )
            self._has_output_value_type_hint_override = True
            if len(serialized_args) == 0:
                # We expect exactly one argument but support more for any future FE protocol migrations.
                raise InternalError(
                    f"Application function call must have at least one argument, got {len(serialized_args)}."
                )
            # This is user code.
            try:
                args, kwargs = deserialize_application_function_call_args(
                    function=self._function,
                    payload=serialized_args[0],
                    function_instance_arg=self._function_instance_arg,
                )
            except DeserializationError as e:
                # Failed due to user error. All other exceptions are out internal FE errors.
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )
        else:
            # Regular function call created by SDK. Uses function call metadata.
            #
            # This is our code.
            self._output_value_serializer_name_override = (
                function_call_metadata.output_serializer_name_override
            )
            if function_call_metadata.has_output_type_hint_override:
                self._output_value_type_hint_override = (
                    function_call_metadata.output_type_hint_override
                )
                self._has_output_value_type_hint_override = True
            # This is user code.
            try:
                arg_values: Dict[str, Value] = deserialize_sdk_function_call_args(
                    serialized_args
                )
            except BaseException as e:
                # This is internal FE code.
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )

            # This is internal FE code.
            args, kwargs = reconstruct_sdk_function_call_args(
                function_call_metadata=function_call_metadata,
                arg_values=arg_values,
                function_instance_arg=self._function_instance_arg,
            )

        # This is user code.
        try:
            output: Any | Future = self._call_user_function(args, kwargs)
        except RequestError as e:
            # This is user code.
            try:
                utf8_message: bytes = e.message.encode("utf-8")
            except BaseException:
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )

            # This is internal FE code.
            request_error_so, uploaded_output_blob = upload_request_error(
                utf8_message=utf8_message,
                destination_blob=self._allocation.inputs.request_error_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )
            return self._result_helper.from_request_error(
                details=self._allocation_event_details,
                request_error=e,
                request_error_output=request_error_so,
                uploaded_request_error_blob=uploaded_output_blob,
            )
        except BaseException as e:
            # This is internal FE code.
            return self._result_helper.from_user_exception(
                self._allocation_event_details, e
            )

        tail_call_output_future: Future | None = None
        if isinstance(output, Future):
            # Function returned tail call. This is user code.
            try:
                validate_tail_call_user_future(
                    function_name=self._function_ref.function_name,
                    tail_call_user_future=output,
                )
            except BaseException as e:
                # This is internal FE code.
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )

            if isinstance(output, ReduceOperationFuture):
                output_future_info: FutureInfo = self._future_infos[output._id]
                if isinstance(output_future_info.reduce_future_output, Future):
                    tail_call_output_future = output_future_info.reduce_future_output
                else:
                    output = output_future_info.reduce_future_output
            else:
                tail_call_output_future = output

        serialized_output: SerializedObjectInsideBLOB | None = None
        uploaded_output_blob: BLOB | None = None
        if tail_call_output_future is None:
            # Function returned regular value. This is user code.
            output_value_serializer: UserDataSerializer = function_output_serializer(
                function=self._function,
                output_serializer_override=self._output_value_serializer_name_override,
            )
            try:
                serialized_output_value: SerializedValue = serialize_user_value(
                    value=output,
                    serializer=output_value_serializer,
                    type_hint=(
                        self._output_value_type_hint_override
                        if self._has_output_value_type_hint_override
                        else type(output)
                    ),
                )
            except BaseException as e:
                # This is internal FE code.
                return self._result_helper.from_user_exception(
                    self._allocation_event_details, e
                )

            # This is internal FE code.
            serialized_objects, blob_data = serialized_values_to_serialized_objects(
                serialized_values={
                    serialized_output_value.metadata.id: serialized_output_value
                }
            )
            serialized_output = serialized_objects[serialized_output_value.metadata.id]
            outputs_blob: BLOB = self._get_new_output_blob(
                size=sum(len(data) for data in blob_data)
            )
            uploaded_output_blob = upload_serialized_objects_to_blob(
                serialized_objects=serialized_objects,
                blob_data=blob_data,
                destination_blob=outputs_blob,
                blob_store=self._blob_store,
                logger=self._logger,
            )

        output_pb: SerializedObjectInsideBLOB | ExecutionPlanUpdates
        # This is user code.
        try:
            if tail_call_output_future is None:
                output_pb = serialized_output
            else:
                tail_call_output_future_info: FutureInfo = self._future_infos[
                    tail_call_output_future._id
                ]
                output_pb = ExecutionPlanUpdates(
                    root_function_call_id=tail_call_output_future_info.durable_id,
                )

        except BaseException as e:
            return self._result_helper.from_user_exception(
                self._allocation_event_details, e
            )

        return self._result_helper.from_function_output(
            output=output_pb, uploaded_outputs_blob=uploaded_output_blob
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
            if inspect.iscoroutinefunction(self._function):
                return asyncio.run(self._function._original_function(*args, **kwargs))
            else:
                return self._function._original_function(*args, **kwargs)
        finally:
            self._logger.info(
                "function finished",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )
