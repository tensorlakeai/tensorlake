import asyncio
import inspect
import queue
import threading
import time
import weakref
from collections.abc import Coroutine, Generator
from typing import Any

from tensorlake.applications import (
    RETURN_WHEN,
    Function,
    Future,
    InternalError,
    RequestContext,
    SDKUsageError,
    SerializationError,
    TensorlakeError,
    TimeoutError,
)
from tensorlake.applications.algorithms import (
    derived_function_call_future,
    dfs_bottom_up_unique_only,
    tail_call_output_future_ids,
    validate_tail_call_user_future,
)
from tensorlake.applications.interface.futures import (
    FunctionCallFuture,
    ListFuture,
    ReduceOperationFuture,
    _FutureListKind,
    _InitialMissing,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.registry import get_function
from tensorlake.applications.request_context.contextvar import (
    set_current_request_context,
)

from ..contextvars import set_allocation_id_context_variable
from .durable_id import future_durable_id
from .future_info import FutureInfo
from .input_events import (
    InputEventEmergencyShutdown,
    InputEventFunctionCallCreated,
    InputEventFunctionCallWatcherResult,
    InputEventType,
    _InputEventShutdown,
)
from .output_events import (
    FunctionCallCollectionRef,
    FunctionCallRef,
    OutputEventBatch,
    OutputEventCreateFunctionCall,
    OutputEventCreateFunctionCallWatcher,
    OutputEventFinishAllocation,
)


class AllocationEventLoop:
    """Deterministic event loop for running user code.

    Runs user code in a thread. When user calls an SDK hook, generates strictly
    ordered output events and blocks the hook (and user code) until input events required to
    complete the current SDK hook are delivered by AllocationRunner. This ensures strict
    step-by-step execution of user code and ensures its deterministic execution.
    """

    def __init__(
        self,
        function: Function,
        function_call_id: str,
        allocation_id: str,
        request_context: RequestContext,
        logger: InternalLogger,
    ):
        self._function: Function = function
        self._function_call_id: str = function_call_id
        self._allocation_id: str = allocation_id
        self._request_context: RequestContext = request_context
        self._logger: InternalLogger = logger.bind(module=__name__)

        # Allocation Execution state.
        #
        # Durable ID of the previous future started by this allocation.
        self._previous_future_durable_id: str = function_call_id
        # Futures that were created (started) during this allocation.
        # Future ID -> FutureInfo.
        # Future Durable ID -> FutureInfo.
        self._future_infos: dict[str, FutureInfo] = {}
        # Coroutine -> Future, use weakref so once a coroutine is no longer used
        # it's deleted from the mapping automatically. This is required because coroutine
        # objects are owned by user code and so is their lifecycle.
        self._coroutine_to_future: weakref.WeakKeyDictionary = (
            weakref.WeakKeyDictionary()
        )

        # Lock taken on entry into every runtime hook. This is only to ensure no parallel execution
        # is happening inside event loop due to customer code spinning up multiple threads.
        # Customer code can currently spin up a new thread that can call a runtime hook. For this customer
        # code can call asyncio.to_thread() because the thread automatically inherits a copy of the current
        # contextvars.Context. Async Agentic frameworks use asyncio.to_thread() for running sync tool functions.
        # Customer code doing this makes their application non-deterministic, we just ensure no race conditions
        # inside event loop by putting this lock in place.
        self._runtime_hook_lock: threading.Lock = threading.Lock()
        self._output_event_queue: queue.Queue[OutputEventBatch] = queue.Queue()
        self._input_event_queue: queue.Queue[InputEventType] = queue.Queue()
        # Thread that runs user code and runtime hooks.
        # Generates output events in deterministic, replayable order.
        self._user_thread: threading.Thread | None = None
        # Thread that processes input events.
        # Applies input events one by one in deterministic way.
        self._input_event_thread: threading.Thread | None = None

    def start(self, args: list[Any], kwargs: dict[str, Any]) -> None:
        """Starts user code in a thread. Non-blocking.

        Doesn't raise any exceptions.
        """
        self._user_thread = threading.Thread(
            target=self._run_user_function,
            args=(args, kwargs),
            daemon=True,
        )
        self._user_thread.start()

    def join(self) -> None:
        """Waits for event loop to exit.

        Exit means stop running user code and other service threads.
        Doesn't raise any exceptions. All exceptions should be delivered via output events and handled by AllocationRunner.
        """
        if self._user_thread is not None:
            try:
                self._user_thread.join()
            except RuntimeError as e:
                self._logger.error(
                    "Error while waiting for user thread to finish",
                    exc_info=e,
                )
        if self._input_event_thread is not None:
            try:
                self._input_event_thread.join()
            except RuntimeError as e:
                self._logger.error(
                    "Error while waiting for input event processing thread to finish",
                    exc_info=e,
                )

    def wait_for_output_event_batch(self) -> OutputEventBatch:
        """Blocks until user code generates an output event batch."""
        return self._output_event_queue.get()

    def add_input_event(self, event: InputEventType) -> None:
        """Delivers a single input event to event loop."""
        self._input_event_queue.put(event)

    def _run_user_function(self, args: list[Any], kwargs: dict[str, Any]) -> None:
        """Runs user function to completion.

        Doesn't raise any exceptions. All exceptions are caught and delivered via output events.
        """
        # The thread context should be empty, because we're running in a new thread.
        #
        # Request context is required for user function running in this thread.
        # Allocation ID context variable is required for both user function, _unwrap_future
        # and all its callers.
        set_current_request_context(self._request_context)
        set_allocation_id_context_variable(self._allocation_id)

        try:
            self._input_event_thread = threading.Thread(
                target=self._process_input_events, daemon=True
            )
            self._input_event_thread.start()
            self.__run_user_function(args, kwargs)
        except BaseException as e:
            # This can only be exception in our code.
            self._output_event_queue.put(
                OutputEventBatch(
                    events=[OutputEventFinishAllocation(internal_exception=e)]
                )
            )
        finally:
            # Cleanup resources, no exceptions must be raised here.
            self._input_event_queue.put(_InputEventShutdown())
            if (
                self._input_event_thread is not None
                and self._input_event_thread.is_alive()
            ):
                try:
                    self._logger.info(
                        "Waiting for input event processing thread to finish"
                    )
                    self._input_event_thread.join()
                except RuntimeError as e:
                    self._logger.error(
                        "Error while waiting for input event processing thread to finish",
                        exc_info=e,
                    )

    def __run_user_function(self, args: list[Any], kwargs: dict[str, Any]) -> None:
        """Runs user function to completion.

        Raises an exception on internal error. All exceptions in user code are caught and delivered via output events.
        """
        # This is user code.
        try:
            output: Any | _TensorlakeFutureWrapper[Future] = self.__call_user_function(
                args, kwargs
            )
        except BaseException as e:
            self._output_event_queue.put(
                OutputEventBatch(events=[OutputEventFinishAllocation(user_exception=e)])
            )
            return

        # This is our code.
        output: Any | Future = _unwrap_future(output)
        output_events: list[OutputEventCreateFunctionCall] = []

        if isinstance(output, Future):
            # Function returned tail call. This is user code.
            try:
                validate_tail_call_user_future(
                    function_name=self._function._function_config.function_name,
                    tail_call_user_future=output,
                )
            except BaseException as e:
                self._output_event_queue.put(
                    OutputEventBatch(
                        events=[OutputEventFinishAllocation(user_exception=e)]
                    )
                )
                return

            # This is our code.
            #
            # SDK automatically starts tail call futures and their inputs.
            # This is why we walk the Futures tree, not just starting the output.
            output_future_ids: set[str] = tail_call_output_future_ids(output)
            for future in dfs_bottom_up_unique_only(output):
                if future._id in self._future_infos:
                    # This is user code.
                    self._output_event_queue.put(
                        OutputEventBatch(
                            events=[
                                OutputEventFinishAllocation(
                                    user_exception=SDKUsageError(
                                        f"A tail call Future {future} is already running, "
                                        "a tail call Future should not be started."
                                    )
                                )
                            ]
                        )
                    )
                    return

                cmds: list[OutputEventCreateFunctionCall] = self._register_future(
                    future, tail_call=(future._id in output_future_ids)
                )
                output_events.extend(cmds)

            if isinstance(output, ReduceOperationFuture):
                output_future_info: FutureInfo = self._future_infos[output._id]
                output = output_future_info.reduce_future_output

        if isinstance(output, Future):
            tail_call_output_info: FutureInfo = self._future_infos[output._id]
            tail_call_ref: FunctionCallRef = FunctionCallRef(
                durable_id=tail_call_output_info.durable_id
            )

            # Tail call: first output events batch creates function calls and waits
            # for server to confirm creation. Second batch finishes the allocation.
            self._create_function_calls(output_events)

            # Check each command completion because tail_call_output_info.future might not create
            # a function call for itself (i.e., it might be a composite future of other function calls).
            for function_call in output_events:
                function_call: OutputEventCreateFunctionCall
                function_call_future_info: FutureInfo = self._future_infos[
                    function_call.durable_id
                ]
                if function_call_future_info.future._exception is not None:
                    exception: TensorlakeError = (
                        function_call_future_info.future._exception
                    )
                    # TODO: Move serialization into event loop so we don't have to have this workaround.
                    if isinstance(exception, SerializationError):
                        event = OutputEventFinishAllocation(user_exception=exception)
                    else:
                        event = OutputEventFinishAllocation(
                            internal_exception=exception
                        )

                    self._output_event_queue.put(OutputEventBatch(events=[event]))
                    return

            self._output_event_queue.put(
                OutputEventBatch(
                    events=[OutputEventFinishAllocation(tail_call=tail_call_ref)]
                )
            )
        else:
            output_events.append(OutputEventFinishAllocation(value=output))
            self._output_event_queue.put(OutputEventBatch(events=output_events))

    def __call_user_function(
        self, args: list[Any], kwargs: dict[str, Any]
    ) -> Any | _TensorlakeFutureWrapper[Future]:
        self._logger.info("running function")
        start_time: float = time.monotonic()

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

    def run_future_runtime_hook(self, future: Future) -> None:
        # NB: This code is called from user function thread. User function thread and aio event loop
        # are blocked. This hook must block the calling thread until all the Futures are fully started.
        # If we do any asyncio await/yield operation here then this will result in concurrent hooks running
        # with non-deterministic completion order.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        with self._runtime_hook_lock:
            try:
                self._run_future_runtime_hook(future)
            except TensorlakeError:
                raise
            except Exception as e:
                self._logger.error(
                    "Unexpected exception in run_future_runtime_hook",
                    exc_info=e,
                )
                raise InternalError("Unexpected error while running futures") from e

    def _create_function_calls(
        self, function_call_output_events: list[OutputEventCreateFunctionCall]
    ) -> None:
        """Creates function calls for the given output events.

        If a function call creation failed, then sets the exception on its future.
        (done by input event processor).
        """
        if len(function_call_output_events) == 0:
            return

        pending_durable_ids: list[str] = [
            event.durable_id for event in function_call_output_events
        ]
        self._output_event_queue.put(
            OutputEventBatch(events=function_call_output_events)
        )

        for durable_id in pending_durable_ids:
            self._future_infos[durable_id].function_call_created.wait()

    def _run_future_runtime_hook(self, user_future: Future) -> None:
        output_events: list[OutputEventCreateFunctionCall] = []

        # NB: To support durability, ordering of running the Futures must be deterministic.
        # SDK automatically starts user futures that are function call
        # or other operation inputs. This is why we walk the Futures tree,
        # not just starting the user_future.
        for future in dfs_bottom_up_unique_only(user_future):
            if future._id in self._future_infos:
                continue  # Future was already started by user.

            # Future is started by user code, cannot be a tail call.
            future_output_events: list[OutputEventCreateFunctionCall] = (
                self._register_future(future, tail_call=False)
            )
            output_events.extend(future_output_events)

        self._create_function_calls(output_events)
        # Check each command completion because tail_call_output_info.future might not create
        # a function call for itself (i.e., it might be a composite future of other function calls).
        for function_call in output_events:
            function_call: OutputEventCreateFunctionCall
            function_call_future_info: FutureInfo = self._future_infos[
                function_call.durable_id
            ]
            if function_call_future_info.future._exception is not None:
                raise function_call_future_info.future._exception

    def wait_futures_runtime_hook(
        self, futures: list[Future], timeout: float | None, return_when: int
    ) -> tuple[list[Future], list[Future]]:
        # NB: This code is called from user function thread. User function code is blocked.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        with self._runtime_hook_lock:
            try:
                return self._wait_futures_runtime_hook(futures, timeout, return_when)
            except TensorlakeError:
                raise
            except Exception as e:
                self._logger.error(
                    "Unexpected exception in wait_futures_runtime_hook",
                    exc_info=e,
                )
                raise InternalError("Unexpected error while waiting for futures")

    def _wait_futures_runtime_hook(
        self, futures: list[Future], timeout: float | None, return_when: int
    ) -> tuple[list[Future], list[Future]]:
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
        done: list[Future] = []
        not_done: list[Future] = []

        # FIXME: We have to keep track of input events timedout waiting.
        # We have to add an event that this happened in SDK so we replay this on replay.
        # Otherwise, if the input event is delivered after timeout, we will have it in
        # the queue and the next call to _read_input_event will get this old event instead
        # of what it expects.
        #
        # FIXME: When FIRST_COMPLETED or FIRST_FAILURE is used we have to wait for all the
        # future in parallel instead of serially. Without this, if the first future takes
        # a long time to complete, but the second one completes quickly, we still wait
        # for the first one to complete before checking the second one. This is not what customers expect.
        for future in futures:
            try:
                self._wait_future_completion(future, deadline)
            except BaseException as e:
                # Something went wrong while waiting for the future.
                self._logger.error(
                    "Unexpected error while waiting for child future completion",
                    future_id=future._id,
                    exc_info=e,
                )
                future._set_exception(
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
        # NB: all exceptions raised here must be derived from TensorlakeError.
        with self._runtime_hook_lock:
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
            # Required for pretty_print called when converting Future to str.
            set_allocation_id_context_variable(self._allocation_id)
            try:
                self._wait_future_completion(future, deadline=None)
            except BaseException as e:
                self._logger.error(
                    "Unexpected error while waiting for child future completion",
                    future_id=future._id,
                    exc_info=e,
                )
                future._set_exception(
                    InternalError(
                        f"Unexpected error while waiting for child future completion: {e}"
                    )
                )
            user_aio_loop.call_soon_threadsafe(user_aio_loop_future.set_result, None)

        threading.Thread(target=background_wait, daemon=True).start()
        # FIXME: The order of self._wait_future_completion() calls depends on OS threading timings.
        # This is not deterministic. We should block the current thread (event loop) until the thread actually
        # starts.
        yield from user_aio_loop_future.__await__()
        # The coroutine is done running. It can't be started again by user code.
        # Clear the hard reference.
        future._coroutine = None

    def register_coroutine_runtime_hook(
        self, coroutine: Coroutine, future: Future
    ) -> None:
        with self._runtime_hook_lock:
            self._coroutine_to_future[coroutine] = future

    def coroutine_to_future_runtime_hook(self, coroutine: Coroutine) -> Future | None:
        # Do not take self._runtime_hook_lock because this hook can be called from another hook
        # via i.e. _unwrap_future() call and because this hook is trivial.
        return self._coroutine_to_future.get(coroutine, None)

    def _register_future(
        self, future: Future, tail_call: bool
    ) -> list[OutputEventCreateFunctionCall]:
        """Register a future and return any OutputEventCreateFunctionCall generated.

        Raises InternalError on error.
        """
        if future._coroutine is not None and not future._run_hook_was_called:
            # We're starting the future for the user.
            # Close the coroutine to prevent "RuntimeWarning: coroutine '...' was never awaited" warning
            # because user code is not expected to await the coroutine and it's a user code bug if it does.
            future._run_hook_was_called = True
            future._coroutine.close()
            future._coroutine = None

        if future._id in self._future_infos:
            raise InternalError(f"Future with ID {future._id} is already registered.")

        durable_id: str = future_durable_id(
            future=future,
            parent_function_call_id=self._function_call_id,
            previous_future_durable_id=self._previous_future_durable_id,
            future_infos=self._future_infos,
        )
        self._previous_future_durable_id = durable_id

        future_info: FutureInfo = FutureInfo(
            future=future,
            durable_id=durable_id,
            map_future_output=None,
            reduce_future_output=None,
        )
        self._future_infos[future._id] = future_info
        self._future_infos[durable_id] = future_info

        if isinstance(future, ListFuture):
            return self._register_list_future(future_info)
        elif isinstance(future, ReduceOperationFuture):
            return self._register_reduce_operation_future(future_info, tail_call)
        elif isinstance(future, FunctionCallFuture):
            return [self._make_call_function_output_event(future_info, tail_call)]
        else:
            raise InternalError(
                f"Unsupported Future type: {type(future)} with ID {future._id}"
            )

    def _register_list_future(
        self, future_info: FutureInfo
    ) -> list[OutputEventCreateFunctionCall]:
        # Server can't run ListFuture, we need to run each list item separately as a new
        # internal (not user visible) Future. All child Futures of user_future are already running.
        user_future: ListFuture = future_info.future

        if user_future._metadata.kind != _FutureListKind.MAP_OPERATION:
            raise InternalError(
                f"Unsupported ListFuture kind: {user_future._metadata.kind}"
            )
        function: Function = get_function(user_future._metadata.function_name)

        items: list[_TensorlakeFutureWrapper[Future] | Any] | ListFuture = (
            _unwrap_future(user_future._items)
        )
        map_inputs: list[_TensorlakeFutureWrapper[Future] | Any]
        if isinstance(items, ListFuture):
            inputs_future_info: FutureInfo = self._future_infos[items._id]
            map_inputs = inputs_future_info.map_future_output
        else:
            map_inputs = items

        output_events: list[OutputEventCreateFunctionCall] = []
        map_outputs: list[FunctionCallFuture] = []
        for input in map_inputs:
            mapped_input: FunctionCallFuture = derived_function_call_future(
                user_future, function, input
            )
            # No output override for list future because it can't be returned from tail call.
            # Strictly one level of recursion per mapped item, so it's okay.
            cmds: list[OutputEventCreateFunctionCall] = self._register_future(
                mapped_input, tail_call=False
            )
            output_events.extend(cmds)
            map_outputs.append(mapped_input)

        future_info.map_future_output = map_outputs
        return output_events

    def _register_reduce_operation_future(
        self, future_info: FutureInfo, tail_call: bool
    ) -> list[OutputEventCreateFunctionCall]:
        # Server can't run ReduceOperationFuture, we need to run each list item separately as a new
        # internal (not user visible) Future. All child Futures of user_future are already running.
        user_future: ReduceOperationFuture = future_info.future
        function: Function = get_function(user_future._function_name)

        inputs: list[_TensorlakeFutureWrapper[Future] | Any] = []
        initial: Future | Any = _unwrap_future(user_future._initial)
        if initial is not _InitialMissing:
            inputs.append(initial)

        items: list[_TensorlakeFutureWrapper[Future] | Any] | ListFuture = (
            _unwrap_future(user_future._items)
        )
        if isinstance(items, ListFuture):
            inputs_future_info: FutureInfo = self._future_infos[items._id]
            inputs.extend(inputs_future_info.map_future_output)
        else:
            inputs.extend(items)

        if len(inputs) == 0:
            raise SDKUsageError("reduce of empty iterable with no initial value")

        if len(inputs) == 1:
            # Child future inputs[0] is already running due to DFS bottom up traversal.
            future_info.reduce_future_output = _unwrap_future(inputs[0])
            return []

        # Create a chain of function calls to reduce all args one by one.
        # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
        # using string concat function into "abcd".

        # inputs now contain at least two items.
        output_events: list[OutputEventCreateFunctionCall] = []
        last_future: Future = derived_function_call_future(
            user_future, function, inputs[0], inputs[1]
        )
        for input in inputs[2:]:
            # Strictly one level of recursion per reduced item, so it's okay.
            cmds: list[OutputEventCreateFunctionCall] = self._register_future(
                last_future, tail_call=False
            )
            output_events.extend(cmds)
            last_future = derived_function_call_future(
                user_future, function, last_future, input
            )
        cmds: list[OutputEventCreateFunctionCall] = self._register_future(
            last_future, tail_call=tail_call
        )
        output_events.extend(cmds)

        future_info.reduce_future_output = last_future
        return output_events

    def _make_call_function_output_event(
        self, future_info: FutureInfo, tail_call: bool
    ) -> OutputEventCreateFunctionCall:
        future: FunctionCallFuture = future_info.future

        # Convert args: replace Future references with FunctionCallRef/FunctionCallCollectionRef.
        args: list[Any | FunctionCallRef | FunctionCallCollectionRef] = [
            self._resolve_arg(arg) for arg in future._args
        ]
        kwargs: dict[str, Any | FunctionCallRef | FunctionCallCollectionRef] = {
            k: self._resolve_arg(v) for k, v in future._kwargs.items()
        }

        return OutputEventCreateFunctionCall(
            durable_id=future_info.durable_id,
            function_name=future._function_name,
            args=args,
            kwargs=kwargs,
            is_tail_call=tail_call,
            start_delay=future._start_delay,
        )

    def _resolve_arg(
        self, value: Any | _TensorlakeFutureWrapper[Future]
    ) -> Any | FunctionCallRef | FunctionCallCollectionRef:
        """Resolve a Future arg to FunctionCallRef/FunctionCallCollectionRef, or keep as raw value."""
        unwrapped: Any | Future = _unwrap_future(value)
        if isinstance(unwrapped, FunctionCallFuture):
            future_info: FutureInfo | None = self._future_infos.get(unwrapped._id)
            if future_info is None:
                raise InternalError(
                    f"FunctionCallFuture arg with ID {unwrapped._id} is not registered."
                )
            return FunctionCallRef(durable_id=future_info.durable_id)
        elif isinstance(unwrapped, ListFuture):
            future_info: FutureInfo | None = self._future_infos.get(unwrapped._id)
            if future_info is None:
                raise InternalError(
                    f"ListFuture arg with ID {unwrapped._id} is not registered."
                )
            durable_ids: list[str] = [
                self._future_infos[item._id].durable_id
                for item in future_info.map_future_output
            ]
            return FunctionCallCollectionRef(durable_ids=durable_ids)
        elif isinstance(unwrapped, ReduceOperationFuture):
            future_info: FutureInfo | None = self._future_infos.get(unwrapped._id)
            if future_info is None:
                raise InternalError(
                    f"ReduceOperationFuture arg with ID {unwrapped._id} is not registered."
                )
            output = future_info.reduce_future_output
            if isinstance(output, Future):
                # Follow the chain to the final FunctionCallFuture.
                return self._resolve_arg(output)
            else:
                # Single-item reduce with plain value output.
                return output
        elif isinstance(unwrapped, Future):
            raise InternalError(
                f"Unsupported Future type as argument: {type(unwrapped)} "
                f"with ID {unwrapped._id}"
            )
        return value

    def _wait_future_completion(self, future: Future, deadline: float | None) -> None:
        """Waits for the completion of the future and sets its result.

        Raises Exception on unexpected internal error. Normally all exceptions are set on the future itself.
        """
        if future.done():
            # Short circuit just for performance optimization
            # so we don't call Server to get the result again.
            return

        future_info: FutureInfo | None = self._future_infos.get(future._id)
        if future_info is None:
            raise InternalError(f"Unknown Future with ID {future._id} is not tracked.")

        if isinstance(future, ListFuture):
            self._wait_list_future_completion(future_info, deadline)
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
        cmd: OutputEventCreateFunctionCallWatcher = (
            OutputEventCreateFunctionCallWatcher(
                function_call_durable_id=future_info.durable_id,
            )
        )
        self._output_event_queue.put(OutputEventBatch(events=[cmd]))

        result_wait_timeout: float | None = (
            deadline - time.monotonic() if deadline is not None else None
        )
        if not future_info.function_call_finished.wait(timeout=result_wait_timeout):
            # FIXME: This timeout is not replayable, add an output event for this and then wait for its input event to arrive
            # or the watcher result to arrive, whichever happens first.
            future_info.future._set_exception(TimeoutError())

    def _wait_list_future_completion(
        self, future_info: FutureInfo, deadline: float | None
    ) -> None:
        """Wait for the completion of the future representing a ListFuture and sets its result or exception.

        Raises Exception on unexpected internal error. Normally all exceptions are set on the future itself.
        """
        future: ListFuture = future_info.future
        # Reconstruct the original collection out of individual futures.
        collection: list[Any] = []
        exception: TensorlakeError | None = None
        is_timeout: bool = False

        for item in future_info.map_future_output:
            if deadline is not None and deadline - time.monotonic() <= 0:
                is_timeout = True
                break

            self._wait_future_completion(item, deadline)
            if item.exception is None:
                collection.append(item.result())
            else:
                exception = item.exception
                break

        if is_timeout:
            future._set_exception(TimeoutError())
        elif exception is not None:
            future._set_exception(exception)
        else:
            future._set_result(collection)

    def _wait_reduce_operation_future_completion(
        self, future_info: FutureInfo, deadline: float | None
    ) -> None:
        """Wait for the completion of the future representing a ReduceOperationFuture and sets its result or exception.

        Raises Exception on unexpected internal error. Normally all exceptions are set on the future itself.
        """
        future: ReduceOperationFuture = future_info.future
        reduce_output: Future | Any | None = future_info.reduce_future_output

        if reduce_output is None:
            future._set_exception(
                InternalError("Reduce operation future is missing the output future.")
            )
            return

        if isinstance(reduce_output, Future):
            # FIXME: Recursive call. Max 1000 recursion depth is allowed in Python by default.
            self._wait_future_completion(reduce_output, deadline)
            if reduce_output.exception is not None:
                future._set_exception(reduce_output.exception)
            else:
                future._set_result(reduce_output.result())
        else:
            # This can happen when we have only one item to reduce, in that case we shortcut
            # and set the output directly without creating a Future for it.
            future._set_result(reduce_output)

    def _process_input_events(self) -> None:
        while True:
            input_event: InputEventType = self._input_event_queue.get()
            if isinstance(input_event, _InputEventShutdown):
                break
            elif isinstance(input_event, InputEventEmergencyShutdown):
                self._process_input_event_emergency_shutdown(input_event)
            elif isinstance(input_event, InputEventFunctionCallCreated):
                self._process_input_event_function_call_created(input_event)
            elif isinstance(input_event, InputEventFunctionCallWatcherResult):
                self._process_input_event_function_call_watcher_result(input_event)
            else:
                self._logger.error(
                    "Unknown input event type received",
                    input_event=input_event,
                )

    def _process_input_event_emergency_shutdown(
        self, event: InputEventEmergencyShutdown
    ) -> None:
        # TODO: Implement.
        self._output_event_queue.put(
            OutputEventBatch(
                events=[
                    OutputEventFinishAllocation(
                        internal_exception=event.internal_exception
                    )
                ]
            )
        )

    def _process_input_event_function_call_created(
        self, event: InputEventFunctionCallCreated
    ) -> None:
        # This event is used for both regular and tail call function calls.
        future_info: FutureInfo | None = self._future_infos.get(event.durable_id)
        if future_info is None:
            # Info because this might be some stale event we're not interested about anymore.
            self._logger.info(
                "Function call created event received for unknown durable ID",
                durable_id=event.durable_id,
            )
            return

        if event.exception is not None:
            future_info.future._set_exception(event.exception)
        future_info.function_call_created.set()
        # FIXME: We can only return once the runtime hook blocked on this returns and reads the current Future state,
        # not some later state that we might update in i.e. the next event.

    def _process_input_event_function_call_watcher_result(
        self, event: InputEventFunctionCallWatcherResult
    ) -> None:
        future_info: FutureInfo | None = self._future_infos.get(
            event.function_call_durable_id
        )
        if future_info is None:
            # Info because this might be some stale event we're not interested about anymore.
            self._logger.info(
                "Function call watcher result event received for unknown durable ID",
                durable_id=event.function_call_durable_id,
            )
            return

        future: Future = future_info.future
        if event.exception is None:
            future._set_result(event.output)
        else:
            future._set_exception(event.exception)

        # Future result is set, we can remove the Future from tracking.
        del self._future_infos[future._id]
        del self._future_infos[future_info.durable_id]
        future_info.function_call_finished.set()
        # FIXME: We can only return once the runtime hook blocked on this returns and reads the current Future state,
        # not some later state that we might update in i.e. the next event.
