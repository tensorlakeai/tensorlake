import asyncio
import inspect
import queue
import threading
import time
import weakref
from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass, field
from typing import Any

from tensorlake.applications import (
    RETURN_WHEN,
    Function,
    Future,
    InternalError,
    RequestContext,
    SDKUsageError,
    TensorlakeError,
)
from tensorlake.applications.algorithms import (
    dfs_bottom_up_unique_only,
)
from tensorlake.applications.interface.futures import (
    FunctionCallFuture,
    Future,
    MapFuture,
    ReduceOperationFuture,
    _InitialMissing,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.metadata import SPLITTER_INPUT_MODE
from tensorlake.applications.request_context.contextvar import (
    set_current_request_context,
)

from ..contextvars import set_allocation_id_context_variable
from .durable_id import future_durable_id
from .input_events import (
    InputEventEmergencyShutdown,
    InputEventFunctionCallCreated,
    InputEventFunctionCallWatcherCreated,
    InputEventFunctionCallWatcherResult,
    InputEventType,
    _InputEventStopInputEventProcessing,
)
from .output_events import (
    FunctionCallRef,
    OutputEventBatch,
    OutputEventCreateFunctionCall,
    OutputEventCreateFunctionCallWatcher,
    OutputEventFinishAllocation,
    SpecialFunctionCallSettings,
)
from .special_function_calls import special_function_call


# Use BaseException as a base class to ensure user code is not catching it.
# All such exception like CancelledError, KeyboardInterrupt, SystemExit do the same.
class _TensorlakeEventLoopExit(BaseException):
    """Internal exception used to shutdown the event loop immediately.

    Used for errors that if reported to user code will lead it through a different code path
    than the one recorded in replay history. This is to ensure deterministic and replayable execution.

    This is not a subclass of TensorlakeError and not part of the SDK interface exceptions because
    it must not be caught by user code. It derives from BaseException (like CancelledError,
    KeyboardInterrupt, SystemExit) to bypass generic `except Exception` handlers in user code.
    """

    def __init__(self):
        super().__init__("Emergency Tensorlake event loop shutdown")


@dataclass
class _FutureInfo:
    # Original Future created by user code.
    future: Future
    # Set when the function call of this Future was created (or failed to get created).
    function_call_created: threading.Event
    # Idempotency check in case we wait for the same Future concurrently.
    watcher_creation_started: bool
    # Set when the function call watcher was created (or failed to get created).
    watcher_created: threading.Event
    # True when the function call of this Future finished (or failed).
    # This means that the Future is fully completed and all its state is final.
    function_call_finished: bool
    # Callbacks invoked by input event thread when function_call_finished is set.
    # Used by _await_future_runtime_hook to deterministically resume asyncio coroutines.
    function_call_finished_callbacks: list[Callable[[], None]]
    # Monotonically increasing counter for deterministic completion ordering.
    # Set by input event thread when function_call_finished is set.
    completion_order: int


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
        self._special_settings: SpecialFunctionCallSettings | None = None

        # Ensures that each runtime hook is executed by only one thread simultaneously.
        # This ensures no concurrency bugs in runtime hooks i.e. where we create the same function
        # call twice. Aquisition of this lock is a non-deterministic operation but this has no impact
        # on determinism because if user code runs multiple concurrent threads and calls runtime hooks
        # from them then the application is non-deterministic regardless of this lock.
        self._runtime_hook_lock: threading.Lock = threading.Lock()
        # Allocation Execution state.
        #
        # Durable ID of the previous future started by this allocation.
        self._previous_future_durable_id: str = function_call_id
        # Futures that were created (started) during this allocation.
        # Future ID -> Future Durable ID.
        self._future_durable_id: dict[str, str] = {}
        # Future Durable ID -> FutureInfo.
        self._future_infos_by_durable_id: dict[str, _FutureInfo] = {}
        # Coroutine -> Future, use weakref so once a coroutine is no longer used
        # it's deleted from the mapping automatically. This is required because coroutine
        # objects are owned by user code and so is their lifecycle.
        self._coroutine_to_future: weakref.WeakKeyDictionary = (
            weakref.WeakKeyDictionary()
        )
        self._is_emergency_shutdown: bool = False
        # Signaled every time a function call is completed.
        self.function_call_completed: threading.Condition = threading.Condition()
        # Monotonic counter for tracking of function call completion ordering.
        self.function_call_completion_counter: int = 0

        # Queue for output events generated by user code. Event loop guarantees that output events are generated in deterministic order
        # if user code is deterministic. This ensures replayability.
        self._output_event_queue: queue.Queue[OutputEventBatch] = queue.Queue()
        # Events coming from Server.
        # WARNING: the events have to be processed in strict order with each event fully processed before the next.
        # Fully processed means that all the runtime hooks that should exit after processing an event actually exit.
        self._input_event_queue: queue.Queue[InputEventType] = queue.Queue()
        # Thread that processes input events.
        # Applies input events one by one in deterministic way.
        self._input_event_thread: threading.Thread | None = None
        # Thread that runs user code and runtime hooks.
        # Generates output events in deterministic, replayable order.
        self._user_thread: threading.Thread | None = None

    def _check_emergency_shutdown(self) -> None:
        """Raises _TensorlakeEventLoopExit if emergency shutdown is in progress.

        Must be called on entry to every runtime hook and after any blocking IO/event wait.
        """
        if self._is_emergency_shutdown:
            raise _TensorlakeEventLoopExit()

    def start(
        self,
        args: list[Any],
        kwargs: dict[str, Any],
        special_settings: SpecialFunctionCallSettings | None,
    ) -> None:
        """Starts user code in a thread. Non-blocking.

        Doesn't raise any exceptions.
        """
        self._special_settings = special_settings
        self._user_thread = threading.Thread(
            target=self._run_user_function,
            args=(args, kwargs),
            daemon=True,
        )
        self._user_thread.start()

    def join(self) -> None:
        """Waits for event loop to exit.

        Event loop exit is when user code and all other service threads exited.
        Doesn't raise any exceptions.
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
        if isinstance(event, InputEventEmergencyShutdown):
            # Minimize any side effects applied before emergency shutdown.
            while not self._input_event_queue.empty():
                try:
                    self._input_event_queue.get_nowait()
                except queue.Empty:
                    break

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
            self._input_event_queue.put(_InputEventStopInputEventProcessing())
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
        except _TensorlakeEventLoopExit as e:
            self._output_event_queue.put(
                OutputEventBatch(
                    events=[OutputEventFinishAllocation(internal_exception=e)]
                )
            )
            return
        except BaseException as e:
            self._output_event_queue.put(
                OutputEventBatch(events=[OutputEventFinishAllocation(user_exception=e)])
            )
            return

        # This is our code.
        output: Any | Future = _unwrap_future(output)
        if isinstance(output, Future):
            self._handle_tail_call_user_function_output(output)
        else:
            self._output_event_queue.put(
                OutputEventBatch(events=[OutputEventFinishAllocation(value=output)])
            )

    def _handle_tail_call_user_function_output(self, output: Future) -> None:
        function_calls: list[OutputEventCreateFunctionCall] = []
        # This is our code.
        #
        # SDK automatically starts tail call futures and their inputs.
        # This is why we walk the Futures tree, not just starting the output.
        for future in dfs_bottom_up_unique_only(output):
            if future._id in self._future_durable_id:
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

            function_calls.extend(
                self._register_future(
                    future,
                    is_tail_call_output=(future is output),
                )
            )

        self._create_function_calls(function_calls)
        if output._exception is None:
            self._output_event_queue.put(
                OutputEventBatch(
                    events=[
                        OutputEventFinishAllocation(
                            tail_call=FunctionCallRef(
                                durable_id=self._future_durable_id[output._id]
                            )
                        )
                    ]
                )
            )
        else:
            self._output_event_queue.put(
                OutputEventBatch(
                    events=[
                        OutputEventFinishAllocation(
                            internal_exception=output._exception
                        )
                    ]
                )
            )

    def __call_user_function(
        self, args: list[Any], kwargs: dict[str, Any]
    ) -> Any | _TensorlakeFutureWrapper[Future]:
        self._logger.info("running function")
        start_time: float = time.monotonic()

        try:
            if self._special_settings is not None:
                return special_function_call(
                    self._special_settings, self._function, args, kwargs, self._logger
                )
            elif inspect.iscoroutinefunction(self._function):
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
        # NB: all exceptions raised here must be TensorlakeError or _TensorlakeEventLoopExit.
        self._check_emergency_shutdown()
        try:
            with self._runtime_hook_lock:
                self._run_future_runtime_hook(future)
        except TensorlakeError:
            raise
        except _TensorlakeEventLoopExit:
            raise
        except BaseException as e:
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
            self._future_infos_by_durable_id[durable_id].function_call_created.wait()
            self._check_emergency_shutdown()

    def _run_future_runtime_hook(self, user_future: Future) -> None:
        output_events: list[OutputEventCreateFunctionCall] = []

        # NB: To support durability, ordering of running the Futures must be deterministic.
        # SDK automatically starts user futures that are function call
        # or other operation inputs. This is why we walk the Futures tree,
        # not just starting the user_future.
        for future in dfs_bottom_up_unique_only(user_future):
            if future._id in self._future_durable_id:
                continue  # Future was already started by user.

            # Future is started by user code, cannot be a tail call.
            future_output_events: list[OutputEventCreateFunctionCall] = (
                self._register_future(future, is_tail_call_output=False)
            )
            output_events.extend(future_output_events)

        self._create_function_calls(output_events)
        user_future_info: _FutureInfo = self._future_infos_by_durable_id[
            self._future_durable_id[user_future._id]
        ]
        if user_future_info.future._exception is not None:
            raise user_future_info.future._exception

    def wait_futures_runtime_hook(
        self, futures: list[Future], timeout: float | None, return_when: int
    ) -> tuple[list[Future], list[Future]]:
        # NB: This code is called from user function thread. User function code is blocked.
        #
        # NB: all exceptions raised here must be TensorlakeError or _TensorlakeEventLoopExit.
        self._check_emergency_shutdown()
        try:
            with self._runtime_hook_lock:
                return self._wait_futures_runtime_hook(futures, timeout, return_when)
        except TensorlakeError:
            raise
        except _TensorlakeEventLoopExit:
            raise
        except BaseException as e:
            self._logger.error(
                "Unexpected exception in wait_futures_runtime_hook",
                exc_info=e,
            )
            raise InternalError("Unexpected error while waiting for futures") from e

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

        # Determinism: all futures in the returned done, not_done lists have the same order
        # as in the original futures list.

        # For FIRST_COMPLETED/FIRST_FAILURE, return immediately if a future is already done.
        if return_when in (RETURN_WHEN.FIRST_COMPLETED, RETURN_WHEN.FIRST_FAILURE):
            already_done: list[Future] = [f for f in futures if f.done()]
            already_not_done: list[Future] = [f for f in futures if not f.done()]
            if len(already_done) > 0:
                # Return all already_done futures becase:
                # 1. User code already has visibility into them. Returning a done Future in not_done list might break user code assumptions.
                # 2. This is deterministic because we take a "snapshot" of deterministic Futures state here.
                return already_done, already_not_done

        # Create watchers for all not done futures.
        futures_that_need_watchers: list[Future] = [f for f in futures if not f.done()]
        self._create_watchers(futures_that_need_watchers, deadline)

        pending: list[tuple[Future, _FutureInfo]] = []
        for future in futures:
            if future.done():
                continue
            # No _FutureInfo exists for already done futures.
            future_info: _FutureInfo = self._future_infos_by_durable_id[
                self._future_durable_id[future._id]
            ]
            pending.append((future, future_info))

        # Parallel wait loop: wait for completion notifications from input event thread.
        done_infos: list[tuple[Future, _FutureInfo]] = []
        with self.function_call_completed:
            while True:
                self._check_emergency_shutdown()

                # Scan for newly completed futures.
                newly_done: list[tuple[Future, _FutureInfo]] = [
                    (f, fi) for f, fi in pending if fi.function_call_finished
                ]
                done_infos.extend(newly_done)
                pending = [
                    (f, fi) for f, fi in pending if not fi.function_call_finished
                ]

                # Check exit condition based on return_when.
                if return_when == RETURN_WHEN.FIRST_COMPLETED:
                    if len(done_infos) > 0:
                        break
                elif return_when == RETURN_WHEN.FIRST_FAILURE:
                    if any(f.exception is not None for f, _ in newly_done):
                        break
                    if len(pending) == 0:
                        break
                else:  # return_when == RETURN_WHEN.ALL_COMPLETED
                    if len(pending) == 0:
                        break

                self.function_call_completed.wait()

        # Build result in original futures list order.
        if return_when == RETURN_WHEN.ALL_COMPLETED:
            done: list[Future] = [f for f in futures]
            not_done: list[Future] = []
        else:
            # Only return a single winner Future with lowest completion_order.
            # If we return all done futures ordered by completion_order,
            # this breaks determinism because the number of done futures
            # depends on OS thread scheduling and delay in delivery of input events.
            winner: Future = min(done_infos, key=lambda item: item[1].completion_order)[
                0
            ]
            done = [f for f in futures if f is winner]
            not_done = [f for f in futures if f is not winner]

        return done, not_done

    def await_future_runtime_hook(self, future: Future) -> Generator[None, None, Any]:
        # NB: This code is called from user async function thread.
        # NB: all exceptions raised here must be TensorlakeError or _TensorlakeEventLoopExit.
        self._check_emergency_shutdown()
        try:
            with self._runtime_hook_lock:
                return self._await_future_runtime_hook(future)
        except TensorlakeError:
            raise
        except _TensorlakeEventLoopExit:
            raise
        except BaseException as e:
            self._logger.error(
                "Unexpected exception in await_future_runtime_hook",
                exc_info=e,
            )
            raise InternalError("Unexpected error while awaiting future") from e

    def _await_future_runtime_hook(self, future: Future) -> Generator[None, None, Any]:
        future_info: _FutureInfo = self._future_infos_by_durable_id[
            self._future_durable_id[future._id]
        ]
        user_aio_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        # aio Future result is set to None because the actual result is stored in the Tensorlake Future.
        # This aio Future is only used for waiting in user code, not for getting the result.
        user_aio_loop_future: asyncio.Future = user_aio_loop.create_future()
        # Use a list of callback in case if multiple concurrent awaits are running on the same Future.
        # The callback is called from input event thread when the Future is completed.
        # This ensures determenistic completion of this await because input events are processed in FIFO order
        # and CPython aio event loops callbacks are processed in FIFO order too.
        future_info.function_call_finished_callbacks.append(
            lambda: user_aio_loop.call_soon_threadsafe(
                user_aio_loop_future.set_result, None
            )
        )

        self._create_watchers([future], deadline=None)

        # Input event thread calls user_aio_loop.call_soon_threadsafe(user_aio_loop_future.set_result, None)
        yield from user_aio_loop_future.__await__()
        self._check_emergency_shutdown()
        # The coroutine is done running. It can't be started again by user code. Clear the hard reference.
        future._coroutine = None

    def register_coroutine_runtime_hook(
        self, coroutine: Coroutine, future: Future
    ) -> None:
        self._check_emergency_shutdown()
        # Not taking self._runtime_hook_lock because this hook is trivial.
        self._coroutine_to_future[coroutine] = future

    def coroutine_to_future_runtime_hook(self, coroutine: Coroutine) -> Future | None:
        self._check_emergency_shutdown()
        # Not taking self._runtime_hook_lock because this hook is trivial and gets called
        # from other hooks by _unwrap_future.
        return self._coroutine_to_future.get(coroutine, None)

    def _register_future(
        self, future: Future, is_tail_call_output: bool
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

        if future._id in self._future_durable_id:
            raise InternalError(f"Future with ID {future._id} is already registered.")

        # If user code calls this durable id computation from multiple concurrent threads,
        # we could read the same previous ID and thus generate wrong ID for different futures.
        # This breaks replay. Locking this section would not help because lock acquisition
        # order between threads is non-deterministic. Multi-threaded user code makes
        # the application non-deterministic regardless if use a lock here or not.
        durable_id: str = future_durable_id(
            future=future,
            parent_function_call_id=self._function_call_id,
            previous_future_durable_id=self._previous_future_durable_id,
            future_durable_ids=self._future_durable_id,
        )
        self._previous_future_durable_id = durable_id

        future_info: _FutureInfo = _FutureInfo(
            future=future,
            function_call_created=threading.Event(),
            watcher_creation_started=False,
            watcher_created=threading.Event(),
            function_call_finished=False,
            function_call_finished_callbacks=[],
            completion_order=-1,
        )
        self._future_durable_id[future._id] = durable_id
        self._future_infos_by_durable_id[durable_id] = future_info

        if isinstance(future, MapFuture):
            return self._register_map_future(future_info, is_tail_call_output)
        elif isinstance(future, ReduceOperationFuture):
            return self._register_reduce_operation_future(
                future_info, is_tail_call_output
            )
        elif isinstance(future, FunctionCallFuture):
            return self._register_function_call_future(future_info, is_tail_call_output)
        else:
            raise InternalError(
                f"Unsupported Future type: {type(future)} with ID {future._id}"
            )

    def _register_map_future(
        self, future_info: _FutureInfo, is_tail_call_output: bool
    ) -> list[OutputEventCreateFunctionCall]:
        user_future: MapFuture = future_info.future

        # Three stage map operation execution:
        # 1. Special splitter function call. Waits for input list to get resolved and creates map function call.
        # 2. Map function calls. Apply map function to its input. (parallel)
        # 3. Special concat function call. Waits for all map function calls to complete and concatenates their results into python list.

        map_inputs: list[_TensorlakeFutureWrapper[Future] | Any] | Future = (
            _unwrap_future(user_future._items)
        )

        # Support user passing a list of Futures.
        splitter_args: list[Any | Future] = (
            map_inputs if isinstance(map_inputs, list) else [map_inputs]
        )
        splitter_input_mode: SPLITTER_INPUT_MODE = (
            SPLITTER_INPUT_MODE.ITEM_PER_ARG
            if isinstance(map_inputs, list)
            else SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG
        )
        return [
            OutputEventCreateFunctionCall(
                durable_id=self._future_durable_id[future_info.future._id],
                function_name=self._function._name,
                args=[self._resolve_arg_value(arg) for arg in splitter_args],
                kwargs={},
                is_tail_call=is_tail_call_output,
                start_delay=user_future._start_delay,
                special_settings=SpecialFunctionCallSettings(
                    is_map_splitter=True,
                    splitter_function_name=user_future._function_name,
                    splitter_input_mode=splitter_input_mode,
                ),
            )
        ]

    def _register_reduce_operation_future(
        self, future_info: _FutureInfo, is_tail_call_output: bool
    ) -> list[OutputEventCreateFunctionCall]:
        user_future: ReduceOperationFuture = future_info.future

        # Two stage reduce operation execution:
        # 1. Special splitter function call. Waits for input list to get resolved and creates chain of reduce function calls.
        # 2. Reduce function calls. Apply reduce function to its inputs one by one until the final result is produced.

        splitter_kwargs: dict[str, Any] = {}
        initial: Future | Any = _unwrap_future(user_future._initial)
        if initial is not _InitialMissing:
            splitter_kwargs["initial"] = initial

        reduce_inputs: list[_TensorlakeFutureWrapper[Future] | Any] | Future = (
            _unwrap_future(user_future._items)
        )
        # Support user passing a list of Futures.
        splitter_args: list[Any | Future] = (
            reduce_inputs if isinstance(reduce_inputs, list) else [reduce_inputs]
        )
        splitter_input_mode: SPLITTER_INPUT_MODE = (
            SPLITTER_INPUT_MODE.ITEM_PER_ARG
            if isinstance(reduce_inputs, list)
            else SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG
        )

        return [
            OutputEventCreateFunctionCall(
                durable_id=self._future_durable_id[future_info.future._id],
                function_name=self._function._name,
                args=[self._resolve_arg_value(arg) for arg in splitter_args],
                kwargs={
                    arg_name: self._resolve_arg_value(arg_value)
                    for arg_name, arg_value in splitter_kwargs.items()
                },
                is_tail_call=is_tail_call_output,
                start_delay=user_future._start_delay,
                special_settings=SpecialFunctionCallSettings(
                    is_reduce_splitter=True,
                    splitter_function_name=user_future._function_name,
                    splitter_input_mode=splitter_input_mode,
                ),
            )
        ]

    def _register_function_call_future(
        self, future_info: _FutureInfo, is_tail_call_output: bool
    ) -> list[OutputEventCreateFunctionCall]:
        future: FunctionCallFuture = future_info.future

        # Convert args: replace Future references with FunctionCallRef.
        args: list[Any | FunctionCallRef] = [
            self._resolve_arg_value(arg) for arg in future._args
        ]
        kwargs: dict[str, Any | FunctionCallRef] = {
            k: self._resolve_arg_value(v) for k, v in future._kwargs.items()
        }

        special_settings: SpecialFunctionCallSettings | None = None
        if (
            is_tail_call_output
            and self._special_settings is not None
            and self._special_settings.is_map_splitter
        ):
            # Tail call output of map splitter is map concat call.
            special_settings = SpecialFunctionCallSettings(
                is_map_concat=True,
            )

        return [
            OutputEventCreateFunctionCall(
                durable_id=self._future_durable_id[future_info.future._id],
                function_name=future._function_name,
                args=args,
                kwargs=kwargs,
                is_tail_call=is_tail_call_output,
                start_delay=future._start_delay,
                special_settings=special_settings,
            )
        ]

    def _resolve_arg_value(
        self, value: Any | _TensorlakeFutureWrapper[Future]
    ) -> Any | FunctionCallRef:
        """Resolve a Future arg to FunctionCallRef, or keep as raw value."""
        unwrapped: Any | Future = _unwrap_future(value)
        if isinstance(unwrapped, Future):
            return FunctionCallRef(
                durable_id=self._future_durable_id[unwrapped._id],
            )
        else:
            return value

    def _create_watchers(self, futures: list[Future], deadline: float | None) -> None:
        """Creates watchers for the supplied Futures.

        Skips Futures whose watchers have already been started (idempotent).
        Blocks until all watcher creations complete. If a watcher creation fails,
        then the exception is set on the corresponding Future.

        Raises Exception on unexpected internal error.
        """
        output_events: list[OutputEventCreateFunctionCallWatcher] = []
        pending_durable_ids: list[str] = []
        for future in futures:
            future_info: _FutureInfo = self._future_infos_by_durable_id[
                self._future_durable_id[future._id]
            ]
            if not future_info.watcher_creation_started:
                future_info.watcher_creation_started = True
                output_events.append(
                    OutputEventCreateFunctionCallWatcher(
                        function_call_durable_id=self._future_durable_id[future._id],
                        deadline=deadline,
                    )
                )
                pending_durable_ids.append(self._future_durable_id[future._id])

        if len(output_events) > 0:
            self._output_event_queue.put(OutputEventBatch(events=output_events))

        for durable_id in pending_durable_ids:
            self._future_infos_by_durable_id[durable_id].watcher_created.wait()
            self._check_emergency_shutdown()

    def _process_input_events(self) -> None:
        while True:
            input_event: InputEventType = self._input_event_queue.get()
            if isinstance(input_event, _InputEventStopInputEventProcessing):
                return
            elif isinstance(input_event, InputEventEmergencyShutdown):
                return self._process_input_event_emergency_shutdown(input_event)
            elif isinstance(input_event, InputEventFunctionCallCreated):
                self._process_input_event_function_call_created(input_event)
            elif isinstance(input_event, InputEventFunctionCallWatcherCreated):
                self._process_input_event_function_call_watcher_created(input_event)
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
        """Processes an emergency shutdown event by waking up all runtime hooks with the given exception.

        Doesn't raise any exceptions.
        """
        self._is_emergency_shutdown = True
        # Wake up all runtime hooks blocked on threading.Event.wait() or asyncio await.
        for future_info in self._future_infos_by_durable_id.values():
            future_info.function_call_created.set()
            future_info.watcher_created.set()
            self._complete_function_call(future_info)

    def _process_input_event_function_call_created(
        self, event: InputEventFunctionCallCreated
    ) -> None:
        """Processes function call created event.

        Doesn't raise any exceptions.
        """
        # This event is used for both regular and tail call function calls.
        future_info: _FutureInfo | None = self._future_infos_by_durable_id.get(
            event.durable_id
        )
        if future_info is None:
            self._logger.warning(
                "Function call created event received for unknown durable ID",
                durable_id=event.durable_id,
            )
            return

        if event.exception is not None:
            future_info.future._set_exception(event.exception)
        future_info.function_call_created.set()
        # Safe to process next input event immediately: each Future's state is written
        # at most once per event type, and subsequent events operate on different Futures.

    def _process_input_event_function_call_watcher_created(
        self, event: InputEventFunctionCallWatcherCreated
    ) -> None:
        """Processes function call watcher created event.

        Doesn't raise any exceptions.
        """
        future_info: _FutureInfo | None = self._future_infos_by_durable_id.get(
            event.durable_id
        )
        if future_info is None:
            self._logger.info(
                "Function call watcher created event received for unknown durable ID",
                durable_id=event.durable_id,
            )
            return

        if event.exception is not None:
            future_info.future._set_exception(event.exception)
        future_info.watcher_created.set()
        # Safe to process next input event immediately: each Future's state is written
        # at most once per event type, and subsequent events operate on different Futures.

    def _process_input_event_function_call_watcher_result(
        self, event: InputEventFunctionCallWatcherResult
    ) -> None:
        """Processes function call watcher result event.

        Doesn't raise any exceptions.
        """
        future_info: _FutureInfo | None = self._future_infos_by_durable_id.get(
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
        del self._future_infos_by_durable_id[self._future_durable_id[future._id]]
        del self._future_durable_id[future._id]
        self._complete_function_call(future_info)

        # Safe to process next input event immediately: the Future was removed from tracking
        # above, so no subsequent input event can modify it. The runtime hook holds a local
        # reference to future_info and reads the Future's final state set before this signal.

    def _complete_function_call(self, future_info: _FutureInfo) -> None:
        future_info.completion_order = self.function_call_completion_counter
        self.function_call_completion_counter += 1
        future_info.function_call_finished = True
        callbacks = future_info.function_call_finished_callbacks
        future_info.function_call_finished_callbacks = []
        for callback in callbacks:
            callback()
        with self.function_call_completed:
            self.function_call_completed.notify_all()
