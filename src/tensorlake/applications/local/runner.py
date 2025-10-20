import threading
import traceback
from concurrent.futures import ALL_COMPLETED as STD_ALL_COMPLETED
from concurrent.futures import FIRST_COMPLETED as STD_FIRST_COMPLETED
from concurrent.futures import FIRST_EXCEPTION as STD_FIRST_EXCEPTION
from concurrent.futures import Future as StdFuture
from concurrent.futures import (
    ThreadPoolExecutor,
)
from concurrent.futures import wait as std_wait
from queue import Empty as QueueEmptyError
from queue import SimpleQueue
from typing import Any, Dict, List

from ..function.application_call import (
    application_function_call_with_serialized_payload,
)
from ..function.function_call import (
    create_self_instance,
)
from ..function.user_data_serializer import (
    deserialize_value,
    function_input_serializer,
    function_output_serializer,
    serialize_value,
)
from ..interface.awaitables import (
    RETURN_WHEN,
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    FunctionCallFuture,
    Future,
    ReduceOperationAwaitable,
    ReduceOperationFuture,
    RuntimeFutureTypes,
)
from ..interface.exceptions import (
    ApplicationValidationError,
    RequestFailureException,
    TensorlakeException,
)
from ..interface.file import File
from ..interface.function import Function
from ..interface.request import Request
from ..interface.request_context import RequestContext
from ..registry import get_function
from ..request_context.request_context_base import RequestContextBase
from ..request_context.request_metrics_recorder import RequestMetricsRecorder
from ..runtime_hooks import (
    clear_run_futures_hook,
    clear_wait_futures_hook,
    set_run_futures_hook,
    set_wait_futures_hook,
)
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .blob_store import BLOB, BLOBStore
from .function_call_future_run import FunctionCallFutureRun
from .future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
    get_current_future_run,
)
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState
from .return_output_future_run import ReturnOutputFutureRun
from .utils import print_user_exception

_LOCAL_REQUEST_ID = "local-request"
# 100 ms  interval for code paths that do polling.
# This keeps our timers accurate enough and doesn't add too much latency and CPU overhead.
_SLEEP_POLL_INTERVAL_SECONDS = 0.1


class LocalRunner:
    def __init__(self, app: Function, app_payload: Any):
        self._app: Function = app
        self._app_payload: Any = app_payload
        # Future ID -> BLOB if future succeeded.
        self._blob_store: BLOBStore = BLOBStore()
        # Future runs that currently exist.
        # Future ID -> LocalFutureRun
        self._future_runs: Dict[str, LocalFutureRun] = {}
        # Exception that caused the request to fail.
        # None when request finished successfully.
        self._request_exception: TensorlakeException | None = None
        # Class name => instance.
        self._class_instances: Dict[str, Any] = {}
        # Class instance constructor can run for minutes,
        # so we need to ensure that we create at most one instance at a time.
        self._class_instances_locks: Dict[str, threading.Lock] = {}
        self._request_context: RequestContext = RequestContextBase(
            request_id=_LOCAL_REQUEST_ID,
            state=LocalRequestState(),
            progress=LocalRequestProgress(),
            metrics=RequestMetricsRecorder(),
        )
        # SimpleQueue[LocalFutureRunResult]
        self._future_run_result_queue: SimpleQueue = SimpleQueue()
        self._future_run_thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            # We need to allow lots of threads at a time because user code blocks
            # on waiting for another function to complete and the chain of blocked
            # user function threads can grow indefinitely.
            max_workers=10000,
            thread_name_prefix="LocalFutureRunner:",
        )

    def run(self) -> Request:
        try:
            set_run_futures_hook(self._runtime_hook_run_futures)
            set_wait_futures_hook(self._runtime_hook_wait_futures)
            input_serializer: UserDataSerializer = function_input_serializer(self._app)
            serialized_payload, content_type = serialize_value(
                self._app_payload, input_serializer
            )
            app_function_call: FunctionCallAwaitable = (
                application_function_call_with_serialized_payload(
                    application=self._app,
                    payload=serialized_payload,
                    payload_content_type=content_type,
                )
            )
            # We can't use awaitable.run() here because we are not running in a LocalFutureRun yet.
            # So we have to create the future run manually.
            self._future_runs[app_function_call.id] = FunctionCallFutureRun(
                local_future=LocalFuture(
                    user_future=FunctionCallFuture(app_function_call),
                    start_delay=None,
                ),
                result_queue=self._future_run_result_queue,
                thread_pool=self._future_run_thread_pool,
                application=self._app,
                function=self._app,
                class_instance=self._function_self_arg(self._app),
                request_context=self._request_context,
            )

            self._control_loop()

            if self._request_exception is not None:
                return LocalRequest(
                    id=_LOCAL_REQUEST_ID,
                    output=None,
                    exception=self._request_exception,
                )

            app_output_blob: BLOB = self._blob_store.get(app_function_call.id)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=_deserialize_blob_value(app_output_blob),
                exception=None,
            )
        except BaseException as e:
            # This is an unexpected exception in LocalRunner code itself.
            # The function run exception is stored in self._exception and handled above.
            print_user_exception(e)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                exception=RequestFailureException("Request failed"),
            )

    def close(self) -> None:
        """Closes the LocalRunner and releases all resources.

        Cancels all running functions and waits for them to finish.
        """
        clear_run_futures_hook()
        clear_wait_futures_hook()
        if self._request_exception is None and not self._finished():
            self._request_exception = RequestFailureException(
                "Request cancelled by user"
            )
        for fr in self._future_runs.values():
            fr.cancel()
        self._future_run_thread_pool.shutdown(wait=True, cancel_futures=True)

    def __enter__(self) -> "LocalRunner":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _runtime_hook_run_futures(
        self, futures: List[Future], start_delay: float | None
    ) -> None:
        """
        Raises TensorlakeException on error (because called from runtime hook).
        """
        # Warning: this is called from user code.
        self._runtime_hook_user_code_cancellation_point()

        for user_future in futures:
            if user_future.id in self._future_runs:
                raise ApplicationValidationError(
                    f"Awaitable {repr(user_future.awaitable)} is already running, \n"
                    f"the same awaitable cannot run multiple times"
                )

            if isinstance(user_future, ReduceOperationFuture):
                self._create_future_run_for_reduce_operation_future(
                    user_future, start_delay
                )
            elif isinstance(user_future, FunctionCallFuture):
                self._create_future_run_for_function_call_future(
                    user_future, start_delay
                )
            else:
                raise TensorlakeException(
                    f"Internal error: unexpected future type: {type(user_future)}"
                )

    def _runtime_hook_wait_futures(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        """
        Raises TensorlakeException on error (because called from runtime hook).
        """
        # Warning: this is called from user code.
        self._runtime_hook_user_code_cancellation_point()

        if return_when == RETURN_WHEN.FIRST_COMPLETED:
            std_return_when = STD_FIRST_COMPLETED
        elif return_when == RETURN_WHEN.FIRST_FAILURE:
            std_return_when = STD_FIRST_EXCEPTION
        elif return_when == RETURN_WHEN.ALL_COMPLETED:
            std_return_when = STD_ALL_COMPLETED
        else:
            raise ApplicationValidationError(
                "Internal error: unexpected return_when value: {}".format(return_when)
            )

        std_futures: List[StdFuture] = []
        for future in futures:
            if future.id not in self._future_runs:
                raise TensorlakeException(
                    "Internal error: future with ID {} is not registered".format(
                        future.id
                    )
                )
            std_futures.append(self._future_runs[future.id].std_future)

        # Std futures are always running as long as their user futures are considered running.
        # Their outcomes (if failed or succeeded) are also synchronized.
        done_std_futures, _ = std_wait(
            std_futures, timeout=timeout, return_when=std_return_when
        )
        done_user_futures: List[Future] = []
        not_done_user_futures: List[Future] = []
        for future in futures:
            local_future_run: LocalFutureRun = self._future_runs[future.id]
            if local_future_run.std_future in done_std_futures:
                done_user_futures.append(future)
            else:
                not_done_user_futures.append(future)

        return done_user_futures, not_done_user_futures

    def _runtime_hook_user_code_cancellation_point(self) -> None:
        """Called from user code to check for cancellation.

        Raises StopLocalFutureRun handled by our LocalFutureRun.
        """
        # Every runtime hook call is a cancellation point for user code.
        # Raise StopFunctionRun to stop executing all futures so LocalRunner.close()
        # can finish asap.
        no_current_future_run = False
        try:
            if get_current_future_run().is_cancelled:
                raise StopLocalFutureRun()
        except LookupError:
            no_current_future_run = True

        # Raise this exception outside of "except" to not have LookupError in
        # "during handling of the above exception, another exception occurred" message.
        if no_current_future_run:
            raise ApplicationValidationError(
                "Tensorlake SDK was called outside of a Tensorlake Function thread."
                "Please only call Tensorlake SDK functions from inside Tensorlake Functions."
            )

    def _finished(self) -> bool:
        if self._request_exception is not None:
            return True

        return all(fr.std_future.done() for fr in self._future_runs.values())

    def _control_loop(self) -> None:
        while not self._finished():
            self._start_runnable_futures()
            # Process one future run result at a time because it takes
            # ~_SLEEP_POLL_INTERVAL_SECONDS at most. This keeps our timers accurate enough.
            self._wait_and_process_future_run_result()
            if self._request_exception is not None:
                # The request failed. Wait until future runs finish and then exit.
                # We have to wait because otherwise future runs will keep printing
                # arbitrary logs to stdout/stderr and hold resources after request
                # should be finished running.
                print("Request failed, waiting for existing future runs to finish...")
                self._wait_all_future_runs()
                break

    def _start_runnable_futures(self) -> None:
        for future_run in self._future_runs.values():
            future: LocalFuture = future_run.local_future

            if not future.start_time_elapsed:
                continue

            if not self._has_all_data_dependencies(future_run):
                continue

            self._start_future_run(future_run)

    def _wait_and_process_future_run_result(self) -> None:
        try:
            # Wait at most 100ms. This is the precision of function call timers.
            result: LocalFutureRunResult = self._future_run_result_queue.get(
                timeout=_SLEEP_POLL_INTERVAL_SECONDS
            )
        except QueueEmptyError:
            # No new result for now.
            return

        self._process_future_run_result(result)

    def _process_future_run_result(self, result: LocalFutureRunResult) -> None:
        future_run: LocalFutureRun = self._future_runs[result.id]
        future: LocalFuture = future_run.local_future
        user_future: RuntimeFutureTypes = future.user_future
        function_name: str = ""
        if isinstance(future_run, FunctionCallFutureRun):
            if not isinstance(user_future, FunctionCallFuture):
                self._handle_future_run_failure(
                    future_run=future_run,
                    exception=TensorlakeException(
                        f"Internal error: unexpected user future type: {type(user_future)} "
                    ),
                )
                return
            function_name = user_future.awaitable.function_name
        elif isinstance(future_run, ReturnOutputFutureRun):
            if not isinstance(user_future, ReduceOperationFuture):
                self._handle_future_run_failure(
                    future_run=future_run,
                    exception=TensorlakeException(
                        f"Internal error: unexpected user future type: {type(user_future)} "
                    ),
                )
                return
            function_name = user_future.awaitable.function_name
        else:
            self._handle_future_run_failure(
                future_run=future_run,
                exception=TensorlakeException(
                    f"Internal error: unexpected future run type: {repr(future_run)} "
                ),
            )
            return

        if isinstance(result.output, Future):
            self._handle_future_run_failure(
                future_run=future_run,
                exception=ApplicationValidationError(
                    f"Function '{function_name}' returned a Future {repr(result.output)}, "
                    "please return an Awaitable or a concrete value instead."
                ),
            )
            return

        elif isinstance(result.output, Awaitable):
            if result.output.id in self._future_runs:
                self._handle_future_run_failure(
                    future_run=future_run,
                    exception=ApplicationValidationError(
                        f"Function '{function_name}' returned an "
                        f"Awaitable {repr(result.output)} which Future is already running. "
                        "Only not running Awaitables can be returned from a function."
                    ),
                )
                return

            if isinstance(result.output, AwaitableList):
                self._handle_future_run_failure(
                    future_run=future_run,
                    exception=ApplicationValidationError(
                        f"Function '{function_name}' returned an AwaitableList {repr(result.output)}. "
                        "An AwaitableList can only be used as a function argument, not returned from it."
                    ),
                )
                return

            try:
                # Recursively call run on the Awaitable. This will create future runs for the Awaitable.
                result.output.run()
            except BaseException as e:
                self._handle_future_run_failure(
                    future_run=future_run,
                    exception=TensorlakeException(
                        f"Failed to run Awaitable returned from function '{function_name}': {str(e)}",
                    ),
                )
                return
        else:
            blob: BLOB | None = None
            if result.exception is None:
                blob = _future_output_value_to_blob(
                    function=function,
                    future_id=future.user_future.id,
                    output=result.output,
                    output_serializer_name_override=future.output_serializer_name_override,
                )
                self._blob_store.put(blob)
            self._handle_future_run_final_output(
                completed_future_run=future_run, blob=blob, exception=result.exception
            )

    def _handle_future_run_failure(
        self,
        future_run: LocalFutureRun,
        exception: TensorlakeException,
    ) -> None:
        self._request_exception = exception
        self._handle_future_run_final_output(
            completed_future_run=future_run,
            blob=None,
            exception=self._request_exception,
        )

    def _wait_all_future_runs(self) -> None:
        for fr in self._future_runs.values():
            if not fr.std_future.done():
                std_wait([fr.std_future])

    def _has_all_data_dependencies(self, future_run: LocalFutureRun) -> bool:
        if isinstance(future_run, FunctionCallFutureRun):
            awaitable: FunctionCallAwaitable = (
                future_run.local_future.user_future.awaitable
            )
            for arg in awaitable.args:
                if isinstance(arg, Awaitable):
                    if not self._blob_store.has(arg.id):
                        return False
            for arg in awaitable.kwargs.values():
                if isinstance(arg, Awaitable):
                    if not self._blob_store.has(arg.id):
                        return False
            return True

        elif isinstance(future_run, ReturnOutputFutureRun):
            # There are not prerequisites for ReturnOutputFutureRun.
            # It just returns an awaitable as a tail call or a value.
            return True

        else:
            self._request_exception = TensorlakeException(
                "Internal error: unexpected future run type: {}".format(
                    type(future_run)
                )
            )
            return False

    def _handle_future_run_final_output(
        self,
        completed_future_run: LocalFutureRun,
        blob: BLOB | None,
        exception: TensorlakeException | None,
    ) -> None:
        """Handles final output of the supplied future run.

        Sets the output or exception on the user future and finishes the future run.
        Called after the future function finished and Awaitable tree that it returned
        is finished and its output is propagated as this future run's output.
        """
        completed_future: LocalFuture = completed_future_run.local_future

        if exception is not None:
            completed_future.user_future.set_exception(exception)
        else:
            # Intentionally do serialize -> deserialize cycle to ensure the same UX as in remote mode.
            completed_future.user_future.set_result(_deserialize_blob_value(blob))

        # Finish std future so wait hooks waiting on it unblock.
        # Success/failure needs to be propagated to std future as well so std wait calls work correctly.
        completed_future_run.finish(is_exception=exception is not None)

        # Propagate output to consumer future if any.
        if completed_future.output_consumer_future_id is None:
            return

        consumer_future_run: LocalFutureRun = self._future_runs[
            completed_future.output_consumer_future_id
        ]
        consumer_future: LocalFuture = consumer_future_run.local_future
        consumer_future_output: BLOB | None = None
        if exception is None:
            consumer_future_output = blob.copy()
            consumer_future_output.id = consumer_future.user_future.id
            self._blob_store.put(consumer_future_output)
        self._handle_future_run_final_output(
            completed_future_run=consumer_future_run,
            blob=consumer_future_output,
            exception=exception,
        )

    def _start_future_run(self, future_run: LocalFutureRun) -> None:
        """Starts the supplied future run.

        The future run's local future must be runnable, i.e. all its
        data dependencies are available in blob store and its start
        time delay has elapsed.
        """
        if isinstance(future_run.local_future.user_future, FunctionCallFuture):
            self._start_function_call_future_run(future_run)
        else:
            self._request_exception = TensorlakeException(
                "Internal error: unexpected future type: {}".format(
                    type(future_run.local_future.user_future)
                )
            )

    def _start_function_call_future_run(self, future_run: LocalFutureRun) -> None:
        local_future: LocalFuture = future_run.local_future
        user_future: FunctionCallFuture = local_future.user_future
        awaitable: FunctionCallAwaitable = user_future.awaitable

        # It's okay to modify the awaitable in place because their internals is not
        # part of SDK interface. We validate when we create a function run that
        # args only contain Awaitables or concrete values.
        for arg_ix, arg in enumerate(awaitable.args):
            if isinstance(arg, Awaitable):
                arg_blob: BLOB = self._blob_store.get(arg.id)
                arg_value: Any = _deserialize_blob_value(arg_blob)
                awaitable.args[arg_ix] = arg_value
        for kwarg_key, kwarg in awaitable.kwargs.items():
            if isinstance(kwarg, Awaitable):
                kwarg_blob: BLOB = self._blob_store.get(kwarg.id)
                kwarg_value: Any = _deserialize_blob_value(kwarg_blob)
                awaitable.kwargs[kwarg_key] = kwarg_value

        future_run.start()

    def _function_self_arg(self, function: Function) -> Any | None:
        fn_class_name: str | None = function._function_config.class_name
        if fn_class_name is None:
            return None

        # No need to lock self._class_instances_lock here because
        # we don't do any IO here so we don't release GIL.
        if fn_class_name not in self._class_instances_locks:
            self._class_instances_locks[fn_class_name] = threading.Lock()

        with self._class_instances_locks[fn_class_name]:
            if fn_class_name not in self._class_instances:
                # NB: This call can take minutes if i.e. a model gets loaded in a GPU.
                self._class_instances[fn_class_name] = create_self_instance(
                    fn_class_name
                )

            return self._class_instances[fn_class_name]

    def _create_future_run_if_awaitable(
        self, arg: Any | Future | Awaitable, start_delay: float | None
    ) -> None:
        """Creates future run for the supplied argument if its a not running Awaitable.

        Raises TensorlakeException on error (because called from runtime hook)."""
        if isinstance(arg, Future):
            raise ApplicationValidationError(
                f"Invalid argument: {repr(arg)} is a Future, "
                "please pass an Awaitable or a concrete value as a function argument."
            )
        elif isinstance(arg, AwaitableList):
            for awaitable in arg.awaitables:
                self._create_future_run_if_awaitable(awaitable, start_delay)
        elif isinstance(arg, Awaitable):
            if arg.id in self._future_runs:
                raise ApplicationValidationError(
                    f"Invalid argument: {repr(arg)} is an Awaitable with already running Future, "
                    "only not running Awaitable can be passed as argument."
                )
            # This will call us recursively to create the future run.
            if start_delay is None:
                arg.run()
            else:
                arg.run_later(start_delay=start_delay)

    def _create_future_run_for_function_call_future(
        self, user_future: FunctionCallFuture, start_delay: float | None
    ) -> None:
        """Called from runtime hook to create LocalFunctionCallFutureRun.

        Raises TensorlakeException on error (because called from runtime hook).
        """
        awaitable: FunctionCallAwaitable = user_future.awaitable

        for arg in awaitable.args:
            self._create_future_run_if_awaitable(arg, start_delay)
        for arg in awaitable.kwargs.values():
            self._create_future_run_if_awaitable(arg, start_delay)

        function: Function = get_function(awaitable.function_name)
        self._future_runs[awaitable.id] = FunctionCallFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                start_delay=start_delay,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            application=self._app,
            function=function,
            class_instance=self._function_self_arg(function),
            request_context=self._request_context,
        )

    def _create_future_run_for_reduce_operation_future(
        self, user_future: ReduceOperationFuture, start_delay: float | None
    ) -> None:
        """Called from runtime hook to create ReduceOperationFuture run.

        Raises TensorlakeException on error (because called from runtime hook).
        """
        awaitable: ReduceOperationAwaitable = user_future.awaitable
        reduce_operation_result_awaitable: Awaitable = awaitable.inputs[0]
        function: Function = get_function(awaitable.function_name)

        # There's no user visible interface to ReturnOutputFutureRun so we have to do everything manually here.
        if len(awaitable.inputs) >= 2:
            # Create a chain of function calls to reduce all inputs one by one.
            # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
            # using string concat function into "abcd".
            previous_function_call_awaitable: FunctionCallAwaitable = (
                function.awaitable(awaitable.inputs[0], awaitable.inputs[1])
            )
            for input_item in awaitable.inputs[2:]:
                previous_function_call_awaitable = function.awaitable(
                    previous_function_call_awaitable, input_item
                )
            reduce_operation_result_awaitable = previous_function_call_awaitable

        self._create_future_run_if_awaitable(
            reduce_operation_result_awaitable, start_delay
        )
        self._future_runs[awaitable.id] = ReturnOutputFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                start_delay=start_delay,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            output=reduce_operation_result_awaitable,
        )


def _deserialize_blob_value(blob: BLOB) -> Any | File:
    return deserialize_value(
        serialized_value=blob.data,
        serialized_value_content_type=blob.content_type,
        serializer=serializer_by_name(blob.serializer_name),
        type_hints=[blob.cls],
    )


def _future_output_value_to_blob(
    function: Function,
    future_id: str,
    output: Any,
    output_serializer_name_override: str | None,
) -> BLOB:
    function_os: UserDataSerializer = function_output_serializer(
        function, output_serializer_name_override
    )
    serialized_output, serialized_output_content_type = serialize_value(
        output, function_os
    )
    return BLOB(
        id=future_id,
        data=serialized_output,
        serializer_name=function_os.name,
        content_type=serialized_output_content_type,
        cls=type(output),
    )
