import threading
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
    ListFuture,
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
from ..user_data_serializer import (
    PickleUserDataSerializer,
    UserDataSerializer,
    serializer_by_name,
)
from .blob_store import BLOB, BLOBStore
from .function_call_future_run import FunctionCallFutureRun
from .future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
    get_current_future_run,
)
from .list_future_run import ListFutureRun
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState
from .return_output_future_run import ReturnOutputFutureRun
from .utils import print_exception

_LOCAL_REQUEST_ID = "local-request"
# 1 ms  interval for code paths that do polling.
# This keeps our timers accurate enough and doesn't add too much latency and CPU overhead.
_SLEEP_POLL_INTERVAL_SECONDS = 0.001


class LocalRunner:
    def __init__(self, app: Function, app_payload: Any):
        self._app: Function = app
        self._app_payload: Any = app_payload
        # Future ID -> BLOB if future run succeeded.
        # TODO: Also serialize all values and store them in BLOBStore
        # to simulate remote mode better.
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

            # Serialize application payload the same way as in remote mode.
            input_serializer: UserDataSerializer = function_input_serializer(self._app)
            serialized_payload, content_type = serialize_value(
                self._app_payload, input_serializer
            )
            app_function_call_awaitable: FunctionCallAwaitable = (
                application_function_call_with_serialized_payload(
                    application=self._app,
                    payload=serialized_payload,
                    payload_content_type=content_type,
                )
            )
            self._create_future_run_for_awaitable(
                awaitable=app_function_call_awaitable,
                existing_awaitable_future=None,
                start_delay=None,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )

            self._control_loop()

            if self._request_exception is not None:
                return LocalRequest(
                    id=_LOCAL_REQUEST_ID,
                    output=None,
                    exception=self._request_exception,
                )

            app_output_blob: BLOB = self._blob_store.get(app_function_call_awaitable.id)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=_deserialize_blob_value(app_output_blob),
                exception=None,
            )
        except BaseException as e:
            # This is an unexpected exception in LocalRunner code itself.
            # The function run exception is stored in self._exception and handled above.
            print_exception(e)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                exception=(
                    RequestFailureException("Request failed")
                    if not isinstance(e, KeyboardInterrupt)
                    else RequestFailureException("Request cancelled by user")
                ),
            )

    def close(self) -> None:
        """Closes the LocalRunner and releases all resources.

        Cancels all running functions and waits for them to finish.
        """
        for fr in self._future_runs.values():
            fr.cancel()
        self._future_run_thread_pool.shutdown(wait=True, cancel_futures=True)
        # Only clear runtime hooks at the very end when nothing can use them.
        clear_run_futures_hook()
        clear_wait_futures_hook()

    def __enter__(self) -> "LocalRunner":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _runtime_hook_run_futures(
        self, futures: List[Future], start_delay: float | None
    ) -> None:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        self._user_code_cancellation_point()

        for user_future in futures:
            self._create_future_run_for_awaitable(
                awaitable=user_future.awaitable,
                existing_awaitable_future=user_future,
                start_delay=start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )

    def _runtime_hook_wait_futures(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        self._user_code_cancellation_point()

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

    def _user_code_cancellation_point(self) -> None:
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

        if not self._future_run_result_queue.empty():
            return False

        return all(fr.std_future.done() for fr in self._future_runs.values())

    def _control_loop(self) -> None:
        while not self._finished():
            self._control_loop_start_runnable_futures()
            # Process one future run result at a time because it takes
            # ~_SLEEP_POLL_INTERVAL_SECONDS at most. This keeps our timers accurate enough.
            self._control_loop_wait_and_process_future_run_result()
            if self._request_exception is not None:
                # The request failed. Wait until future runs finish and then exit.
                # We have to wait because otherwise future runs will keep printing
                # arbitrary logs to stdout/stderr and hold resources after request
                # should be finished running.
                print_exception(self._request_exception)
                break

    def _control_loop_start_runnable_futures(self) -> None:
        for future_run in self._future_runs.values():
            future: LocalFuture = future_run.local_future

            if not future.start_time_elapsed:
                continue

            if not self._has_all_data_dependencies(future_run):
                continue

            self._start_future_run(future_run)

    def _control_loop_wait_and_process_future_run_result(self) -> None:
        try:
            # Wait at most 100ms. This is the precision of function call timers.
            result: LocalFutureRunResult = self._future_run_result_queue.get(
                timeout=_SLEEP_POLL_INTERVAL_SECONDS
            )
        except QueueEmptyError:
            # No new result for now.
            return

        self._control_loop_process_future_run_result(result)

    def _control_loop_process_future_run_result(
        self, result: LocalFutureRunResult
    ) -> None:
        future_run: LocalFutureRun = self._future_runs[result.id]
        future: LocalFuture = future_run.local_future
        user_future: RuntimeFutureTypes = future.user_future

        function_name: str = "<unknown>"
        output_blob_serializer: UserDataSerializer | None = None
        if isinstance(future_run, FunctionCallFutureRun):
            function_name = user_future.awaitable.function_name
            output_blob_serializer = function_output_serializer(
                get_function(function_name),
                future.output_serializer_name_override,
            )
        elif isinstance(future_run, ReturnOutputFutureRun):
            function_name = user_future.awaitable.function_name
            output_blob_serializer = function_output_serializer(
                get_function(function_name),
                future.output_serializer_name_override,
            )
        elif isinstance(future_run, ListFutureRun):
            function_name = "Assembly awaitable list"
            # In remote mode we assemble the list locally and never store it in BLOB store.
            # As we store everything in local mode then we just use the most flexible serializer
            # here that always works.
            output_blob_serializer = PickleUserDataSerializer()
        else:
            self._handle_future_run_failure(
                future_run=future_run,
                exception=TensorlakeException(
                    f"Internal error: unexpected future run type: {repr(future_run)} "
                ),
            )
            return

        if isinstance(result.output, (Awaitable, Future)):
            # This is a very important check for our UX. We can await for AwaitableList
            # but we cannot return it from a function because there's no Python code to
            # reassemble the list from individual resolved awaitables.
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
                self._create_future_run_for_user_object(
                    object=result.output,
                    start_delay=None,
                    output_consumer_future_id=future.user_future.id,
                    output_serializer_name_override=output_blob_serializer.name,
                )
            except BaseException as e:
                print_exception(e)
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
                    future_id=future.user_future.id,
                    output=result.output,
                    output_serializer=output_blob_serializer,
                )
                self._blob_store.put(blob)
                self._handle_future_run_final_output(
                    future_run=future_run, blob=blob, exception=result.exception
                )
            else:
                self._handle_future_run_failure(
                    future_run=future_run, exception=result.exception
                )

    def _handle_future_run_failure(
        self,
        future_run: LocalFutureRun,
        exception: TensorlakeException,
    ) -> None:
        self._request_exception = exception
        self._handle_future_run_final_output(
            future_run=future_run,
            blob=None,
            exception=self._request_exception,
        )

    def _has_all_data_dependencies(self, future_run: LocalFutureRun) -> bool:
        if isinstance(future_run, FunctionCallFutureRun):
            awaitable: FunctionCallAwaitable = (
                future_run.local_future.user_future.awaitable
            )
            for arg in awaitable.args:
                if not self._value_is_available(arg):
                    return False
            for arg in awaitable.kwargs.values():
                if not self._value_is_available(arg):
                    return False
            return True

        elif isinstance(future_run, ReturnOutputFutureRun):
            # There are no prerequisites for ReturnOutputFutureRun.
            # It just returns an awaitable as a tail call or a value.
            return True

        elif isinstance(future_run, ListFutureRun):
            awaitable_list: AwaitableList = (
                future_run.local_future.user_future.awaitable
            )
            return self._value_is_available(awaitable_list)

        else:
            self._handle_future_run_failure(
                future_run=future_run,
                exception=TensorlakeException(
                    "Internal error: unexpected future run type: {}".format(
                        type(future_run)
                    )
                ),
            )
            return False

    def _value_is_available(self, user_object: Any | Awaitable) -> bool:
        if isinstance(user_object, AwaitableList):
            for awaitable in user_object.awaitables:
                if not self._value_is_available(awaitable):
                    return False
            return True
        elif isinstance(user_object, Awaitable):
            return self._blob_store.has(user_object.id)
        else:
            return True  # regular value is always available

    def _handle_future_run_final_output(
        self,
        future_run: LocalFutureRun,
        blob: BLOB | None,
        exception: TensorlakeException | None,
    ) -> None:
        """Handles final output of the supplied future run.

        Sets the output or exception on the user future and finishes the future run.
        Called after the future function finished and Awaitable tree that it returned
        is finished and its output is propagated as this future run's output.
        """
        future: LocalFuture = future_run.local_future

        if exception is not None:
            future.user_future.set_exception(exception)
        else:
            # Intentionally do serialize -> deserialize cycle to ensure the same UX as in remote mode.
            future.user_future.set_result(_deserialize_blob_value(blob))

        # Finish std future so wait hooks waiting on it unblock.
        # Success/failure needs to be propagated to std future as well so std wait calls work correctly.
        future_run.finish(is_exception=exception is not None)

        # Propagate output to consumer future if any.
        if future.output_consumer_future_id is None:
            return

        consumer_future_run: LocalFutureRun = self._future_runs[
            future.output_consumer_future_id
        ]
        consumer_future: LocalFuture = consumer_future_run.local_future
        consumer_future_output: BLOB | None = None
        if exception is None:
            consumer_future_output = blob.copy()
            consumer_future_output.id = consumer_future.user_future.id
            self._blob_store.put(consumer_future_output)
        self._handle_future_run_final_output(
            future_run=consumer_future_run,
            blob=consumer_future_output,
            exception=exception,
        )

    def _start_future_run(self, future_run: LocalFutureRun) -> None:
        """Starts the supplied future run.

        The future run's local future must be runnable, i.e. all its
        data dependencies are available in blob store and its start
        time delay has elapsed.
        """
        if isinstance(future_run, FunctionCallFutureRun):
            self._start_function_call_future_run(future_run)
        elif isinstance(future_run, ReturnOutputFutureRun):
            future_run.start()
        elif isinstance(future_run, ListFutureRun):
            self._start_list_future_run(future_run)
        else:
            self._request_exception = TensorlakeException(
                "Internal error: unexpected future type: {}".format(
                    type(future_run.local_future.user_future)
                )
            )

    def _start_function_call_future_run(
        self, future_run: FunctionCallFutureRun
    ) -> None:
        local_future: LocalFuture = future_run.local_future
        user_future: FunctionCallFuture = local_future.user_future
        awaitable: FunctionCallAwaitable = user_future.awaitable

        # It's okay to modify the awaitable in place because their internals is not
        # part of SDK interface. We validate when we create a function run that
        # args only contain Awaitables or concrete values.
        for arg_ix, arg in enumerate(awaitable.args):
            awaitable.args[arg_ix] = self._reconstruct_value(arg)
        for kwarg_key, kwarg in awaitable.kwargs.items():
            awaitable.kwargs[kwarg_key] = self._reconstruct_value(kwarg)

        future_run.start()

    def _start_list_future_run(self, future_run: ListFutureRun) -> None:
        local_future: LocalFuture = future_run.local_future
        user_future: ListFuture = local_future.user_future
        awaitable_list: AwaitableList = user_future.awaitable

        values: List[Any] = []
        for awaitable in awaitable_list.awaitables:
            values.append(self._reconstruct_value(awaitable))

        future_run.set_resolved_values(values)
        future_run.start()

    def _reconstruct_value(self, user_object: Any | Awaitable) -> Any:
        """Reconstructs the supplied user object's value."""
        if isinstance(user_object, AwaitableList):
            return [self._reconstruct_value(item) for item in user_object.awaitables]
        elif isinstance(user_object, Awaitable):
            blob: BLOB = self._blob_store.get(user_object.id)
            return _deserialize_blob_value(blob)
        else:
            return user_object

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

    def _create_future_run_for_user_object(
        self,
        object: Any | Future | Awaitable,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates future run for the supplied user object if it's an Awaitable.

        Doesn't do anything if it's a concrete value. Raises TensorlakeException on error.
        """
        if isinstance(object, Future):
            raise ApplicationValidationError(
                f"Invalid argument: cannot run Future {repr(object)}, "
                "please pass an Awaitable or a concrete value."
            )

        if isinstance(object, Awaitable):
            return self._create_future_run_for_awaitable(
                awaitable=object,
                existing_awaitable_future=None,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
                output_serializer_name_override=output_serializer_name_override,
            )

    def _create_future_run_for_awaitable(
        self,
        awaitable: Awaitable,
        existing_awaitable_future: Future | None,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates future run for the supplied Awaitable.

        Doesn't create a user Future if existing_awaitable_future is supplied.
        Raises TensorlakeException on error.
        output_consumer_future_id is the ID of the Future that will consume the output of the future run.
        output_serializer_name_override is the name of the serializer to use for serializing
        the output of the future run. This is used when propagating output to consumer future when the
        consumer future expects a specific serialization format.
        """
        if awaitable.id in self._future_runs:
            raise ApplicationValidationError(
                f"Invalid argument: {repr(awaitable)} is an Awaitable with already running Future, "
                "only not running Awaitable can be passed as function argument or returned from function."
            )

        if isinstance(awaitable, AwaitableList):
            self._create_future_run_for_awaitable_list(
                awaitable=awaitable,
                existing_awaitable_future=existing_awaitable_future,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
                output_serializer_name_override=output_serializer_name_override,
            )
        elif isinstance(awaitable, ReduceOperationAwaitable):
            self._create_future_run_for_reduce_operation_awaitable(
                awaitable=awaitable,
                existing_awaitable_future=existing_awaitable_future,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
                output_serializer_name_override=output_serializer_name_override,
            )
        elif isinstance(awaitable, FunctionCallAwaitable):
            self._create_future_run_for_function_call_awaitable(
                awaitable=awaitable,
                existing_awaitable_future=existing_awaitable_future,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
                output_serializer_name_override=output_serializer_name_override,
            )
        else:
            raise ApplicationValidationError(
                f"Unexpected type of awaitable: {type(awaitable)}"
            )

    def _create_future_run_for_awaitable_list(
        self,
        awaitable: AwaitableList,
        existing_awaitable_future: ListFuture | None,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates ListFutureRun for the supplied awaitable.

        Raises TensorlakeException on error.
        """
        if output_consumer_future_id is not None:
            raise TensorlakeException(
                "Internal error: cannot set output consumer future ID on AwaitableList because it can't be returned from a function."
            )
        if output_serializer_name_override is not None:
            raise TensorlakeException(
                "Internal error: cannot set output serializer name override on AwaitableList because it can't be returned from a function."
            )

        user_future: ListFuture = (
            ListFuture(awaitable)
            if existing_awaitable_future is None
            else existing_awaitable_future
        )

        for item in awaitable.awaitables:
            self._create_future_run_for_user_object(
                item,
                start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )

        self._future_runs[awaitable.id] = ListFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                start_delay=start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
        )

    def _create_future_run_for_function_call_awaitable(
        self,
        awaitable: FunctionCallAwaitable,
        existing_awaitable_future: FunctionCallFuture | None,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates LocalFunctionCallFutureRun for the supplied awaitable.

        Raises TensorlakeException on error.
        """
        user_future: FunctionCallFuture = (
            FunctionCallFuture(awaitable)
            if existing_awaitable_future is None
            else existing_awaitable_future
        )

        for arg in awaitable.args:
            self._create_future_run_for_user_object(
                arg,
                start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )
        for arg in awaitable.kwargs.values():
            self._create_future_run_for_user_object(
                arg,
                start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )

        function: Function = get_function(awaitable.function_name)
        self._future_runs[awaitable.id] = FunctionCallFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
                output_serializer_name_override=output_serializer_name_override,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            application=self._app,
            function=function,
            class_instance=self._function_self_arg(function),
            request_context=self._request_context,
        )

    def _create_future_run_for_reduce_operation_awaitable(
        self,
        awaitable: ReduceOperationAwaitable,
        existing_awaitable_future: ReduceOperationFuture | None,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates LocalReduceOperationFutureRun for the supplied awaitable.

        Raises TensorlakeException on error.
        """
        reduce_operation_result_awaitable: Awaitable = awaitable.inputs[0]

        # There's no user visible interface to ReturnOutputFutureRun so we have to do everything manually here.
        if len(awaitable.inputs) >= 2:
            function: Function = get_function(awaitable.function_name)
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

        # Don't create future runs for the function call chain because we're
        # going to return it from ReturnOutputFutureRun.
        user_future: ReduceOperationFuture = (
            ReduceOperationFuture(awaitable)
            if existing_awaitable_future is None
            else existing_awaitable_future
        )
        self._future_runs[awaitable.id] = ReturnOutputFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
                output_serializer_name_override=output_serializer_name_override,
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
    future_id: str, output: Any, output_serializer: UserDataSerializer
) -> BLOB:
    serialized_output, serialized_output_content_type = serialize_value(
        output, output_serializer
    )
    return BLOB(
        id=future_id,
        data=serialized_output,
        serializer_name=output_serializer.name,
        content_type=serialized_output_content_type,
        cls=type(output),
    )
