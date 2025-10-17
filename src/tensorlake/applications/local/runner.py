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
    FunctionCallAwaitable,
    FunctionCallFuture,
    Future,
    ReduceOperationAwaitable,
    ReduceOperationFuture,
    RuntimeAwaitableTypes,
    RuntimeFutureTypes,
)
from ..interface.exceptions import (
    RequestError,
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
    set_run_futures_hook,
    set_wait_futures_hook,
)
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .blob_store import BLOB, BLOBStore
from .function_call_future_run import LocalFunctionCallFutureRun
from .future import LocalFuture
from .future_run import LocalFutureRun, LocalFutureRunResult, StopLocalFutureRun
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState

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
        self._request_exception: RequestFailureException | RequestError | None = None
        # Class name => instance.
        self._class_instances: Dict[str, Any] = {}
        self._request_context: RequestContext = RequestContextBase(
            request_id=_LOCAL_REQUEST_ID,
            state=LocalRequestState(),
            progress=LocalRequestProgress(),
            metrics=RequestMetricsRecorder(),
        )
        # SimpleQueue[LocalFutureRunResult]
        self._feature_run_result_queue: SimpleQueue = SimpleQueue()
        self._feature_run_thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            # We need to allow lots of threads at a time because user code blocks
            # on waiting for another function to complete and the chain of blocked
            # user function threads can grow indefinitely.
            max_workers=10000,
            thread_name_prefix="LocalFutureRunner:",
        )

    def run(self) -> Request:
        try:
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
            set_run_futures_hook(self._runtime_hook_run_futures)
            set_wait_futures_hook(self._runtime_hook_wait_futures)
            # This will call our runtime hooks to register the future created by .run() call.
            app_function_call.run()

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
            #
            # We only print exceptions in remote mode but don't propagate them to SDK
            # and return a generic RequestFailureException instead. Do the same here.
            traceback.print_exception(e)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                exception=RequestFailureException("Request failed"),
            )

    def close(self) -> None:
        """Closes the LocalRunner and releases all resources.

        Cancels all running functions and waits for them to finish.
        """
        if self._request_exception is None:
            self._request_exception = RequestFailureException(
                "Request cancelled by user"
            )
        for fr in self._future_runs.values():
            fr.cancel()
        self._feature_run_thread_pool.shutdown(wait=True, cancel_futures=True)

    def __enter__(self) -> "LocalRunner":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _runtime_hook_run_futures(
        self, futures: List[Future], start_delay: float | None
    ) -> None:
        # Warning: this is called from user code.
        self._user_code_cancellation_point()

        for future in futures:
            if future.id in self._future_runs:
                raise TensorlakeException(
                    "Internal error: future with ID {} is already registered".format(
                        future.id
                    )
                )

            if isinstance(future, ReduceOperationFuture):
                self._break_reduce_operation_into_function_calls(future)
            elif isinstance(future, FunctionCallFuture):
                self._future_runs[future.id] = LocalFutureRun(
                    application=self._app,
                    local_future=LocalFuture(
                        future=future,
                        start_delay=start_delay,
                    ),
                    request_context=self._request_context,
                    result_queue=self._feature_run_result_queue,
                    thread_pool=self._feature_run_thread_pool,
                )
            else:
                raise TensorlakeException(
                    "Internal error: unexpected future type: {}".format(type(future))
                )

    def _runtime_hook_wait_futures(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        # Warning: this is called from user code.
        self._user_code_cancellation_point()

        if return_when == RETURN_WHEN.FIRST_COMPLETED:
            std_return_when = STD_FIRST_COMPLETED
        elif return_when == RETURN_WHEN.FIRST_FAILURE:
            std_return_when = STD_FIRST_EXCEPTION
        elif return_when == RETURN_WHEN.ALL_COMPLETED:
            std_return_when = STD_ALL_COMPLETED
        else:
            raise ValueError(
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

        done_std_futures, _ = std_wait(
            std_futures, timeout=timeout, return_when=std_return_when
        )
        done_futures: List[Future] = []
        not_done_futures: List[Future] = []
        for future in futures:
            local_future_run: LocalFutureRun = self._future_runs[future.id]
            if local_future_run.std_future in done_std_futures:
                done_futures.append(future)
            else:
                not_done_futures.append(future)

        return done_futures, not_done_futures

    def _user_code_cancellation_point(self) -> None:
        # Every runtime hook call is a cancellation point for user code.
        # Raise StopFunctionRun to stop executing all futures so LocalRunner.close()
        # can finish asap.
        if self._finished():
            raise StopLocalFutureRun()

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
        for future in self._futures.values():
            future: LocalFuture
            if future.id in self._future_runs:
                continue

            if not future.start_time_elapsed:
                continue

            if not self._has_all_data_dependencies(future):
                continue

            self._run_future(future)

    def _wait_and_process_future_run_result(self) -> None:
        try:
            # Wait at most 100ms. This is the precision of function call timers.
            result: LocalFunctionRunResult | LocalReduceRunResult = (
                self._feature_run_result_queue.get(timeout=_SLEEP_POLL_INTERVAL_SECONDS)
            )
        except QueueEmptyError:
            # No new result for now.
            return

        self._process_future_run_result(result)

    def _process_future_run_result(self, result: LocalFutureRunResult) -> None:
        if result.exception is None:
            self._request_exception = result.exception
            return

        future: LocalFuture = self._futures[result.id]
        function: Function = get_function(future.future.function_name)
        if isinstance(result.output, Future):
            # The future is already registered using runtime hook.
            if result.output.id not in self._futures:
                raise TensorlakeException(
                    "Internal error: future returned from future run is not registered"
                )
            output_future: LocalFuture = self._futures[result.output.id]

            if output_future.output_consumer_future_id is not None:
                raise TensorlakeException(
                    "Internal error: future returned from future run is already consumed by another future"
                )
            output_future.output_consumer_future_id = result.id

            if self._blob_store.has(output_future.future.id):
                # The output future is already finished.
                self._propagate_future_output_to_consumers(output_future)
        else:
            self._blob_store.put(
                _future_output_value_to_blob(
                    function=function,
                    future_id=future.future.id,
                    output=result.output,
                    output_serializer_name_override=future.future.output_serializer_name_override,
                )
            )
            self._propagate_future_output_to_consumers(future)

    def _wait_all_future_runs(self) -> None:
        for fr in self._future_runs.values():
            if not fr.std_future.done():
                std_wait([fr.std_future])

    def _has_all_data_dependencies(self, future: LocalFuture) -> bool:
        future: RuntimeFutureTypes = future.future
        if isinstance(future, FunctionCallFuture):
            for arg in future.args:
                if isinstance(arg, Future):
                    if not self._blob_store.has(arg.id):
                        return False
            for arg in future.kwargs.values():
                if isinstance(arg, Future):
                    if not self._blob_store.has(arg.id):
                        return False
            return True
        elif isinstance(future, ReduceOperationFuture):
            for input_item in future.inputs:
                if isinstance(input_item, Future):
                    if not self._blob_store.has(input_item.id):
                        return False
            return True

        raise TensorlakeException(
            "Internal error: unexpected future type: {}".format(type(future))
        )

    def _propagate_future_output_to_consumers(self, future: LocalFuture) -> None:
        current_future: LocalFuture = future
        while current_future.output_consumer_future_id is not None:
            consumer_future: LocalFuture = self._futures[
                current_future.output_consumer_future_id
            ]
            consumer_future_output: BLOB = self._blob_store.get(
                current_future.future.id
            )
            consumer_future_output.id = consumer_future.future.id
            self._blob_store.put(consumer_future_output)
            current_future = consumer_future

    def _run_future(self, future: LocalFuture) -> None:
        """Runs the supplied future using a new LocalFutureRun object.

        The future must be runnable, i.e. all its data dependencies are available
        in blob store and its start time delay has elapsed.
        """
        if isinstance(future.future, FunctionCallFuture):
            self._run_function_call_future(future)
        elif isinstance(future.future, ReduceOperationFuture):
            self._run_reduce_operation_future(future)

    def _run_function_call_future(self, future: LocalFuture) -> None:
        future: FunctionCallFuture = future.future
        function: Function = get_function(future.function_name)

        for arg_ix, arg in enumerate(future.args):
            if isinstance(arg, Future):
                arg_blob: BLOB = self._blob_store.get(arg.id)
                arg_value: Any = _deserialize_blob_value(arg_blob)
                future.args[arg_ix] = arg_value
        for kwarg_key, kwarg in future.kwargs.items():
            if isinstance(kwarg, Future):
                kwarg_blob: BLOB = self._blob_store.get(kwarg.id)
                kwarg_value: Any = _deserialize_blob_value(kwarg_blob)
                future.kwargs[kwarg_key] = kwarg_value

        function_run: LocalFunctionCallFutureRun = LocalFunctionCallFutureRun(
            application=self._app,
            function=function,
            function_call=future,
            class_instance=self._function_self_arg(function),
            request_context=self._request_context,
            result_queue=self._feature_run_result_queue,
        )
        function_run.start()
        self._future_runs[future.id] = function_run

    def _function_self_arg(self, function: Function) -> Any | None:
        if function._function_config.class_name is None:
            return None

        if function._function_config.class_name not in self._class_instances:
            self._class_instances[function._function_config.class_name] = (
                create_self_instance(function._function_config.class_name)
            )

        return self._class_instances[function._function_config.class_name]

    def _run_reduce_operation_future(self, future: LocalFuture) -> None:
        # TODO
        pass

    def _break_reduce_operation_into_function_calls(
        self, reduce_operation: ReduceOperationFuture
    ) -> None:
        if len(reduce_operation.inputs) == 1:
            self._futures[reduce_operation.id] = LocalFuture(
                future=reduce_operation,
            )
            self._process_future_run_result(
                LocalFunctionRunResult(
                    id=reduce_operation.id,
                    output=reduce_operation.inputs[0],
                    exception=None,
                )
            )
        else:
            # Create a chain of function calls to reduce all inputs one by one.
            # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
            # using string concat function into "abcd".
            function_calls: List[FunctionCallFuture] = [
                FunctionCallFuture(
                    id=request_scoped_id(),
                    function_name=user_call.function_name,
                    args=[user_call.inputs[0], user_call.inputs[1]],
                    kwargs={},
                    start_delay=user_call.start_delay,
                )
            ]
            for input_item in user_call.inputs[2:]:
                function_calls.append(
                    FunctionCallFuture(
                        id=request_scoped_id(),
                        function_name=user_call.function_name,
                        args=[function_calls[-1], input_item],
                        kwargs={},
                        start_delay=user_call.start_delay,
                    )
                )
            # The last function call's output is the ReduceOperationFuture's output.
            function_calls[-1].id = user_call.id


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
