import traceback
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
from ..interface.exceptions import (
    RequestError,
    RequestFailureException,
    TensorlakeException,
)
from ..interface.file import File
from ..interface.function import Function
from ..interface.futures import (
    FunctionCallFuture,
    Future,
    ReduceOperationFuture,
    request_scoped_id,
)
from ..interface.request import Request
from ..interface.request_context import RequestContext
from ..registry import get_function
from ..request_context.request_context_base import RequestContextBase
from ..request_context.request_metrics_recorder import RequestMetricsRecorder
from ..runtime_hooks import (
    set_start_and_wait_function_calls_hook,
    set_start_function_calls_hook,
    set_wait_futures_hook,
)
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .blob_store import BLOB, BLOBStore
from .function_run import LocalFunctionRun
from .future import FutureType, LocalFuture
from .future_run import LocalFutureRun, LocalFutureRunResult, StopLocalFutureRun
from .reduce_run import LocalReduceRun
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState

_LOCAL_REQUEST_ID = "local-request"


class LocalRunner:
    def __init__(self, app: Function, app_payload: Any):
        self._app: Function = app
        self._app_payload: Any = app_payload
        # Future ID -> BLOB if future succeeded.
        self._blob_store: BLOBStore = BLOBStore()
        # Futures currently known to the runner.
        # Future ID -> LocalFuture
        self._futures: Dict[str, LocalFuture] = {}
        # Function runs that are currently running or completed already.
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

    def run(self) -> Request:
        try:
            input_serializer: UserDataSerializer = function_input_serializer(self._app)
            serialized_payload, content_type = serialize_value(
                self._app_payload, input_serializer
            )
            app_function_call: FunctionCallFuture = (
                application_function_call_with_serialized_payload(
                    application=self._app,
                    payload=serialized_payload,
                    payload_content_type=content_type,
                )
            )
            self._futures[app_function_call.id] = LocalFuture(
                future=app_function_call,
            )

            set_start_function_calls_hook(self._runtime_hook_start_function_calls)
            set_start_and_wait_function_calls_hook(
                self._runtime_hook_start_and_wait_function_calls
            )
            set_wait_futures_hook(self._runtime_hook_wait_futures)

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

    def _runtime_hook_start_function_calls(
        self, function_calls: List[FunctionCallFuture | ReduceOperationFuture]
    ) -> None:
        self._user_code_cancellation_point()
        # TODO

    def _runtime_hook_start_and_wait_function_calls(
        self, function_calls: List[FunctionCallFuture | ReduceOperationFuture]
    ) -> None:
        self._user_code_cancellation_point()
        # TODO

    def _runtime_hook_wait_futures(self, futures: List[Future]) -> None:
        self._user_code_cancellation_point()
        # TODO

    def _user_code_cancellation_point(self) -> None:
        # Every runtime hook call is a cancellation point for user code.
        # Raise StopFunctionRun to stop executing further user code
        # and reduce wait time for existing function run completion.
        if self._request_exception is not None:
            raise StopLocalFutureRun()

    def _finished(self) -> bool:
        if self._request_exception is not None:
            return True

        for future in self._futures.values():
            if future.id not in self._future_runs:
                # The future didn't run yet.
                return False

        return all(fr.finished for fr in self._future_runs.values())

    def _control_loop(self) -> None:
        while not self._finished():
            self._start_runnable_futures()
            # Process one future run result at a time because it takes ~0.1s at most.
            # This keeps our timers accurate enough.
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
                self._feature_run_result_queue.get(timeout=0.1)
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
            fr.wait()

    def _has_all_data_dependencies(self, future: LocalFuture) -> bool:
        future: FutureType = future.future
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

        function_run: LocalFunctionRun = LocalFunctionRun(
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
