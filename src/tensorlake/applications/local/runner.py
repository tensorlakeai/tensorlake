import shutil
import tempfile
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

import httpx

from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.multiprocessing import setup_multiprocessing

from ..algorithms.validate_user_object import validate_user_object
from ..function.application_call import (
    deserialize_application_function_call_payload,
)
from ..function.user_data_serializer import (
    deserialize_value,
    function_input_serializer,
    function_output_serializer,
    serialize_value,
)
from ..interface import (
    RETURN_WHEN,
    Awaitable,
    File,
    Function,
    Future,
    InternalError,
    Request,
    RequestError,
    RequestFailed,
    SDKUsageError,
    TensorlakeError,
)
from ..interface.awaitables import (
    AwaitableList,
    FunctionCallAwaitable,
    FunctionCallFuture,
    ListFuture,
    ReduceOperationAwaitable,
    ReduceOperationFuture,
    _request_scoped_id,
)
from ..metadata import (
    CollectionItemMetadata,
    CollectionMetadata,
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ReduceOperationMetadata,
)
from ..registry import get_function
from ..request_context.http_client.context import RequestContextHTTPClient
from ..request_context.http_server.server import RequestContextHTTPServer
from ..runtime_hooks import (
    clear_run_futures_hook,
    clear_wait_futures_hook,
    set_run_futures_hook,
    set_wait_futures_hook,
)
from ..user_data_serializer import (
    PickleUserDataSerializer,
    UserDataSerializer,
)
from ..validation import (
    ValidationMessage,
    has_error_message,
    print_validation_messages,
    validate_loaded_applications,
)
from .class_instance_store import ClassInstanceStore
from .future import LocalFuture, UserFutureMetadataType
from .future_run.function_call_future_run import FunctionCallFutureRun
from .future_run.future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
    get_current_future_run,
)
from .future_run.list_future_run import ListFutureRun
from .future_run.return_output_future_run import ReturnOutputFutureRun
from .request import LocalRequest
from .request_context.http_handler_factory import LocalRequestContextHTTPHandlerFactory
from .utils import print_exception
from .value_store import SerializedValue, SerializedValueStore

_LOCAL_REQUEST_ID = "local-request-id"
# 2 ms  interval for code paths that do polling.
# This keeps our timers very accurate and doesn't add too much latency and CPU overhead.
_SLEEP_POLL_INTERVAL_SECONDS = 0.002


# TODO: Implement Exception propagation from called function to its caller.
# Only mark request as failed with an exception if Application function raised/didn't
# catch the exception or if application function finished successfully but one of non-blocking
# function calls failed later.


class LocalRunner:
    def __init__(self, app: Function, app_payload: Any):
        self._app: Function = app
        self._app_payload: Any = app_payload

        self._logger: InternalLogger = InternalLogger.get_logger().bind(module=__name__)
        self._blob_store_dir_path: str = tempfile.mkdtemp(
            prefix="tensorlake_local_blob_store_"
        )
        # local FS blob store is used in local mode so we get high performance without parallelism.
        self._blob_store: BLOBStore = BLOBStore(available_cpu_count=1)
        # Value ID/Future ID -> SerializedValue.
        self._value_store: SerializedValueStore = SerializedValueStore(
            blob_store_dir_path=self._blob_store_dir_path,
            blob_store=self._blob_store,
            logger=self._logger,
        )

        # Future runs that currently exist.
        # Future Awaitable ID -> LocalFutureRun
        self._future_runs: Dict[str, LocalFutureRun] = {}
        # Exception that caused the request to fail.
        # None when request finished successfully.
        self._request_failed_exception: RequestFailed | None = None
        # Share class instances between all functions. If we don't do this then there's
        # going to be >1 instance of the same class per process.
        self._class_instance_store: ClassInstanceStore = ClassInstanceStore.singleton()
        # SimpleQueue[LocalFutureRunResult]
        self._future_run_result_queue: SimpleQueue = SimpleQueue()
        self._future_run_thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            # We need to allow lots of threads at a time because user code blocks
            # on waiting for another function to complete and the chain of blocked
            # user function threads can grow indefinitely.
            max_workers=10000,
            thread_name_prefix="LocalFutureRunner:",
        )

        self._request_context_http_server: RequestContextHTTPServer = (
            RequestContextHTTPServer(
                server_router_class=LocalRequestContextHTTPHandlerFactory(
                    blob_store_dir_path=self._blob_store_dir_path,
                    logger=self._logger,
                ),
            )
        )
        self._request_context_http_server_thread: threading.Thread = threading.Thread(
            target=self._request_context_http_server.start,
            name="LocalRequestContextHTTPServerThread",
            daemon=True,
        )
        # Use a single HTTP client for the whole LocalRunner. It's thread-safe.
        # It reduces resource usage and makes it easy to close just one client at the end.
        self._request_context_http_client: httpx.Client = (
            RequestContextHTTPClient.create_http_client(
                server_base_url=self._request_context_http_server.base_url,
            )
        )

    def run(self) -> Request:
        try:
            validation_messages: list[ValidationMessage] = (
                validate_loaded_applications()
            )
            print_validation_messages(validation_messages)
            if has_error_message(validation_messages):
                return LocalRequest(
                    id=_LOCAL_REQUEST_ID,
                    output=None,
                    error=RequestFailed(
                        "Local application run aborted due to code validation errors, "
                        "please address them before running the application."
                    ),
                )

            return self._run()
        except BaseException as e:
            # This is an unexpected exception in LocalRunner code itself.
            # The function run exception is stored in self._request_failed_exception and handled above.
            print_exception(e)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                error=(
                    RequestFailed("Cancelled by user")
                    if isinstance(e, KeyboardInterrupt)
                    else RequestFailed("Unexpected exception: " + str(e))
                ),
            )

    def _run(self) -> LocalRequest:
        """Runs the request.

        Doesn't raise any exceptions unless there's a coding bug.
        """
        self._request_context_http_server_thread.start()

        set_run_futures_hook(self._run_futures_runtime_hook)
        set_wait_futures_hook(self._wait_futures_runtime_hook)
        setup_multiprocessing()

        # Serialize application payload the same way as in remote mode.
        input_serializer: UserDataSerializer = function_input_serializer(self._app)

        try:
            serialized_payload, payload_metadata = serialize_value(
                value=self._app_payload, serializer=input_serializer, value_id="fake_id"
            )
            payload: Any = deserialize_application_function_call_payload(
                application=self._app,
                payload=serialized_payload,
                payload_content_type=payload_metadata.content_type,
            )
            app_function_call_awaitable: FunctionCallAwaitable = self._app.awaitable(
                payload
            )
            self._create_future_run_for_awaitable(
                awaitable=app_function_call_awaitable,
                existing_awaitable_future=None,
                start_delay=None,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )
        except TensorlakeError as e:
            # Handle exceptions that depend on user inputs. All other exceptions are
            # unexpected and usually mean a bug in local runner.
            self._handle_user_exception(e)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                error=self._request_failed_exception,
            )

        self._control_loop()

        if self._request_failed_exception is not None:
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                error=self._request_failed_exception,
            )

        ser_request_output: SerializedValue = self._value_store.get(
            app_function_call_awaitable.id
        )
        try:
            request_output: Any = _deserialize_value(ser_request_output)
        except TensorlakeError as e:
            # Handle exceptions that depend on user inputs. All other exceptions are
            # unexpected and usually mean a bug in local runner.
            self._handle_user_exception(e)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                error=self._request_failed_exception,
            )

        return LocalRequest(
            id=_LOCAL_REQUEST_ID,
            output=request_output,
            error=None,
        )

    def close(self) -> None:
        """Closes the LocalRunner and releases all resources.

        Cancels all running functions and waits for them to finish.
        Doesn't raise any exceptions.
        """
        # Future runs can be modified concurrently so iterate over a copy.
        for fr in self._future_runs.copy().values():
            fr.cancel()
        self._future_run_thread_pool.shutdown(wait=True, cancel_futures=True)

        # Only shutdown the HTTP server after all function runs are stopped so
        # they don't use it. The http server thread exits when we stop the server.
        self._request_context_http_server.stop()
        try:
            self._request_context_http_client.close()
        except Exception as e:
            self._logger.error(
                "Failed to close request context HTTP client", exc_info=e
            )

        self._blob_store.close()
        try:
            shutil.rmtree(self._blob_store_dir_path)
        except OSError as e:
            self._logger.error(
                f"Failed to delete temporary blob store directory '{self._blob_store_dir_path}': {e}"
            )

        # Only clear runtime hooks at the very end when nothing can use them.
        clear_run_futures_hook()
        clear_wait_futures_hook()

    def __enter__(self) -> "LocalRunner":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _run_futures_runtime_hook(
        self, futures: List[Future], start_delay: float | None
    ) -> None:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.

        try:
            self._user_code_cancellation_point()
            for user_future in futures:
                self._create_future_run_for_awaitable(
                    awaitable=user_future.awaitable,
                    existing_awaitable_future=user_future,
                    start_delay=start_delay,
                    output_consumer_future_id=None,
                    output_serializer_name_override=None,
                )
        except TensorlakeError:
            raise
        except Exception as e:
            raise InternalError(f"Unexpected error while running futures") from e

    def _wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        try:
            return self.__wait_futures_runtime_hook(futures, timeout, return_when)
        except TensorlakeError:
            raise
        except Exception as e:
            raise InternalError(f"Unexpected error while waiting futures") from e

    def __wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        self._user_code_cancellation_point()

        if return_when == RETURN_WHEN.FIRST_COMPLETED:
            std_return_when = STD_FIRST_COMPLETED
        elif return_when == RETURN_WHEN.FIRST_FAILURE:
            std_return_when = STD_FIRST_EXCEPTION
        elif return_when == RETURN_WHEN.ALL_COMPLETED:
            std_return_when = STD_ALL_COMPLETED
        else:
            raise SDKUsageError(f"Not supported return_when value: '{return_when}'")

        std_futures: List[StdFuture] = []
        for future in futures:
            if future.awaitable.id not in self._future_runs:
                raise InternalError(
                    f"Future with Awaitable ID {future.awaitable.id} is not registered"
                )
            std_futures.append(self._future_runs[future.awaitable.id].std_future)

        # Std futures are always running as long as their user futures are considered running.
        # Their outcomes (if failed or succeeded) are also synchronized.
        done_std_futures, _ = std_wait(
            std_futures, timeout=timeout, return_when=std_return_when
        )
        done_user_futures: List[Future] = []
        not_done_user_futures: List[Future] = []
        for future in futures:
            local_future_run: LocalFutureRun = self._future_runs[future.awaitable.id]
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
            raise SDKUsageError(
                "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
                "Please only call Tensorlake SDK from Tensorlake Functions."
            )

    def _finished(self) -> bool:
        if self._request_failed_exception is not None:
            return True

        if not self._future_run_result_queue.empty():
            return False

        return all(fr.std_future.done() for fr in self._future_runs.values())

    def _control_loop(self) -> None:
        # NB: any exception raised here in control loop is unexpected and means a bug in LocalRunner.
        while not self._finished():
            self._control_loop_start_runnable_futures()
            # Process one future run result at a time because it takes
            # ~_SLEEP_POLL_INTERVAL_SECONDS at most. This keeps our timers accurate enough.
            self._control_loop_wait_and_process_future_run_result()
            if self._request_failed_exception is not None:
                break

    def _control_loop_start_runnable_futures(self) -> None:
        # Future runs can be modified concurrently so iterate over a copy.
        for future_run in self._future_runs.copy().values():
            future: LocalFuture = future_run.local_future

            if not future.start_time_elapsed:
                continue

            if not self._future_run_data_dependencies_are_resolved(future_run):
                continue

            try:
                self._start_future_run(future_run)
            except TensorlakeError as e:
                # Handle exceptions that depend on user inputs. All other exceptions are
                # unexpected and usually mean a bug in local runner.
                self._handle_future_run_failure(
                    future_run=future_run,
                    error=e,
                )

    def _control_loop_wait_and_process_future_run_result(self) -> None:
        try:
            # Wait at most 100ms. This is the precision of function call timers.
            result: LocalFutureRunResult = self._future_run_result_queue.get(
                timeout=_SLEEP_POLL_INTERVAL_SECONDS
            )
        except QueueEmptyError:
            # No new result for now.
            return

        future_run: LocalFutureRun = self._future_runs[result.id]
        try:
            self._control_loop_process_future_run_result(future_run, result)
        except TensorlakeError as e:
            # Handle exceptions that depend on user inputs. All other exceptions are
            # unexpected and usually mean a bug in local runner.
            self._handle_future_run_failure(
                future_run=future_run,
                error=e,
            )

    def _control_loop_process_future_run_result(
        self, future_run: LocalFutureRun, result: LocalFutureRunResult
    ) -> None:
        user_future: Future = future_run.local_future.user_future
        metadata: UserFutureMetadataType = future_run.local_future.user_future_metadata

        function_name: str = "<unknown>"
        output_blob_serializer: UserDataSerializer | None = None
        if isinstance(future_run, FunctionCallFutureRun):
            function_name = user_future.awaitable.function_name
            output_blob_serializer = function_output_serializer(
                get_function(function_name),
                metadata.output_serializer_name_override,
            )
        elif isinstance(future_run, ReturnOutputFutureRun):
            function_name = user_future.awaitable.function_name
            output_blob_serializer = function_output_serializer(
                get_function(function_name),
                metadata.output_serializer_name_override,
            )
        elif isinstance(future_run, ListFutureRun):
            function_name = "Assembly awaitable list"
            # In remote mode we assemble the list locally and only store its individual items in BLOB store.
            # As we store everything in local mode then we just use the most flexible serializer
            # here that always works.
            output_blob_serializer = PickleUserDataSerializer()
        else:
            self._handle_future_run_failure(
                future_run=future_run,
                error=InternalError(
                    f"Unexpected LocalFutureRun subclass: {type(future_run)}."
                ),
            )
            return

        if isinstance(result.output, (Awaitable, Future)):
            # This is a very important check for our UX. We can await for AwaitableList
            # in user code but we cannot return it from a function as tail call because
            # there's no Python code to reassemble the list from individual resolved awaitables.
            if isinstance(result.output, AwaitableList):
                self._handle_future_run_failure(
                    future_run=future_run,
                    error=SDKUsageError(
                        f"Function '{function_name}' returned {result.output}. "
                        f"A {result.output.kind_str} can only be used as a function argument, not returned from it."
                    ),
                )
                return

            self._create_future_run_for_user_object(
                object=result.output,
                start_delay=None,
                output_consumer_future_id=user_future.awaitable.id,
                output_serializer_name_override=output_blob_serializer.name,
            )
        else:
            ser_value: SerializedValue | None = None
            if result.error is None:
                ser_value = _to_serialized_value(
                    value_id=user_future.awaitable.id,
                    value=result.output,
                    value_serializer=output_blob_serializer,
                )
                self._value_store.put(ser_value)
                self._handle_future_run_final_output(
                    future_run=future_run, ser_value=ser_value, error=None
                )
            else:
                self._handle_future_run_failure(
                    future_run=future_run, error=result.error
                )

    def _handle_future_run_failure(
        self,
        future_run: LocalFutureRun,
        error: TensorlakeError,
    ) -> None:
        self._handle_user_exception(error)

        self._handle_future_run_final_output(
            future_run=future_run,
            ser_value=None,
            error=error,
        )

    def _handle_user_exception(self, error: TensorlakeError) -> None:
        """Handles an exception raised from user code.

        Marks the request as failed. User code includes code that we run with user
        supplied inputs, like serialization.
        """
        # Always print the user exception.
        # This aligns with remote mode UX where the user exception is printed in FE.
        print_exception(error)
        if isinstance(error, RequestError):
            self._request_failed_exception = error
        else:
            # Consistent with remote mode, full exception trace is printed separately.
            self._request_failed_exception = RequestFailed("function_error")

    def _collection_is_resolved(self, collection_metadata: CollectionMetadata) -> bool:
        for item in collection_metadata.items:
            if item.collection is not None:
                if not self._collection_is_resolved(item.collection):
                    return False
            else:
                if not self._value_store.has(item.value_id):
                    return False
        return True

    def _function_arg_is_resolved(
        self, arg_metadata: FunctionCallArgumentMetadata
    ) -> bool:
        if arg_metadata.collection is not None:
            return self._collection_is_resolved(arg_metadata.collection)
        else:
            return self._value_store.has(arg_metadata.value_id)

    def _future_run_data_dependencies_are_resolved(
        self, future_run: LocalFutureRun
    ) -> bool:
        if isinstance(future_run, FunctionCallFutureRun):
            metadata: FunctionCallMetadata = (
                future_run.local_future.user_future_metadata
            )
            for arg_metadata in metadata.args:
                if not self._function_arg_is_resolved(arg_metadata):
                    return False
            for arg_metadata in metadata.kwargs.values():
                if not self._function_arg_is_resolved(arg_metadata):
                    return False
            return True

        elif isinstance(future_run, ReturnOutputFutureRun):
            # There are no prerequisites for ReturnOutputFutureRun.
            # It just returns an awaitable as a tail call or a value.
            return True

        elif isinstance(future_run, ListFutureRun):
            metadata: CollectionMetadata = future_run.local_future.user_future_metadata
            return self._collection_is_resolved(metadata)

        else:
            self._handle_future_run_failure(
                future_run=future_run,
                error=InternalError(
                    f"Unexpected LocalFutureRun subclass: {type(future_run)}"
                ),
            )
            return False

    def _handle_future_run_final_output(
        self,
        future_run: LocalFutureRun,
        ser_value: SerializedValue | None,
        error: TensorlakeError | None,
    ) -> None:
        """Handles final output of the supplied future run.

        Sets the output or exception on the user future and finishes the future run.
        Called after the future function finished and Awaitable tree that it returned
        is finished and its output is propagated as this future run's output.
        """
        future: LocalFuture = future_run.local_future

        if error is not None:
            future.user_future.set_exception(error)
        else:
            # Intentionally do serialize -> deserialize cycle to ensure the same UX as in remote mode.
            future.user_future.set_result(_deserialize_value(ser_value))

        # Finish std future so wait hooks waiting on it unblock.
        # Success/failure needs to be propagated to std future as well so std wait calls work correctly.
        future_run.finish(is_exception=error is not None)

        # Propagate output to consumer future if any.
        if future.output_consumer_future_id is None:
            return

        consumer_future_run: LocalFutureRun = self._future_runs[
            future.output_consumer_future_id
        ]
        consumer_future: LocalFuture = consumer_future_run.local_future
        consumer_future_output: SerializedValue | None = None
        if error is None:
            consumer_future_output = SerializedValue(
                data=ser_value.data,
                metadata=ser_value.metadata.model_copy(),
            )
            consumer_future_output.metadata.id = (
                consumer_future.user_future.awaitable.id
            )
            self._value_store.put(consumer_future_output)
        self._handle_future_run_final_output(
            future_run=consumer_future_run,
            ser_value=consumer_future_output,
            error=error,
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
            raise InternalError(
                f"Unexpcted LocalFutureRun subclass: {type(future_run)}"
            )

    def _start_function_call_future_run(
        self, future_run: FunctionCallFutureRun
    ) -> None:
        local_future: LocalFuture = future_run.local_future
        metadata: FunctionCallMetadata = local_future.user_future_metadata
        arg_values: List[Any] = []
        kwarg_values: Dict[str, Any] = {}

        for arg_metadata in metadata.args:
            arg_values.append(self._reconstruct_function_arg_value(arg_metadata))
        for kwarg_key, kwarg_metadata in metadata.kwargs.items():
            kwarg_values[kwarg_key] = self._reconstruct_function_arg_value(
                kwarg_metadata
            )

        future_run.start(arg_values=arg_values, kwarg_values=kwarg_values)

    def _reconstruct_function_arg_value(
        self, arg_metadata: FunctionCallArgumentMetadata
    ) -> Any:
        """Reconstructs the original value from function arg metadata."""
        if arg_metadata.collection is None:
            return _deserialize_value(self._value_store.get(arg_metadata.value_id))
        else:
            return self._reconstruct_collection_value(arg_metadata.collection)

    def _start_list_future_run(self, future_run: ListFutureRun) -> None:
        metadata: CollectionMetadata = future_run.local_future.user_future_metadata
        values: List[Any] = self._reconstruct_collection_value(metadata)
        future_run.start(values)

    def _reconstruct_collection_value(
        self, collection_metadata: CollectionMetadata
    ) -> List[Any]:
        """Reconstructs the original values from the supplied collection metadata."""
        values: List[Any] = []
        for item in collection_metadata.items:
            if item.collection is None:
                values.append(_deserialize_value(self._value_store.get(item.value_id)))
            else:
                values.append(self._reconstruct_collection_value(item.collection))
        return values

    def _create_future_run_for_user_object(
        self,
        object: Any | Future | Awaitable,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates future run for the supplied user object if it's an Awaitable.

        Doesn't do anything if it's a concrete value. Raises TensorlakeError on error.
        """
        validate_user_object(object, running_awaitable_ids=self._future_runs.keys())
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
        output_consumer_future_id is the ID of the Future that will consume the output of the future run.
        output_serializer_name_override is the name of the serializer to use for serializing
        the output of the future run. This is used when propagating output to consumer future when the
        consumer future expects a specific serialization format.
        Raises TensorlakeError on error.
        """
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

    def _create_future_run_for_awaitable_list(
        self,
        awaitable: AwaitableList,
        existing_awaitable_future: ListFuture | None,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates ListFutureRun for the supplied awaitable.

        Raises TensorlakeError on error.
        """
        # Checking for errors in our own logic.
        if output_consumer_future_id is not None:
            raise InternalError(
                "Cannot set output consumer future ID on AwaitableList because it can't be returned from a function."
            )
        if output_serializer_name_override is not None:
            raise InternalError(
                "Cannot set output serializer name override on AwaitableList because it can't be returned from a function."
            )

        user_future: ListFuture = (
            ListFuture(awaitable)
            if existing_awaitable_future is None
            else existing_awaitable_future
        )

        metadata: CollectionMetadata = self._collection_metadata(
            collection=awaitable,
            # It doesn't matter which serializer we're using cause we'll deserialize
            # the list items locally anyway and create a Python list out of them.
            value_serializer=PickleUserDataSerializer(),
        )

        for item in awaitable.items:
            self._create_future_run_for_user_object(
                item,
                start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )

        self._future_runs[awaitable.id] = ListFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                user_future_metadata=metadata,
                start_delay=start_delay,
                output_consumer_future_id=None,
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

        Raises TensorlakeError on error.
        """
        function: Function = get_function(awaitable.function_name)
        user_input_serializer: UserDataSerializer = function_input_serializer(function)
        user_future: FunctionCallFuture = (
            FunctionCallFuture(awaitable)
            if existing_awaitable_future is None
            else existing_awaitable_future
        )

        metadata: FunctionCallMetadata = FunctionCallMetadata(
            id=awaitable.id,
            output_serializer_name_override=output_serializer_name_override,
            args=[],
            kwargs={},
        )

        # Arguments don't inherit serializer overrides because only the
        # root of the call tree needs to have its output serialized in a
        # specific way so its output consumer gets value in expected format.
        for arg in awaitable.args:
            self._create_future_run_for_user_object(
                arg,
                start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )
            metadata.args.append(
                self._function_arg_metadata(arg, user_input_serializer)
            )
        for key, arg in awaitable.kwargs.items():
            self._create_future_run_for_user_object(
                arg,
                start_delay,
                output_consumer_future_id=None,
                output_serializer_name_override=None,
            )
            metadata.kwargs[key] = self._function_arg_metadata(
                arg, user_input_serializer
            )

        function_run_request_context: RequestContextHTTPClient = (
            RequestContextHTTPClient(
                request_id=_LOCAL_REQUEST_ID,
                allocation_id=awaitable.id,
                function_name=awaitable.function_name,
                server_base_url=self._request_context_http_server.base_url,
                http_client=self._request_context_http_client,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        )
        self._future_runs[awaitable.id] = FunctionCallFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                user_future_metadata=metadata,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            application=self._app,
            function=function,
            class_instance=self._class_instance_store.get(function),
            request_context=function_run_request_context,
        )

    def _function_arg_metadata(
        self, arg: Any | Awaitable, value_serializer: UserDataSerializer
    ) -> FunctionCallArgumentMetadata:
        if isinstance(arg, Awaitable):
            # Embedding the awaitable list.
            if isinstance(arg, AwaitableList):
                return FunctionCallArgumentMetadata(
                    value_id=None,
                    collection=self._collection_metadata(arg, value_serializer),
                )
            else:
                return FunctionCallArgumentMetadata(
                    value_id=arg.id,
                    collection=None,
                )
        else:
            value_id: str = _request_scoped_id()
            self._value_store.put(
                _to_serialized_value(
                    value_id=value_id, value=arg, value_serializer=value_serializer
                )
            )
            return FunctionCallArgumentMetadata(
                value_id=value_id,
                collection=None,
            )

    def _collection_metadata(
        self, collection: AwaitableList, value_serializer: UserDataSerializer
    ) -> CollectionMetadata:
        """Builds recursive collection metadata for the supplied AwaitableList."""
        items_metadata: List[CollectionItemMetadata] = []
        for item in collection.items:
            if isinstance(item, Awaitable):
                if isinstance(item, AwaitableList):
                    items_metadata.append(
                        CollectionItemMetadata(
                            value_id=None,
                            collection=self._collection_metadata(
                                item, value_serializer
                            ),
                        )
                    )
                else:
                    items_metadata.append(
                        CollectionItemMetadata(
                            value_id=item.id,
                            collection=None,
                        )
                    )
            else:
                value_id: str = _request_scoped_id()
                self._value_store.put(
                    _to_serialized_value(
                        value_id=value_id, value=item, value_serializer=value_serializer
                    )
                )
                items_metadata.append(
                    CollectionItemMetadata(
                        value_id=value_id,
                        collection=None,
                    )
                )
        return CollectionMetadata(items=items_metadata)

    def _create_future_run_for_reduce_operation_awaitable(
        self,
        awaitable: ReduceOperationAwaitable,
        existing_awaitable_future: ReduceOperationFuture | None,
        start_delay: float | None,
        output_consumer_future_id: str | None,
        output_serializer_name_override: str | None,
    ) -> None:
        """Creates LocalReduceOperationFutureRun for the supplied awaitable.

        Raises TensorlakeError on error.
        """
        # inputs have at least two items.
        function: Function = get_function(awaitable.function_name)
        # Create a chain of function calls to reduce all inputs one by one.
        # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
        # using string concat function into "abcd".
        previous_function_call_awaitable: FunctionCallAwaitable = function.awaitable(
            awaitable.inputs[0], awaitable.inputs[1]
        )
        for input_item in awaitable.inputs[2:]:
            previous_function_call_awaitable = function.awaitable(
                previous_function_call_awaitable, input_item
            )
        reduce_operation_result = previous_function_call_awaitable

        # Don't create future runs for the function calls chain because we're
        # going to return it from ReturnOutputFutureRun as a tail call.
        user_future: ReduceOperationFuture = (
            ReduceOperationFuture(awaitable)
            if existing_awaitable_future is None
            else existing_awaitable_future
        )

        metadata: ReduceOperationMetadata = ReduceOperationMetadata(
            id=awaitable.id,
            output_serializer_name_override=output_serializer_name_override,
        )

        self._future_runs[awaitable.id] = ReturnOutputFutureRun(
            local_future=LocalFuture(
                user_future=user_future,
                user_future_metadata=metadata,
                start_delay=start_delay,
                output_consumer_future_id=output_consumer_future_id,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            output=reduce_operation_result,
        )


def _deserialize_value(ser_value: SerializedValue) -> Any | File:
    return deserialize_value(
        serialized_value=ser_value.data,
        metadata=ser_value.metadata,
    )


def _to_serialized_value(
    value_id: str, value: Any, value_serializer: UserDataSerializer
) -> SerializedValue:
    serialized_value, metadata = serialize_value(
        value, value_serializer, value_id=value_id
    )
    return SerializedValue(
        data=serialized_value,
        metadata=metadata,
    )
