import asyncio
import inspect
import shutil
import tempfile
import threading
from collections.abc import Generator
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
from tensorlake.applications.metadata import ValueMetadata
from tensorlake.applications.multiprocessing import setup_multiprocessing

from ..algorithms import (
    derived_function_call_future,
    validate_tail_call_user_future,
)
from ..function.application_call import (
    ApplicationArgument,
    deserialize_application_function_call_arguments,
    serialize_application_function_call_arguments,
)
from ..function.type_hints import (
    function_parameters,
    function_signature,
    parameter_type_hint,
    return_type_hint,
)
from ..function.user_data_serializer import (
    deserialize_value_with_metadata,
    function_input_serializer,
    function_output_serializer,
    serialize_value,
)
from ..interface import (
    RETURN_WHEN,
    DeserializationError,
    File,
    Function,
    Future,
    InternalError,
    Request,
    RequestError,
    RequestFailed,
    SDKUsageError,
    SerializationError,
    TensorlakeError,
)
from ..interface.futures import (
    FunctionCallFuture,
    ListFuture,
    ReduceOperationFuture,
    _FutureListKind,
    _InitialMissing,
    _request_scoped_id,
)
from ..metadata import (
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ReduceOperationMetadata,
)
from ..registry import get_function
from ..request_context.http_client.context import RequestContextHTTPClient
from ..request_context.http_server.server import RequestContextHTTPServer
from ..runtime_hooks import (
    clear_await_future_hook,
    clear_run_futures_hook,
    clear_wait_futures_hook,
    set_await_future_hook,
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
    def __init__(self, app: Function, app_args: list[Any], app_kwargs: dict[str, Any]):
        self._app: Function = app
        self._app_args: list[Any] = app_args
        self._app_kwargs: dict[str, Any] = app_kwargs

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
        # Future ID -> LocalFutureRun
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
        """Creates and runs the local request.

        Raises TensorlakeError on error.
        """
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        if has_error_message(validation_messages):
            # Don't print non-error messages for now to reduce noise for users.
            print_validation_messages(validation_messages)
            raise SDKUsageError(
                "Local application run aborted due to code validation errors, "
                "please address them before running the application."
            )

        self._serialize_and_deserialize_application_arguments()

        # All work that is logically done before the request is created in remote mode must be done by this point.
        try:
            return self._run_request()
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

    def _serialize_and_deserialize_application_arguments(
        self,
    ) -> None:
        """Serializes and deserializes application arguments.

        This is required to bring local mode UX closer to remote mode UX.
        Doesn't raise any exceptions unless there's a bug in our code.
        Raises SerializationError or DeserializationError if serialization/deserialization failed.
        """
        app_parameters: list[inspect.Parameter] = function_parameters(self._app)
        app_args: list[ApplicationArgument] = []
        for i, arg_value in enumerate(self._app_args):
            if i >= len(app_parameters):
                # Allow users to pass unknown args, this gives them more flexibility
                # i.e. when they change their code but not request payload yet.
                continue
            arg_type_hint: Any = parameter_type_hint(app_parameters[i])
            app_args.append(
                ApplicationArgument(
                    value=arg_value,
                    type_hint=arg_type_hint,
                )
            )

        app_signature: inspect.Signature = function_signature(self._app)
        app_kwargs: dict[str, ApplicationArgument] = {}
        for kwarg_key, kwarg_value in self._app_kwargs.items():
            if kwarg_key not in app_signature.parameters:
                # Allow users to pass unknown args, this gives them more flexibility
                # i.e. when they change their code but not request payload yet.
                continue

            kwarg_type_hint: Any = parameter_type_hint(
                app_signature.parameters[kwarg_key]
            )
            app_kwargs[kwarg_key] = ApplicationArgument(
                value=kwarg_value,
                type_hint=kwarg_type_hint,
            )

        # Serialize application payload the same way as in remote mode.
        input_serializer: UserDataSerializer = function_input_serializer(
            self._app, app_call=True
        )
        try:
            serialized_app_args, serialized_app_kwargs = (
                serialize_application_function_call_arguments(
                    input_serializer=input_serializer,
                    args=app_args,
                    kwargs=app_kwargs,
                )
            )
            deserialized_app_args, deserialized_app_kwargs = (
                deserialize_application_function_call_arguments(
                    application=self._app,
                    serialized_args=serialized_app_args,
                    serialized_kwargs=serialized_app_kwargs,
                )
            )
        except (SerializationError, DeserializationError):
            raise  # All other exception raised by this function are bugs in our code.

        # Use copies of the deserialized args/kwargs, this is consisntent with remote mode UX.
        self._app_args = deserialized_app_args
        self._app_kwargs = deserialized_app_kwargs

    def _run_request(self) -> LocalRequest:
        """Runs the request.

        Doesn't raise any exceptions unless there's a bug in our code.
        """
        self._request_context_http_server_thread.start()

        set_run_futures_hook(self._run_futures_runtime_hook)
        set_await_future_hook(self._await_future_runtime_hook)
        set_wait_futures_hook(self._wait_futures_runtime_hook)
        setup_multiprocessing()

        try:
            app_signature: inspect.Signature = function_signature(self._app)
            app_function_call_future: FunctionCallFuture = (
                self._app._make_function_call_future(self._app_args, self._app_kwargs)
            )
            app_output_serializer: UserDataSerializer = function_output_serializer(
                self._app, None
            )
            app_function_call_future._tail_call = True
            self._create_future_run(
                future=app_function_call_future,
                output_serializer_name_override=app_output_serializer.name,
                has_output_type_hint_override=True,
                output_type_hint_override=return_type_hint(
                    app_signature.return_annotation
                ),
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
            app_function_call_future._id
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
        clear_await_future_hook()
        clear_run_futures_hook()
        clear_wait_futures_hook()

    def __enter__(self) -> "LocalRunner":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _run_futures_runtime_hook(self, futures: List[Future]) -> None:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.

        try:
            self._user_code_cancellation_point()
            for user_future in futures:
                if not isinstance(user_future, Future):
                    raise SDKUsageError(
                        f"Cannot run a non-Future object {user_future}."
                    )
                if user_future._id in self._future_runs:
                    raise InternalError(
                        f"Future with ID {user_future._id} is already running, this should never happen."
                    )
                output_serializer_name_override: str | None = None
                has_output_type_hint_override: bool = False
                output_type_hint_override: Any = None
                if user_future._tail_call:
                    # Tail call futures inherit output overrides from the parent
                    # function that created them. This is the LocalRunner equivalent
                    # of AllocationRunner's instance-level override variables.
                    parent_run: LocalFutureRun = get_current_future_run()
                    parent_metadata: UserFutureMetadataType = (
                        parent_run.local_future.future_metadata
                    )
                    output_serializer_name_override = (
                        parent_metadata.output_serializer_name_override
                    )
                    has_output_type_hint_override = (
                        parent_metadata.has_output_type_hint_override
                    )
                    output_type_hint_override = (
                        parent_metadata.output_type_hint_override
                    )
                self._create_future_run(
                    future=user_future,
                    output_serializer_name_override=output_serializer_name_override,
                    has_output_type_hint_override=has_output_type_hint_override,
                    output_type_hint_override=output_type_hint_override,
                )
        except TensorlakeError:
            raise
        except Exception as e:
            raise InternalError(f"Unexpected error while running futures") from e

    def _await_future_runtime_hook(self, future: Future) -> Generator[None, None, Any]:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        try:
            return self.__await_future_runtime_hook(future)
        except TensorlakeError:
            raise
        except Exception as e:
            raise InternalError("Unexpected error while awaiting future") from e

    def __await_future_runtime_hook(self, future: Future) -> Generator[None, None, Any]:
        self._user_code_cancellation_point()

        if future._id not in self._future_runs:
            raise InternalError(f"Future with ID {future._id} is not registered")
        future_run_std_future: StdFuture = self._future_runs[future._id].std_future
        # To allow the calling user code to await we need to create a Future in the
        # calling code event loop.
        user_aio_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        user_aio_loop_future: asyncio.Future = user_aio_loop.create_future()

        def _resolve_user_aio_loop_future(_: StdFuture) -> None:
            if not user_aio_loop_future.done():
                # Set result to None because the actual result is stored in Tensorlake
                # SDK Future passed to us a parameter.
                user_aio_loop.call_soon_threadsafe(
                    user_aio_loop_future.set_result, None
                )

        # std_future done callback fires from the FunctionCallFutureRun's worker thread,
        # so call_soon_threadsafe is needed to resolve on the event loop thread.
        future_run_std_future.add_done_callback(_resolve_user_aio_loop_future)

        # Handle race: future_run_std_future may have completed before we added the callback.
        if future_run_std_future.done():
            _resolve_user_aio_loop_future(future_run_std_future)

        yield from user_aio_loop_future.__await__()

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
            if future._id not in self._future_runs:
                raise InternalError(f"Future with ID {future._id} is not registered")
            std_futures.append(self._future_runs[future._id].std_future)

        # Std futures are always running as long as their user futures are considered running.
        # Their outcomes (if failed or succeeded) are also synchronized.
        done_std_futures, _ = std_wait(
            std_futures, timeout=timeout, return_when=std_return_when
        )
        done_user_futures: List[Future] = []
        not_done_user_futures: List[Future] = []
        for future in futures:
            local_future_run: LocalFutureRun = self._future_runs[future._id]
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
        user_future: Future = future_run.local_future.future
        metadata: UserFutureMetadataType = future_run.local_future.future_metadata

        function_name: str = "<unknown>"
        output_blob_serializer: UserDataSerializer | None = None
        has_output_type_hint_override: bool = False
        output_type_hint_override: Any = None
        if isinstance(future_run, FunctionCallFutureRun):
            function_name = user_future._function_name
            output_blob_serializer = function_output_serializer(
                get_function(function_name),
                metadata.output_serializer_name_override,
            )
            if metadata.has_output_type_hint_override:
                has_output_type_hint_override = True
                output_type_hint_override = metadata.output_type_hint_override
        elif isinstance(future_run, ReturnOutputFutureRun):
            function_name = user_future._function_name
            output_blob_serializer = function_output_serializer(
                get_function(function_name),
                metadata.output_serializer_name_override,
            )
            if metadata.has_output_type_hint_override:
                has_output_type_hint_override = True
                output_type_hint_override = metadata.output_type_hint_override
        elif isinstance(future_run, ListFutureRun):
            function_name = "Assembly future list"
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

        if isinstance(result.output, Future):
            future: Future = result.output
            try:
                # ReturnOutputFutureRun and ListFutureRun are not doing real tail calls when returning a Future.
                if isinstance(future_run, FunctionCallFutureRun):
                    validate_tail_call_user_future(
                        function_name=function_name, tail_call_user_future=future
                    )
            except SDKUsageError as e:
                self._handle_future_run_failure(
                    future_run=future_run,
                    error=e,
                )
                return

            # The future returned by user is already running so must be in the dict.
            source_future_run: LocalFutureRun = self._future_runs[future._id]
            self._link_future_run_output_to_consumer(
                source=source_future_run,
                consumer=future_run,
            )
        else:
            ser_value: SerializedValue | None = None
            if result.error is None:
                try:
                    ser_value = _to_serialized_value(
                        value_id=user_future._id,
                        value=result.output,
                        value_serializer=output_blob_serializer,
                        type_hint=(
                            output_type_hint_override
                            if has_output_type_hint_override
                            else type(result.output)
                        ),
                    )
                except SerializationError as e:
                    self._handle_future_run_failure(future_run=future_run, error=e)
                    return

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

    def _future_run_data_dependencies_are_resolved(
        self, future_run: LocalFutureRun
    ) -> bool:
        if isinstance(future_run, FunctionCallFutureRun):
            metadata: FunctionCallMetadata = future_run.local_future.future_metadata
            for arg_metadata in metadata.args:
                if not self._value_store.has(arg_metadata.value_id):
                    return False
            for arg_metadata in metadata.kwargs.values():
                if not self._value_store.has(arg_metadata.value_id):
                    return False
            return True

        elif isinstance(future_run, ReturnOutputFutureRun):
            # There are no prerequisites for ReturnOutputFutureRun.
            # It just returns a Future as a tail call or a value.
            return True

        elif isinstance(future_run, ListFutureRun):
            for item in future_run.items:
                if isinstance(item, Future):
                    if not self._value_store.has(item._id):
                        return False
            return True

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
        Called after the future function finished and Future tree that it returned
        is finished and its output is propagated as this future run's output.
        """
        future_runs: list[LocalFutureRun] = [future_run]
        future_ser_values: list[SerializedValue | None] = [ser_value]

        while len(future_runs) > 0:
            future_run = future_runs.pop()
            ser_value = future_ser_values.pop()
            future: LocalFuture = future_run.local_future

            if error is not None:
                future.future.set_exception(error)
            else:
                # Intentionally do serialize -> deserialize cycle to ensure the same UX as in remote mode.
                future.future.set_result(_deserialize_value(ser_value))

            # Finish std future so wait hooks waiting on it unblock.
            # Success/failure needs to be propagated to std future as well so std wait calls work correctly.
            future_run.finish(is_exception=error is not None)

            # Propagate output to consumer futures if any.
            for consumer_future_id in future.output_consumer_future_ids:
                consumer_future_run: LocalFutureRun = self._future_runs[
                    consumer_future_id
                ]
                consumer_future: LocalFuture = consumer_future_run.local_future
                consumer_future_output: SerializedValue | None = None
                if error is None:
                    consumer_future_output = SerializedValue(
                        data=ser_value.data,
                        metadata=ser_value.metadata.model_copy(),
                    )
                    consumer_future_output.metadata.id = consumer_future.future._id
                    self._value_store.put(consumer_future_output)
                future_runs.append(consumer_future_run)
                future_ser_values.append(consumer_future_output)

    def _link_future_run_output_to_consumer(
        self,
        source: LocalFutureRun,
        consumer: LocalFutureRun,
    ) -> None:
        """Links a FutureRun's output to the consumer FutureRun.

        Used for tail calls.
        """
        source_user_future: Future = source.local_future.future
        if not source_user_future.done():
            source.local_future.add_output_consumer_future_id(
                consumer.local_future.future._id
            )
            return

        # If the source Future already completed, the consumer link was not set when
        # the source Future result was processed. Manually propagate the result now.
        consumer_ser_value: SerializedValue | None = None
        consumer_error: TensorlakeError | None = source_user_future.exception

        if source_user_future.exception is None:
            source_ser_value: SerializedValue = self._value_store.get(
                source_user_future._id
            )
            consumer_ser_value_metadata: ValueMetadata = (
                source_ser_value.metadata.model_copy()
            )
            consumer_ser_value_metadata.id = consumer.local_future.future._id
            consumer_ser_value = SerializedValue(
                data=source_ser_value.data,
                metadata=consumer_ser_value_metadata,
            )
            self._value_store.put(consumer_ser_value)

        self._handle_future_run_final_output(
            future_run=consumer,
            ser_value=consumer_ser_value,
            error=consumer_error,
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
        metadata: FunctionCallMetadata = local_future.future_metadata
        arg_values: List[Any] = []
        kwarg_values: Dict[str, Any] = {}

        for arg_metadata in metadata.args:
            arg_values.append(
                _deserialize_value(self._value_store.get(arg_metadata.value_id))
            )
        for kwarg_key, kwarg_metadata in metadata.kwargs.items():
            kwarg_values[kwarg_key] = _deserialize_value(
                self._value_store.get(kwarg_metadata.value_id)
            )

        future_run.start(arg_values=arg_values, kwarg_values=kwarg_values)

    def _start_list_future_run(self, future_run: ListFutureRun) -> None:
        resolved_items: list[Any] = []
        for item in future_run.items:
            if isinstance(item, Future):
                resolved_items.append(
                    _deserialize_value(self._value_store.get(item._id))
                )
            else:
                resolved_items.append(item)

        future_run.start(resolved_items)

    def _create_future_run(
        self,
        future: Future,
        output_serializer_name_override: str | None,
        has_output_type_hint_override: bool,
        output_type_hint_override: Any,
    ) -> None:
        """Creates future run for the supplied Future created by user.

        future is a Future that needs to run.
        output_serializer_name_override is the name of the serializer to use for serializing
        the output of the future run. This is used when propagating output to consumer future when the
        consumer future expects a specific serialization format.

        Raises TensorlakeError on error.
        """
        if isinstance(future, ListFuture):
            self._create_future_run_for_list(
                future=future,
            )
        elif isinstance(future, ReduceOperationFuture):
            self._create_future_run_for_reduce_operation(
                future=future,
                output_serializer_name_override=output_serializer_name_override,
                has_output_type_hint_override=has_output_type_hint_override,
                output_type_hint_override=output_type_hint_override,
            )
        elif isinstance(future, FunctionCallFuture):
            self._create_future_run_for_function_call(
                future=future,
                output_serializer_name_override=output_serializer_name_override,
                has_output_type_hint_override=has_output_type_hint_override,
                output_type_hint_override=output_type_hint_override,
            )
        else:
            raise InternalError(f"Unexpected future type: {type(future)}.")

    def _create_future_run_for_list(
        self,
        future: ListFuture,
    ) -> None:
        """Creates ListFutureRun for the supplied future.

        output_serializer_name_override is not in args because ListFuture
        cannot be returned from a function.

        Raises TensorlakeError on error.
        """
        if future._metadata.kind != _FutureListKind.MAP_OPERATION:
            raise InternalError(f"Unsupported ListFuture kind: {future._metadata.kind}")
        function: Function = get_function(future._metadata.function_name)

        inputs: list[Future | Any]
        if isinstance(future._items, ListFuture):
            self._check_future_run_for_user_object_exists(future._items)
            inputs_future_run: ListFutureRun = self._future_runs[future._items._id]
            inputs = inputs_future_run.items
        else:
            for item in future._items:
                self._check_future_run_for_user_object_exists(item)
            inputs = future._items

        outputs: list[Future] = []
        for input in inputs:
            # Calling SDK recursively here. The depth of recursion is strictly one.
            # This is because the input is an already running Future, we won't decend into it.
            mapped_input: FunctionCallFuture = derived_function_call_future(
                future, function, input
            )
            outputs.append(mapped_input)

        self._future_runs[future._id] = ListFutureRun(
            local_future=LocalFuture(
                future=future,
                future_metadata=None,
                start_delay=future._start_delay,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            items=outputs,
        )

    def _create_future_run_for_function_call(
        self,
        future: FunctionCallFuture,
        output_serializer_name_override: str | None,
        has_output_type_hint_override: bool,
        output_type_hint_override: Any,
    ) -> None:
        """Creates LocalFunctionCallFutureRun for the supplied future.

        Raises TensorlakeError on error.
        """
        function: Function = get_function(future._function_name)
        user_input_serializer: UserDataSerializer = function_input_serializer(
            function, app_call=False
        )

        metadata: FunctionCallMetadata = FunctionCallMetadata(
            id=future._id,
            output_serializer_name_override=output_serializer_name_override,
            output_type_hint_override=output_type_hint_override,
            has_output_type_hint_override=has_output_type_hint_override,
            args=[],
            kwargs={},
        )

        # Arguments don't inherit serializer overrides because only the
        # root of the call tree needs to have its output serialized in a
        # specific way so its output consumer gets value in expected format.
        for arg in future._args:
            self._check_future_run_for_user_object_exists(arg)
            metadata.args.append(
                self._function_arg_metadata(arg, user_input_serializer)
            )
        for key, arg in future._kwargs.items():
            self._check_future_run_for_user_object_exists(arg)
            metadata.kwargs[key] = self._function_arg_metadata(
                arg, user_input_serializer
            )

        function_run_request_context: RequestContextHTTPClient = (
            RequestContextHTTPClient(
                request_id=_LOCAL_REQUEST_ID,
                allocation_id=future._id,
                function_name=future._function_name,
                # In local mode, the allocation id and the function run id are the same.
                function_run_id=future._id,
                server_base_url=self._request_context_http_server.base_url,
                http_client=self._request_context_http_client,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        )
        self._future_runs[future._id] = FunctionCallFutureRun(
            local_future=LocalFuture(
                future=future,
                future_metadata=metadata,
                start_delay=future._start_delay,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            application=self._app,
            function=function,
            class_instance=self._class_instance_store.get(function),
            request_context=function_run_request_context,
        )

    def _function_arg_metadata(
        self, arg: Any | Future, value_serializer: UserDataSerializer
    ) -> FunctionCallArgumentMetadata:
        # Raises TensorlakeError on error.
        if isinstance(arg, Future):
            return FunctionCallArgumentMetadata(
                value_id=arg._id,
                collection=None,
            )
        else:
            value_id: str = _request_scoped_id()
            self._value_store.put(
                _to_serialized_value(
                    value_id=value_id,
                    value=arg,
                    value_serializer=value_serializer,
                    type_hint=type(arg),
                )
            )
            return FunctionCallArgumentMetadata(
                value_id=value_id,
                collection=None,
            )

    def _create_future_run_for_reduce_operation(
        self,
        future: ReduceOperationFuture,
        output_serializer_name_override: str | None,
        has_output_type_hint_override: bool,
        output_type_hint_override: Any,
    ) -> None:
        """Creates ReduceExpansionFutureRun for the supplied reduce operation.

        Raises TensorlakeError on error.
        """
        function: Function = get_function(future._function_name)
        inputs: list[Future | Any] = []
        if future._initial is not _InitialMissing:
            inputs.append(future._initial)

        if isinstance(future._items, ListFuture):
            self._check_future_run_for_user_object_exists(future._items)
            inputs_future_run: ListFutureRun = self._future_runs[future._items._id]
            inputs.extend(inputs_future_run.items)
        else:
            for item in future._items:
                self._check_future_run_for_user_object_exists(item)
            inputs.extend(future._items)

        if len(inputs) == 0:
            raise SDKUsageError("reduce of empty iterable with no initial value")

        reduce_operation_output: Future | Any
        if len(inputs) == 1:
            reduce_operation_output = inputs[0]
        else:
            # Create a chain of function calls to reduce all args one by one.
            # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
            # using string concat function into "abcd".

            # inputs now contain at least two items.
            last_future: FunctionCallFuture = derived_function_call_future(
                future, function, inputs[0], inputs[1]
            )
            for input in inputs[2:]:
                # Calling SDK recursively here. The depth of recursion is strictly one.
                # This is because the input is an already running Future, we won't descend into it.
                last_future = derived_function_call_future(
                    future, function, last_future, input
                )

            reduce_operation_output = last_future

        metadata: ReduceOperationMetadata = ReduceOperationMetadata(
            id=future._id,
            output_serializer_name_override=output_serializer_name_override,
            output_type_hint_override=output_type_hint_override,
            has_output_type_hint_override=has_output_type_hint_override,
        )

        self._future_runs[future._id] = ReturnOutputFutureRun(
            local_future=LocalFuture(
                future=future,
                future_metadata=metadata,
                start_delay=future._start_delay,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            output=reduce_operation_output,
        )

    def _check_future_run_for_user_object_exists(self, user_object: Any) -> None:
        if isinstance(user_object, Future):
            if user_object._id not in self._future_runs:
                raise InternalError(
                    f"Future with ID {user_object._id} has no future run."
                )


def _deserialize_value(ser_value: SerializedValue) -> Any | File:
    return deserialize_value_with_metadata(
        serialized_value=ser_value.data,
        metadata=ser_value.metadata,
    )


def _to_serialized_value(
    value_id: str,
    value: Any,
    value_serializer: UserDataSerializer,
    type_hint: Any,
) -> SerializedValue:
    """Serializes the supplied value with the supplied serializer.

    Raises SerializationError if value serialization fails.
    Raises InternalError if type hints is empty.
    """
    serialized_value, metadata = serialize_value(
        value, value_serializer, value_id=value_id, type_hint=type_hint
    )
    return SerializedValue(
        data=serialized_value,
        metadata=metadata,
    )
