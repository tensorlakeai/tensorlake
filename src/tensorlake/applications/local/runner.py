import asyncio
import inspect
import shutil
import sys
import tempfile
import threading
import weakref
from collections.abc import Coroutine, Generator
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

from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.metadata import SPLITTER_INPUT_MODE, ValueMetadata
from tensorlake.applications.multiprocessing import setup_multiprocessing

from ..algorithms import (
    dfs_bottom_up_unique_only,
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
    TimeoutError,
)
from ..interface.futures import (
    FunctionCallFuture,
    MapFuture,
    ReduceOperationFuture,
    _InitialMissing,
    _InitialMissingType,
    _request_scoped_id,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)
from ..metadata import (
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
)
from ..registry import get_function
from ..request_context.http_client.context import RequestContextHTTPClient
from ..request_context.http_client.transport import RequestContextHTTPTransport
from ..request_context.http_server.server import RequestContextHTTPServer
from ..runtime_hooks import (
    clear_await_future_hook,
    clear_coroutine_to_future_hook,
    clear_register_coroutine_hook,
    clear_run_future_hook,
    clear_wait_futures_hook,
    set_await_future_hook,
    set_coroutine_to_future_hook,
    set_register_coroutine_hook,
    set_run_future_hook,
    set_wait_futures_hook,
)
from ..user_data_serializer import (
    PickleUserDataSerializer,
    UserDataSerializer,
)
from ..validation import (
    ValidationMessage,
    format_validation_messages,
    has_error_message,
    validate_loaded_applications,
)
from .class_instance_store import ClassInstanceStore
from .future import LocalFunctionCallFuture
from .future_run.function_call_future_run import FunctionCallFutureRun
from .future_run.future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
    get_current_future_run,
)
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
        # Coroutine -> Future, use weakref so once a coroutine is no longer used
        # it's deleted from the mapping automatically. This is required because coroutine
        # objects are owned by user code and so is their lifecycle.
        self._coroutine_to_future: weakref.WeakKeyDictionary[Coroutine, Future] = (
            weakref.WeakKeyDictionary()
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
        self._request_context_http_client: RequestContextHTTPTransport = (
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
            for msg in format_validation_messages(validation_messages):
                print(
                    f"{msg['severity']}: {msg['location']}{msg['message']}",
                    file=sys.stderr,
                )
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

        set_run_future_hook(self._run_future_runtime_hook)
        set_await_future_hook(self._await_future_runtime_hook)
        set_wait_futures_hook(self._wait_futures_runtime_hook)
        set_register_coroutine_hook(self._register_coroutine_runtime_hook)
        set_coroutine_to_future_hook(self._coroutine_to_future_runtime_hook)
        setup_multiprocessing()

        try:
            app_signature: inspect.Signature = function_signature(self._app)
            app_function_call_future: FunctionCallFuture = self._app.future(
                *self._app_args, **self._app_kwargs
            )
            app_output_serializer: UserDataSerializer = function_output_serializer(
                self._app, None
            )
            self._create_future_run(
                future=app_function_call_future,
                output_serializer_name_override=app_output_serializer.name,
                has_output_type_hint_override=True,
                output_type_hint_override=return_type_hint(
                    app_signature.return_annotation
                ),
                is_map_concat=False,
                parent_function_name=self._app._name,
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
        clear_run_future_hook()
        clear_wait_futures_hook()
        clear_register_coroutine_hook()
        clear_coroutine_to_future_hook()

    def __enter__(self) -> "LocalRunner":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _register_coroutine_runtime_hook(
        self, coroutine: Coroutine, future: Future
    ) -> None:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        try:
            self.__register_coroutine_runtime_hook(coroutine, future)
        except TensorlakeError:
            raise
        except Exception as e:
            raise InternalError("Unexpected error while registering coroutine") from e

    def __register_coroutine_runtime_hook(
        self, coroutine: Coroutine, future: Future
    ) -> None:
        self._coroutine_to_future[coroutine] = future

    def _coroutine_to_future_runtime_hook(self, coroutine: Coroutine) -> Future | None:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        try:
            return self.__coroutine_to_future_runtime_hook(coroutine)
        except TensorlakeError:
            raise
        except Exception as e:
            raise InternalError(
                "Unexpected error while converting coroutine to future"
            ) from e

    def __coroutine_to_future_runtime_hook(self, coroutine: Coroutine) -> Future | None:
        return self._coroutine_to_future.get(coroutine, None)

    def _run_future_runtime_hook(self, user_future: Future) -> None:
        # Don't catch any exceptions here because this is called from user code
        # and we want to propagate them to the user. We don't know what user gave
        # so it's easy to fail for any reason here.
        #
        # NB: all exceptions raised here must be derived from TensorlakeError.
        # NB: this hook must block the calling thread until all the Futures are fully started.
        # If we do any asyncio await/yield operation here then this will result in concurrent
        # hooks running with non-deterministic completion order.

        try:
            self._user_code_cancellation_point()
            # SDK automatically starts user futures that are tail calls and
            # function call or other operation inputs. This is why we walk
            # the Futures tree, not just starting the user_future.
            for future in dfs_bottom_up_unique_only(user_future):
                if future._id in self._future_runs:
                    continue  # Future was already started by user.

                # Future is started by user code, cannot be a tail call.
                current_future_run: LocalFutureRun = get_current_future_run()
                self._create_future_run(
                    future=future,
                    output_serializer_name_override=None,
                    has_output_type_hint_override=False,
                    output_type_hint_override=None,
                    is_map_concat=False,
                    parent_function_name=current_future_run.local_future.future_metadata.function_name,
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

        # The coroutine is done running. It can't be started again by user code.
        # Clear the hard reference.
        future._coroutine = None

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
            elif timeout is not None:
                future._set_exception(TimeoutError())
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
            future: LocalFunctionCallFuture = future_run.local_future

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
        metadata: FunctionCallMetadata = future_run.local_future.future_metadata
        output_blob_serializer: UserDataSerializer = function_output_serializer(
            get_function(metadata.function_name),
            metadata.output_serializer_name_override,
        )
        output: Future | Any = result.output

        if isinstance(output, Future):
            # SDK automatically starts user futures that are tail calls and
            # function call or other operation inputs. This is why we walk
            # the Futures tree, not just starting the output.
            for future in dfs_bottom_up_unique_only(output):
                future: Future
                if future._id in self._future_runs:
                    self._handle_future_run_failure(
                        future_run=future_run,
                        error=SDKUsageError(
                            f"A tail call Future {future} returned from function '{metadata.function_name}' is already running, "
                            "a tail call Future should not be started."
                        ),
                    )
                    return

                is_tail_call_output: bool = future is output
                self._create_future_run(
                    future=future,
                    output_serializer_name_override=(
                        metadata.output_serializer_name_override
                        if is_tail_call_output
                        else None
                    ),
                    has_output_type_hint_override=(
                        metadata.has_output_type_hint_override
                        if is_tail_call_output
                        else False
                    ),
                    output_type_hint_override=(
                        metadata.output_type_hint_override
                        if is_tail_call_output
                        else None
                    ),
                    is_map_concat=metadata.is_map_splitter and is_tail_call_output,
                    parent_function_name=metadata.function_name,
                )

            # The future returned by user is already running so must be in the dict.
            source_future_run: LocalFutureRun = self._future_runs[output._id]
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
                            metadata.output_type_hint_override
                            if metadata.has_output_type_hint_override
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
        metadata: FunctionCallMetadata = future_run.local_future.future_metadata
        for arg_metadata in metadata.args:
            if not self._value_store.has(arg_metadata.value_id):
                return False
        for arg_metadata in metadata.kwargs.values():
            if not self._value_store.has(arg_metadata.value_id):
                return False
        return True

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
            local_future: LocalFunctionCallFuture = future_run.local_future

            # Don't overwrite if the future is already done (e.g. Future.wait timed out).
            if not local_future.future.done():
                if error is not None:
                    local_future.future._set_exception(error)
                else:
                    # Intentionally do serialize -> deserialize cycle to ensure the same UX as in remote mode.
                    local_future.future._set_result(_deserialize_value(ser_value))

            # Finish std future so wait hooks waiting on it unblock.
            # Success/failure needs to be propagated to std future as well so std wait calls work correctly.
            future_run.finish(is_exception=error is not None)

            # Propagate output to consumer futures if any.
            for consumer_future_id in local_future.output_consumer_future_ids:
                consumer_future_run: LocalFutureRun = self._future_runs[
                    consumer_future_id
                ]
                consumer_future: LocalFunctionCallFuture = (
                    consumer_future_run.local_future
                )
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
        self._start_function_call_future_run(future_run)

    def _start_function_call_future_run(
        self, future_run: FunctionCallFutureRun
    ) -> None:
        local_future: LocalFunctionCallFuture = future_run.local_future
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

    def _create_future_run(
        self,
        future: Future,
        output_serializer_name_override: str | None,
        has_output_type_hint_override: bool,
        output_type_hint_override: Any,
        is_map_concat: bool,
        parent_function_name: str,
    ) -> None:
        """Creates future run for the supplied Future created by user.

        future is a Future that needs to run.
        output_serializer_name_override is the name of the serializer to use for serializing
        the output of the future run. This is used when propagating output to consumer future when the
        consumer future expects a specific serialization format.

        Raises TensorlakeError on error.
        """
        if future._coroutine is not None and not future._run_hook_was_called:
            # We're starting the future for the user.
            # Close the coroutine to prevent "RuntimeWarning: coroutine '...' was never awaited" warning
            # because user code is not expected to await the coroutine and it's a user code bug if it does.
            future._run_hook_was_called = True
            future._coroutine.close()
            future._coroutine = None

        if isinstance(future, MapFuture):
            self._create_future_run_for_map_splitter(
                future=future,
                output_serializer_name_override=output_serializer_name_override,
                has_output_type_hint_override=has_output_type_hint_override,
                output_type_hint_override=output_type_hint_override,
                parent_function_name=parent_function_name,
            )
        elif isinstance(future, ReduceOperationFuture):
            self._create_future_run_for_reduce_operation(
                future=future,
                output_serializer_name_override=output_serializer_name_override,
                has_output_type_hint_override=has_output_type_hint_override,
                output_type_hint_override=output_type_hint_override,
                parent_function_name=parent_function_name,
            )
        elif isinstance(future, FunctionCallFuture):
            self._create_future_run_for_function_call(
                future=future,
                output_serializer_name_override=output_serializer_name_override,
                has_output_type_hint_override=has_output_type_hint_override,
                output_type_hint_override=output_type_hint_override,
                is_map_concat=is_map_concat,
            )
        else:
            raise InternalError(f"Unexpected future type: {type(future)}.")

    def _create_future_run_for_map_splitter(
        self,
        future: MapFuture,
        output_serializer_name_override: str | None,
        has_output_type_hint_override: bool,
        output_type_hint_override: Any,
        parent_function_name: str,
    ) -> None:
        """Creates FunctionCallFutureRun with is_map_splitter for the supplied MapFuture.

        The splitter waits for its inputs to resolve, then creates individual
        map function calls and a concat future to collect the results.

        Raises TensorlakeError on error.
        """
        parent_function: Function = get_function(parent_function_name)
        user_input_serializer: UserDataSerializer = function_input_serializer(
            parent_function, app_call=False
        )

        items: list[_TensorlakeFutureWrapper[Future] | Any] | Future = _unwrap_future(
            future._items
        )
        if isinstance(items, Future):
            self._check_future_run_for_user_object_exists(items)
            splitter_args = [items]
        else:
            splitter_args = []
            for item in items:
                self._check_future_run_for_user_object_exists(item)
                splitter_args.append(_unwrap_future(item))

        # Non-tail-call splitters have to use their splitter function output serializer.
        # I.e. an application function with json output serializer doing reduce
        # operation with reduce function that returns a non-json-serializable
        # (but picklable) object. `output_serializer_name_override is None`
        # means this is not a tail call.
        if output_serializer_name_override is None:
            splitter_function: Function = get_function(future._function_name)
            output_serializer_name_override = function_output_serializer(
                splitter_function, None
            ).name

        splitter_metadata: FunctionCallMetadata = FunctionCallMetadata(
            id=future._id,
            function_name=parent_function_name,
            output_serializer_name_override=output_serializer_name_override,
            output_type_hint_override=output_type_hint_override,
            has_output_type_hint_override=has_output_type_hint_override,
            args=[
                self._function_arg_metadata(arg, user_input_serializer)
                for arg in splitter_args
            ],
            kwargs={},
            is_map_splitter=True,
            is_reduce_splitter=False,
            splitter_function_name=future._function_name,
            splitter_input_mode=(
                SPLITTER_INPUT_MODE.ITEM_PER_ARG
                if isinstance(items, list)
                else SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG
            ),
            is_map_concat=False,
        )

        function_run_request_context: RequestContextHTTPClient = (
            RequestContextHTTPClient(
                request_id=_LOCAL_REQUEST_ID,
                allocation_id=future._id,
                function_name=parent_function_name,
                function_run_id=future._id,
                server_base_url=self._request_context_http_server.base_url,
                http_client=self._request_context_http_client,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        )

        self._future_runs[future._id] = FunctionCallFutureRun(
            local_future=LocalFunctionCallFuture(
                future=future,
                future_metadata=splitter_metadata,
                start_delay=future._start_delay,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            application=self._app,
            function=parent_function,
            class_instance=self._class_instance_store.get(parent_function),
            request_context=function_run_request_context,
        )

    def _create_future_run_for_function_call(
        self,
        future: FunctionCallFuture,
        output_serializer_name_override: str | None,
        has_output_type_hint_override: bool,
        output_type_hint_override: Any,
        is_map_concat: bool,
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
            function_name=future._function_name,
            output_serializer_name_override=output_serializer_name_override,
            output_type_hint_override=output_type_hint_override,
            has_output_type_hint_override=has_output_type_hint_override,
            args=[],
            kwargs={},
            is_map_splitter=False,
            is_reduce_splitter=False,
            splitter_function_name=None,
            is_map_concat=is_map_concat,
            splitter_input_mode=None,
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
            local_future=LocalFunctionCallFuture(
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
        self,
        arg: Any | _TensorlakeFutureWrapper[Future],
        value_serializer: UserDataSerializer,
    ) -> FunctionCallArgumentMetadata:
        # Raises TensorlakeError on error.
        arg: Future | Any = _unwrap_future(arg)
        if isinstance(arg, Future):
            return FunctionCallArgumentMetadata(
                value_id=arg._id,
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
            )

    def _create_future_run_for_reduce_operation(
        self,
        future: ReduceOperationFuture,
        output_serializer_name_override: str | None,
        has_output_type_hint_override: bool,
        output_type_hint_override: Any,
        parent_function_name: str,
    ) -> None:
        """Creates FunctionCallFutureRun with is_reduce_splitter for the supplied reduce operation.

        The splitter waits for its inputs (and optional initial value) to resolve,
        then creates a chain of derived FunctionCallFutures that reduce the inputs.

        Raises TensorlakeError on error.
        """
        parent_function: Function = get_function(parent_function_name)
        user_input_serializer: UserDataSerializer = function_input_serializer(
            parent_function, app_call=False
        )

        splitter_args: list[Future | Any] = []
        items: list[_TensorlakeFutureWrapper[Future] | Any] | Future = _unwrap_future(
            future._items
        )
        if isinstance(items, Future):
            self._check_future_run_for_user_object_exists(items)
            splitter_args = [items]
        else:
            for item in items:
                self._check_future_run_for_user_object_exists(item)
                splitter_args.append(_unwrap_future(item))

        splitter_kwargs: dict[str, Any] = {}
        initial: Any | _InitialMissingType = _unwrap_future(future._initial)
        if initial is not _InitialMissing:
            if isinstance(initial, Future):
                self._check_future_run_for_user_object_exists(initial)
            splitter_kwargs["initial"] = initial

        # Non-tail-call splitters have to use their splitter function output serializer.
        # I.e. an application function with json output serializer doing reduce
        # operation with reduce function that returns a non-json-serializable
        # (but picklable) object.`output_serializer_name_override is None`
        # means this is not a tail call.
        if output_serializer_name_override is None:
            splitter_function: Function = get_function(future._function_name)
            output_serializer_name_override = function_output_serializer(
                splitter_function, None
            ).name

        splitter_metadata: FunctionCallMetadata = FunctionCallMetadata(
            id=future._id,
            function_name=parent_function_name,
            output_serializer_name_override=output_serializer_name_override,
            output_type_hint_override=output_type_hint_override,
            has_output_type_hint_override=has_output_type_hint_override,
            args=[],
            kwargs={},
            is_map_splitter=False,
            is_reduce_splitter=True,
            splitter_function_name=future._function_name,
            splitter_input_mode=(
                SPLITTER_INPUT_MODE.ITEM_PER_ARG
                if isinstance(items, list)
                else SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG
            ),
            is_map_concat=False,
        )

        for arg in splitter_args:
            splitter_metadata.args.append(
                self._function_arg_metadata(arg, user_input_serializer)
            )
        for key, arg in splitter_kwargs.items():
            splitter_metadata.kwargs[key] = self._function_arg_metadata(
                arg, user_input_serializer
            )

        function_run_request_context: RequestContextHTTPClient = (
            RequestContextHTTPClient(
                request_id=_LOCAL_REQUEST_ID,
                allocation_id=future._id,
                function_name=parent_function_name,
                function_run_id=future._id,
                server_base_url=self._request_context_http_server.base_url,
                http_client=self._request_context_http_client,
                blob_store=self._blob_store,
                logger=self._logger,
            )
        )
        self._future_runs[future._id] = FunctionCallFutureRun(
            local_future=LocalFunctionCallFuture(
                future=future,
                future_metadata=splitter_metadata,
                start_delay=future._start_delay,
            ),
            result_queue=self._future_run_result_queue,
            thread_pool=self._future_run_thread_pool,
            application=self._app,
            function=parent_function,
            class_instance=self._class_instance_store.get(parent_function),
            request_context=function_run_request_context,
        )

    def _check_future_run_for_user_object_exists(
        self, user_object: Any | _TensorlakeFutureWrapper[Future]
    ) -> None:
        user_object: Future | Any = _unwrap_future(user_object)
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
