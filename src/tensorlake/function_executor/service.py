import importlib
import json
import os
import sys
import tempfile
import threading
import time
import zipfile
from types import ModuleType
from typing import Any, Callable, Dict, Generator, List

import grpc

from tensorlake.applications import (
    RETURN_WHEN,
    Function,
    Future,
    InternalError,
    SDKUsageError,
)
from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.function.function_call import create_self_instance
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.multiprocessing import setup_multiprocessing
from tensorlake.applications.registry import (
    get_function,
    get_functions,
    has_function,
    restore_registry,
    snapshot_registry,
)
from tensorlake.applications.remote.code.zip import (
    CODE_ZIP_MANIFEST_FILE_NAME,
    CodeZIPManifest,
    FunctionZIPManifest,
)
from tensorlake.applications.request_context.http_client.context import (
    RequestContextHTTPClient,
)
from tensorlake.applications.request_context.http_client.transport import (
    RequestContextHTTPTransport,
)
from tensorlake.applications.request_context.http_server.server import (
    RequestContextHTTPServer,
)
from tensorlake.applications.runtime_hooks import (
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

from .allocation_info import AllocationInfo
from .allocation_runner.allocation_runner import AllocationRunner
from .allocation_runner.contextvars import get_allocation_id_context_variable
from .health_check import HealthCheckHandler
from .info import info_response_kv_args
from .message_validators import InitializeRequestValidator, validate_new_allocation
from .proto.function_executor_pb2 import (
    AdvanceAllocationExecutionLogBatchRequest,
    Allocation,
    AllocationExecutionEvent,
    AllocationState,
    AllocationUpdate,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    Empty,
    FunctionRef,
    GetAllocationExecutionLogBatchRequest,
    GetAllocationExecutionLogBatchResponse,
    HealthCheckRequest,
    HealthCheckResponse,
    InfoRequest,
    InfoResponse,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    ListAllocationsRequest,
    ListAllocationsResponse,
    ReadAllocationEventLogRequest,
    ReadAllocationEventLogResponse,
    WatchAllocationEventLogReads,
    WatchAllocationStateRequest,
)
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer
from .request_context.http_handler_factory import RequestContextHTTPHandlerFactory
from .user_events import (
    InitializationEventDetails,
    log_user_event_initialization_failed,
    log_user_event_initialization_finished,
    log_user_event_initialization_started,
)

SHUTDOWN_TERMINAL_DRAIN_TIMEOUT_SECONDS = 1.0


def _module_was_loaded_from_archive(module: ModuleType, archive_path: str) -> bool:
    archive_prefix = f"{archive_path}{os.sep}"
    spec = getattr(module, "__spec__", None)
    for origin in (getattr(module, "__file__", None), getattr(spec, "origin", None)):
        if isinstance(origin, str) and (
            origin == archive_path or origin.startswith(archive_prefix)
        ):
            return True
    return False


def _restore_modules_after_failed_import(
    modules_before: dict[str, ModuleType], archive_path: str
) -> None:
    for name, module in list(sys.modules.items()):
        if not isinstance(module, ModuleType):
            continue
        if not _module_was_loaded_from_archive(module, archive_path):
            continue
        previous = modules_before.get(name)
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


class Service(FunctionExecutorServicer):
    def __init__(self, logger: InternalLogger):
        # All the fields are set during the initialization call.
        self._logger: InternalLogger = logger.bind(
            module=__name__, **info_response_kv_args()
        )
        self._function_ref: FunctionRef | None = None
        self._function: Function | None = None
        self._function_instance_arg: Any | None = None
        self._blob_store: BLOBStore | None = None
        self._request_context_http_server: RequestContextHTTPServer | None = None
        self._request_context_http_server_thread: threading.Thread | None = None
        self._request_context_http_client: RequestContextHTTPTransport | None = None
        self._health_check_handler: HealthCheckHandler | None = None
        self._initialization_lock = threading.Lock()
        self._initialization_condition = threading.Condition()
        self._initialization_in_progress = False
        self._initialization_request_count = 0
        self._initialization_attempted = False
        self._stopping = False
        self._allocation_lock = threading.Lock()
        # Tracks all existing allocations.
        # Added by create_allocation RPC, removed by delete_allocation RPC.
        self._allocation_infos: Dict[str, AllocationInfo] = {}

    def shutdown(self) -> None:
        """Stops admission and lets execution-log clients drain terminal results."""
        with self._allocation_lock:
            if self._stopping:
                return
            self._stopping = True
            allocation_infos = list(self._allocation_infos.values())
        for allocation_info in allocation_infos:
            allocation_info.runner.cancel_for_shutdown()
        deadline = time.monotonic() + SHUTDOWN_TERMINAL_DRAIN_TIMEOUT_SECONDS
        for allocation_info in allocation_infos:
            remaining = deadline - time.monotonic()
            if allocation_info.runner.wait_for_shutdown_terminal(remaining):
                continue
            self._logger.warning(
                "shutdown terminal execution batch was not observed before drain deadline",
                allocation_id=allocation_info.allocation.allocation_id,
            )

    def _abort_if_stopping(self, context: grpc.ServicerContext) -> None:
        with self._allocation_lock:
            stopping = self._stopping
        if stopping:
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "Function Executor is shutting down",
            )

    def initialize(
        self, request: InitializeRequest, context: grpc.ServicerContext
    ) -> InitializeResponse:
        self._abort_if_stopping(context)
        # Mark the request before waiting for the serial initialization lock.
        # Otherwise an allocation can observe "not initialized" in the small
        # window after this RPC starts but before the initializer publishes its
        # in-progress state.
        with self._initialization_condition:
            self._initialization_request_count += 1
            self._initialization_in_progress = True
            self._initialization_attempted = True
        try:
            with self._initialization_lock:
                self._abort_if_stopping(context)
                return self._initialize(request, context)
        finally:
            with self._initialization_condition:
                self._initialization_request_count -= 1
                self._initialization_in_progress = (
                    self._initialization_request_count > 0
                )
                if not self._initialization_in_progress:
                    self._initialization_condition.notify_all()

    def _initialize(
        self, request: InitializeRequest, context: grpc.ServicerContext
    ) -> InitializeResponse:
        start_time: float = time.monotonic()
        self._logger.info("initializing function executor service")
        if self._function is not None:
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
                failure_reason=InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
                error_message="Function Executor is already initialized",
            )

        try:
            InitializeRequestValidator(request).check()
        except BaseException as e:
            self._logger.error(
                "function executor service initialization failed",
                reason="invalid initialization request",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
                failure_reason=InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
                error_message=f"{type(e).__name__}: {e}",
            )

        event_details: InitializationEventDetails = InitializationEventDetails(
            namespace=request.function.namespace,
            application_name=request.function.application_name,
            application_version=request.function.application_version,
            function_name=request.function.function_name,
        )
        log_user_event_initialization_started(event_details)

        logger_before = self._logger
        self._logger = self._logger.bind(
            namespace=request.function.namespace,
            app=request.function.application_name,
            app_version=request.function.application_version,
            fn=request.function.function_name,
        )
        registry_before = snapshot_registry()
        modules_before = dict(sys.modules)
        app_modules_zip_path: str | None = None
        app_modules_path_installed = False
        blob_store: BLOBStore | None = None
        request_context_http_server: RequestContextHTTPServer | None = None
        request_context_http_server_thread: threading.Thread | None = None
        request_context_http_server_thread_started = False
        request_context_http_client: RequestContextHTTPTransport | None = None
        installed_hook_clearers: list[Callable[[], None]] = []
        failure_reason = (
            InitializationFailureReason.INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR
        )

        try:
            app_modules_zip_fd, app_modules_zip_path = tempfile.mkstemp(suffix=".zip")
            with open(app_modules_zip_fd, "wb") as graph_modules_zip_file:
                graph_modules_zip_file.write(request.application_code.data)
            sys.path.insert(
                0, app_modules_zip_path
            )  # Add as the first entry so user modules have highest priority
            app_modules_path_installed = True

            # Process user controlled input in a try-except block to not treat errors here as our
            # internal platform errors.
            failure_reason = (
                InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR
            )
            with zipfile.ZipFile(app_modules_zip_path, "r") as zf:
                with zf.open(CODE_ZIP_MANIFEST_FILE_NAME) as code_zip_manifest_file:
                    code_zip_manifest: CodeZIPManifest = CodeZIPManifest.model_validate(
                        json.load(code_zip_manifest_file)
                    )

            if request.function.function_name not in code_zip_manifest.functions:
                raise ValueError(
                    (
                        f"Function '{request.function.function_name}' not found in ZIP manifest of application '{request.function.application_name}'. "
                        f"Available functions: {list(code_zip_manifest.functions.keys())}"
                    )
                )

            # Load the function module so that the function is available in the registry.
            function_zip_manifest: FunctionZIPManifest = code_zip_manifest.functions[
                request.function.function_name
            ]
            importlib.import_module(function_zip_manifest.module_import_name)

            # Verify that the function exists in the registry now.
            if not has_function(request.function.function_name):
                raise ValueError(
                    (
                        f"Function '{request.function.function_name}' not found in the application '{request.function.application_name}'. "
                        f"Available functions: {repr(get_functions())}"
                    )
                )

            function = get_function(request.function.function_name)
            # The function is only loaded once per Function Executor. It's important to use a single
            # loaded function so all the allocations when executed are sharing the same memory. This allows
            # implementing smart caching in customer code. E.g. load a model into GPU only once and
            # share the model's file descriptor between all allocs or download function configuration
            # only once.
            function_instance_arg: Any | None = None
            if function._function_config.class_name is not None:
                function_instance_arg = create_self_instance(
                    function._function_config.class_name
                )

            failure_reason = (
                InitializationFailureReason.INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR
            )
            available_cpu_count: int = int(function._function_config.cpu)
            blob_store = BLOBStore(available_cpu_count=available_cpu_count)
            request_context_http_server = RequestContextHTTPServer(
                server_router_class=RequestContextHTTPHandlerFactory(
                    allocation_infos=self._allocation_infos,
                    logger=self._logger,
                ),
            )
            request_context_http_client = RequestContextHTTPClient.create_http_client(
                server_base_url=request_context_http_server.base_url
            )
            health_check_handler = HealthCheckHandler(self._logger)

            # Install process-global runtime hooks only after all fallible
            # runtime resources have been created. Track each hook so partial
            # installation can be rolled back without disturbing older hooks.
            setup_multiprocessing()
            set_run_future_hook(self._run_future_runtime_hook)
            installed_hook_clearers.append(clear_run_future_hook)
            set_await_future_hook(self._await_future_runtime_hook)
            installed_hook_clearers.append(clear_await_future_hook)
            set_wait_futures_hook(self._wait_futures_runtime_hook)
            installed_hook_clearers.append(clear_wait_futures_hook)
            set_register_coroutine_hook(self._register_coroutine_runtime_hook)
            installed_hook_clearers.append(clear_register_coroutine_hook)
            set_coroutine_to_future_hook(self._coroutine_to_future_runtime_hook)
            installed_hook_clearers.append(clear_coroutine_to_future_hook)

            request_context_http_server_thread = threading.Thread(
                target=request_context_http_server.start,
                name="FunctionExecutorRequestContextHTTPServerThread",
                daemon=True,
            )
            request_context_http_server_thread.start()
            request_context_http_server_thread_started = True

            self._function_ref = request.function
            self._function = function
            self._function_instance_arg = function_instance_arg
            self._blob_store = blob_store
            self._request_context_http_server = request_context_http_server
            self._request_context_http_server_thread = (
                request_context_http_server_thread
            )
            self._request_context_http_client = request_context_http_client
            # Only pass health checks if FE was initialized successfully.
            self._health_check_handler = health_check_handler
        except BaseException as e:
            for clear_hook in reversed(installed_hook_clearers):
                clear_hook()
            if request_context_http_client is not None:
                try:
                    request_context_http_client.close()
                except BaseException:
                    pass
            if request_context_http_server is not None:
                try:
                    request_context_http_server.stop()
                except BaseException:
                    pass
            if (
                request_context_http_server_thread_started
                and request_context_http_server_thread is not None
            ):
                try:
                    request_context_http_server_thread.join(timeout=1)
                except BaseException:
                    pass
            if blob_store is not None:
                try:
                    blob_store.close()
                except BaseException:
                    pass
            restore_registry(registry_before)
            if app_modules_zip_path is not None:
                _restore_modules_after_failed_import(
                    modules_before=modules_before,
                    archive_path=app_modules_zip_path,
                )
                if app_modules_path_installed:
                    try:
                        sys.path.remove(app_modules_zip_path)
                    except ValueError:
                        pass
                try:
                    os.unlink(app_modules_zip_path)
                except FileNotFoundError:
                    pass
            self._function_ref = None
            self._function = None
            self._function_instance_arg = None
            self._blob_store = None
            self._request_context_http_server = None
            self._request_context_http_server_thread = None
            self._request_context_http_client = None
            self._health_check_handler = None
            self._logger.error(
                "function executor service initialization failed",
                reason=(
                    "failed to load customer function"
                    if failure_reason
                    == InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR
                    else "failed to initialize runtime resources"
                ),
                duration_sec=f"{time.monotonic() - start_time:.3f}",
                # Don't log the exception to FE log as it contains customer data
            )
            log_user_event_initialization_failed(event_details, error=e)
            self._logger = logger_before
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
                failure_reason=failure_reason,
                error_message=f"{type(e).__name__}: {e}",
            )

        self._logger.info(
            "initialized function executor service",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        log_user_event_initialization_finished(event_details)
        return InitializeResponse(
            outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
        )

    def check_health(
        self, request: HealthCheckRequest, context: grpc.ServicerContext
    ) -> HealthCheckResponse:
        if self._health_check_handler is None:
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "Function Executor is not initialized, please initialize it first",
            )
        return self._health_check_handler.run(request)

    def get_info(
        self, request: InfoRequest, context: grpc.ServicerContext
    ) -> InfoResponse:
        return InfoResponse(**info_response_kv_args())

    def list_allocations(
        self, request: ListAllocationsRequest, context: grpc.ServicerContext
    ) -> ListAllocationsResponse:
        with self._allocation_lock:
            allocations = [
                alloc_info.allocation for alloc_info in self._allocation_infos.values()
            ]
        return ListAllocationsResponse(
            allocations=allocations,
        )

    def create_allocation(
        self, request: CreateAllocationRequest, context: grpc.ServicerContext
    ) -> Empty:
        self._abort_if_stopping(context)
        # Admission may block while initialization is in progress. Recheck
        # shutdown after every blocking or fallible preparation step and make
        # the final duplicate check, registration, and thread start atomic.
        self._wait_for_initialization(context)
        self._abort_if_stopping(context)
        allocation: Allocation = request.allocation
        try:
            validate_new_allocation(allocation)
        except ValueError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        self._abort_if_allocation_call_inactive(context)
        allocation_logger: InternalLogger = self._logger.bind(
            request_id=allocation.request_id,
            fn_call_id=allocation.function_call_id,
            allocation_id=allocation.allocation_id,
        )
        request_headers = (
            [
                (header.name, header.value)
                for header in allocation.inputs.request_context.headers
            ]
            if allocation.inputs.HasField("request_context")
            else []
        )
        allocation_runner: AllocationRunner = AllocationRunner(
            allocation=allocation,
            function_ref=self._function_ref,
            function=self._function,
            function_instance_arg=self._function_instance_arg,
            blob_store=self._blob_store,
            request_context=RequestContextHTTPClient(
                request_id=allocation.request_id,
                allocation_id=allocation.allocation_id,
                function_name=self._function_ref.function_name,
                function_run_id=allocation.function_call_id,
                server_base_url=self._request_context_http_server.base_url,
                http_client=self._request_context_http_client,
                blob_store=self._blob_store,
                logger=allocation_logger,
                headers=request_headers,
            ),
            logger=allocation_logger,
        )
        self._abort_if_allocation_call_inactive(context)
        with self._allocation_lock:
            if self._stopping:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "Function Executor is shutting down",
                )
            if allocation.allocation_id in self._allocation_infos:
                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    f"Allocation {allocation.allocation_id} already exists",
                )
            self._allocation_infos[allocation.allocation_id] = AllocationInfo(
                allocation=allocation,
                runner=allocation_runner,
            )
            try:
                allocation_runner.run()
            except BaseException:
                del self._allocation_infos[allocation.allocation_id]
                raise

        return Empty()

    def _wait_for_initialization(self, context: grpc.ServicerContext) -> None:
        waiting_logged = False
        with self._initialization_condition:
            while self._initialization_in_progress:
                if not waiting_logged:
                    self._logger.info(
                        "create allocation RPC waiting for initialization"
                    )
                    waiting_logged = True

                remaining = context.time_remaining()
                if isinstance(remaining, (int, float)):
                    if remaining <= 0:
                        context.abort(
                            grpc.StatusCode.DEADLINE_EXCEEDED,
                            "Initialization did not finish before the allocation deadline",
                        )
                    wait_timeout = min(remaining, 0.1)
                else:
                    wait_timeout = 0.1

                is_active = context.is_active()
                if isinstance(is_active, bool) and not is_active:
                    context.abort(
                        grpc.StatusCode.CANCELLED,
                        "Create allocation RPC was cancelled during initialization",
                    )
                self._initialization_condition.wait(timeout=wait_timeout)

            self._abort_if_allocation_call_inactive(
                context,
                deadline_message=(
                    "Initialization did not finish before the allocation deadline"
                ),
                cancellation_message=(
                    "Create allocation RPC was cancelled during initialization"
                ),
            )
            initialized = (
                self._function_ref is not None
                and self._function is not None
                and self._blob_store is not None
                and self._request_context_http_server is not None
                and self._request_context_http_client is not None
            )
            initialization_attempted = self._initialization_attempted

        if waiting_logged:
            self._logger.info(
                "create allocation RPC resumed after initialization",
                initialized=initialized,
            )
        if not initialized:
            message = (
                "Function Executor initialization failed"
                if initialization_attempted
                else "Function Executor is not initialized"
            )
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, message)

    def _abort_if_allocation_call_inactive(
        self,
        context: grpc.ServicerContext,
        *,
        deadline_message: str = "Create allocation RPC deadline exceeded",
        cancellation_message: str = (
            "Create allocation RPC was cancelled before allocation start"
        ),
    ) -> None:
        remaining = context.time_remaining()
        if isinstance(remaining, (int, float)) and remaining <= 0:
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, deadline_message)
        is_active = context.is_active()
        if isinstance(is_active, bool) and not is_active:
            context.abort(grpc.StatusCode.CANCELLED, cancellation_message)

    def watch_allocation_state(
        self, request: WatchAllocationStateRequest, context: grpc.ServicerContext
    ) -> Generator[AllocationState, None, None]:
        runner: AllocationRunner = self._get_runner_or_abort(
            request.allocation_id, context
        )

        # Stream allocation state updates until the allocation completes.
        last_seen_hash: str | None = None
        while True:
            allocation_state: AllocationState | None = (
                runner.allocation_state.wait_for_update(last_seen_hash)
            )
            if allocation_state is None:
                break
            last_seen_hash = allocation_state.sha256_hash
            yield allocation_state

    def send_allocation_update(
        self, request: AllocationUpdate, context: grpc.ServicerContext
    ) -> Empty:
        runner: AllocationRunner = self._get_runner_or_abort(
            request.allocation_id, context
        )
        if runner.allocation_state.finished:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Allocation {request.allocation_id} is already finished",
            )

        if request.HasField("request_state_operation_result"):
            runner.request_state.deliver_operation_result(
                request.request_state_operation_result
            )
        elif request.HasField("output_blob"):
            runner.blob_manager.deliver_output_blob(request.output_blob)
        else:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Unknown update type in AllocationUpdate for allocation {request.allocation_id}",
            )
        return Empty()

    def get_allocation_execution_log_batch(
        self,
        request: GetAllocationExecutionLogBatchRequest,
        context: grpc.ServicerContext,
    ) -> GetAllocationExecutionLogBatchResponse:
        runner: AllocationRunner = self._get_runner_or_abort(
            request.allocation_id, context
        )
        batch: list[AllocationExecutionEvent] | None = (
            runner.execution_log_buffer.get_current_batch()
        )
        if batch is None:
            # This should never happen normally because the previous
            # log batch must contain "finish allocation" event.
            return GetAllocationExecutionLogBatchResponse(events=[])
        return GetAllocationExecutionLogBatchResponse(events=batch)

    def advance_allocation_execution_log_batch(
        self,
        request: AdvanceAllocationExecutionLogBatchRequest,
        context: grpc.ServicerContext,
    ) -> Empty:
        runner: AllocationRunner = self._get_runner_or_abort(
            request.allocation_id, context
        )
        runner.execution_log_buffer.advance()
        return Empty()

    def watch_allocation_event_log_reads(
        self, request: WatchAllocationEventLogReads, context: grpc.ServicerContext
    ) -> Generator[ReadAllocationEventLogRequest, None, None]:
        runner: AllocationRunner = self._get_runner_or_abort(
            request.allocation_id, context
        )
        for read_request in runner.event_log_reader.watch_read_requests():
            is_active = context.is_active()
            if isinstance(is_active, bool) and not is_active:
                break
            yield read_request

    def send_allocation_event_log_read_response(
        self,
        request: ReadAllocationEventLogResponse,
        context: grpc.ServicerContext,
    ) -> Empty:
        runner: AllocationRunner = self._get_runner_or_abort(
            request.allocation_id, context
        )
        delivered = runner.event_log_reader.deliver_read_response(request)
        if not delivered:
            self._logger.warning(
                "late or duplicate allocation event-log response ignored",
                allocation_id=request.allocation_id,
                entries_count=len(request.entries),
            )
        return Empty()

    def _get_runner_or_abort(
        self, allocation_id: str, context: grpc.ServicerContext
    ) -> AllocationRunner:
        """Returns the AllocationRunner for the given allocation ID, or aborts with NOT_FOUND."""
        with self._allocation_lock:
            allocation_info = self._allocation_infos.get(allocation_id)
        if allocation_info is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Allocation {allocation_id} not found",
            )
        return allocation_info.runner

    def delete_allocation(
        self, request: DeleteAllocationRequest, context: grpc.ServicerContext
    ) -> Empty:
        runner: AllocationRunner = self._get_runner_or_abort(
            request.allocation_id, context
        )
        if not runner.allocation_state.finished:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Allocation {request.allocation_id} is still running and cannot be deleted",
            )

        with self._allocation_lock:
            current = self._allocation_infos.get(request.allocation_id)
            if current is not None and current.runner is runner:
                del self._allocation_infos[request.allocation_id]

        return Empty()

    def _thread_allocation_runner(self) -> AllocationRunner:
        """Returns the AllocationRunner for the current thread's allocation.

        Uses the allocation ID context variable to look up the runner.
        """
        try:
            allocation_id: str = get_allocation_id_context_variable()
        except LookupError:
            raise SDKUsageError(
                "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
                "Please only call Tensorlake SDK from Tensorlake Functions."
            )

        with self._allocation_lock:
            allocation_info = self._allocation_infos.get(allocation_id)
        if allocation_info is None:
            raise InternalError(
                f"allocation id '{allocation_id}' not found in Function Executor."
            )

        return allocation_info.runner

    def _await_future_runtime_hook(self, future: Future) -> Generator[None, None, Any]:
        return self._thread_allocation_runner().event_loop.await_future_runtime_hook(
            future
        )

    def _run_future_runtime_hook(self, future: Future) -> None:
        self._thread_allocation_runner().event_loop.run_future_runtime_hook(future)

    def _wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        return self._thread_allocation_runner().event_loop.wait_futures_runtime_hook(
            futures, timeout, return_when
        )

    def _register_coroutine_runtime_hook(self, coroutine: Any, future: Future) -> None:
        self._thread_allocation_runner().event_loop.register_coroutine_runtime_hook(
            coroutine, future
        )

    def _coroutine_to_future_runtime_hook(self, coroutine: Any) -> Any:
        return self._thread_allocation_runner().event_loop.coroutine_to_future_runtime_hook(
            coroutine
        )
