import importlib
import json
import sys
import tempfile
import threading
import time
import zipfile
from typing import Any, Dict, Generator, List

import grpc
import httpx

from tensorlake.applications import (
    RETURN_WHEN,
    Function,
    Future,
    RequestFailed,
    SDKUsageError,
)
from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.function.function_call import create_self_instance
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.multiprocessing import setup_multiprocessing
from tensorlake.applications.registry import get_function, get_functions, has_function
from tensorlake.applications.remote.code.zip import (
    CODE_ZIP_MANIFEST_FILE_NAME,
    CodeZIPManifest,
    FunctionZIPManifest,
)
from tensorlake.applications.request_context.http_client.context import (
    RequestContextHTTPClient,
)
from tensorlake.applications.request_context.http_server.server import (
    RequestContextHTTPServer,
)
from tensorlake.applications.runtime_hooks import (
    set_run_futures_hook,
    set_wait_futures_hook,
)

from .allocation_info import AllocationInfo
from .allocation_runner.allocation_runner import AllocationRunner
from .allocation_runner.contextvars import get_allocation_id_context_variable
from .health_check import HealthCheckHandler
from .info import info_response_kv_args
from .message_validators import InitializeRequestValidator, validate_new_allocation
from .proto.function_executor_pb2 import (
    Allocation,
    AllocationState,
    AllocationUpdate,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    Empty,
    FunctionRef,
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
        self._request_context_http_client: httpx.Client | None = None
        self._health_check_handler: HealthCheckHandler | None = None
        # Tracks all existing allocations.
        # Added by create_allocation RPC, removed by delete_allocation RPC.
        self._allocation_infos: Dict[str, AllocationInfo] = {}

    def initialize(
        self, request: InitializeRequest, context: grpc.ServicerContext
    ) -> InitializeResponse:
        start_time = time.monotonic()
        self._logger.info("initializing function executor service")

        InitializeRequestValidator(request).check()

        event_details: InitializationEventDetails = InitializationEventDetails(
            namespace=request.function.namespace,
            application_name=request.function.application_name,
            application_version=request.function.application_version,
            function_name=request.function.function_name,
        )
        log_user_event_initialization_started(event_details)

        self._function_ref = request.function
        self._logger = self._logger.bind(
            namespace=request.function.namespace,
            app=request.function.application_name,
            app_version=request.function.application_version,
            fn=request.function.function_name,
        )
        set_run_futures_hook(self._run_futures_runtime_hook)
        set_wait_futures_hook(self._wait_futures_runtime_hook)
        setup_multiprocessing()

        app_modules_zip_fd, app_modules_zip_path = tempfile.mkstemp(suffix=".zip")
        with open(app_modules_zip_fd, "wb") as graph_modules_zip_file:
            graph_modules_zip_file.write(request.application_code.data)
        sys.path.insert(
            0, app_modules_zip_path
        )  # Add as the first entry so user modules have highest priority

        try:
            # Process user controlled input in a try-except block to not treat errors here as our
            # internal platform errors.
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

            self._function = get_function(request.function.function_name)
            # The function is only loaded once per Function Executor. It's important to use a single
            # loaded function so all the allocations when executed are sharing the same memory. This allows
            # implementing smart caching in customer code. E.g. load a model into GPU only once and
            # share the model's file descriptor between all allocs or download function configuration
            # only once.
            if self._function._function_config.class_name is not None:
                self._function_instance_arg = create_self_instance(
                    self._function._function_config.class_name
                )
        except BaseException as e:
            self._logger.error(
                "function executor service initialization failed",
                reason="failed to load customer function",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
                # Don't log the exception to FE log as it contains customer data
            )
            log_user_event_initialization_failed(event_details, error=e)
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
                failure_reason=InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
            )

        available_cpu_count: int = int(self._function._function_config.cpu)
        self._blob_store = BLOBStore(available_cpu_count=available_cpu_count)

        self._request_context_http_server = RequestContextHTTPServer(
            server_router_class=RequestContextHTTPHandlerFactory(
                allocation_infos=self._allocation_infos,
                logger=self._logger,
            ),
        )
        self._request_context_http_server_thread = threading.Thread(
            target=self._request_context_http_server.start,
            name="FunctionExecutorRequestContextHTTPServerThread",
            daemon=True,
        )
        self._request_context_http_server_thread.start()
        self._request_context_http_client = RequestContextHTTPClient.create_http_client(
            server_base_url=self._request_context_http_server.base_url
        )

        # Only pass health checks if FE was initialized successfully.
        self._health_check_handler = HealthCheckHandler(self._logger)
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
        # No need to lock self._allocation_infos because we're not blocking here so we
        # hold GIL non stop.
        return ListAllocationsResponse(
            allocations=[
                alloc_info.allocation for alloc_info in self._allocation_infos.values()
            ]
        )

    def create_allocation(
        self, request: CreateAllocationRequest, context: grpc.ServicerContext
    ) -> Empty:
        # No need to lock self._allocation_infos because we're not blocking here so we
        # hold GIL non stop.
        allocation: Allocation = request.allocation
        validate_new_allocation(allocation)

        if allocation.allocation_id in self._allocation_infos:
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                f"Allocation {allocation.allocation_id} already exists",
            )

        allocation_logger: InternalLogger = self._logger.bind(
            request_id=allocation.request_id,
            fn_call_id=allocation.function_call_id,
            allocation_id=allocation.allocation_id,
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
                server_base_url=self._request_context_http_server.base_url,
                http_client=self._request_context_http_client,
                blob_store=self._blob_store,
                logger=allocation_logger,
            ),
            logger=allocation_logger,
        )
        self._allocation_infos[allocation.allocation_id] = AllocationInfo(
            allocation=allocation,
            runner=allocation_runner,
        )
        allocation_runner.run()

        return Empty()

    def watch_allocation_state(
        self, request: WatchAllocationStateRequest, context: grpc.ServicerContext
    ) -> Generator[AllocationState, None, None]:
        # No need to lock self._allocation_infos because we're not blocking here so we
        # hold GIL non stop.
        if request.allocation_id not in self._allocation_infos:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Allocation {request.allocation_id} not found",
            )

        allocation_info: AllocationInfo = self._allocation_infos[request.allocation_id]

        # Stream allocation state updates until the allocation completes.
        last_seen_hash: str | None = None
        while True:
            allocation_state: AllocationState = (
                allocation_info.runner.wait_allocation_state_update(last_seen_hash)
            )
            last_seen_hash = allocation_state.sha256_hash
            yield allocation_state
            if AllocationRunner.is_terminal_state(allocation_state):
                break

    def send_allocation_update(
        self, request: AllocationUpdate, context: grpc.ServicerContext
    ) -> Empty:
        # No need to lock self._allocation_infos because we're not blocking here so we
        # hold GIL non stop.
        if request.allocation_id not in self._allocation_infos:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Allocation {request.allocation_id} not found",
            )

        allocation_info: AllocationInfo = self._allocation_infos[request.allocation_id]
        if allocation_info.runner.finished:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Allocation {request.allocation_id} is already finished",
            )

        allocation_info.runner.deliver_allocation_update(request)
        return Empty()

    def delete_allocation(
        self, request: DeleteAllocationRequest, context: grpc.ServicerContext
    ) -> Empty:
        # No need to lock self._allocation_infos because we're not blocking here so we
        # hold GIL non stop.
        if request.allocation_id not in self._allocation_infos:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Allocation {request.allocation_id} not found",
            )

        allocation_info: AllocationInfo = self._allocation_infos[request.allocation_id]
        if not allocation_info.runner.finished:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Allocation {request.allocation_id} is still running and cannot be deleted",
            )

        del self._allocation_infos[request.allocation_id]

        return Empty()

    def _run_futures_runtime_hook(
        self, futures: List[Future], start_delay: float | None
    ) -> None:
        # NB: This function is called by user code in user function thread.
        try:
            allocation_id: str = get_allocation_id_context_variable()
        except LookupError:
            raise SDKUsageError(
                "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
                "Please only call Tensorlake SDK from Tensorlake Functions."
            )

        # No need to lock self._allocation_infos because we're not blocking here so we
        # hold GIL non stop.
        if not allocation_id in self._allocation_infos:
            raise RequestFailed(
                f"Internal error: allocation id '{allocation_id}' not found in Function Executor."
            )

        # Blocks the user function thread until done.
        # Any exception raised here goes to the calling user function.
        self._allocation_infos[allocation_id].runner.run_futures_runtime_hook(
            futures, start_delay
        )

    def _wait_futures_runtime_hook(
        self, futures: List[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[List[Future], List[Future]]:
        # NB: This function is called by user code in user function thread.
        try:
            allocation_id: str = get_allocation_id_context_variable()
        except LookupError:
            raise SDKUsageError(
                "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
                "Please only call Tensorlake SDK from Tensorlake Functions."
            )

        # No need to lock self._allocation_infos because we're not blocking here so we
        # hold GIL non stop.
        if not allocation_id in self._allocation_infos:
            raise RequestFailed(
                f"Internal error: allocation id '{allocation_id}' not found in Function Executor."
            )

        # Blocks the user function thread until done.
        # Any exception raised here goes to the calling user function.
        return self._allocation_infos[allocation_id].runner.wait_futures_runtime_hook(
            futures, timeout, return_when
        )
