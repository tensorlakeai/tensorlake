import importlib
import json
import os
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
        """Initialize from gRPC request with zipped code."""
        start_time = time.monotonic()
        self._logger.info("initializing function executor service")

        InitializeRequestValidator(request).check()

        function_ref = request.function
        event_details = self._create_event_details(function_ref)
        log_user_event_initialization_started(event_details)

        # Set up function ref and logger
        self._setup_function_ref_and_logger(function_ref)

        # Load function from zip
        try:
            self._load_function_from_zip(
                request.application_code.data, function_ref.function_name
            )
        except BaseException as e:
            self._logger.error(
                "function executor service initialization failed",
                reason="failed to load customer function",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )
            log_user_event_initialization_failed(event_details, error=e)
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
                failure_reason=InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
            )

        # Complete initialization
        self._complete_initialization()
        self._logger.info(
            "initialized function executor service",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        log_user_event_initialization_finished(event_details)
        return InitializeResponse(
            outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
        )

    def initialize_from_code_path(
        self,
        code_path: str,
        namespace: str,
        app_name: str,
        app_version: str,
        function_name: str,
    ) -> bool:
        """Initialize from a local code path (for container entrypoint mode).

        Args:
            code_path: Path to the function code directory.
            namespace: Function namespace.
            app_name: Application name.
            app_version: Application version.
            function_name: Function name.

        Returns:
            True if initialization succeeded, False otherwise.
        """
        start_time = time.monotonic()
        self._logger.info(
            "initializing function executor service from code path",
            code_path=code_path,
        )

        function_ref = FunctionRef(
            namespace=namespace,
            application_name=app_name,
            application_version=app_version,
            function_name=function_name,
        )
        event_details = self._create_event_details(function_ref)
        log_user_event_initialization_started(event_details)

        # Set up function ref and logger
        self._setup_function_ref_and_logger(function_ref)

        # Load function from code path
        try:
            self._load_function_from_code_path(code_path, function_name)
        except BaseException as e:
            self._logger.error(
                "function executor service initialization failed",
                reason="failed to load customer function",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )
            log_user_event_initialization_failed(event_details, error=e)
            return False

        # Complete initialization
        self._complete_initialization()
        self._logger.info(
            "initialized function executor service from code path",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        log_user_event_initialization_finished(event_details)
        return True

    def _create_event_details(
        self, function_ref: FunctionRef
    ) -> InitializationEventDetails:
        """Create event details for logging."""
        return InitializationEventDetails(
            namespace=function_ref.namespace,
            application_name=function_ref.application_name,
            application_version=function_ref.application_version,
            function_name=function_ref.function_name,
        )

    def _setup_function_ref_and_logger(self, function_ref: FunctionRef) -> None:
        """Set up function reference, logger bindings, and runtime hooks."""
        self._function_ref = function_ref
        self._logger = self._logger.bind(
            namespace=function_ref.namespace,
            app=function_ref.application_name,
            app_version=function_ref.application_version,
            fn=function_ref.function_name,
        )
        set_run_futures_hook(self._run_futures_runtime_hook)
        set_wait_futures_hook(self._wait_futures_runtime_hook)
        setup_multiprocessing()

    def _load_function_from_zip(self, zip_data: bytes, function_name: str) -> None:
        """Load function from zipped code bytes."""
        app_modules_zip_fd, app_modules_zip_path = tempfile.mkstemp(suffix=".zip")
        with open(app_modules_zip_fd, "wb") as graph_modules_zip_file:
            graph_modules_zip_file.write(zip_data)
        self._load_function_from_zip_path(app_modules_zip_path, function_name)

    def _load_function_from_zip_path(self, zip_path: str, function_name: str) -> None:
        """Load function from a ZIP file path using Python's zipimport.

        This adds the ZIP file to sys.path and reads the manifest to import
        the specific module for the function.
        """
        zip_path = os.path.abspath(zip_path)
        sys.path.insert(0, zip_path)

        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(CODE_ZIP_MANIFEST_FILE_NAME) as code_zip_manifest_file:
                code_zip_manifest: CodeZIPManifest = CodeZIPManifest.model_validate(
                    json.load(code_zip_manifest_file)
                )

        if function_name not in code_zip_manifest.functions:
            raise ValueError(
                f"Function '{function_name}' not found in ZIP manifest. "
                f"Available functions: {list(code_zip_manifest.functions.keys())}"
            )

        function_zip_manifest: FunctionZIPManifest = code_zip_manifest.functions[
            function_name
        ]
        self._logger.info(
            "Importing module from ZIP manifest",
            module=function_zip_manifest.module_import_name,
            function=function_name,
            zip_path=zip_path,
        )
        importlib.import_module(function_zip_manifest.module_import_name)

        self._load_function_from_registry(function_name)

    def _load_function_from_code_path(self, code_path: str, function_name: str) -> None:
        """Load function from a local code path (ZIP file or directory).

        - If code_path is a ZIP file, load directly from it using zipimport
          (same as the gRPC initialize path).
        - If code_path is a directory with a manifest file, use manifest-based loading.
        - If code_path is a directory without a manifest, walk and import all .py files.
        """
        code_path = os.path.abspath(code_path)

        # Check if code_path is a ZIP file
        if os.path.isfile(code_path) and zipfile.is_zipfile(code_path):
            self._logger.info(
                "Loading function from ZIP file",
                code_path=code_path,
                function_name=function_name,
            )
            self._load_function_from_zip_path(code_path, function_name)
        elif os.path.isdir(code_path):
            # It's a directory
            if code_path not in sys.path:
                sys.path.insert(0, code_path)

            # Check if manifest file exists (from extracted ZIP)
            manifest_path = os.path.join(code_path, CODE_ZIP_MANIFEST_FILE_NAME)
            if os.path.exists(manifest_path):
                self._logger.info(
                    "Found code manifest, using manifest-based loading",
                    manifest_path=manifest_path,
                )
                self._load_function_from_manifest(
                    code_path, manifest_path, function_name
                )
            else:
                # Fallback: Import all Python modules to register functions
                self._logger.info(
                    "No manifest found, using directory walk", code_path=code_path
                )
                self._import_modules_from_path(code_path)
                self._load_function_from_registry(function_name)
        else:
            raise ValueError(
                f"code_path must be a ZIP file or directory, got: {code_path}"
            )

    def _load_function_from_manifest(
        self, code_path: str, manifest_path: str, function_name: str
    ) -> None:
        """Load function using the manifest file from an extracted ZIP."""
        with open(manifest_path, "r") as f:
            code_zip_manifest: CodeZIPManifest = CodeZIPManifest.model_validate(
                json.load(f)
            )

        if function_name not in code_zip_manifest.functions:
            raise ValueError(
                f"Function '{function_name}' not found in manifest. "
                f"Available functions: {list(code_zip_manifest.functions.keys())}"
            )

        function_zip_manifest: FunctionZIPManifest = code_zip_manifest.functions[
            function_name
        ]
        self._logger.info(
            "Importing module from manifest",
            module=function_zip_manifest.module_import_name,
            function=function_name,
        )
        importlib.import_module(function_zip_manifest.module_import_name)

        self._load_function_from_registry(function_name)

    def _load_function_from_registry(self, function_name: str) -> None:
        """Load function from registry and create instance if needed."""
        if not has_function(function_name):
            raise ValueError(
                f"Function '{function_name}' not found. "
                f"Available functions: {repr(get_functions())}"
            )

        self._function = get_function(function_name)
        # Create instance for class-based functions
        if self._function._function_config.class_name is not None:
            self._function_instance_arg = create_self_instance(
                self._function._function_config.class_name
            )

    def _complete_initialization(self) -> None:
        """Complete initialization by setting up blob store, HTTP server, and health check."""
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

        self._health_check_handler = HealthCheckHandler(self._logger)

    def _import_modules_from_path(self, code_path: str) -> None:
        """Import all Python modules from the given path to register functions."""
        for root, dirs, files in os.walk(code_path):
            # Skip __pycache__ and hidden directories
            dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]

            for file in files:
                if not file.endswith(".py") or file.startswith("."):
                    continue

                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, code_path)
                module_name = os.path.splitext(rel_path)[0].replace(os.sep, ".")

                try:
                    importlib.import_module(module_name)
                except Exception as e:
                    self._logger.debug(
                        "failed to import module",
                        module=module_name,
                        error=str(e),
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
                function_run_id=allocation.function_call_id,
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
