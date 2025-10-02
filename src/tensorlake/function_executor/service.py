import importlib
import json
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from typing import Any, Dict, Generator, Iterator, List

import grpc

from tensorlake.applications import Function, RequestProgress
from tensorlake.applications.function.function_call import create_self_instance
from tensorlake.applications.registry import get_function, get_functions, has_function
from tensorlake.applications.remote.code.zip import (
    CODE_ZIP_MANIFEST_FILE_NAME,
    CodeZIPManifest,
    FunctionZIPManifest,
)
from tensorlake.applications.request_context.request_context_base import (
    RequestContextBase,
)
from tensorlake.applications.request_context.request_metrics_recorder import (
    RequestMetricsRecorder,
)

from .blob_store.blob_store import BLOBStore
from .handlers.check_health.handler import Handler as CheckHealthHandler
from .handlers.run_function.handler import Handler as RunAllocationHandler
from .info import info_response_kv_args
from .initialize_request_validator import InitializeRequestValidator
from .logger import FunctionExecutorLogger
from .proto.function_executor_pb2 import (
    Allocation,
    AllocationResult,
    AwaitAllocationProgress,
    AwaitAllocationRequest,
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
    ProgressUpdate,
    RequestStateRequest,
    RequestStateResponse,
    SerializedObjectEncoding,
)
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer
from .proto.message_validator import MessageValidator
from .request_state.proxied_request_state import ProxiedRequestState
from .request_state.request_state_proxy_server import RequestStateProxyServer
from .user_events import (
    InitializationEventDetails,
    log_user_event_initialization_finished,
    log_user_event_initialization_started,
)


class _AllocationExecution:
    def __init__(self):
        self.complete: bool = False
        self.thread: threading.Thread | None = None
        self.updated: threading.Condition = threading.Condition()
        self.progress: ProgressUpdate = ProgressUpdate(current=0.0, total=1.0)


class _AllocationInfo:
    def __init__(self, allocation: Allocation, logger: FunctionExecutorLogger):
        self.allocation: Allocation = allocation
        self.execution: _AllocationExecution = _AllocationExecution()
        self.logger: FunctionExecutorLogger = logger.bind(
            invocation_id=allocation.request_id,
            request_id=allocation.request_id,
            task_id=allocation.task_id,
            allocation_id=allocation.allocation_id,
        )


class TaskAllocationRequestProgress(RequestProgress):
    def __init__(self, alloc_info: _AllocationInfo):
        self._alloc_info: _AllocationInfo = alloc_info

    def update(self, current: float, total: float) -> None:
        with self._alloc_info.execution.updated:
            self._alloc_info.execution.progress = ProgressUpdate(
                current=current, total=total
            )
            self._alloc_info.execution.updated.notify_all()
        # sleep(0) here momentarily releases the GIL, giving other
        # threads a chance to run - e.g. allowing the FE to handle
        # incoming RPCs, to report back await_task() progress
        # messages, &c.
        time.sleep(0)


class Service(FunctionExecutorServicer):
    def __init__(self, logger: FunctionExecutorLogger):
        # All the fields are set during the initialization call.
        self._logger: FunctionExecutorLogger = logger.bind(
            module=__name__, **info_response_kv_args()
        )
        self._function_ref: FunctionRef | None = None
        self._function: Function | None = None
        self._function_instance_arg: Any | None = None
        self._blob_store: BLOBStore | None = None
        self._request_state_proxy_server: RequestStateProxyServer | None = None
        self._check_health_handler: CheckHealthHandler | None = None
        # Task management for create_allocation/await_allocation/delete_allocation
        self._allocations: Dict[str, _AllocationInfo] = {}
        self._allocations_lock = threading.Lock()

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
            graph=request.function.application_name,
            application=request.function.application_name,
            graph_version=request.function.application_version,
            application_version=request.function.application_version,
            fn=request.function.function_name,
        )

        graph_modules_zip_fd, graph_modules_zip_path = tempfile.mkstemp(suffix=".zip")
        with open(graph_modules_zip_fd, "wb") as graph_modules_zip_file:
            graph_modules_zip_file.write(request.application_code.data)
        sys.path.insert(
            0, graph_modules_zip_path
        )  # Add as the first entry so user modules have highest priority

        try:
            # Process user controlled input in a try-except block to not treat errors here as our
            # internal platform errors.
            with zipfile.ZipFile(graph_modules_zip_path, "r") as zf:
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
            # loaded function so all the tasks when executed are sharing the same memory. This allows
            # implementing smart caching in customer code. E.g. load a model into GPU only once and
            # share the model's file descriptor between all tasks or download function configuration
            # only once.
            if self._function.function_config.class_name is not None:
                self._function_instance_arg = create_self_instance(
                    self._function.function_config.class_name
                )
        except BaseException as e:
            self._logger.error(
                "function executor service initialization failed",
                reason="failed to load customer function",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
                # Don't log the exception to FE log as it contains customer data
            )
            log_user_event_initialization_finished(event_details, success=False)
            # Print the exception to stderr so customer can see it there.
            traceback.print_exception(e)
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
                failure_reason=InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
            )

        available_cpu_count: int = int(self._function.function_config.cpu)
        self._blob_store = BLOBStore(
            available_cpu_count=available_cpu_count, logger=self._logger
        )
        # Only pass health checks if FE was initialized successfully.
        self._check_health_handler = CheckHealthHandler(self._logger)
        self._logger.info(
            "initialized function executor service",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        log_user_event_initialization_finished(event_details, success=True)
        return InitializeResponse(
            outcome_code=InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
        )

    def initialize_request_state_server(
        self,
        client_responses: Iterator[RequestStateResponse],
        context: grpc.ServicerContext,
    ) -> Generator[RequestStateRequest, None, None]:
        start_time = time.monotonic()
        self._logger.info("initializing request state proxy server")

        if self._request_state_proxy_server is not None:
            self._logger.error(
                "request state proxy server already exists, looks like client reconnected without disconnecting first"
            )
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "request state proxy server already exists, please disconnect first",
            )

        self._request_state_proxy_server = RequestStateProxyServer(
            # Should match RequestState object user serializer in RequestContext
            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
            client_responses=client_responses,
            logger=self._logger,
        )

        self._logger.info(
            "initialized request state proxy server",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        yield from self._request_state_proxy_server.run()
        self._request_state_proxy_server = None

    def _validate_new_allocation(self, allocation: Allocation):
        """Validates the allocation before creating it.

        Raises ValueError if the allocation is invalid or is not for this FE.
        This is internal error due to wrong use of FE protocol.
        """
        # Validate required fields
        (
            MessageValidator(allocation)
            .required_field("request_id")
            .required_field("task_id")
            .required_field("allocation_id")
            .required_field("inputs")
            .not_set_field("result")
        )

        # Validate allocation inputs
        (
            MessageValidator(allocation.inputs)
            .optional_serialized_objects_inside_blob("args")
            .optional_blobs("arg_blobs")
            .required_blob("function_outputs_blob")
            .required_blob("request_error_blob")
        )
        if len(allocation.inputs.args) != len(allocation.inputs.arg_blobs):
            raise ValueError(
                "Mismatched function arguments and functions argument blobs lengths, "
                f"{len(allocation.inputs.args)} != {len(allocation.inputs.arg_blobs)}"
            )

    def check_health(
        self, request: HealthCheckRequest, context: grpc.ServicerContext
    ) -> HealthCheckResponse:
        if self._check_health_handler is None:
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "Function Executor is not initialized, please initialize it first",
            )
        return self._check_health_handler.run(request)

    def get_info(
        self, request: InfoRequest, context: grpc.ServicerContext
    ) -> InfoResponse:
        return InfoResponse(**info_response_kv_args())

    def _execute_allocation_in_thread(self, alloc_info: _AllocationInfo):
        request_context: RequestContextBase = RequestContextBase(
            alloc_info.allocation.request_id,
            state=ProxiedRequestState(
                allocation_id=alloc_info.allocation.allocation_id,
                proxy_server=self._request_state_proxy_server,
            ),
            progress=TaskAllocationRequestProgress(alloc_info),
            metrics=RequestMetricsRecorder(),
        )

        try:
            result: AllocationResult = RunAllocationHandler(
                allocation=alloc_info.allocation,
                function_ref=self._function_ref,
                request_context=request_context,
                function=self._function,
                function_instance_arg=self._function_instance_arg,
                blob_store=self._blob_store,
                logger=alloc_info.logger,
            ).run()
            # We don't store any large objects in the result so it's okay to copy it.
            alloc_info.allocation.result.CopyFrom(result)
        except BaseException as e:
            # Only exceptions in our code can be raised here so we have to log them.
            alloc_info.logger.error(
                "task allocation execution failed in background thread",
                exc_info=e,
            )
        finally:
            with alloc_info.execution.updated:
                alloc_info.execution.complete = True
                alloc_info.execution.updated.notify_all()

    def list_allocations(
        self, request: ListAllocationsRequest, context: grpc.ServicerContext
    ) -> ListAllocationsResponse:
        allocations: List[Allocation] = []
        with self._allocations_lock:
            for allocation_info in self._allocations.values():
                with allocation_info.execution.updated:
                    allocations.append(_trimmed_allocation(allocation_info.allocation))
        return ListAllocationsResponse(allocations=allocations)

    def create_allocation(
        self, request: CreateAllocationRequest, context: grpc.ServicerContext
    ) -> Allocation:
        alloc: Allocation = request.allocation
        self._validate_new_allocation(alloc)

        with self._allocations_lock:
            if alloc.allocation_id in self._allocations:
                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    f"Allocation {alloc.allocation_id} already exists",
                )

            alloc_info: _AllocationInfo = _AllocationInfo(alloc, self._logger)
            self._allocations[alloc.allocation_id] = alloc_info

            alloc_info.execution.thread = threading.Thread(
                target=self._execute_allocation_in_thread,
                args=(alloc_info,),
                daemon=True,
            )
            alloc_info.execution.thread.start()

        return _trimmed_allocation(alloc)

    def await_allocation(
        self, request: AwaitAllocationRequest, context: grpc.ServicerContext
    ) -> Generator[AwaitAllocationProgress, None, None]:
        """Wait for allocation completion and stream progress updates."""
        alloc_info: _AllocationInfo | None = None

        with self._allocations_lock:
            alloc_info = self._allocations.get(request.allocation_id)
            if alloc_info is None:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    f"Allocation {request.allocation_id} not found",
                )

        # Stream progress updates until allocation completes
        final_result: AllocationResult | None = None
        sent_first_progress: bool = False
        while True:
            with alloc_info.execution.updated:
                if alloc_info.execution.complete:
                    # Don't send default AllocationResult() with all fields not set as its meaning is undefined.
                    # This happens when the function thread finishes with an unexpected error.
                    final_result = (
                        alloc_info.allocation.result
                        if alloc_info.allocation.HasField("result")
                        else None
                    )
                    break

                if sent_first_progress:
                    alloc_info.execution.updated.wait()
                else:
                    sent_first_progress = True

                progress = alloc_info.execution.progress

            # Do all blocking calls outside of the lock.
            if progress:
                yield AwaitAllocationProgress(progress=progress)

        # If the final result doesn't get sent before the stream is closed by FE
        # then client treats this as a grey failure with unknown exact cause.
        if final_result is not None:
            yield AwaitAllocationProgress(allocation_result=final_result)

    def delete_allocation(
        self, request: DeleteAllocationRequest, context: grpc.ServicerContext
    ) -> Empty:
        """Delete an allocation and clean up resources."""
        allocation_id: str = request.allocation_id

        with self._allocations_lock:
            alloc_info = self._allocations.get(allocation_id)

            if alloc_info is None:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    f"Allocation {allocation_id} not found",
                )

            with alloc_info.execution.updated:
                if not alloc_info.execution.complete:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        f"Allocation {allocation_id} is still running",
                    )

            self._allocations.pop(allocation_id, None)

        return Empty()


def _trimmed_allocation(alloc: Allocation) -> Allocation:
    """Returns metadata fields of the allocation without any large fields like inputs and result."""
    alloc_copy = Allocation()
    # We don't store any large objects in the inputs and result so it's okay to copy it.
    alloc_copy.CopyFrom(alloc)
    alloc_copy.ClearField("inputs")
    alloc_copy.ClearField("result")
    return alloc_copy
