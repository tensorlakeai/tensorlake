import importlib
import json
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from typing import Dict, Generator, Iterator, List, Optional

import grpc

from tensorlake.functions_sdk.functions import Progress, TensorlakeFunctionWrapper
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.graph_serialization import (
    GRAPH_MANIFEST_FILE_NAME,
    GRAPH_METADATA_FILE_NAME,
    FunctionManifest,
    GraphManifest,
)

from .blob_store.blob_store import BLOBStore
from .handlers.check_health.handler import Handler as CheckHealthHandler
from .handlers.run_function.handler import Handler as RunTaskHandler
from .info import info_response_kv_args
from .initialize_request_validator import InitializeRequestValidator
from .invocation_state.invocation_state_proxy_server import InvocationStateProxyServer
from .invocation_state.proxied_invocation_state import ProxiedInvocationState
from .logger import FunctionExecutorLogger
from .proto.function_executor_pb2 import (
    AwaitTaskProgress,
    AwaitTaskRequest,
    CreateTaskRequest,
    DeleteTaskRequest,
    Empty,
    HealthCheckRequest,
    HealthCheckResponse,
    InfoRequest,
    InfoResponse,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeDiagnostics,
    InitializeRequest,
    InitializeResponse,
    InvocationStateRequest,
    InvocationStateResponse,
    ListTasksRequest,
    ListTasksResponse,
    ProgressUpdate,
    Task,
    TaskResult,
)
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer
from .proto.message_validator import MessageValidator
from .user_events import (
    InitializationEventDetails,
    log_user_event_initialization_finished,
    log_user_event_initialization_started,
)


class _TaskExecution:
    def __init__(self):
        self.complete: bool = False
        self.thread: Optional[threading.Thread] = None
        self.updated: threading.Condition = threading.Condition()
        self.progress: ProgressUpdate = ProgressUpdate(current=0.0, total=1.0)


class _TaskInfo:
    def __init__(self, task: Task, logger: FunctionExecutorLogger):
        self.task: Task = task
        self.execution: _TaskExecution = _TaskExecution()
        self.logger: FunctionExecutorLogger = logger.bind(
            invocation_id=task.graph_invocation_id,
            task_id=task.task_id,
            allocation_id=task.allocation_id,
        )


class Service(FunctionExecutorServicer):
    def __init__(self, logger: FunctionExecutorLogger):
        # All the fields are set during the initialization call.
        self._logger: FunctionExecutorLogger = logger.bind(
            module=__name__, **info_response_kv_args()
        )
        self._namespace: Optional[str] = None
        self._graph_name: Optional[str] = None
        self._graph_version: Optional[str] = None
        self._function_name: Optional[str] = None
        self._function_wrapper: Optional[TensorlakeFunctionWrapper] = None
        self._blob_store: Optional[BLOBStore] = None
        self._graph_metadata: Optional[ComputeGraphMetadata] = None
        self._invocation_state_proxy_server: Optional[InvocationStateProxyServer] = None
        self._check_health_handler: Optional[CheckHealthHandler] = None
        # Task management for create_task/await_task/delete_task
        self._tasks: Dict[str, _TaskInfo] = {}
        self._tasks_lock = threading.Lock()

    def initialize(
        self, request: InitializeRequest, context: grpc.ServicerContext
    ) -> InitializeResponse:
        start_time = time.monotonic()
        self._logger.info("initializing function executor service")

        request_validator: InitializeRequestValidator = InitializeRequestValidator(
            request
        )
        request_validator.check()

        event_details: InitializationEventDetails = InitializationEventDetails(
            namespace=request.namespace,
            graph_name=request.graph_name,
            graph_version=request.graph_version,
            function_name=request.function_name,
        )
        log_user_event_initialization_started(event_details)

        self._namespace = request.namespace
        self._graph_name = request.graph_name
        self._graph_version = request.graph_version
        self._function_name = request.function_name
        self._logger = self._logger.bind(
            namespace=request.namespace,
            graph=request.graph_name,
            graph_version=request.graph_version,
            fn=request.function_name,
        )

        graph_modules_zip_fd, graph_modules_zip_path = tempfile.mkstemp(suffix=".zip")
        with open(graph_modules_zip_fd, "wb") as graph_modules_zip_file:
            graph_modules_zip_file.write(request.graph.data)
        sys.path.insert(
            0, graph_modules_zip_path
        )  # Add as the first entry so user modules have highest priority

        try:
            # Process user controlled input in a try-except block to not treat errors here as our
            # internal platform errors.
            with zipfile.ZipFile(graph_modules_zip_path, "r") as zf:
                with zf.open(GRAPH_MANIFEST_FILE_NAME) as graph_manifest_file:
                    graph_manifest: GraphManifest = GraphManifest.model_validate(
                        json.load(graph_manifest_file)
                    )
                with zf.open(GRAPH_METADATA_FILE_NAME) as graph_metadata_file:
                    self._graph_metadata: ComputeGraphMetadata = (
                        ComputeGraphMetadata.model_validate(
                            json.load(graph_metadata_file)
                        )
                    )
            if request.function_name not in graph_manifest.functions:
                raise ValueError(
                    f"Function {request.function_name} is not defined in the graph manifest {graph_manifest}"
                )

            function_manifest: FunctionManifest = graph_manifest.functions[
                request.function_name
            ]

            # Capture output before loading function code.
            function_module = importlib.import_module(
                function_manifest.module_import_name
            )
            function_class = getattr(
                function_module, function_manifest.class_import_name
            )
            # The function is only loaded once per Function Executor. It's important to use a single
            # loaded function so all the tasks when executed are sharing the same memory. This allows
            # implementing smart caching in customer code. E.g. load a model into GPU only once and
            # share the model's file descriptor between all tasks or download function configuration
            # only once.
            self._function_wrapper = TensorlakeFunctionWrapper(function_class)
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
                diagnostics=InitializeDiagnostics(
                    function_executor_log=self._logger.read_till_the_end(start=0),
                ),
            )

        available_cpu_count: int = int(
            self._graph_metadata.functions[request.function_name].resources.cpus
        )
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
            diagnostics=InitializeDiagnostics(
                function_executor_log=self._logger.read_till_the_end(start=0),
            ),
        )

    def initialize_invocation_state_server(
        self,
        client_responses: Iterator[InvocationStateResponse],
        context: grpc.ServicerContext,
    ) -> Generator[InvocationStateRequest, None, None]:
        start_time = time.monotonic()
        self._logger.info("initializing invocation proxy server")

        if self._invocation_state_proxy_server is not None:
            self._logger.error(
                "invocation state proxy server already exists, looks like client reconnected without disconnecting first"
            )
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "invocation state proxy server already exists, please disconnect first",
            )

        self._invocation_state_proxy_server = InvocationStateProxyServer(
            client_responses, self._logger
        )

        self._logger.info(
            "initialized invocation proxy server",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        yield from self._invocation_state_proxy_server.run()
        self._invocation_state_proxy_server = None

    def _validate_new_task(self, task: Task):
        """Validates the task before creating it.

        Raises ValueError if the task is invalid or is not for this FE.
        This is internal error due to wrong use of FE protocol.
        """
        # Validate required fields
        (
            MessageValidator(task)
            .required_field("namespace")
            .required_field("graph_name")
            .required_field("graph_version")
            .required_field("function_name")
            .required_field("graph_invocation_id")
            .required_field("task_id")
            .required_field("allocation_id")
            .required_field("request")
            .not_set_field("result")
        )

        # Validate task request (input data)
        (
            MessageValidator(task.request)
            .required_blob("function_input_blob")
            .required_serialized_object_inside_blob("function_input")
            .optional_blob("function_init_value_blob")
            .optional_serialized_object_inside_blob("function_init_value")
            .required_blob("function_outputs_blob")
            .required_blob("invocation_error_blob")
        )

        # If we run the wrongly routed task then it can steal data from this Server if it belongs
        # to a different customer.
        if task.namespace != self._namespace:
            raise ValueError(
                f"This Function Executor is not initialized for this namespace {task.namespace}"
            )
        if task.graph_name != self._graph_name:
            raise ValueError(
                f"This Function Executor is not initialized for this graph_name {task.graph_name}"
            )
        if task.graph_version != self._graph_version:
            raise ValueError(
                f"This Function Executor is not initialized for this graph_version {task.graph_version}"
            )
        if task.function_name != self._function_name:
            raise ValueError(
                f"This Function Executor is not initialized for this function_name {task.function_name}"
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

    def _execute_task_in_thread(self, task_info: _TaskInfo):
        def progress_reporter(progress: Progress):
            with task_info.execution.updated:
                task_info.execution.progress = ProgressUpdate(
                    current=progress.current, total=progress.total
                )
                task_info.execution.updated.notify_all()
            # sleep(0) here momentarily releases the GIL, giving other
            # threads a chance to run - e.g. allowing the FE to handle
            # incoming RPCs, to report back await_task() progress
            # messages, &c.
            time.sleep(0)

        try:
            result: TaskResult = RunTaskHandler(
                task=task_info.task,
                invocation_state=ProxiedInvocationState(
                    task_info.task.task_id, self._invocation_state_proxy_server
                ),
                function_wrapper=self._function_wrapper,
                graph_metadata=self._graph_metadata,
                progress_reporter=progress_reporter,
                blob_store=self._blob_store,
                logger=task_info.logger,
            ).run()
            # We don't store any large objects in the result so it's okay to copy it.
            task_info.task.result.CopyFrom(result)
        except BaseException as e:
            # Only exceptions in our code can be raised here so we have to log them.
            task_info.logger.error(
                "task execution failed in background thread",
                exc_info=e,
            )
        finally:
            with task_info.execution.updated:
                task_info.execution.complete = True
                task_info.execution.updated.notify_all()

    def list_tasks(
        self, request: ListTasksRequest, context: grpc.ServicerContext
    ) -> ListTasksResponse:
        tasks: List[Task] = []
        with self._tasks_lock:
            for task_info in self._tasks.values():
                with task_info.execution.updated:
                    tasks.append(_trimmed_task(task_info.task))
        return ListTasksResponse(tasks=tasks)

    def create_task(
        self, request: CreateTaskRequest, context: grpc.ServicerContext
    ) -> Task:
        task: Task = request.task
        self._validate_new_task(task)

        with self._tasks_lock:
            if task.task_id in self._tasks:
                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    f"Task {task.task_id} already exists",
                )

            task_info: _TaskInfo = _TaskInfo(task, self._logger)
            self._tasks[task.task_id] = task_info

            task_info.execution.thread = threading.Thread(
                target=self._execute_task_in_thread,
                args=(task_info,),
                daemon=True,
            )
            task_info.execution.thread.start()

        return _trimmed_task(task)

    def await_task(
        self, request: AwaitTaskRequest, context: grpc.ServicerContext
    ) -> Generator[AwaitTaskProgress, None, None]:
        """Wait for task completion and stream progress updates."""
        task_info: Optional[_TaskInfo] = None

        with self._tasks_lock:
            task_info = self._tasks.get(request.task_id)
            if task_info is None:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    f"Task {request.task_id} not found",
                )

        # Stream progress updates until task completes
        final_result: Optional[TaskResult] = None
        sent_first_progress: bool = False
        while True:
            with task_info.execution.updated:
                if task_info.execution.complete:
                    # Don't send default TaskResult() with all fields not set as its meaning is undefined.
                    # This happens when the function thread finishes with an unexpected error.
                    final_result = (
                        task_info.task.result
                        if task_info.task.HasField("result")
                        else None
                    )
                    break

                if sent_first_progress:
                    task_info.execution.updated.wait()
                else:
                    sent_first_progress = True

                progress = task_info.execution.progress

            # Do all blocking calls outside of the lock.
            if progress:
                yield AwaitTaskProgress(progress=progress)

        # If the final result doesn't get sent before the stream is closed by FE
        # then client treats this as a grey failure with unknown exact cause.
        if final_result is not None:
            yield AwaitTaskProgress(task_result=final_result)

    def delete_task(
        self, request: DeleteTaskRequest, context: grpc.ServicerContext
    ) -> Empty:
        """Delete a task and clean up resources."""
        task_id: str = request.task_id

        with self._tasks_lock:
            task_info = self._tasks.get(task_id)

            if task_info is None:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    f"Task {task_id} not found",
                )

            with task_info.execution.updated:
                if not task_info.execution.complete:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        f"Task {task_id} is still running",
                    )

            self._tasks.pop(task_id, None)

        return Empty()


def _trimmed_task(task: Task) -> Task:
    """Returns metadata fields of the task without any large fields like request and result."""
    task_copy = Task()
    # We don't store any large objects in the request and result
    # so it's okay to copy it.
    task_copy.CopyFrom(task)
    task_copy.ClearField("request")
    task_copy.ClearField("result")
    return task_copy
