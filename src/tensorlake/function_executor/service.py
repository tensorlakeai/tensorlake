import importlib
import io
import json
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from typing import Any, Dict, Generator, Iterator, Optional

import grpc

from tensorlake.functions_sdk.functions import Progress, TensorlakeFunctionWrapper
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.graph_serialization import (
    GRAPH_MANIFEST_FILE_NAME,
    GRAPH_METADATA_FILE_NAME,
    FunctionManifest,
    GraphManifest,
)

from .handlers.check_health.handler import Handler as CheckHealthHandler
from .handlers.run_function.handler import Handler as RunTaskHandler
from .info import info_response_kv_args
from .initialize_request_validator import InitializeRequestValidator
from .invocation_state.invocation_state_proxy_server import InvocationStateProxyServer
from .invocation_state.proxied_invocation_state import ProxiedInvocationState
from .proto.function_executor_pb2 import (
    AwaitTaskProgress,
    AwaitTaskRequest,
    CreateTaskRequest,
    DeleteTaskRequest,
    Empty,
    FunctionInputs,
    HealthCheckRequest,
    HealthCheckResponse,
    InfoRequest,
    InfoResponse,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    InvocationStateRequest,
    InvocationStateResponse,
    ListTasksRequest,
    ListTasksResponse,
    ProgressUpdate,
    RunTaskRequest,
    RunTaskResponse,
    Task,
    TaskFailureReason,
    TaskOutcomeCode,
    TaskResult,
)
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer
from .proto.message_validator import MessageValidator
from .std_outputs_capture import flush_logs, read_till_the_end


def _run_task_request_to_function_inputs(run_request: RunTaskRequest) -> FunctionInputs:
    """Convert RunTaskRequest to FunctionInputs."""
    request = FunctionInputs()

    if run_request.HasField("function_input"):
        request.function_input.CopyFrom(run_request.function_input)

    if run_request.HasField("function_init_value"):
        request.function_init_value.CopyFrom(run_request.function_init_value)

    return request


def _run_task_request_to_task(run_request: RunTaskRequest) -> Task:
    """Convert RunTaskRequest to Task."""
    return Task(
        task_id=run_request.task_id,
        namespace=run_request.namespace,
        graph_name=run_request.graph_name,
        graph_version=run_request.graph_version,
        function_name=run_request.function_name,
        graph_invocation_id=run_request.graph_invocation_id,
        allocation_id=run_request.allocation_id,
        request=_run_task_request_to_function_inputs(run_request),
    )


def _task_result_to_run_task_response(
    task_id: str, result: TaskResult
) -> RunTaskResponse:
    """Convert TaskResult to RunTaskResponse."""
    response = RunTaskResponse(
        task_id=task_id,
        function_outputs=list(result.function_outputs),
        next_functions=list(result.next_functions),
        stdout=result.stdout,
        stderr=result.stderr,
        is_reducer=result.is_reducer,
        outcome_code=result.outcome_code,
        failure_reason=result.failure_reason,
    )

    if result.HasField("metrics"):
        response.metrics.CopyFrom(result.metrics)

    if result.HasField("invocation_error_output"):
        response.invocation_error_output.CopyFrom(result.invocation_error_output)

    return response


class _TaskExecution:
    def __init__(self):
        self.complete = False
        self.thread: threading.Thread | None = None
        self.updated = threading.Condition()
        self.progress = ProgressUpdate(current=0.0, total=1.0)


class _TaskInfo:
    def __init__(self, task: Task):
        self.task = task
        self.execution = _TaskExecution()
        self.task.result.Clear()


class Service(FunctionExecutorServicer):
    def __init__(self, logger: Any):
        # All the fields are set during the initialization call.
        self._logger = logger.bind(module=__name__, **info_response_kv_args())
        self._namespace: Optional[str] = None
        self._graph_name: Optional[str] = None
        self._graph_version: Optional[str] = None
        self._function_name: Optional[str] = None
        self._function_wrapper: Optional[TensorlakeFunctionWrapper] = None
        self._function_stdout: Optional[io.StringIO] = None
        self._function_stderr: Optional[io.StringIO] = None
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

        # The files don't have paths in filesystem so they get deleted on process exit including crashes.
        # Using buffered files instead of memory buffers for stdout, stderr puts a natural rate limit on the rate
        # of writes and allows to not consume expensive memory for function logs.
        # The same files should be used for stdout and stderr throughout the function lifetime because its code or
        # dependencies save the files during function creation and the can write to them later while we're running it.
        self._function_stdout = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
        self._function_stderr = tempfile.TemporaryFile(mode="w+", encoding="utf-8")

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

            # Flush any logs buffered in memory before doing stdout, stderr capture.
            # Otherwise our logs logged before this point will end up in the function's stdout capture.
            flush_logs(self._function_stdout, self._function_stderr)
            with redirect_stdout(self._function_stdout), redirect_stderr(
                self._function_stderr
            ):
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
                # Ensure that whatever outputted by the function gets captured.
                flush_logs(self._function_stdout, self._function_stderr)
        except Exception as e:
            self._logger.error(
                "function executor service initialization failed",
                reason="failed to load customer function",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
                # Don't log the exception to FE log as it contains customer data
            )
            formatted_exception: str = "".join(traceback.format_exception(e))
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_FAILURE,
                failure_reason=InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
                stdout=read_till_the_end(self._function_stdout, 0),
                stderr="\n".join(
                    [read_till_the_end(self._function_stderr, 0), formatted_exception]
                ),
            )

        # Only pass health checks if FE was initialized successfully.
        self._check_health_handler = CheckHealthHandler(self._logger)
        self._logger.info(
            "initialized function executor service",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        return InitializeResponse(
            outcome_code=InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS,
            stdout=read_till_the_end(self._function_stdout, 0),
            stderr=read_till_the_end(self._function_stderr, 0),
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

    def _validate_task(self, task: Task):
        # Customer function code never raises an exception because we catch all of them and add
        # their details to the response. We can only get an exception here if our own code failed.
        # If our code raises an exception the grpc framework converts it into GRPC_STATUS_UNKNOWN
        # error with the exception message. Differentiating errors is not needed for now.

        # Validate required fields
        validator = MessageValidator(task)
        validator.required_field("namespace")
        validator.required_field("graph_name")
        validator.required_field("graph_version")
        validator.required_field("function_name")
        validator.required_field("graph_invocation_id")
        validator.required_field("task_id")
        validator.required_field("allocation_id")
        validator.required_field("request")

        # Validate task request (input data)
        request_validator = MessageValidator(task.request)
        request_validator.required_serialized_object("function_input")

        # Fail with internal error as this happened due to wrong task routing to this Server.
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

    def run_task(
        self, request: RunTaskRequest, context: grpc.ServicerContext
    ) -> RunTaskResponse:
        # Convert RunTaskRequest to Task for create_task
        task = _run_task_request_to_task(request)
        self.create_task(CreateTaskRequest(task=task), context)

        try:
            for response in self.await_task(
                AwaitTaskRequest(task_id=request.task_id), context
            ):
                last_response = response
        finally:
            self.delete_task(DeleteTaskRequest(task_id=request.task_id), context)

        assert last_response.WhichOneof("response") == "task_result"
        # Convert TaskResult back to RunTaskResponse for backward compatibility
        return _task_result_to_run_task_response(
            request.task_id, last_response.task_result
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
            # Run the task handler
            result = RunTaskHandler(
                task=task_info.task,
                invocation_state=ProxiedInvocationState(
                    task_info.task.task_id, self._invocation_state_proxy_server
                ),
                function_wrapper=self._function_wrapper,
                function_stdout=self._function_stdout,
                function_stderr=self._function_stderr,
                graph_metadata=self._graph_metadata,
                progress_reporter=progress_reporter,
                logger=self._logger,
            ).run()

            task_info.task.result.CopyFrom(result)

        except BaseException as e:
            # Handle any errors by creating a failed task result
            self._logger.error(
                "task execution failed in background thread",
                task_id=task_info.task.task_id,
                exc_info=e,
            )

            raise

        finally:
            with task_info.execution.updated:
                task_info.execution.complete = True
                task_info.execution.updated.notify_all()

    def list_tasks(
        self, request: ListTasksRequest, context: grpc.ServicerContext
    ) -> ListTasksResponse:
        with self._tasks_lock:
            tasks = []
            for task_info in self._tasks.values():
                with task_info.execution.updated:
                    # Create a copy of the task without the request field for listing
                    task_copy = Task()
                    task_copy.CopyFrom(task_info.task)
                    task_copy.ClearField(
                        "request"
                    )  # Don't return input data when listing
                    task_copy.ClearField(
                        "response"
                    )  # Don't return output data when listing
                    tasks.append(task_copy)

            return ListTasksResponse(tasks=tasks)

    def create_task(
        self, request: CreateTaskRequest, context: grpc.ServicerContext
    ) -> Task:
        task = request.task

        self._validate_task(task)

        with self._tasks_lock:
            if task.task_id in self._tasks:
                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    f"Task {task.task_id} already exists",
                )

            task_info = _TaskInfo(task)
            self._tasks[task.task_id] = task_info

            task_info.execution.thread = threading.Thread(
                target=self._execute_task_in_thread,
                args=(task_info,),
                daemon=True,
            )
            task_info.execution.thread.start()

        # Return a minimal task with no optional fields set; we'll
        # extend this iff the server provides new info.
        return Task()

    def await_task(
        self, request: AwaitTaskRequest, context: grpc.ServicerContext
    ) -> Generator[AwaitTaskProgress, None, None]:
        """Wait for task completion and stream progress updates."""
        task_id = request.task_id

        with self._tasks_lock:
            task_info = self._tasks.get(task_id)

        if task_info is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Task {task_id} not found",
            )

        # Stream progress updates until task completes
        final_result = None
        sent_first_progress = False
        while True:
            with task_info.execution.updated:
                if task_info.execution.complete:
                    # Use TaskResult directly
                    final_result = task_info.task.result
                    break

                if sent_first_progress:
                    task_info.execution.updated.wait()
                else:
                    sent_first_progress = True

                progress = task_info.execution.progress

            if progress:
                yield AwaitTaskProgress(progress=progress)

        yield AwaitTaskProgress(task_result=final_result)

    def delete_task(
        self, request: DeleteTaskRequest, context: grpc.ServicerContext
    ) -> Empty:
        """Delete a task and clean up resources."""
        task_id = request.task_id

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
