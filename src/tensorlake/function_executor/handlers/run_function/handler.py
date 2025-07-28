import time
from typing import List, Optional

from tensorlake.functions_sdk.functions import (
    FunctionCallResult,
    GraphRequestContext,
    ProgressReporter,
    TensorlakeFunctionWrapper,
)
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.invocation_state.invocation_state import RequestState

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import FunctionInputs, Task, TaskResult
from ...user_events import (
    TaskAllocationEventDetails,
    log_user_event_task_allocations_finished,
    log_user_event_task_allocations_started,
)
from .function_inputs_loader import FunctionInputs, FunctionInputsLoader
from .response_helper import ResponseHelper


class Handler:
    def __init__(
        self,
        task: Task,
        invocation_state: RequestState,
        function_wrapper: TensorlakeFunctionWrapper,
        graph_metadata: ComputeGraphMetadata,
        progress_reporter: ProgressReporter,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._task: Task = task
        self._invocation_state: RequestState = invocation_state
        self._logger = logger.bind(module=__name__)
        self._function_wrapper: TensorlakeFunctionWrapper = function_wrapper
        self._input_loader = FunctionInputsLoader(task, blob_store, self._logger)
        self._response_helper = ResponseHelper(
            function_name=task.function_name,
            inputs=task.request,
            graph_metadata=graph_metadata,
            blob_store=blob_store,
            logger=self._logger,
        )
        self._progress_reporter: ProgressReporter = progress_reporter

    def run(self) -> TaskResult:
        """Runs the task.

        Raises an exception if our own code failed, customer function failure doesn't result in any exception.
        """
        event_details: List[TaskAllocationEventDetails] = [
            TaskAllocationEventDetails(
                namespace=self._task.namespace,
                graph_name=self._task.graph_name,
                graph_version=self._task.graph_version,
                function_name=self._task.function_name,
                graph_invocation_id=self._task.graph_invocation_id,
                task_id=self._task.task_id,
                allocation_id=self._task.allocation_id,
            )
        ]
        log_user_event_task_allocations_started(event_details)
        try:
            return self._run()
        finally:
            log_user_event_task_allocations_finished(event_details)

    def _run(self) -> TaskResult:
        inputs: FunctionInputs = self._input_loader.load()
        fe_log_start: int = self._logger.end()
        result: Optional[FunctionCallResult] = None

        try:
            result = self._run_func(inputs)
        except BaseException as e:
            return self._response_helper.from_function_exception(
                exception=e,
                fe_log_start=fe_log_start,
                metrics=None,
            )

        return self._response_helper.from_function_call(
            result=result, fe_log_start=fe_log_start
        )

    def _run_func(self, inputs: FunctionInputs) -> FunctionCallResult:
        self._logger.info("running function")
        start_time = time.monotonic()

        try:
            ctx: GraphRequestContext = GraphRequestContext(
                request_id=self._task.graph_invocation_id,
                graph_name=self._task.graph_name,
                graph_version=self._task.graph_version,
                request_state=self._invocation_state,
                progress_reporter=self._progress_reporter,
            )
            return self._function_wrapper.invoke_fn_ser(
                ctx, inputs.input, inputs.init_value
            )
        finally:
            self._logger.info(
                "function finished",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
            )
