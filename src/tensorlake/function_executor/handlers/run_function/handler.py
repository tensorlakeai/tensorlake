import io
import time
from contextlib import redirect_stderr, redirect_stdout
from typing import Any

from tensorlake.functions_sdk.functions import (
    FunctionCallResult,
    GraphInvocationContext,
    TensorlakeFunctionWrapper,
)
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.invocation_state.invocation_state import InvocationState

from ...proto.function_executor_pb2 import RunTaskRequest, RunTaskResponse
from ...std_outputs_capture import flush_logs, read_till_the_end
from .function_inputs_loader import FunctionInputs, FunctionInputsLoader
from .response_helper import ResponseHelper


class Handler:
    def __init__(
        self,
        request: RunTaskRequest,
        invocation_state: InvocationState,
        function_wrapper: TensorlakeFunctionWrapper,
        function_stdout: io.StringIO,
        function_stderr: io.StringIO,
        graph_metadata: ComputeGraphMetadata,
        logger: Any,
    ):
        self._request: RunTaskRequest = request
        self._invocation_state: InvocationState = invocation_state
        self._logger = logger.bind(
            module=__name__,
            invocation_id=request.graph_invocation_id,
            task_id=request.task_id,
            allocation_id=request.allocation_id,
        )
        self._function_wrapper: TensorlakeFunctionWrapper = function_wrapper
        self._function_stdout: io.StringIO = function_stdout
        self._function_stderr: io.StringIO = function_stderr
        self._input_loader = FunctionInputsLoader(request)
        self._response_helper = ResponseHelper(
            task_id=request.task_id,
            function_name=request.function_name,
            graph_metadata=graph_metadata,
            logger=self._logger,
        )

    def run(self) -> RunTaskResponse:
        """Runs the task.

        Raises an exception if our own code failed, customer function failure doesn't result in any exception.
        Details of customer function failure are returned in the response.
        """
        self._logger.info("running function")
        start_time = time.monotonic()
        inputs: FunctionInputs = self._input_loader.load()
        response: RunTaskResponse = self._run_task(inputs)
        self._logger.info(
            "function finished",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        return response

    def _run_task(self, inputs: FunctionInputs) -> RunTaskResponse:
        """Runs the customer function while capturing what happened in it.

        Function stdout and stderr are captured so they don't get into Function Executor process stdout
        and stderr. Raises an exception if our own code failed, customer function failure doesn't result in any exception.
        Details of customer function failure are returned in the response.
        """
        # Flush any logs buffered in memory before doing stdout, stderr capture.
        # Otherwise our logs logged before this point will end up in the function's stdout capture.
        flush_logs(self._function_stdout, self._function_stderr)
        stdout_start: int = self._function_stdout.tell()
        stderr_start: int = self._function_stderr.tell()

        try:
            with redirect_stdout(self._function_stdout), redirect_stderr(
                self._function_stderr
            ):
                result: FunctionCallResult = self._run_func(inputs)
                # Ensure that whatever outputted by the function gets captured.
                flush_logs(self._function_stdout, self._function_stderr)
                return self._response_helper.from_function_call(
                    result=result,
                    is_reducer=_function_is_reducer(self._function_wrapper),
                    stdout=read_till_the_end(self._function_stdout, stdout_start),
                    stderr=read_till_the_end(self._function_stderr, stderr_start),
                )
        except BaseException as e:
            return self._response_helper.from_function_exception(
                exception=e,
                stdout=read_till_the_end(self._function_stdout, stdout_start),
                stderr=read_till_the_end(self._function_stderr, stderr_start),
                metrics=None,
            )

    def _run_func(self, inputs: FunctionInputs) -> FunctionCallResult:
        ctx: GraphInvocationContext = GraphInvocationContext(
            invocation_id=self._request.graph_invocation_id,
            graph_name=self._request.graph_name,
            graph_version=self._request.graph_version,
            invocation_state=self._invocation_state,
        )
        return self._function_wrapper.invoke_fn_ser(
            ctx, inputs.input, inputs.init_value
        )


def _function_is_reducer(func_wrapper: TensorlakeFunctionWrapper) -> bool:
    return func_wrapper.indexify_function.accumulate is not None
