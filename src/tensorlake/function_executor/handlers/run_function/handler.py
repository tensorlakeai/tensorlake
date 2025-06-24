import io
import os
import sys
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
from .function_inputs_loader import FunctionInputs, FunctionInputsLoader
from .response_helper import ResponseHelper


class Handler:
    def __init__(
        self,
        request: RunTaskRequest,
        graph_name: str,
        graph_version: str,
        function_name: str,
        invocation_state: InvocationState,
        function_wrapper: TensorlakeFunctionWrapper,
        graph_metadata: ComputeGraphMetadata,
        logger: Any,
    ):
        self._invocation_id: str = request.graph_invocation_id
        self._graph_name: str = graph_name
        self._graph_version: str = graph_version
        self._function_name: str = function_name
        self._invocation_state: InvocationState = invocation_state
        self._logger = logger.bind(
            module=__name__,
            invocation_id=request.graph_invocation_id,
            task_id=request.task_id,
            allocation_id=(
                request.allocation_id if request.HasField("allocation_id") else None
            ),
        )
        self._function_wrapper: TensorlakeFunctionWrapper = function_wrapper
        self._input_loader = FunctionInputsLoader(request)
        self._response_helper = ResponseHelper(
            task_id=request.task_id,
            function_name=function_name,
            graph_metadata=graph_metadata,
            logger=self._logger,
        )
        # TODO: use files for stdout, stderr capturing. This puts a natural and thus reasonable
        # rate limit on the rate of writes and allows to not consume expensive memory for function logs.
        self._func_stdout: io.StringIO = io.StringIO()
        self._func_stderr: io.StringIO = io.StringIO()

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
        and stderr. Never throws an Exception. Caller can determine if the function succeeded
        using the response.
        """
        try:
            if (
                os.getenv("INDEXIFY_FUNCTION_EXECUTOR_DISABLE_OUTPUT_CAPTURE", "0")
                == "1"
            ):
                return self._response_helper.from_function_call(
                    result=self._run_func(inputs),
                    is_reducer=_function_is_reducer(self._function_wrapper),
                    stdout="Function output capture is disabled using INDEXIFY_FUNCTION_EXECUTOR_DISABLE_OUTPUT_CAPTURE env var.\n",
                    stderr="",
                )

            # Flush any logs buffered in memory before doing stdout, stderr capture.
            # Otherwise our logs logged before this point will end up in the function's stdout capture.
            self._flush_logs()
            with redirect_stdout(self._func_stdout), redirect_stderr(self._func_stderr):
                try:
                    return self._response_helper.from_function_call(
                        result=self._run_func(inputs),
                        is_reducer=_function_is_reducer(self._function_wrapper),
                        stdout=self._func_stdout.getvalue(),
                        stderr=self._func_stderr.getvalue(),
                    )
                finally:
                    # Ensure that whatever outputted by the function gets captured.
                    self._flush_logs()
        except BaseException as e:
            return self._response_helper.from_function_exception(
                exception=e,
                stdout=self._func_stdout.getvalue(),
                stderr=self._func_stderr.getvalue(),
                metrics=None,
            )

    def _run_func(self, inputs: FunctionInputs) -> FunctionCallResult:
        ctx: GraphInvocationContext = GraphInvocationContext(
            invocation_id=self._invocation_id,
            graph_name=self._graph_name,
            graph_version=self._graph_version,
            invocation_state=self._invocation_state,
        )
        return self._function_wrapper.invoke_fn_ser(
            ctx, inputs.input, inputs.init_value
        )

    def _flush_logs(self) -> None:
        # structlog.PrintLogger uses print function. This is why flushing with print works.
        print("", flush=True)
        sys.stdout.flush()
        sys.stderr.flush()


def _function_is_reducer(func_wrapper: TensorlakeFunctionWrapper) -> bool:
    return func_wrapper.indexify_function.accumulate is not None
