import contextvars
from queue import SimpleQueue

from ..interface.function import Function
from ..interface.futures import (
    ReduceOperationFuture,
)
from ..interface.request_context import RequestContext
from ..request_context.contextvar import set_current_request_context
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    LocalFutureRunState,
    StopLocalFutureRun,
)


class LocalReduceRun(LocalFutureRun):
    """Runs a reduce operation in a separate thread and returns its results.

    The reducer operation call future must has all its data dependecies resolved and
    instance (self) argument set.
    """

    def __init__(
        self,
        application: Function,
        function: Function,
        reduce_operation: ReduceOperationFuture,
        request_context: RequestContext,
        result_queue: SimpleQueue,
    ):
        super().__init__(
            application=application,
            future=reduce_operation,
            request_context=request_context,
            result_queue=result_queue,
        )
        self._function: Function = function
        self._reduce_operation: ReduceOperationFuture = reduce_operation

    def _run_in_thread(self) -> None:
        context: contextvars.Context = contextvars.Context()
        # TODO
        result: LocalFutureRunResult = context.run(self._run_reduce_operation)
        if result.exception is None:
            self._state = LocalFutureRunState.SUCCESS
        else:
            self._state = LocalFutureRunState.FAILED
        self._result_queue.put(result)

    def _run_reduce_operation(self) -> None:
        raise NotImplementedError()
