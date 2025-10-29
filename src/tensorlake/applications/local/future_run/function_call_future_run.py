from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any, Dict, List

from ...function.function_call import (
    set_self_arg,
)
from ...interface.awaitables import (
    FunctionCallAwaitable,
    FunctionCallFuture,
)
from ...interface.exceptions import RequestError, RequestFailureException
from ...interface.function import Function
from ...interface.request_context import RequestContext
from ...interface.retries import Retries
from ...request_context.contextvar import set_current_request_context
from ..future import LocalFuture
from ..utils import print_exception
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
)


class FunctionCallFutureRun(LocalFutureRun):
    """LocalFutureRun that runs a function call and returns its result."""

    def __init__(
        self,
        local_future: LocalFuture,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
        application: Function,
        function: Function,
        class_instance: Any | None,
        request_context: RequestContext,
    ):
        super().__init__(
            local_future=local_future,
            result_queue=result_queue,
            thread_pool=thread_pool,
        )
        if not isinstance(local_future.user_future, FunctionCallFuture):
            raise ValueError("local_future must be a LocalFuture of FunctionCallFuture")
        self._application: Function = application
        self._function: Function = function
        self._class_instance: Any | None = class_instance
        self._request_context: RequestContext = request_context
        self._arg_values: List[Any] | None = None
        self._kwarg_values: Dict[str, Any] | None = None

    def start(self, arg_values: List[Any], kwarg_values: Dict[str, Any]) -> None:
        """Starts the function call future run with resolved argument values.

        The argument values must be fully resolved (no awaitables or futures among them).
        """
        self._arg_values = arg_values
        self._kwarg_values = kwarg_values
        super().start()

    def _run_future(self) -> LocalFutureRunResult:
        """Runs the function call awaitable and returns its result.

        The awaitable must have all its arguments resolved (no awaitables or futures among them).
        If class_instance is not None, it is set as the self argument of the function call.

        Must be run in contextvars.Context of the Tensorlake Function call.

        Doesn't raise any exceptions, instead returns them in LocalFutureRunResult.exception.
        """
        set_current_request_context(self._request_context)

        awaitable: FunctionCallAwaitable = self._local_future.user_future.awaitable
        if self._class_instance is not None:
            set_self_arg(args=self._arg_values, self_instance=self._class_instance)

        # Application retries are used if function retries are not set.
        retries: Retries = (
            self._application._application_config.retries
            if self._function._function_config.retries is None
            else self._function._function_config.retries
        )
        runs_left: int = 1 + retries.max_retries
        while True:
            try:
                result: Any = self._function._original_function(
                    *self._arg_values, **self._kwarg_values
                )
                return LocalFutureRunResult(
                    id=awaitable.id, output=result, exception=None
                )
            except RequestError as e:
                # Never retry on RequestError.
                return LocalFutureRunResult(id=awaitable.id, output=None, exception=e)
            except StopLocalFutureRun:
                return LocalFutureRunResult(
                    id=awaitable.id,
                    output=None,
                    exception=RequestFailureException("Function run stopped"),
                )
            except BaseException as e:
                runs_left -= 1
                if runs_left == 0:
                    print_exception(e)
                    return LocalFutureRunResult(
                        id=awaitable.id,
                        output=None,
                        exception=RequestFailureException("Function failed"),
                    )
