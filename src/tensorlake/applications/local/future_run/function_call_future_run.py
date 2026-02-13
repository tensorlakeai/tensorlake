import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any

from ...function.function_call import (
    create_function_error,
    set_self_arg,
)
from ...interface.exceptions import InternalError, RequestError
from ...interface.function import Function
from ...interface.futures import (
    FunctionCallFuture,
    Future,
)
from ...interface.request_context import RequestContext
from ...interface.retries import Retries
from ...request_context.contextvar import set_current_request_context
from ..future import LocalFuture
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
        if not isinstance(local_future.future, FunctionCallFuture):
            raise InternalError(
                "local_future must be a LocalFuture of FunctionCallFuture"
            )

        self._application: Function = application
        self._function: Function = function
        self._class_instance: Any | None = class_instance
        self._request_context: RequestContext = request_context
        self._arg_values: list[Any] | None = None
        self._kwarg_values: list[str, Any] | None = None

    def start(self, arg_values: list[Any], kwarg_values: dict[str, Any]) -> None:
        """Starts the function call future run with resolved argument values.

        The argument values must be fully resolved (no unresolved futures among them).
        """
        self._arg_values = arg_values
        self._kwarg_values = kwarg_values
        super().start()

    def _run_future(self) -> LocalFutureRunResult:
        """Runs the function call and returns its result.

        The function call must have all its arguments resolved (no futures among them).
        If self._class_instance is not None, it is set as the self argument of the function call.

        Must be run in contextvars.Context of the Tensorlake Function call.

        Doesn't raise any exceptions, instead returns them in LocalFutureRunResult.exception.
        """
        set_current_request_context(self._request_context)

        future: FunctionCallFuture = self._local_future.future
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
                if inspect.iscoroutinefunction(self._function):
                    result: Any | Future = asyncio.run(
                        self._function._original_function(
                            *self._arg_values, **self._kwarg_values
                        )
                    )
                else:
                    result: Any | Future = self._function._original_function(
                        *self._arg_values, **self._kwarg_values
                    )
                return LocalFutureRunResult(id=future._id, output=result, error=None)
            except RequestError as e:
                # Never retry on RequestError.
                return LocalFutureRunResult(id=future._id, output=None, error=e)
            except StopLocalFutureRun:
                return LocalFutureRunResult(
                    id=future._id,
                    output=None,
                    error=create_function_error(future, cause="stopped"),
                )
            except BaseException as e:
                runs_left -= 1
                if runs_left == 0:
                    return LocalFutureRunResult(
                        id=future._id,
                        output=None,
                        error=create_function_error(future, cause=e),
                    )
