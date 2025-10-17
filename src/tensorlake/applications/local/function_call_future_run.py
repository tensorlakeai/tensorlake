import contextvars
import traceback
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any

from ..function.function_call import (
    set_self_arg,
)
from ..interface.awaitables import (
    FunctionCallAwaitable,
    FunctionCallFuture,
)
from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.function import Function
from ..interface.request_context import RequestContext
from ..interface.retries import Retries
from ..request_context.contextvar import set_current_request_context
from .future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
)


class LocalFunctionCallFutureRun(LocalFutureRun):
    """Runs a function call in a separate thread and returns its results in a queue.

    The function call must has all its data dependecies resolved.
    """

    def __init__(
        self,
        application: Function,
        function: Function,
        local_future: LocalFuture,
        class_instance: Any | None,
        request_context: RequestContext,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
    ):
        super().__init__(
            application=application,
            local_future=local_future,
            request_context=request_context,
            result_queue=result_queue,
            thread_pool=thread_pool,
        )
        if not isinstance(local_future.future, FunctionCallFuture):
            raise ValueError("local_future must be a LocalFuture of FunctionCallFuture")
        self._function: Function = function
        self._class_instance: Any | None = class_instance

    def _run_future(self) -> None:
        context: contextvars.Context = contextvars.Context()
        result: LocalFutureRunResult = context.run(
            run_function_call,
            application=self._application,
            function=self._function,
            awaitable=self._local_future.future.awaitable,
            class_instance=self._class_instance,
            request_context=self._request_context,
        )
        if result.exception is None:
            self._std_future.set_result(result.output)
        else:
            self._std_future.set_exception(result.exception)
        self._result_queue.put(result)


def run_function_call(
    application: Function,
    function: Function,
    awaitable: FunctionCallAwaitable,
    class_instance: Any | None,
    request_context: RequestContext,
) -> LocalFutureRunResult:
    """Runs the function call awaitable and returns its result.

    The awaitable must have all its arguments resolved (no awaitables or futures among them).
    If class_instance is not None, it is set as the self argument of the function call.

    Must be run in contextvars.Context of the Tensorlake Function call.

    Doesn't raise any exceptions, instead returns them in LocalFutureRunResult.exception.
    """
    set_current_request_context(request_context)
    if class_instance is not None:
        set_self_arg(arts=awaitable.args, self_instance=class_instance)

    # Application retries are used if function retries are not set.
    retries: Retries = (
        application._application_config.retries
        if function._function_config.retries is None
        else function._function_config.retries
    )
    runs_left: int = 1 + retries.max_retries
    while True:
        try:
            result: Any = function._original_function(
                *awaitable.args, **awaitable.kwargs
            )
            return LocalFutureRunResult(id=awaitable.id, output=result, exception=None)
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
                # We only print exceptions in remote mode but don't propagate them to SDK
                # and return a generic RequestFailureException instead. Do the same here.
                traceback.print_exception(e)
                return LocalFutureRunResult(
                    id=awaitable.id,
                    output=None,
                    exception=RequestFailureException("Function failed"),
                )
