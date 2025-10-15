import contextvars
import traceback
from queue import SimpleQueue
from typing import Any

from ..function.function_call import (
    set_self_arg,
)
from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.function import Function
from ..interface.futures import (
    FunctionCallFuture,
)
from ..interface.request_context import RequestContext
from ..interface.retries import Retries
from ..request_context.contextvar import set_current_request_context
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    LocalFutureRunState,
    StopLocalFutureRun,
)


class LocalFunctionRun(LocalFutureRun):
    """Runs a function call in a separate thread and returns its results.

    The function call future must has all its data dependecies resolved and
    instance (self) argument set.

    This class vaguely resembles Server FunctionRun.
    """

    def __init__(
        self,
        application: Function,
        function: Function,
        function_call: FunctionCallFuture,
        class_instance: Any | None,
        request_context: RequestContext,
        result_queue: SimpleQueue,
    ):
        super().__init__(
            application=application,
            future=function_call,
            request_context=request_context,
            result_queue=result_queue,
        )
        self._function: Function = function
        self._function_call: FunctionCallFuture = function_call
        self._class_instance: Any | None = class_instance

    def _run_in_thread(self) -> None:
        context: contextvars.Context = contextvars.Context()
        result: LocalFutureRunResult = context.run(
            run_function_call,
            application=self._application,
            function=self._function,
            function_call=self._function_call,
            class_instance=self._class_instance,
            request_context=self._request_context,
        )
        if result.exception is None:
            self._state = LocalFutureRunState.SUCCESS
        else:
            self._state = LocalFutureRunState.FAILED
        self._result_queue.put(result)


def run_function_call(
    application: Function,
    function: Function,
    function_call: FunctionCallFuture,
    class_instance: Any | None,
    request_context: RequestContext,
) -> LocalFutureRunResult:
    """Runs the function call and returns its result.

    Doesn't raise any exceptions, instead returns them in LocalFutureRunResult.exception.
    """
    # This function is executed in contextvars.Context of the Tensorlake Function call.
    set_current_request_context(request_context)
    if class_instance is not None:
        set_self_arg(function_call, class_instance)

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
                *function_call.args, **function_call.kwargs
            )
            return LocalFutureRunResult(
                id=function_call.id, output=result, exception=None
            )
        except RequestError as e:
            # Never retry on RequestError.
            return LocalFutureRunResult(id=function_call.id, output=None, exception=e)
        except StopLocalFutureRun:
            return LocalFutureRunResult(
                id=function_call.id,
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
                    id=function_call.id,
                    output=None,
                    exception=RequestFailureException("Function failed"),
                )
