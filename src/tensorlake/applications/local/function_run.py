import contextvars
import threading
import traceback
from dataclasses import dataclass
from enum import Enum
from queue import SimpleQueue
from typing import Any

from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.function import Function
from ..interface.futures import (
    FunctionCallFuture,
    Future,
)
from ..interface.request_context import RequestContext
from ..interface.retries import Retries
from ..request_context.contextvar import set_current_request_context


@dataclass
class LocalFunctionRunResult:
    # Either value or exception are set.
    value: Any | Future | None
    exception: RequestError | RequestFailureException | None


class LocalFunctionRunState(Enum):
    RUNNING = 1
    SUCCESS = 2
    FAILED = 3


class LocalFunctionRun:
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
        request_context: RequestContext,
        result_queue: SimpleQueue,  # SimpleQueue[LocalFunctionRunResult]
    ):
        self._application: Function = application
        self._function: Function = function
        self._function_call: FunctionCallFuture = function_call
        self._request_context: RequestContext = request_context
        self._result_queue: SimpleQueue = result_queue
        self._state: LocalFunctionRunState = LocalFunctionRunState.RUNNING
        self._thread: threading.Thread = threading.Thread(target=self._run_in_thread)
        self._thread.start()

    @property
    def finished(self) -> bool:
        return (
            self._state == LocalFunctionRunState.SUCCESS
            or self._state == LocalFunctionRunState.FAILED
        )

    def _run_in_thread(self) -> None:
        context: contextvars.Context = contextvars.Context()
        # Application retries are used if function retries are not set.
        retries: Retries = (
            self._application._application_config.retries
            if self._function._function_config.retries is None
            else self._function._function_config.retries
        )
        runs_left: int = 1 + retries.max_retries
        while True:
            try:
                return context.run(self._run_with_context)
            except RequestError as e:
                # Never retry on RequestError.
                self._state = LocalFunctionRunState.FAILED
                self._result_queue.put(LocalFunctionRunResult(value=None, exception=e))
                return
            except BaseException as e:
                runs_left -= 1
                if runs_left == 0:
                    self._state = LocalFunctionRunState.FAILED
                    # We only print exceptions in remote mode, do the same here.
                    traceback.print_exception(e)
                    self._result_queue.put(
                        LocalFunctionRunResult(
                            value=None,
                            exception=RequestFailureException("Function failed"),
                        )
                    )
                    return

    def _run_with_context(self) -> None:
        # This function is executed in contextvars.Context of the Tensorlake Function call.
        set_current_request_context(self._request_context)
        result: Any = self._function._original_function(
            *self._function_call._args, **self._function_call._kwargs
        )
        self._state = LocalFunctionRunState.SUCCESS
        self._result_queue.put(LocalFunctionRunResult(value=result, exception=None))
