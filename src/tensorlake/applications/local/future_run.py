import threading
from dataclasses import dataclass
from enum import Enum
from queue import SimpleQueue
from typing import Any

from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.function import Function
from ..interface.futures import Future
from ..interface.request_context import RequestContext
from .future import FutureType


@dataclass
class LocalFutureRunResult:
    id: str
    # Either output or exception are set.
    output: Any | Future | None
    exception: RequestError | RequestFailureException | None


class LocalFutureRunState(Enum):
    STOPPED = 0
    RUNNING = 1
    SUCCESS = 2
    FAILED = 3


class StopLocalFutureRun(BaseException):
    """Exception raised in a future run to stop it immediately.

    Expected to be caught by local runner LocalFutureRun thread and silently dropped.
    Inherited from BaseException so that it is not caught by most exception handlers
    i.e. in user code. If caught by user code then stopping LocalFutureRun thread
    will not happen quickly.
    """

    def __init__(self):
        super().__init__("Future run stopped")


class LocalFutureRun:
    """Abstract base class for local future runs."""

    def __init__(
        self,
        application: Function,
        future: FutureType,
        request_context: RequestContext,
        result_queue: SimpleQueue,
    ):
        self._application: Function = application
        self._future = future
        self._request_context: RequestContext = request_context
        # Queue to put LocalFutureRunResult into when finished.
        self._result_queue: SimpleQueue = result_queue
        self._state: LocalFutureRunState = LocalFutureRunState.STOPPED
        # daemon = True doesn't block the program from exiting if the thread is still running.
        self._thread: threading.Thread = threading.Thread(
            target=self._run_in_thread, daemon=True
        )

    @property
    def future(self) -> FutureType:
        return self._future

    @property
    def finished(self) -> bool:
        return (
            self._state == LocalFutureRunState.SUCCESS
            or self._state == LocalFutureRunState.FAILED
        )

    def wait(self) -> None:
        if self._thread.is_alive():
            self._thread.join()

    def start(self) -> None:
        self._state = LocalFutureRunState.RUNNING
        self._thread.start()

    def _run_in_thread(self) -> None:
        raise NotImplementedError("_run_in_thread must be implemented by subclasses")
