from concurrent.futures import Future as StdFuture
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import SimpleQueue
from threading import Event
from typing import Any

from ..interface.awaitables import Awaitable
from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.function import Function
from ..interface.request_context import RequestContext
from .future import LocalFuture


@dataclass
class LocalFutureRunResult:
    id: str
    # Either output or exception are set.
    output: Any | Awaitable | None
    exception: RequestError | RequestFailureException | None


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
        local_future: LocalFuture,
        request_context: RequestContext,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
    ):
        self._application: Function = application
        self._local_future = local_future
        self._request_context: RequestContext = request_context
        # Queue to put LocalFutureRunResult into when finished.
        self._result_queue: SimpleQueue = result_queue
        self._thread_pool: ThreadPoolExecutor = thread_pool
        self._std_future: StdFuture = self._thread_pool.submit(self._wait_run_signal)
        self._run_event: Event = Event()
        # Used to cancel the future run before entering the _run_future method.
        self._cancelled: bool = False

    @property
    def local_future(self) -> LocalFuture:
        return self._local_future

    @property
    def std_future(self) -> StdFuture:
        return self._std_future

    def run(self) -> None:
        self._run_event.set()

    def cancel(self) -> None:
        self._cancelled = True
        self._run_event.set()

    def _wait_run_signal(self) -> None:
        while not self._run_event.is_set():
            self._run_event.wait()

        if self._cancelled:
            return
        else:
            self._run_future()

    def _run_future(self) -> None:
        raise NotImplementedError(
            "_run_future must be implemented by LocalFutureRun subclasses"
        )
