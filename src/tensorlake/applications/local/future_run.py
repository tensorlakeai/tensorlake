from concurrent.futures import Future as StdFuture
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import SimpleQueue
from threading import Event
from typing import Any

from ..interface.awaitables import Awaitable
from ..interface.exceptions import RequestError, RequestFailureException
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
        local_future: LocalFuture,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
    ):
        self._local_future = local_future
        # Queue to put LocalFutureRunResult into when finished.
        self._result_queue: SimpleQueue = result_queue
        self._thread_pool: ThreadPoolExecutor = thread_pool
        # Std future that tracks the execution of the user future run in thread pool.
        # Std future runs as long as the user future is running.
        # Std future result is always None because user future result is stored in user future itself.
        self._std_future: StdFuture = self._thread_pool.submit(self._future_entry_point)
        # Future run waits on this event until all data dependencies in its Awaitable are resolved.
        self._start_event: Event = Event()
        # Future run waits on this event until it can exit.
        self._finish_event: Event = Event()
        # Used to cancel the future run before entering the _run_future method.
        self._cancelled: bool = False

    @property
    def local_future(self) -> LocalFuture:
        return self._local_future

    @property
    def std_future(self) -> StdFuture:
        return self._std_future

    def start(self) -> None:
        self._start_event.set()

    def finish(self) -> None:
        self._finish_event.set()

    def cancel(self) -> None:
        self._cancelled = True
        self._start_event.set()
        self._finish_event.set()

    def _future_entry_point(self) -> None:
        # Wait until all data dependencies are resolved.
        while not self._start_event.is_set():
            self._start_event.wait()

        if self._cancelled:
            return

        result: LocalFutureRunResult = self._run_future()
        self._result_queue.put(result)

        # Wait until the user future is considered finished.
        while not self._finish_event.is_set():
            self._finish_event.wait()

        # Std future result is always None.
        return None

    def _run_future(self) -> LocalFutureRunResult:
        raise NotImplementedError(
            "_run_future must be implemented by LocalFutureRun subclasses"
        )
