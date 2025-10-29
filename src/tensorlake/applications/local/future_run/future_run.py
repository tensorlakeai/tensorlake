import contextvars
from concurrent.futures import Future as StdFuture
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import SimpleQueue
from threading import Event
from typing import Any

from ...interface.awaitables import Awaitable
from ...interface.exceptions import RequestError, RequestFailureException
from ..future import LocalFuture


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
        # Future run waits on this event until all data dependencies in its Awaitable are resolved.
        self._start_event: Event = Event()
        # Future run waits on this event until it can exit.
        self._finish_event: Event = Event()
        self._finish_with_exception: bool = False
        # Used to cancel the future run before entering the _run_future method.
        self._cancelled: bool = False
        # Starts the future run in thread pool. Must be the last operation in __init__.
        # Std future that tracks the execution of the user future run in thread pool.
        # Std future runs as long as the user future is running.
        # Std future result is always None because user future result is stored in user future itself.
        self._std_future: StdFuture = self._thread_pool.submit(self._future_entry_point)

    @property
    def local_future(self) -> LocalFuture:
        return self._local_future

    @property
    def std_future(self) -> StdFuture:
        return self._std_future

    def start(self) -> None:
        self._start_event.set()

    def finish(self, is_exception: bool) -> None:
        self._finish_with_exception = is_exception
        self._finish_event.set()

    def cancel(self) -> None:
        self._cancelled = True
        self._start_event.set()
        self._finish_event.set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def _future_entry_point(self) -> None:
        # Wait until all data dependencies are resolved.
        while not self._start_event.is_set():
            self._start_event.wait()

        if self._cancelled:
            return

        context: contextvars.Context = contextvars.Context()
        result: LocalFutureRunResult = context.run(self._run_future_in_context)
        self._result_queue.put(result)

        # Wait until the user future is considered finished.
        while not self._finish_event.is_set():
            self._finish_event.wait()

        if self._finish_with_exception:
            # sets self._std_future.exception to propagate the failure to waiters.
            raise Exception("Future run finished with exception")
        else:
            # sets self._std_future.result to propagate the success to waiters.
            return None

    def _run_future_in_context(self) -> LocalFutureRunResult:
        _set_current_future_run(self)
        return self._run_future()

    def _run_future(self) -> LocalFutureRunResult:
        """Runs the user future and returns its result.

        A new contextvars.Context is created for running the user future.
        """
        raise NotImplementedError(
            "_run_future must be implemented by LocalFutureRun subclasses"
        )


_current_future_run_context_var = contextvars.ContextVar("CURRENT_FUTURE_RUN")


def get_current_future_run() -> LocalFutureRun:
    """Raises LookupError if no current future run is set."""
    return _current_future_run_context_var.get()


def _set_current_future_run(future_run: LocalFutureRun) -> None:
    _current_future_run_context_var.set(future_run)
