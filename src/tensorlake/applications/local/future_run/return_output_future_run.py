from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any

from ...interface.awaitables import (
    Awaitable,
)
from ..future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
)


class ReturnOutputFutureRun(LocalFutureRun):
    """LocalFutureRun that returns a predefined value or an awaitable as its output.

    Useful when an awaitable needs to consume output of another awaitable or
    when a predefined value needs to be returned as output of an awaitable.
    """

    def __init__(
        self,
        local_future: LocalFuture,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
        output: Any | Awaitable,
    ):
        super().__init__(
            local_future=local_future,
            result_queue=result_queue,
            thread_pool=thread_pool,
        )
        self._output: Any | Awaitable = output

    @property
    def output(self) -> Any | Awaitable:
        return self._output

    def _run_future(self) -> LocalFutureRunResult:
        return LocalFutureRunResult(
            id=self.local_future.user_future.id,
            output=self.output,
            exception=None,
        )
