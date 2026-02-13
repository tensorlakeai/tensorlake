from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any

from ...interface.futures import (
    Future,
)
from ..future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
)


class ReturnOutputFutureRun(LocalFutureRun):
    """LocalFutureRun that returns a predefined value or a Future as its output.

    Useful when a Future needs to consume output of another Future or
    when a predefined value needs to be returned as output of a Future.
    """

    def __init__(
        self,
        local_future: LocalFuture,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
        output: Any | Future,
    ):
        super().__init__(
            local_future=local_future,
            result_queue=result_queue,
            thread_pool=thread_pool,
        )
        self._output: Any | Future = output

    def _run_future(self) -> LocalFutureRunResult:
        return LocalFutureRunResult(
            id=self.local_future.future._id,
            output=self._output,
            error=None,
        )
