from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any, List

from ..interface.awaitables import (
    ListFuture,
)
from ..interface.exceptions import TensorlakeException
from .future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
)


class ListFutureRun(LocalFutureRun):
    """LocalFutureRun that awaits a list of awaitables and returns a list with their values."""

    def __init__(
        self,
        local_future: LocalFuture,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
    ):
        super().__init__(
            local_future=local_future,
            result_queue=result_queue,
            thread_pool=thread_pool,
        )
        if not isinstance(local_future.user_future, ListFuture):
            raise ValueError("local_future must be a LocalFuture of ListFuture")
        self._values: List[Any] | None = None

    def set_resolved_values(self, values: List[Any]) -> None:
        # Called by runtime when all the values are resolved before calling _run_future.
        self._values = values

    def _run_future(self) -> LocalFutureRunResult:
        if self._values is None:
            raise TensorlakeException(
                "Internal error: ListFutureRun has no resolved values set before running the future."
            )

        return LocalFutureRunResult(
            id=self._local_future.user_future.id,
            output=self._values,
            exception=None,
        )
