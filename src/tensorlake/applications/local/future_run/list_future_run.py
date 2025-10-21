from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any, List

from ...interface.exceptions import TensorlakeException
from ..future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
)


class ListFutureRun(LocalFutureRun):
    """LocalFutureRun that awaits a list of awaitables and returns a list with their values.

    ListFutureRun is currently only used when the user manually awaits a ListFuture.
    In this case, the runtime resolves all the awaitables in the list (recursively)
    and then calls _run_future which returns the resolved list.
    """

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
        self._values: List[Any] | None = None

    def start(self, values: List[Any]) -> None:
        self._values = values
        super().start()

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
