from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any

from ...interface.exceptions import InternalError
from ...interface.futures import Future
from ..future import LocalFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
)


class ListFutureRun(LocalFutureRun):
    """LocalFutureRun that awaits a list of futures and returns a list with their values.

    In this case, the runtime resolves all the futures in the list and then calls
    _run_future which returns the resolved list.
    """

    def __init__(
        self,
        local_future: LocalFuture,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
        items: list[Future | Any],
    ):
        super().__init__(
            local_future=local_future,
            result_queue=result_queue,
            thread_pool=thread_pool,
        )
        self._items: list[Future | Any] = items
        self._resolved_items: list[Any] | None = None

    @property
    def items(self) -> list[Future | Any]:
        return self._items

    def start(self, resolved_items: list[Any]) -> None:
        self._resolved_items = resolved_items
        super().start()

    def _run_future(self) -> LocalFutureRunResult:
        if self._resolved_items is None:
            raise InternalError(
                "ListFutureRun has no resolved items set before running the future."
            )

        return LocalFutureRunResult(
            id=self._local_future.future._id,
            output=self._resolved_items,
            error=None,
        )
