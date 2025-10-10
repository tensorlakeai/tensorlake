from collections.abc import Iterable
from typing import Any, List

from .callable import TensorlakeCallable
from .function import Function
from .future import Future, FutureList


class GatherCallable(TensorlakeCallable):
    def __init__(self):
        pass

    def run(self, items: Iterable[Any | Future]) -> FutureList:
        """Converts the iterable into a future that resolves into a list of values from the iterable.

        The returned FutureList object can be used in Tensorlake Function call arguments.
        It cannot be returned from a Tensorlake Function.
        """
        return FutureList(items)

    def run_later(
        self, start_delay: float, items: Iterable[Any | Future]
    ) -> FutureList:
        """Converts the iterable into a future that resolves into a list of values from the iterable.

        The returned FutureList object can be used in Tensorlake Function call arguments.
        It cannot be returned from a Tensorlake Function.
        """
        return FutureList(items)

    def __call__(self, items: Iterable[Any | Future]) -> Any:
        """Converts the iterable into a future that resolves into a list of values from the iterable.

        The returned FutureList object can be used in Tensorlake Function call arguments.
        It cannot be returned from a Tensorlake Function.
        """
        # TODO: Implement blocking call.
        return FutureList(items)


gather = GatherCallable()


class MapCallable(TensorlakeCallable):
    def __init__(self):
        pass

    def run(self, function: Function, iterable: Iterable) -> FutureList:
        gather.run(self._make_futures(function, iterable))

    def run_later(
        self, start_delay: float, function: Function, iterable: Iterable
    ) -> FutureList:
        gather.run_later(start_delay, self._make_futures(function, iterable))

    def __call__(self, function: Function, iterable: Iterable) -> Any:
        # TODO: Make sync.
        """Returns a future that resolves into a list with every item transformed using the supplied function.

        Similar to https://docs.python.org/3/library/functions.html#map.
        If the function accepts request context then it should be its first argument
        and have `tensorlake.RequestContext` type annotation.
        """
        return gather(self._make_futures(function, iterable))

    @classmethod
    def _make_futures(
        cls, function: Function, items: Iterable[Any | Future]
    ) -> List[Future]:
        function_calls: List[Future] = []
        for item in items:
            args = [item]
            function_calls.append(function.run(*args))
        return function_calls


map = MapCallable()
