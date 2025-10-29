from ..interface.request_context import (
    FunctionProgress,
    RequestContext,
    RequestMetrics,
    RequestState,
)


class RequestContextBase(RequestContext):
    def __init__(
        self,
        request_id: str,
        state: RequestState,
        progress: FunctionProgress,
        metrics: RequestMetrics,
    ):
        self._request_id = request_id
        self._state = state
        self._progress = progress
        self._metrics = metrics

    @property
    def request_id(self) -> str:
        return self._request_id

    @property
    def state(self) -> RequestState:
        return self._state

    @property
    def progress(self) -> FunctionProgress:
        return self._progress

    @property
    def metrics(self) -> RequestMetrics:
        return self._metrics
