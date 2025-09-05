from .interface.request_context import RequestContext, RequestProgress, RequestState


class RequestContextBase(RequestContext):
    def __init__(
        self,
        request_id: str,
        state: RequestState,
        progress: RequestProgress,
    ):
        self._request_id = request_id
        self._state = state
        self._progress = progress

    @property
    def request_id(self) -> str:
        return self._request_id

    @property
    def state(self) -> RequestState:
        return self._state

    @property
    def progress(self) -> RequestProgress:
        return self._progress
