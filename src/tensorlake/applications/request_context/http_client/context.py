from typing import Any

import httpx

from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.internal_logger import InternalLogger

from ...interface.request_context import (
    FunctionProgress,
    RequestContext,
    RequestMetrics,
    RequestState,
)
from .metrics import RequestMetricsHTTPClient
from .progress import FunctionProgressHTTPClient
from .state import RequestStateHTTPClient

# No long running HTTP requests are done in request context HTTP client.
_DEFAULT_HTTP_REQUEST_TIMEOUT_SEC: float = 5.0


class RequestContextHTTPClient(RequestContext):
    """HTTP client for accessing request context in subprocesses and child threads.

    Thread-safe for use in multiple threaded applications.
    """

    def __init__(
        self,
        request_id: str,
        allocation_id: str,
        server_base_url: str,
        blob_store: BLOBStore,
        logger: InternalLogger,
    ):
        self._request_id: str = request_id
        self._allocation_id: str = allocation_id
        self._server_base_url: str = server_base_url
        self._blob_store: BLOBStore = blob_store
        self._logger: InternalLogger = logger.bind(module=__name__)

        # httpx.Client is thread-safe.
        self._http_client: httpx.Client = httpx.Client(
            timeout=_DEFAULT_HTTP_REQUEST_TIMEOUT_SEC, base_url=server_base_url
        )

        self._state: RequestStateHTTPClient = RequestStateHTTPClient(
            request_id=request_id,
            allocation_id=allocation_id,
            http_client=self._http_client,
            blob_store=self._blob_store,
            logger=self._logger,
        )
        self._progress: FunctionProgressHTTPClient = FunctionProgressHTTPClient(
            request_id=request_id,
            allocation_id=allocation_id,
            http_client=self._http_client,
        )
        self._metrics: RequestMetricsHTTPClient = RequestMetricsHTTPClient(
            request_id=request_id,
            allocation_id=allocation_id,
            http_client=self._http_client,
        )

    def __getstate__(self):
        """Get the state for pickling."""
        # This is called when user creates a new subprocess to capture the request ctx client state.
        # When a user creates a new child thread, this is not called.
        return {
            "request_id": self._request_id,
            "allocation_id": self._allocation_id,
            "server_base_url": self._server_base_url,
            "blob_store": self._blob_store,
            "logger": self._logger,
        }

    def __setstate__(self, state: dict[str, Any]):
        """Set the state for unpickling."""
        # This is called when user creates a new subprocess to restore the request ctx client state in a new object.
        # When a user creates a new child thread, this is not called.
        self.__init__(
            request_id=state["request_id"],
            allocation_id=state["allocation_id"],
            server_base_url=state["server_base_url"],
            blob_store=state["blob_store"],
            logger=state["logger"],
        )

    def close(self) -> None:
        """Releases all resources.

        Doesn't raise any exceptions.
        """
        try:
            self._http_client.close()
        except Exception as e:
            self._logger.error("Failed to close HTTP client", exc_info=e)

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
