import os.path

from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.request_context.http_server.handlers.add_metrics import (
    ADD_METRICS_PATH,
    ADD_METRICS_VERB,
)
from tensorlake.applications.request_context.http_server.handlers.handler import Handler
from tensorlake.applications.request_context.http_server.handlers.progress_update import (
    PROGRESS_UPDATE_PATH,
    PROGRESS_UPDATE_VERB,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.commit_write import (
    COMMIT_WRITE_PATH,
    COMMIT_WRITE_VERB,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.prepare_read import (
    PREPARE_READ_PATH,
    PREPARE_READ_VERB,
)
from tensorlake.applications.request_context.http_server.handlers.request_state.prepare_write import (
    PREPARE_WRITE_PATH,
    PREPARE_WRITE_VERB,
)

from ...request_context.http_server.route import Route
from ...request_context.http_server.router import Router
from .handlers.add_metrics import LocalAddMetricsHandler
from .handlers.progress_update import LocalProgressUpdateHandler
from .handlers.request_state.commit_write import LocalCommitWriteHandler
from .handlers.request_state.prepare_read import LocalPrepareReadHandler
from .handlers.request_state.prepare_write import LocalPrepareWriteHandler


class LocalRequestContextHTTPHandlerFactory:
    """Creates and reuses handlers for local HTTP request context server operations.

    The handlers are cached so they share the same state.
    """

    def __init__(
        self,
        blob_store_dir_path: str,
        logger: InternalLogger,
    ):
        request_state_dir_path: str = os.path.join(blob_store_dir_path, "request_state")
        self._logger: InternalLogger = logger.bind(module=__name__)
        self._routes: dict[Route, Handler] = {
            Route(
                path=PROGRESS_UPDATE_PATH, verb=PROGRESS_UPDATE_VERB
            ): LocalProgressUpdateHandler(),
            Route(
                path=ADD_METRICS_PATH, verb=ADD_METRICS_VERB
            ): LocalAddMetricsHandler(),
            Route(
                path=PREPARE_READ_PATH, verb=PREPARE_READ_VERB
            ): LocalPrepareReadHandler(
                request_state_dir_path=request_state_dir_path,
            ),
            Route(
                path=PREPARE_WRITE_PATH, verb=PREPARE_WRITE_VERB
            ): LocalPrepareWriteHandler(
                request_state_dir_path=request_state_dir_path,
            ),
            Route(
                path=COMMIT_WRITE_PATH, verb=COMMIT_WRITE_VERB
            ): LocalCommitWriteHandler(
                request_state_dir_path=request_state_dir_path,
            ),
        }

    def __call__(self, *args, **kwargs) -> Router:
        # This method is called by SimpleHTTPServer to create an HTTPHandler instance.
        # The instance can be called multiple times to handle multiple requests.
        # So it has to be stateless and support multi-threading.
        return Router(self._routes, self._logger, *args, **kwargs)
