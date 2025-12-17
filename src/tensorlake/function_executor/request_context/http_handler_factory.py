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
from tensorlake.applications.request_context.http_server.route import Route
from tensorlake.applications.request_context.http_server.router import Router

from ..allocation_info import AllocationInfo
from .handlers.add_metrics import AddMetricsHandler
from .handlers.progress_update import ProgressUpdateHandler
from .handlers.request_state.commit_write import CommitWriteHandler
from .handlers.request_state.prepare_read import PrepareReadHandler
from .handlers.request_state.prepare_write import PrepareWriteHandler


class RequestContextHTTPHandlerFactory:
    """Creates and reuses handlers for FunctionExecutor HTTP request context server operations.

    The handlers are cached so they share the same state.
    """

    def __init__(
        self,
        allocation_infos: dict[str, AllocationInfo],
        logger: InternalLogger,
    ):
        self._logger: InternalLogger = logger.bind(module=__name__)
        self._routes: dict[Route, Handler] = {
            Route(
                path=PROGRESS_UPDATE_PATH, verb=PROGRESS_UPDATE_VERB
            ): ProgressUpdateHandler(
                allocation_infos=allocation_infos,
                logger=logger,
            ),
            Route(path=ADD_METRICS_PATH, verb=ADD_METRICS_VERB): AddMetricsHandler(),
            Route(path=PREPARE_READ_PATH, verb=PREPARE_READ_VERB): PrepareReadHandler(
                allocation_infos=allocation_infos,
                logger=logger,
            ),
            Route(
                path=PREPARE_WRITE_PATH, verb=PREPARE_WRITE_VERB
            ): PrepareWriteHandler(
                allocation_infos=allocation_infos,
                logger=logger,
            ),
            Route(path=COMMIT_WRITE_PATH, verb=COMMIT_WRITE_VERB): CommitWriteHandler(
                allocation_infos=allocation_infos,
                logger=logger,
            ),
        }

    def __call__(self, *args, **kwargs) -> Router:
        # This method is called by SimpleHTTPServer to create an HTTPHandler instance.
        # The instance can be called multiple times to handle multiple requests.
        # So it has to be stateless and support multi-threading.
        return Router(self._routes, self._logger, *args, **kwargs)
