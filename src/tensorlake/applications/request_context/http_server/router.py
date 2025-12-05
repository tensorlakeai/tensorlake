import http.server

from tensorlake.applications.internal_logger import InternalLogger

from .handlers.handler import Handler, Request, Response
from .route import Route


class Router(http.server.BaseHTTPRequestHandler):
    """An HTTP request handler that routes the requests to appropriate handlers based on preconfigured routes."""

    def __init__(
        self,
        tensorlake_routes: dict[Route, Handler],
        tensorlake_logger: InternalLogger,
        *args,
        **kwargs,
    ):
        # All self.fields must be initialized before calling super().__init__().
        # Prefix with "tensorlake_" to avoid any name collisions with base class fields and constructor args
        # that we don't control.
        self._tensorlake_routes: dict[Route, Handler] = tensorlake_routes
        self._tensorlake_logger: InternalLogger = tensorlake_logger.bind(
            module=__name__
        )
        super().__init__(*args, **kwargs)

    def _find_handler(self, request: Request) -> Handler | None:
        """Finds the appropriate handler for the given request.

        Doesn't raise any exceptions. Returns None if no handler is found.
        """
        for route, handler in self._tensorlake_routes.items():
            if route.verb == request.method and route.path == request.path:
                return handler
        return None

    def _handle(self):
        try:
            content_length: int = int(self.headers.get("Content-Length", 0))
            request: Request = Request(
                method=self.command,
                path=self.path,
                headers=self.headers,
                body=self.rfile.read(content_length),
            )
        except Exception as e:
            self._internal_server_error(e)
            return

        handler: Handler | None = self._find_handler(request)
        if handler is None:
            self._tensorlake_logger.error(
                "No handler found", verb=self.command, path=self.path
            )
            self.send_response(404)
            self.end_headers()
        else:
            try:
                response: Response = handler.handle(request)
                self.send_response(response.status_code)
                for header_name, header_value in response.headers.items():
                    self.send_header(header_name, header_value)
                self.end_headers()
                self.wfile.write(response.body)
                return
            except Exception as e:
                self._internal_server_error(e)
                return

    def _internal_server_error(self, exception: BaseException) -> None:
        message: str = f"Internal Server Error: {exception}"
        self._tensorlake_logger.error(message, exc_info=exception)
        self.send_response(500)
        self.end_headers()
        self.wfile.write(message.encode("utf-8"))

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def do_PUT(self):
        self._handle()

    def do_DELETE(self):
        self._handle()

    def do_PATCH(self):
        self._handle()

    # Disable all default request logging done by BaseHTTPRequestHandler.
    def log_message(self, *args, **kwargs):
        pass

    def log_request(self, *args, **kwargs):
        pass

    def log_error(self, *args, **kwargs):
        pass
