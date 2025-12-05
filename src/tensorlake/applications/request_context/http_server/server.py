import http.server

from .router import Router


class RequestContextHTTPServer:
    """HTTP server for handling request context operations."""

    def __init__(self, server_router_class: type[Router]):
        """Initializes the HTTP server to listen on localhost."""
        self._httpd: http.server.ThreadingHTTPServer = http.server.ThreadingHTTPServer(
            ("localhost", 0), server_router_class
        )
        self._running: bool = False

    @property
    def base_url(self) -> str:
        """Returns the base URL of the server."""
        port: int = self._httpd.server_address[1]
        return f"http://localhost:{port}"

    def start(self):
        """Starts the HTTP server in the current thread.

        Blocks until the server is stopped.
        """
        if self._running:
            raise RuntimeError("Server is already running.")

        self._running = True
        self._httpd.serve_forever()
        self._running = False

    def stop(self):
        """Stops the HTTP server and releases its resources.

        Blocks until the server is fully stopped.
        Does nothing if the server is not running.
        """
        if self._running:
            self._running = False
            # self._running must be checked, otherwise shutdown() will deadlock.
            self._httpd.shutdown()

        self._httpd.server_close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
