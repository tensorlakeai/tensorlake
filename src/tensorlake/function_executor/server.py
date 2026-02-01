import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

from tensorlake.applications.internal_logger import InternalLogger

from .http_server import FunctionExecutorHTTPServer
from .proto.function_executor_pb2_grpc import add_FunctionExecutorServicer_to_server
from .proto.server_configuration import GRPC_SERVER_OPTIONS
from .service import Service

# Temporary limit until we have a better way to control this.
# This limits the number of concurrent tasks that Function Executor can run.
MAX_RPC_CONCURRENCY = 100


class Server:
    def __init__(
        self,
        server_address: str | None,
        service: Service,
        http_port: int | None = None,
        logger: InternalLogger | None = None,
    ):
        self._server_address: str | None = server_address
        self._service: Service = service
        self._http_port: int | None = http_port
        self._logger: InternalLogger | None = logger
        self._http_server: FunctionExecutorHTTPServer | None = None
        self._http_server_thread: threading.Thread | None = None
        self._shutdown_event: threading.Event = threading.Event()

    def run(self):
        """Runs Function Executor Service.

        Starts HTTP server if http_port is configured.
        Starts gRPC server if server_address is configured.
        At least one must be configured.
        """
        # Start HTTP server if configured
        if self._http_port is not None and self._logger is not None:
            self._http_server = FunctionExecutorHTTPServer(
                port=self._http_port,
                service=self._service,
                logger=self._logger,
            )
            # In HTTP-only mode, run HTTP server in main thread
            # In dual mode, run HTTP server in background thread
            if self._server_address is None:
                # HTTP-only mode
                self._http_server.start()
                return
            else:
                # Dual mode - HTTP in background
                self._http_server_thread = threading.Thread(
                    target=self._http_server.start,
                    name="FunctionExecutorHTTPServerThread",
                    daemon=True,
                )
                self._http_server_thread.start()

        # Start gRPC server if configured
        if self._server_address is not None:
            server = grpc.server(
                thread_pool=ThreadPoolExecutor(max_workers=MAX_RPC_CONCURRENCY),
                maximum_concurrent_rpcs=MAX_RPC_CONCURRENCY,
                options=GRPC_SERVER_OPTIONS,
            )
            add_FunctionExecutorServicer_to_server(self._service, server)
            server.add_insecure_port(self._server_address)
            server.start()
            server.wait_for_termination()
