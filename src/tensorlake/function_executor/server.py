import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import grpc

from .proto.function_executor_pb2_grpc import add_FunctionExecutorServicer_to_server
from .proto.server_configuration import GRPC_SERVER_OPTIONS
from .service import Service

# Temporary limit until we have a better way to control this.
# This limits the number of concurrent tasks that Function Executor can run.
MAX_RPC_CONCURRENCY = 100
# Terminal draining and gRPC each receive up to one second. Leave additional
# scheduling margin before treating the shutdown as irrecoverably stuck.
FORCED_SHUTDOWN_TIMEOUT_SECONDS = 4


class Server:
    def __init__(
        self,
        server_address: str,
        service: Service,
        force_exit: Callable[[], None] | None = None,
    ):
        self._server_address: str = server_address
        self._service: Service = service
        self._force_exit = force_exit

    def run(self):
        """Runs Function Executor Service at the configured address."""
        server = grpc.server(
            thread_pool=ThreadPoolExecutor(max_workers=MAX_RPC_CONCURRENCY),
            maximum_concurrent_rpcs=MAX_RPC_CONCURRENCY,
            options=GRPC_SERVER_OPTIONS,
        )
        add_FunctionExecutorServicer_to_server(self._service, server)
        server.add_insecure_port(self._server_address)
        server.start()
        stopping = False
        forced_exit: threading.Timer | None = None

        def stop(_signum, _frame):
            nonlocal stopping, forced_exit
            if stopping:
                return
            stopping = True
            # User code runs in-process and can close descriptors owned by gRPC.
            # In that state gRPC shutdown can stop making progress, including
            # inside server.stop() itself. Arm the hard-exit fallback before
            # touching either the service or transport so SIGTERM always has a
            # bounded completion path.
            if self._force_exit is not None:
                forced_exit = threading.Timer(
                    FORCED_SHUTDOWN_TIMEOUT_SECONDS,
                    self._force_exit,
                )
                forced_exit.daemon = True
                forced_exit.start()
            self._service.shutdown()
            # Service shutdown keeps this transport open while execution-log
            # clients drain terminal batches, including any terminal queued
            # behind an older unacknowledged batch.
            server.stop(grace=1)

        previous_sigterm = signal.signal(signal.SIGTERM, stop)
        previous_sigint = signal.signal(signal.SIGINT, stop)
        try:
            server.wait_for_termination()
        finally:
            if forced_exit is not None:
                forced_exit.cancel()
            signal.signal(signal.SIGTERM, previous_sigterm)
            signal.signal(signal.SIGINT, previous_sigint)
