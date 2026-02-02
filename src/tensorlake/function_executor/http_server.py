"""HTTP server for Function Executor.

Provides HTTP API endpoints for the dataplane to communicate with
function executors in fork-exec mode (without gRPC).
"""

import hashlib
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, Dict, Optional

from google.protobuf.json_format import MessageToDict, ParseDict

from tensorlake.applications.internal_logger import InternalLogger

from .info import info_response_kv_args
from .proto.function_executor_pb2 import (
    Allocation,
    AllocationState,
    AllocationUpdate,
    CreateAllocationRequest,
)

if TYPE_CHECKING:
    from .service import Service


class FunctionExecutorHTTPServer:
    """HTTP server that exposes Function Executor API endpoints."""

    def __init__(
        self,
        port: int,
        service: "Service",
        logger: InternalLogger,
    ):
        self._port = port
        self._service = service
        self._logger = logger.bind(module=__name__)
        self._server: Optional[ThreadingHTTPServer] = None
        self._shutdown_event = threading.Event()

    def start(self) -> None:
        """Start the HTTP server (blocking)."""
        handler = self._create_handler()
        self._server = ThreadingHTTPServer(("127.0.0.1", self._port), handler)
        self._logger.info("HTTP server starting", port=self._port)

        try:
            self._server.serve_forever()
        except Exception as e:
            self._logger.error("HTTP server error", error=str(e))
        finally:
            self._logger.info("HTTP server stopped")

    def stop(self) -> None:
        """Stop the HTTP server."""
        self._shutdown_event.set()
        if self._server:
            self._server.shutdown()

    def _create_handler(self) -> type:
        """Create a request handler class with access to the service."""
        service = self._service
        logger = self._logger

        class Handler(BaseHTTPRequestHandler):
            """HTTP request handler for Function Executor API."""

            # Disable default logging
            def log_message(self, format: str, *args) -> None:
                pass

            def _send_json_response(
                self, status: int, data: Any, headers: Optional[Dict[str, str]] = None
            ) -> None:
                """Send a JSON response."""
                body = json.dumps(data).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                if headers:
                    for key, value in headers.items():
                        self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)

            def _send_error_response(self, status: int, message: str) -> None:
                """Send an error response."""
                self._send_json_response(status, {"error": message})

            def _read_json_body(self) -> Optional[Dict]:
                """Read and parse JSON request body."""
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length == 0:
                    return None
                body = self.rfile.read(content_length)
                return json.loads(body.decode("utf-8"))

            def do_GET(self) -> None:
                """Handle GET requests."""
                path = self.path.split("?")[0]  # Remove query string

                if path == "/health":
                    self._handle_health()
                elif path == "/info":
                    self._handle_info()
                elif path == "/allocations":
                    self._handle_list_allocations()
                elif path.startswith("/allocations/"):
                    # Extract allocation_id from path
                    match = re.match(r"/allocations/([^/]+)$", path)
                    if match:
                        allocation_id = match.group(1)
                        self._handle_get_allocation_state(allocation_id)
                    else:
                        self._send_error_response(404, "Not found")
                else:
                    self._send_error_response(404, "Not found")

            def do_POST(self) -> None:
                """Handle POST requests."""
                path = self.path.split("?")[0]

                if path == "/initialize":
                    self._handle_initialize()
                elif path == "/allocations":
                    self._handle_create_allocation()
                elif path.startswith("/allocations/") and path.endswith("/updates"):
                    # Extract allocation_id from path
                    match = re.match(r"/allocations/([^/]+)/updates$", path)
                    if match:
                        allocation_id = match.group(1)
                        self._handle_send_allocation_update(allocation_id)
                    else:
                        self._send_error_response(404, "Not found")
                else:
                    self._send_error_response(404, "Not found")

            def do_DELETE(self) -> None:
                """Handle DELETE requests."""
                path = self.path.split("?")[0]

                if path.startswith("/allocations/"):
                    match = re.match(r"/allocations/([^/]+)$", path)
                    if match:
                        allocation_id = match.group(1)
                        self._handle_delete_allocation(allocation_id)
                    else:
                        self._send_error_response(404, "Not found")
                else:
                    self._send_error_response(404, "Not found")

            def _handle_health(self) -> None:
                """Handle GET /health."""
                # Check if service is initialized
                initialized = service._health_check_handler is not None
                if not initialized:
                    self._send_json_response(
                        503,
                        {
                            "healthy": False,
                            "error": "Service not initialized",
                        },
                    )
                else:
                    self._send_json_response(
                        200,
                        {
                            "healthy": True,
                        },
                    )

            def _handle_info(self) -> None:
                """Handle GET /info."""
                info = info_response_kv_args()
                info["initialized"] = service._health_check_handler is not None
                self._send_json_response(200, info)

            def _handle_initialize(self) -> None:
                """Handle POST /initialize."""
                # This endpoint is not used in fork-exec mode since
                # initialization happens via command-line args
                self._send_json_response(200, {"success": True})

            def _handle_list_allocations(self) -> None:
                """Handle GET /allocations."""
                allocations = []
                for alloc_id, alloc_info in service._allocation_infos.items():
                    allocations.append(
                        {
                            "allocation_id": alloc_info.allocation.allocation_id,
                            "request_id": alloc_info.allocation.request_id,
                            "function_call_id": alloc_info.allocation.function_call_id,
                        }
                    )
                self._send_json_response(200, {"allocations": allocations})

            def _handle_create_allocation(self) -> None:
                """Handle POST /allocations."""
                try:
                    body = self._read_json_body()
                    if not body:
                        self._send_error_response(400, "Missing request body")
                        return

                    # Parse allocation from JSON
                    allocation_dict = body.get("allocation", body)
                    allocation = ParseDict(allocation_dict, Allocation())

                    # Check if allocation already exists
                    if allocation.allocation_id in service._allocation_infos:
                        self._send_json_response(
                            409,
                            {
                                "code": "ALREADY_EXISTS",
                                "error": f"Allocation {allocation.allocation_id} already exists",
                            },
                        )
                        return

                    # Create the allocation using a mock context
                    request = CreateAllocationRequest(allocation=allocation)

                    # Call service method directly (simplified)
                    from tensorlake.applications.request_context.http_client.context import (
                        RequestContextHTTPClient,
                    )

                    from .allocation_info import AllocationInfo
                    from .allocation_runner.allocation_runner import AllocationRunner

                    allocation_logger = logger.bind(
                        request_id=allocation.request_id,
                        fn_call_id=allocation.function_call_id,
                        allocation_id=allocation.allocation_id,
                    )
                    allocation_runner = AllocationRunner(
                        allocation=allocation,
                        function_ref=service._function_ref,
                        function=service._function,
                        function_instance_arg=service._function_instance_arg,
                        blob_store=service._blob_store,
                        request_context=RequestContextHTTPClient(
                            request_id=allocation.request_id,
                            allocation_id=allocation.allocation_id,
                            function_name=service._function_ref.function_name,
                            function_run_id=allocation.function_call_id,
                            server_base_url=service._request_context_http_server.base_url,
                            http_client=service._request_context_http_client,
                            blob_store=service._blob_store,
                            logger=allocation_logger,
                        ),
                        logger=allocation_logger,
                    )
                    service._allocation_infos[allocation.allocation_id] = (
                        AllocationInfo(
                            allocation=allocation,
                            runner=allocation_runner,
                        )
                    )
                    allocation_runner.run()

                    self._send_json_response(201, {"status": "created"})
                except Exception as e:
                    logger.error("Failed to create allocation", error=str(e))
                    self._send_error_response(500, str(e))

            def _handle_get_allocation_state(self, allocation_id: str) -> None:
                """Handle GET /allocations/{allocation_id}."""
                if allocation_id not in service._allocation_infos:
                    self._send_error_response(
                        404, f"Allocation {allocation_id} not found"
                    )
                    return

                allocation_info = service._allocation_infos[allocation_id]

                # Check for long-polling headers
                last_hash = self.headers.get("X-Last-Hash")
                timeout_str = self.headers.get("X-Timeout")
                timeout = float(timeout_str) if timeout_str else None

                if last_hash and timeout:
                    # Long-polling: wait for state change or timeout
                    allocation_state = (
                        allocation_info.runner.wait_allocation_state_update(
                            last_hash, timeout=timeout
                        )
                    )
                else:
                    # Immediate response - use timeout=0 to get current state
                    allocation_state = (
                        allocation_info.runner.wait_allocation_state_update(
                            None, timeout=0
                        )
                    )

                # Convert protobuf to dict
                state_dict = MessageToDict(
                    allocation_state,
                    preserving_proto_field_name=True,
                )

                # Ensure sha256_hash is included
                if "sha256_hash" not in state_dict:
                    state_dict["sha256_hash"] = allocation_state.sha256_hash or ""

                self._send_json_response(200, state_dict)

            def _handle_send_allocation_update(self, allocation_id: str) -> None:
                """Handle POST /allocations/{allocation_id}/updates."""
                if allocation_id not in service._allocation_infos:
                    self._send_error_response(
                        404, f"Allocation {allocation_id} not found"
                    )
                    return

                allocation_info = service._allocation_infos[allocation_id]
                if allocation_info.runner.finished:
                    self._send_error_response(
                        409, f"Allocation {allocation_id} is already finished"
                    )
                    return

                try:
                    body = self._read_json_body()
                    if not body:
                        self._send_error_response(400, "Missing request body")
                        return

                    # Parse update from JSON
                    update = ParseDict(body, AllocationUpdate())
                    allocation_info.runner.deliver_allocation_update(update)
                    self._send_json_response(200, {"success": True})
                except Exception as e:
                    logger.error("Failed to send allocation update", error=str(e))
                    self._send_error_response(500, str(e))

            def _handle_delete_allocation(self, allocation_id: str) -> None:
                """Handle DELETE /allocations/{allocation_id}."""
                if allocation_id not in service._allocation_infos:
                    self._send_error_response(
                        404, f"Allocation {allocation_id} not found"
                    )
                    return

                allocation_info = service._allocation_infos[allocation_id]
                if not allocation_info.runner.finished:
                    self._send_error_response(
                        409, f"Allocation {allocation_id} is still running"
                    )
                    return

                del service._allocation_infos[allocation_id]
                self._send_json_response(200, {"success": True})

        return Handler
