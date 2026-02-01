"""Tests for Function Executor HTTP endpoints.

These tests verify that the HTTP endpoints work correctly alongside the gRPC service.
Uses stdlib http.client for HTTP requests (zero external dependencies).
"""

import hashlib
import http.client
import json
import os
import time
import unittest

import grpc
from testing import (
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    create_tmp_blob,
    initialize,
    rpc_channel,
)

from tensorlake.applications import (
    application,
    function,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    Allocation,
    AllocationOutcomeCode,
    AllocationOutputBLOB,
    AllocationUpdate,
    CreateAllocationRequest,
    InitializationOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.status_pb2 import Status

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

# HTTP port for tests (different from gRPC port)
HTTP_PORT = 60001
GRPC_PORT = 60000


@application()
@function()
def simple_function(x: int) -> int:
    return x * 2


class HTTPClient:
    """Simple HTTP client for testing using stdlib http.client."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        headers: dict | None = None,
    ) -> tuple[int, dict | None]:
        """Make an HTTP request and return (status_code, response_json)."""
        conn = http.client.HTTPConnection(self.host, self.port, timeout=30)
        try:
            request_headers = {"Content-Type": "application/json"}
            if headers:
                request_headers.update(headers)

            body_bytes = json.dumps(body).encode("utf-8") if body else None
            conn.request(method, path, body=body_bytes, headers=request_headers)

            response = conn.getresponse()
            response_body = response.read()

            if response_body:
                try:
                    return response.status, json.loads(response_body.decode("utf-8"))
                except json.JSONDecodeError:
                    return response.status, None
            return response.status, None
        finally:
            conn.close()

    def get(
        self, path: str, headers: dict | None = None
    ) -> tuple[int, dict | None]:
        return self._request("GET", path, headers=headers)

    def post(
        self, path: str, body: dict | None = None, headers: dict | None = None
    ) -> tuple[int, dict | None]:
        return self._request("POST", path, body=body, headers=headers)

    def delete(
        self, path: str, headers: dict | None = None
    ) -> tuple[int, dict | None]:
        return self._request("DELETE", path, headers=headers)


class TestHTTPEndpoints(unittest.TestCase):
    """Tests for HTTP endpoints."""

    def test_health_endpoint_before_initialization(self):
        """Test that health endpoint returns 503 before initialization."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            # Wait for HTTP server to start
            time.sleep(1)

            client = HTTPClient("localhost", HTTP_PORT)
            status, body = client.get("/health")

            self.assertEqual(status, 503)
            self.assertIn("error", body)
            self.assertIn("not initialized", body["error"].lower())

    def test_health_endpoint_after_initialization(self):
        """Test that health endpoint returns 200 after initialization."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            # Initialize via gRPC first
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                response = initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )
                self.assertEqual(
                    response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                # Now test HTTP health endpoint
                client = HTTPClient("localhost", HTTP_PORT)
                status, body = client.get("/health")

                self.assertEqual(status, 200)
                self.assertTrue(body["healthy"])

    def test_info_endpoint(self):
        """Test the info endpoint returns version information."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            # Wait for HTTP server to start
            time.sleep(1)

            client = HTTPClient("localhost", HTTP_PORT)
            status, body = client.get("/info")

            self.assertEqual(status, 200)
            self.assertIn("version", body)
            self.assertIn("sdk_version", body)
            self.assertIn("sdk_language", body)
            self.assertEqual(body["sdk_language"], "python")

    def test_list_allocations_empty(self):
        """Test that list allocations returns empty list when no allocations exist."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            # Initialize first
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                client = HTTPClient("localhost", HTTP_PORT)
                status, body = client.get("/allocations")

                self.assertEqual(status, 200)
                self.assertEqual(body["allocations"], [])

    def test_create_allocation_via_http(self):
        """Test creating an allocation via HTTP."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
            capture_std_outputs=True,
        ) as process:
            # Initialize via gRPC
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                # Create allocation via HTTP
                client = HTTPClient("localhost", HTTP_PORT)

                # Build allocation request
                inputs = application_function_inputs(42)
                allocation_request = {
                    "allocation": {
                        "request_id": "test-request-123",
                        "function_call_id": "test-function-call",
                        "allocation_id": "test-http-allocation",
                        "inputs": {
                            "args": [
                                {
                                    "manifest": {
                                        "encoding": inputs.args[0].manifest.encoding,
                                        "encoding_version": inputs.args[
                                            0
                                        ].manifest.encoding_version,
                                        "size": inputs.args[0].manifest.size,
                                        "sha256_hash": inputs.args[
                                            0
                                        ].manifest.sha256_hash,
                                    },
                                    "offset": 0,
                                }
                            ],
                            "arg_blobs": [
                                {
                                    "id": inputs.arg_blobs[0].id,
                                    "chunks": [
                                        {
                                            "uri": chunk.uri,
                                            "size": chunk.size,
                                        }
                                        for chunk in inputs.arg_blobs[0].chunks
                                    ],
                                }
                            ],
                            "request_error_blob": {
                                "id": inputs.request_error_blob.id,
                                "chunks": [
                                    {
                                        "uri": chunk.uri,
                                        "size": chunk.size,
                                    }
                                    for chunk in inputs.request_error_blob.chunks
                                ],
                            },
                            "function_call_metadata": "",
                        },
                    }
                }

                status, body = client.post("/allocations", allocation_request)
                self.assertEqual(status, 201)
                self.assertEqual(body["status"], "created")

                # Verify allocation exists via list
                status, body = client.get("/allocations")
                self.assertEqual(status, 200)
                self.assertEqual(len(body["allocations"]), 1)
                self.assertEqual(
                    body["allocations"][0]["allocation_id"], "test-http-allocation"
                )

    def test_get_allocation_state_not_found(self):
        """Test getting state for non-existent allocation returns 404."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                client = HTTPClient("localhost", HTTP_PORT)
                status, body = client.get("/allocations/nonexistent")

                self.assertEqual(status, 404)
                self.assertIn("not found", body["error"].lower())

    def test_get_allocation_state_immediate(self):
        """Test getting allocation state immediately (no long-polling)."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                # Create allocation via gRPC (simpler)
                allocation_id = "test-state-allocation"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-123",
                            function_call_id="fc-123",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(42),
                        ),
                    )
                )

                # Get state via HTTP
                client = HTTPClient("localhost", HTTP_PORT)
                status, body = client.get(f"/allocations/{allocation_id}")

                self.assertEqual(status, 200)
                self.assertIn("sha256_hash", body)

    def test_get_allocation_state_long_polling_timeout(self):
        """Test long-polling returns after timeout when state doesn't change."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                # Create allocation
                allocation_id = "test-longpoll-allocation"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-123",
                            function_call_id="fc-123",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(42),
                        ),
                    )
                )

                # Get initial state
                client = HTTPClient("localhost", HTTP_PORT)
                status, initial_state = client.get(f"/allocations/{allocation_id}")
                self.assertEqual(status, 200)
                initial_hash = initial_state["sha256_hash"]

                # Long-poll with short timeout - should return quickly
                start_time = time.time()
                status, body = client.get(
                    f"/allocations/{allocation_id}",
                    headers={"X-Timeout": "1", "X-Last-Hash": initial_hash},
                )
                elapsed = time.time() - start_time

                self.assertEqual(status, 200)
                # Should return after ~1 second timeout (allow some slack)
                self.assertGreater(elapsed, 0.5)
                self.assertLess(elapsed, 3.0)

    def test_delete_allocation_not_found(self):
        """Test deleting non-existent allocation returns 404."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                client = HTTPClient("localhost", HTTP_PORT)
                status, body = client.delete("/allocations/nonexistent")

                self.assertEqual(status, 404)

    def test_create_duplicate_allocation_returns_409(self):
        """Test creating duplicate allocation returns 409 Conflict."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                # Create allocation via gRPC
                allocation_id = "test-duplicate-allocation"
                inputs = application_function_inputs(42)
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-123",
                            function_call_id="fc-123",
                            allocation_id=allocation_id,
                            inputs=inputs,
                        ),
                    )
                )

                # Try to create same allocation via HTTP with valid inputs
                client = HTTPClient("localhost", HTTP_PORT)
                allocation_request = {
                    "allocation": {
                        "request_id": "req-456",
                        "function_call_id": "fc-456",
                        "allocation_id": allocation_id,  # Same ID - should cause 409
                        "inputs": {
                            "args": [
                                {
                                    "manifest": {
                                        "encoding": inputs.args[0].manifest.encoding,
                                        "encoding_version": inputs.args[
                                            0
                                        ].manifest.encoding_version,
                                        "size": inputs.args[0].manifest.size,
                                        "sha256_hash": inputs.args[
                                            0
                                        ].manifest.sha256_hash,
                                    },
                                    "offset": 0,
                                }
                            ],
                            "arg_blobs": [
                                {
                                    "id": inputs.arg_blobs[0].id,
                                    "chunks": [
                                        {
                                            "uri": chunk.uri,
                                            "size": chunk.size,
                                        }
                                        for chunk in inputs.arg_blobs[0].chunks
                                    ],
                                }
                            ],
                            "request_error_blob": {
                                "id": inputs.request_error_blob.id,
                                "chunks": [
                                    {
                                        "uri": chunk.uri,
                                        "size": chunk.size,
                                    }
                                    for chunk in inputs.request_error_blob.chunks
                                ],
                            },
                            "function_call_metadata": "",
                        },
                    }
                }

                status, body = client.post("/allocations", allocation_request)
                self.assertEqual(status, 409)
                self.assertEqual(body["code"], "ALREADY_EXISTS")


class TestHTTPAndGRPCInterop(unittest.TestCase):
    """Tests for interoperability between HTTP and gRPC interfaces."""

    def test_allocation_created_via_grpc_visible_via_http(self):
        """Test that allocations created via gRPC are visible via HTTP."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                # Create via gRPC
                allocation_id = "grpc-created-allocation"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-123",
                            function_call_id="fc-123",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(42),
                        ),
                    )
                )

                # Verify via HTTP
                client = HTTPClient("localhost", HTTP_PORT)
                status, body = client.get("/allocations")

                self.assertEqual(status, 200)
                allocation_ids = [a["allocation_id"] for a in body["allocations"]]
                self.assertIn(allocation_id, allocation_ids)

    def test_send_update_via_http_for_grpc_allocation(self):
        """Test sending allocation update via HTTP for allocation created via gRPC."""
        with FunctionExecutorProcessContextManager(
            port=GRPC_PORT,
            http_port=HTTP_PORT,
        ) as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_function",
                )

                # Create allocation via gRPC
                allocation_id = "test-update-allocation"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-123",
                            function_call_id="fc-123",
                            allocation_id=allocation_id,
                            inputs=application_function_inputs(42),
                        ),
                    )
                )

                # Wait for allocation to request output blob
                time.sleep(0.5)

                # Get state to see if there's a blob request
                client = HTTPClient("localhost", HTTP_PORT)
                status, state = client.get(f"/allocations/{allocation_id}")
                self.assertEqual(status, 200)

                # If there's an output blob request, send update via HTTP
                if state.get("output_blob_requests"):
                    blob_request = state["output_blob_requests"][0]
                    # Note: size comes as string from JSON, convert to int
                    blob = create_tmp_blob(
                        id=blob_request["id"],
                        chunks_count=1,
                        chunk_size=int(blob_request["size"]),
                    )

                    update = {
                        "output_blob": {
                            "status": {"code": 0},
                            "blob": {
                                "id": blob.id,
                                "chunks": [
                                    {"uri": chunk.uri, "size": chunk.size}
                                    for chunk in blob.chunks
                                ],
                            },
                        }
                    }

                    status, body = client.post(
                        f"/allocations/{allocation_id}/updates", update
                    )
                    self.assertEqual(status, 200)


class TestAutoInitialization(unittest.TestCase):
    """Tests for auto-initialization from code path."""

    def test_auto_init_http_only_mode(self):
        """Test FE starts in HTTP-only mode with auto-initialization."""
        # Start FE with auto-init args (no gRPC address, HTTP only)
        import subprocess
        import os

        code_path = APPLICATION_CODE_DIR_PATH
        http_port = 60002

        args = [
            "function-executor",
            "--http-port", str(http_port),
            "--code-path", code_path,
            "--namespace", "test",
            "--app-name", "simple_function",
            "--app-version", "0.1",
            "--function-name", "simple_function",
            "--executor-id", "test-executor",
        ]

        process = subprocess.Popen(args)
        try:
            # Wait for server to start
            time.sleep(2)

            # Verify HTTP server is running and healthy
            client = HTTPClient("localhost", http_port)
            status, body = client.get("/health")
            self.assertEqual(status, 200)
            self.assertTrue(body["healthy"])

            # Verify we can list allocations (service is initialized)
            status, body = client.get("/allocations")
            self.assertEqual(status, 200)
            self.assertEqual(body["allocations"], [])

        finally:
            process.terminate()
            process.wait()


if __name__ == "__main__":
    unittest.main()
