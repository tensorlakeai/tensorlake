import asyncio
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlparse

from tensorlake.sandbox import AsyncSandboxClient, SandboxClient


def test_real_native_bindings_read_scoped_telemetry_over_http() -> None:
    requests: list[tuple[str, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append((self.path, self.headers.get("x-forwarded-project-id")))
            resource = urlparse(self.path).path.rsplit("/", 1)[-1]
            if resource == "events":
                body = {"events": [], "from_ms": 1, "to_ms": 2, "next_cursor": "next"}
            elif resource == "destinations":
                body = {
                    "destinations": [],
                    "from_ms": 1,
                    "to_ms": 2,
                    "truncated": False,
                }
            elif resource == "status":
                body = {
                    "state": "unknown",
                    "last_observed_at_ms": None,
                    "allocation_id": None,
                    "coverage": [],
                    "limitations": [],
                }
            else:
                self.send_error(404)
                return
            encoded = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
    )
    thread.start()
    options = dict(
        api_url=f"http://127.0.0.1:{server.server_port}",
        api_key="test-only",
        organization_id="org-a",
        project_id="project-a",
        namespace="project-a",
        request_timeout=2,
    )
    try:
        client = SandboxClient(**options)
        try:
            result = client.network_events(
                "removed-sandbox", from_ms=1, to_ms=2, cursor="opaque&cursor"
            )
            assert result.value.next_cursor == "next"
            assert result.trace_id
            assert not client.network_destinations("removed-sandbox").value.truncated
        finally:
            client.close()

        async def read_status() -> None:
            client = AsyncSandboxClient(**options)
            try:
                result = await client.network_status("removed-sandbox")
                assert result.value.state == "unknown"
                assert result.trace_id
            finally:
                await client.close()

        asyncio.run(read_status())
        assert len(requests) == 3
        assert all(
            path.startswith(
                "/v1/namespaces/project-a/sandboxes/removed-sandbox/network/"
            )
            and project == "project-a"
            for path, project in requests
        )
        assert parse_qs(urlparse(requests[0][0]).query)["cursor"] == ["opaque&cursor"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


def test_retained_network_metadata_keeps_cursor_and_unknown_counters() -> None:
    client = object.__new__(SandboxClient)
    client._rust_client = Mock()
    client._rust_client.network_destinations_json.return_value = (
        "trace-network",
        json.dumps(
            {
                "destinations": [
                    {
                        "destination_ip": "192.0.2.1",
                        "destination_port": 443,
                        "transport": "tcp",
                        "observed_connections": 1,
                        "original_bytes": None,
                        "reply_bytes": None,
                        "unknown_byte_connections": 1,
                        "degraded_connections": 0,
                    }
                ],
                "truncated": False,
                "from_ms": 1,
                "to_ms": 2,
            }
        ),
    )
    response = client.network_destinations("removed-sandbox", from_ms=1, to_ms=2)
    assert response.trace_id == "trace-network"
    assert response.value.destinations[0].original_bytes is None
    assert response.value.destinations[0].observed_connections == 1
    assert client._rust_client.mock_calls[0].args[0] == "removed-sandbox"
    assert json.loads(client._rust_client.mock_calls[0].args[1])["from_ms"] == 1


class TestNetworkObservations(unittest.IsolatedAsyncioTestCase):
    async def test_async_status_does_not_connect_to_or_resume_a_guest(self) -> None:
        client = object.__new__(AsyncSandboxClient)
        client._rust_client = Mock()
        client._rust_client.network_status_json_async = AsyncMock(
            return_value=(
                "trace-status",
                json.dumps(
                    {
                        "state": "unknown",
                        "last_observed_at_ms": None,
                        "allocation_id": None,
                        "coverage": [],
                        "limitations": ["No HTTP visibility"],
                    }
                ),
            )
        )
        response = await client.network_status("removed-sandbox")
        assert response.value.state == "unknown"
        assert response.value.last_observed_at_ms is None
        client._rust_client.network_status_json_async.assert_awaited_once_with(
            "removed-sandbox"
        )
        client._rust_client.connect_proxy.assert_not_called()
