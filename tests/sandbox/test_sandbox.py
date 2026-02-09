"""Tests for the Sandbox class (container daemon interaction)."""

import httpx
import pytest
import respx

from tensorlake.sandbox import (
    CommandResult,
    DaemonInfo,
    DirectoryEntry,
    HealthResponse,
    ListDirectoryResponse,
    OutputEvent,
    OutputResponse,
    ProcessInfo,
    ProcessStatus,
    RemoteAPIError,
    Sandbox,
    SandboxClient,
    SandboxError,
    SendSignalResponse,
)


@pytest.fixture
def sandbox():
    """Create a Sandbox pointing to a remote proxy."""
    return Sandbox(
        sandbox_id="sb_test123",
        proxy_url="https://sandbox.tensorlake.ai",
        api_key="test-key",
    )


@pytest.fixture
def localhost_sandbox():
    """Create a Sandbox pointing to localhost proxy."""
    return Sandbox(
        sandbox_id="sb_test123",
        proxy_url="http://localhost:9443",
        api_key="test-key",
    )


@pytest.fixture
def mock_proxy():
    """Mock the sandbox proxy for remote client."""
    with respx.mock(base_url="https://sb_test123.sandbox.tensorlake.ai") as mock:
        yield mock


@pytest.fixture
def mock_localhost_proxy():
    """Mock the sandbox proxy for localhost client."""
    with respx.mock(base_url="http://localhost:9443") as mock:
        yield mock


PROCESS_JSON = {
    "pid": 42,
    "status": "running",
    "exit_code": None,
    "signal": None,
    "stdin_writable": False,
    "command": "python",
    "args": ["-c", "print('hello')"],
    "started_at": 1700000000,
    "ended_at": None,
}

EXITED_PROCESS_JSON = {
    "pid": 42,
    "status": "exited",
    "exit_code": 0,
    "signal": None,
    "stdin_writable": False,
    "command": "python",
    "args": ["-c", "print('hello')"],
    "started_at": 1700000000,
    "ended_at": 1700000005,
}


class TestSandboxInit:
    """Tests for Sandbox initialization and URL routing."""

    def test_remote_url_construction(self):
        """Remote proxy constructs subdomain URL."""
        sandbox = Sandbox(
            sandbox_id="abc123",
            proxy_url="https://sandbox.tensorlake.ai",
        )
        # The httpx client should have base_url with sandbox_id as subdomain
        assert "abc123.sandbox.tensorlake.ai" in str(sandbox._client.base_url)

    def test_localhost_url_construction(self):
        """Localhost proxy uses Host header override."""
        sandbox = Sandbox(
            sandbox_id="abc123",
            proxy_url="http://localhost:9443",
        )
        assert "localhost:9443" in str(sandbox._client.base_url)
        assert sandbox._client.headers.get("Host") == "abc123.local"

    def test_127_0_0_1_url_construction(self):
        """127.0.0.1 is treated as localhost."""
        sandbox = Sandbox(
            sandbox_id="abc123",
            proxy_url="http://127.0.0.1:9443",
        )
        assert "127.0.0.1:9443" in str(sandbox._client.base_url)
        assert sandbox._client.headers.get("Host") == "abc123.local"

    def test_auth_headers_set(self):
        """API key and org/project headers are set on the client."""
        sandbox = Sandbox(
            sandbox_id="abc123",
            proxy_url="https://sandbox.tensorlake.ai",
            api_key="my-key",
            organization_id="org-1",
            project_id="proj-2",
        )
        assert sandbox._client.headers["Authorization"] == "Bearer my-key"
        assert sandbox._client.headers["X-Forwarded-Organization-Id"] == "org-1"
        assert sandbox._client.headers["X-Forwarded-Project-Id"] == "proj-2"

    def test_no_auth_headers_when_none(self):
        """No auth headers when credentials are not provided."""
        sandbox = Sandbox(
            sandbox_id="abc123",
            proxy_url="https://sandbox.tensorlake.ai",
            api_key=None,
        )
        assert "Authorization" not in sandbox._client.headers

    def test_context_manager(self):
        """Test context manager support."""
        with Sandbox(
            sandbox_id="abc123",
            proxy_url="https://sandbox.tensorlake.ai",
        ) as sandbox:
            assert isinstance(sandbox, Sandbox)


class TestSandboxClientConnect:
    """Tests for SandboxClient.connect()."""

    def test_connect_remote(self):
        """Connect from remote client uses sandbox.tensorlake.ai."""
        client = SandboxClient(
            api_url="https://api.tensorlake.ai",
            api_key="test-key",
            organization_id="org-1",
            project_id="proj-2",
        )
        sandbox = client.connect("sb_123")
        assert "sb_123.sandbox.tensorlake.ai" in str(sandbox._client.base_url)
        assert sandbox._client.headers["Authorization"] == "Bearer test-key"
        assert sandbox._client.headers["X-Forwarded-Organization-Id"] == "org-1"
        assert sandbox._client.headers["X-Forwarded-Project-Id"] == "proj-2"

    def test_connect_localhost(self):
        """Connect from localhost client uses localhost:9443."""
        client = SandboxClient(
            api_url="http://localhost:8900",
            api_key="test-key",
        )
        sandbox = client.connect("sb_123")
        assert "localhost:9443" in str(sandbox._client.base_url)
        assert sandbox._client.headers.get("Host") == "sb_123.local"

    def test_connect_custom_proxy_url(self):
        """Custom proxy_url overrides auto-detection."""
        client = SandboxClient(api_url="https://api.tensorlake.ai")
        sandbox = client.connect("sb_123", proxy_url="https://custom-proxy.example.com")
        assert "sb_123.custom-proxy.example.com" in str(sandbox._client.base_url)


class TestProcessManagement:
    """Tests for process management APIs."""

    def test_start_process(self, sandbox, mock_proxy):
        """Test starting a process."""
        route = mock_proxy.post("/api/v1/processes").mock(
            return_value=httpx.Response(200, json=PROCESS_JSON)
        )

        result = sandbox.start_process(command="python", args=["-c", "print('hello')"])
        assert isinstance(result, ProcessInfo)
        assert result.pid == 42
        assert result.status == ProcessStatus.RUNNING
        assert result.command == "python"

        request = route.calls.last.request
        payload = request.read()
        assert b'"command":"python"' in payload or b'"command": "python"' in payload

    def test_start_process_with_all_options(self, sandbox, mock_proxy):
        """Test starting a process with all options."""
        route = mock_proxy.post("/api/v1/processes").mock(
            return_value=httpx.Response(
                200,
                json={
                    **PROCESS_JSON,
                    "stdin_writable": True,
                },
            )
        )

        result = sandbox.start_process(
            command="python",
            args=["script.py"],
            env={"FOO": "bar"},
            working_dir="/app",
            stdin_mode="pipe",
            stdout_mode="capture",
            stderr_mode="discard",
        )
        assert result.stdin_writable is True

        request = route.calls.last.request
        payload = request.read()
        assert b"stdin_mode" in payload
        assert b"stderr_mode" in payload

    def test_list_processes(self, sandbox, mock_proxy):
        """Test listing processes."""
        mock_proxy.get("/api/v1/processes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "processes": [
                        PROCESS_JSON,
                        {**EXITED_PROCESS_JSON, "pid": 43},
                    ]
                },
            )
        )

        result = sandbox.list_processes()
        assert len(result) == 2
        assert result[0].pid == 42
        assert result[0].status == ProcessStatus.RUNNING
        assert result[1].pid == 43
        assert result[1].status == ProcessStatus.EXITED

    def test_get_process(self, sandbox, mock_proxy):
        """Test getting a specific process."""
        mock_proxy.get("/api/v1/processes/42").mock(
            return_value=httpx.Response(200, json=EXITED_PROCESS_JSON)
        )

        result = sandbox.get_process(42)
        assert result.pid == 42
        assert result.status == ProcessStatus.EXITED
        assert result.exit_code == 0

    def test_kill_process(self, sandbox, mock_proxy):
        """Test killing a process."""
        mock_proxy.delete("/api/v1/processes/42").mock(return_value=httpx.Response(204))
        sandbox.kill_process(42)

    def test_send_signal(self, sandbox, mock_proxy):
        """Test sending a signal to a process."""
        route = mock_proxy.post("/api/v1/processes/42/signal").mock(
            return_value=httpx.Response(200, json={"success": True})
        )

        result = sandbox.send_signal(42, signal=15)
        assert result.success is True

        request = route.calls.last.request
        payload = request.read()
        assert b'"signal":15' in payload or b'"signal": 15' in payload

    def test_process_not_found(self, sandbox, mock_proxy):
        """Test error when process doesn't exist."""
        mock_proxy.get("/api/v1/processes/999").mock(
            return_value=httpx.Response(
                404, json={"error": "Process not found", "code": "NOT_FOUND"}
            )
        )

        with pytest.raises(RemoteAPIError) as exc_info:
            sandbox.get_process(999)
        assert exc_info.value.status_code == 404


class TestProcessIO:
    """Tests for process I/O APIs."""

    def test_write_stdin(self, sandbox, mock_proxy):
        """Test writing to stdin."""
        mock_proxy.post("/api/v1/processes/42/stdin").mock(
            return_value=httpx.Response(204)
        )
        sandbox.write_stdin(42, b"hello\n")

    def test_close_stdin(self, sandbox, mock_proxy):
        """Test closing stdin."""
        mock_proxy.post("/api/v1/processes/42/stdin/close").mock(
            return_value=httpx.Response(204)
        )
        sandbox.close_stdin(42)

    def test_get_stdout(self, sandbox, mock_proxy):
        """Test getting stdout."""
        mock_proxy.get("/api/v1/processes/42/stdout").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pid": 42,
                    "lines": ["hello", "world"],
                    "line_count": 2,
                },
            )
        )

        result = sandbox.get_stdout(42)
        assert isinstance(result, OutputResponse)
        assert result.pid == 42
        assert result.lines == ["hello", "world"]
        assert result.line_count == 2

    def test_get_stderr(self, sandbox, mock_proxy):
        """Test getting stderr."""
        mock_proxy.get("/api/v1/processes/42/stderr").mock(
            return_value=httpx.Response(
                200,
                json={"pid": 42, "lines": ["error!"], "line_count": 1},
            )
        )

        result = sandbox.get_stderr(42)
        assert result.lines == ["error!"]

    def test_get_output(self, sandbox, mock_proxy):
        """Test getting combined output."""
        mock_proxy.get("/api/v1/processes/42/output").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pid": 42,
                    "lines": ["stdout line", "stderr line"],
                    "line_count": 2,
                },
            )
        )

        result = sandbox.get_output(42)
        assert result.line_count == 2


class TestFileOperations:
    """Tests for file operation APIs."""

    def test_read_file(self, sandbox, mock_proxy):
        """Test reading a file."""
        mock_proxy.get("/api/v1/files").mock(
            return_value=httpx.Response(200, content=b"file contents here")
        )

        result = sandbox.read_file("/app/data.txt")
        assert result == b"file contents here"

    def test_write_file(self, sandbox, mock_proxy):
        """Test writing a file."""
        route = mock_proxy.put("/api/v1/files").mock(return_value=httpx.Response(204))

        sandbox.write_file("/app/script.py", b"print('hello')")

        request = route.calls.last.request
        assert request.content == b"print('hello')"

    def test_delete_file(self, sandbox, mock_proxy):
        """Test deleting a file."""
        mock_proxy.delete("/api/v1/files").mock(return_value=httpx.Response(204))
        sandbox.delete_file("/app/temp.txt")

    def test_list_directory(self, sandbox, mock_proxy):
        """Test listing a directory."""
        mock_proxy.get("/api/v1/files/list").mock(
            return_value=httpx.Response(
                200,
                json={
                    "path": "/app",
                    "entries": [
                        {
                            "name": "script.py",
                            "is_dir": False,
                            "size": 1234,
                            "modified_at": 1700000000,
                        },
                        {
                            "name": "data",
                            "is_dir": True,
                            "size": None,
                            "modified_at": 1700000000,
                        },
                    ],
                },
            )
        )

        result = sandbox.list_directory("/app")
        assert isinstance(result, ListDirectoryResponse)
        assert result.path == "/app"
        assert len(result.entries) == 2
        assert result.entries[0].name == "script.py"
        assert result.entries[0].is_dir is False
        assert result.entries[0].size == 1234
        assert result.entries[1].name == "data"
        assert result.entries[1].is_dir is True

    def test_file_not_found(self, sandbox, mock_proxy):
        """Test error when file doesn't exist."""
        mock_proxy.get("/api/v1/files").mock(
            return_value=httpx.Response(
                404, json={"error": "File not found", "code": "NOT_FOUND"}
            )
        )

        with pytest.raises(RemoteAPIError) as exc_info:
            sandbox.read_file("/app/nonexistent.txt")
        assert exc_info.value.status_code == 404

    def test_path_traversal_error(self, sandbox, mock_proxy):
        """Test error on path traversal attempt."""
        mock_proxy.get("/api/v1/files").mock(
            return_value=httpx.Response(
                403,
                json={
                    "error": "Path traversal detected",
                    "code": "PATH_TRAVERSAL",
                },
            )
        )

        with pytest.raises(RemoteAPIError) as exc_info:
            sandbox.read_file("/../../../etc/passwd")
        assert exc_info.value.status_code == 403


class TestHealthAndInfo:
    """Tests for health and info APIs."""

    def test_health(self, sandbox, mock_proxy):
        """Test health check."""
        mock_proxy.get("/api/v1/health").mock(
            return_value=httpx.Response(200, json={"healthy": True})
        )

        result = sandbox.health()
        assert isinstance(result, HealthResponse)
        assert result.healthy is True

    def test_info(self, sandbox, mock_proxy):
        """Test daemon info."""
        mock_proxy.get("/api/v1/info").mock(
            return_value=httpx.Response(
                200,
                json={
                    "version": "0.1.0",
                    "uptime_secs": 3600,
                    "running_processes": 2,
                    "total_processes": 5,
                },
            )
        )

        result = sandbox.info()
        assert isinstance(result, DaemonInfo)
        assert result.version == "0.1.0"
        assert result.uptime_secs == 3600
        assert result.running_processes == 2
        assert result.total_processes == 5


class TestLocalhostProxy:
    """Tests for localhost proxy routing."""

    def test_start_process_localhost(self, localhost_sandbox, mock_localhost_proxy):
        """Test that localhost proxy routes correctly."""
        mock_localhost_proxy.post("/api/v1/processes").mock(
            return_value=httpx.Response(200, json=PROCESS_JSON)
        )

        result = localhost_sandbox.start_process(command="python")
        assert result.pid == 42

    def test_read_file_localhost(self, localhost_sandbox, mock_localhost_proxy):
        """Test file read through localhost proxy."""
        mock_localhost_proxy.get("/api/v1/files").mock(
            return_value=httpx.Response(200, content=b"local file")
        )

        result = localhost_sandbox.read_file("/app/file.txt")
        assert result == b"local file"


class TestRun:
    """Tests for the high-level run() method."""

    def test_run_success(self, sandbox, mock_proxy):
        """Run a command, poll until exited, return stdout/stderr."""
        mock_proxy.post("/api/v1/processes").mock(
            return_value=httpx.Response(200, json=PROCESS_JSON)
        )
        mock_proxy.get("/api/v1/processes/42").mock(
            return_value=httpx.Response(200, json=EXITED_PROCESS_JSON)
        )
        mock_proxy.get("/api/v1/processes/42/stdout").mock(
            return_value=httpx.Response(
                200, json={"pid": 42, "lines": ["hello"], "line_count": 1}
            )
        )
        mock_proxy.get("/api/v1/processes/42/stderr").mock(
            return_value=httpx.Response(
                200, json={"pid": 42, "lines": [], "line_count": 0}
            )
        )

        result = sandbox.run("python", args=["-c", "print('hello')"])
        assert isinstance(result, CommandResult)
        assert result.exit_code == 0
        assert result.stdout == "hello"
        assert result.stderr == ""

    def test_run_nonzero_exit(self, sandbox, mock_proxy):
        """Run returns non-zero exit code without raising."""
        mock_proxy.post("/api/v1/processes").mock(
            return_value=httpx.Response(200, json=PROCESS_JSON)
        )
        mock_proxy.get("/api/v1/processes/42").mock(
            return_value=httpx.Response(
                200, json={**EXITED_PROCESS_JSON, "exit_code": 1}
            )
        )
        mock_proxy.get("/api/v1/processes/42/stdout").mock(
            return_value=httpx.Response(
                200, json={"pid": 42, "lines": [], "line_count": 0}
            )
        )
        mock_proxy.get("/api/v1/processes/42/stderr").mock(
            return_value=httpx.Response(
                200, json={"pid": 42, "lines": ["error"], "line_count": 1}
            )
        )

        result = sandbox.run("false")
        assert result.exit_code == 1
        assert result.stderr == "error"

    def test_run_signaled(self, sandbox, mock_proxy):
        """Run returns negative exit code when killed by signal."""
        mock_proxy.post("/api/v1/processes").mock(
            return_value=httpx.Response(200, json=PROCESS_JSON)
        )
        mock_proxy.get("/api/v1/processes/42").mock(
            return_value=httpx.Response(
                200,
                json={
                    **EXITED_PROCESS_JSON,
                    "status": "signaled",
                    "exit_code": None,
                    "signal": 9,
                },
            )
        )
        mock_proxy.get("/api/v1/processes/42/stdout").mock(
            return_value=httpx.Response(
                200, json={"pid": 42, "lines": [], "line_count": 0}
            )
        )
        mock_proxy.get("/api/v1/processes/42/stderr").mock(
            return_value=httpx.Response(
                200, json={"pid": 42, "lines": [], "line_count": 0}
            )
        )

        result = sandbox.run("sleep", args=["999"])
        assert result.exit_code == -9

    def test_run_timeout(self, sandbox, mock_proxy):
        """Run raises SandboxError when command exceeds timeout."""
        mock_proxy.post("/api/v1/processes").mock(
            return_value=httpx.Response(200, json=PROCESS_JSON)
        )
        # Always return running status
        mock_proxy.get("/api/v1/processes/42").mock(
            return_value=httpx.Response(200, json=PROCESS_JSON)
        )
        mock_proxy.delete("/api/v1/processes/42").mock(return_value=httpx.Response(204))

        with pytest.raises(SandboxError, match="timed out"):
            sandbox.run("sleep", args=["999"], timeout=0.2)


class TestTerminate:
    """Tests for sandbox terminate and context manager auto-cleanup."""

    def test_terminate_with_lifecycle_client(self, mock_proxy):
        """Terminate calls delete on the lifecycle client."""
        mock_client = SandboxClient(api_url="http://test.local")
        sandbox = Sandbox(
            sandbox_id="sb_123",
            proxy_url="https://sandbox.tensorlake.ai",
        )
        sandbox._lifecycle_client = mock_client

        # Mock the delete call on the lifecycle client
        with respx.mock(base_url="http://test.local") as mock_api:
            mock_api.delete("/sandboxes/sb_123").mock(return_value=httpx.Response(200))
            sandbox.terminate()

        assert sandbox._lifecycle_client is None

    def test_context_manager_auto_terminates(self, mock_proxy):
        """Exiting context manager calls terminate when lifecycle_client is set."""
        mock_client = SandboxClient(api_url="http://test.local")

        with respx.mock(base_url="http://test.local") as mock_api:
            route = mock_api.delete("/sandboxes/sb_123").mock(
                return_value=httpx.Response(200)
            )
            with Sandbox(
                sandbox_id="sb_123",
                proxy_url="https://sandbox.tensorlake.ai",
            ) as sandbox:
                sandbox._lifecycle_client = mock_client

            assert route.called

    def test_context_manager_no_terminate_without_lifecycle(self):
        """Exiting context manager only closes (no terminate) without lifecycle_client."""
        with Sandbox(
            sandbox_id="sb_123",
            proxy_url="https://sandbox.tensorlake.ai",
        ) as sandbox:
            assert sandbox._lifecycle_client is None
        # No error — just closed, no terminate attempted

    def test_sandbox_id_property(self):
        """Test sandbox_id property."""
        sandbox = Sandbox(
            sandbox_id="sb_abc",
            proxy_url="https://sandbox.tensorlake.ai",
        )
        assert sandbox.sandbox_id == "sb_abc"


class TestCreateAndConnect:
    """Tests for SandboxClient.create_and_connect()."""

    def test_create_and_connect_immediate_running(self):
        """Sandbox starts Running immediately — returns connected Sandbox."""
        client = SandboxClient(api_url="http://test.local")

        with respx.mock(base_url="http://test.local") as mock_api:
            mock_api.post("/sandboxes").mock(
                return_value=httpx.Response(
                    200, json={"sandbox_id": "sb_fast", "status": "Pending"}
                )
            )
            mock_api.get("/sandboxes/sb_fast").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "sandbox_id": "sb_fast",
                        "namespace": "default",
                        "status": "Running",
                        "resources": {
                            "cpus": 1.0,
                            "memory_mb": 512,
                            "ephemeral_disk_mb": 1024,
                        },
                    },
                )
            )

            sandbox = client.create_and_connect(image="python:3.11")
            assert sandbox.sandbox_id == "sb_fast"
            assert sandbox._lifecycle_client is client
            sandbox.close()

    def test_create_and_connect_terminated_raises(self):
        """Sandbox terminates during startup — raises SandboxError."""
        client = SandboxClient(api_url="http://test.local")

        with respx.mock(base_url="http://test.local") as mock_api:
            mock_api.post("/sandboxes").mock(
                return_value=httpx.Response(
                    200, json={"sandbox_id": "sb_fail", "status": "Pending"}
                )
            )
            mock_api.get("/sandboxes/sb_fail").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "sandbox_id": "sb_fail",
                        "namespace": "default",
                        "status": "Terminated",
                        "resources": {
                            "cpus": 1.0,
                            "memory_mb": 512,
                            "ephemeral_disk_mb": 1024,
                        },
                    },
                )
            )

            with pytest.raises(SandboxError, match="terminated during startup"):
                client.create_and_connect(image="python:3.11")

    def test_create_and_connect_timeout_raises(self):
        """Sandbox stays Pending past timeout — raises SandboxError and cleans up."""
        client = SandboxClient(api_url="http://test.local")

        with respx.mock(base_url="http://test.local") as mock_api:
            mock_api.post("/sandboxes").mock(
                return_value=httpx.Response(
                    200, json={"sandbox_id": "sb_slow", "status": "Pending"}
                )
            )
            mock_api.get("/sandboxes/sb_slow").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "sandbox_id": "sb_slow",
                        "namespace": "default",
                        "status": "Pending",
                        "resources": {
                            "cpus": 1.0,
                            "memory_mb": 512,
                            "ephemeral_disk_mb": 1024,
                        },
                    },
                )
            )
            cleanup_route = mock_api.delete("/sandboxes/sb_slow").mock(
                return_value=httpx.Response(200)
            )

            with pytest.raises(SandboxError, match="did not start"):
                client.create_and_connect(image="python:3.11", startup_timeout=1)

            assert cleanup_route.called

    def test_create_and_connect_with_pool(self):
        """Create from pool — same flow, just passes pool_id."""
        client = SandboxClient(api_url="http://test.local")

        with respx.mock(base_url="http://test.local") as mock_api:
            create_route = mock_api.post("/sandboxes").mock(
                return_value=httpx.Response(
                    200, json={"sandbox_id": "sb_pool", "status": "Pending"}
                )
            )
            mock_api.get("/sandboxes/sb_pool").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "sandbox_id": "sb_pool",
                        "namespace": "default",
                        "status": "Running",
                        "resources": {
                            "cpus": 1.0,
                            "memory_mb": 512,
                            "ephemeral_disk_mb": 1024,
                        },
                        "pool_id": "pool_123",
                    },
                )
            )

            sandbox = client.create_and_connect(pool_id="pool_123")
            assert sandbox.sandbox_id == "sb_pool"
            sandbox.close()

            request = create_route.calls.last.request
            assert b"pool_id" in request.read()
