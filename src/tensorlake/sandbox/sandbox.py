"""Client for interacting with a running sandbox."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Iterator
from urllib.parse import urlparse

import httpx

from . import _defaults
from .exceptions import RemoteAPIError, SandboxError
from .models import (
    CommandResult,
    DaemonInfo,
    HealthResponse,
    ListDirectoryResponse,
    ListProcessesResponse,
    OutputEvent,
    OutputMode,
    OutputResponse,
    ProcessInfo,
    ProcessStatus,
    SendSignalResponse,
    StdinMode,
)

# Avoid circular import: sandbox.py ↔ client.py.  With ``from __future__
# import annotations`` the type string is never evaluated at runtime, but
# the import itself would still execute and trigger a cycle.
if TYPE_CHECKING:
    from .client import SandboxClient


class Sandbox:
    """Client for interacting with a running sandbox.

    Provides process management, file operations, and I/O streaming
    through the sandbox proxy.

    Can be used as a context manager. If created via
    ``SandboxClient.create_and_connect()``, exiting the context manager
    automatically terminates the sandbox. Otherwise, it only closes the
    HTTP connection while the sandbox continues running.
    """

    def __init__(
        self,
        sandbox_id: str,
        proxy_url: str = _defaults.SANDBOX_PROXY_URL,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
    ):
        self._sandbox_id = sandbox_id
        self._owns_sandbox: bool = False
        self._lifecycle_client: SandboxClient | None = None

        headers: dict[str, str] = {}
        if api_key is not None:
            headers["Authorization"] = f"Bearer {api_key}"
        if organization_id is not None:
            headers["X-Forwarded-Organization-Id"] = organization_id
        if project_id is not None:
            headers["X-Forwarded-Project-Id"] = project_id

        parsed = urlparse(proxy_url)
        if parsed.hostname in ("localhost", "127.0.0.1"):
            base_url = proxy_url.rstrip("/")
            headers["Host"] = f"{sandbox_id}.local"
        else:
            port_part = f":{parsed.port}" if parsed.port else ""
            base_url = f"{parsed.scheme}://{sandbox_id}.{parsed.hostname}{port_part}"

        # Each Sandbox connects to a different proxy endpoint (subdomain per
        # sandbox), so it needs its own httpx.Client with a unique base_url.
        self._client: httpx.Client = httpx.Client(
            base_url=base_url,
            headers=headers,
            # Sandbox proxy operations (process start, file I/O) are generally
            # fast, but container startup or large file transfers can take time.
            timeout=_defaults.DEFAULT_HTTP_TIMEOUT_SEC,
        )

    @property
    def sandbox_id(self) -> str:
        return self._sandbox_id

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager.

        If this sandbox was created via ``create_and_connect()``, the
        sandbox is terminated (deleted from the server). Otherwise only
        the HTTP connection is closed and the sandbox keeps running.
        """
        if self._owns_sandbox:
            self.terminate()
        else:
            self.close()

    def close(self):
        """Close the HTTP connection. The sandbox keeps running."""
        self._client.close()

    def terminate(self):
        """Terminate the sandbox and close the connection."""
        lifecycle_client = self._lifecycle_client
        self._owns_sandbox = False
        self._lifecycle_client = None
        self.close()
        if lifecycle_client is not None:
            lifecycle_client.delete(self._sandbox_id)

    def _handle_response(self, response: httpx.Response) -> httpx.Response:
        if response.is_success:
            return response
        try:
            error_data = response.json()
            message = error_data.get("error", response.text)
        except Exception:
            message = response.text
        raise RemoteAPIError(response.status_code, message)

    # --- High-level convenience ---

    def run(
        self,
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        timeout: float | None = None,
    ) -> CommandResult:
        """Run a command to completion and return its output.

        Args:
            command: Command to execute
            args: Command arguments
            env: Environment variables
            working_dir: Working directory
            timeout: Maximum seconds to wait (None = no limit)

        Returns:
            CommandResult with exit_code, stdout, and stderr
        """
        proc = self.start_process(
            command=command,
            args=args,
            env=env,
            working_dir=working_dir,
        )

        deadline = time.time() + timeout if timeout else None
        while True:
            info = self.get_process(proc.pid)
            if info.status != ProcessStatus.RUNNING:
                break
            if deadline and time.time() > deadline:
                self.kill_process(proc.pid)
                raise SandboxError(f"Command timed out after {timeout}s")
            # Poll at 100ms — fast enough for interactive commands while
            # keeping overhead low for longer-running processes.
            time.sleep(0.1)

        stdout_resp = self.get_stdout(proc.pid)
        stderr_resp = self.get_stderr(proc.pid)

        if info.exit_code is not None:
            exit_code = info.exit_code
        elif info.signal is not None:
            exit_code = -info.signal
        else:
            exit_code = -1

        return CommandResult(
            exit_code=exit_code,
            stdout="\n".join(stdout_resp.lines),
            stderr="\n".join(stderr_resp.lines),
        )

    # --- Process management ---

    def start_process(
        self,
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        stdin_mode: StdinMode = StdinMode.CLOSED,
        stdout_mode: OutputMode = OutputMode.CAPTURE,
        stderr_mode: OutputMode = OutputMode.CAPTURE,
    ) -> ProcessInfo:
        """Start a new process in the sandbox.

        Args:
            command: Command to execute
            args: Command arguments
            env: Environment variables
            working_dir: Working directory
            stdin_mode: StdinMode.CLOSED or StdinMode.PIPE
            stdout_mode: OutputMode.CAPTURE or OutputMode.DISCARD
            stderr_mode: OutputMode.CAPTURE or OutputMode.DISCARD

        Returns:
            ProcessInfo with pid and status
        """
        payload: dict = {"command": command}
        if args is not None:
            payload["args"] = args
        if env is not None:
            payload["env"] = env
        if working_dir is not None:
            payload["working_dir"] = working_dir
        if stdin_mode != StdinMode.CLOSED:
            payload["stdin_mode"] = stdin_mode
        if stdout_mode != OutputMode.CAPTURE:
            payload["stdout_mode"] = stdout_mode
        if stderr_mode != OutputMode.CAPTURE:
            payload["stderr_mode"] = stderr_mode

        response = self._handle_response(
            self._client.post("/api/v1/processes", json=payload)
        )
        return ProcessInfo.model_validate(response.json())

    def list_processes(self) -> list[ProcessInfo]:
        """List all processes in the sandbox."""
        response = self._handle_response(self._client.get("/api/v1/processes"))
        data = ListProcessesResponse.model_validate(response.json())
        return data.processes

    def get_process(self, pid: int) -> ProcessInfo:
        """Get information about a specific process."""
        response = self._handle_response(self._client.get(f"/api/v1/processes/{pid}"))
        return ProcessInfo.model_validate(response.json())

    def kill_process(self, pid: int) -> None:
        """Kill a process."""
        self._handle_response(self._client.delete(f"/api/v1/processes/{pid}"))

    def send_signal(self, pid: int, signal: int) -> SendSignalResponse:
        """Send a signal to a process.

        Args:
            pid: Process ID
            signal: Signal number (e.g. 15 for SIGTERM, 9 for SIGKILL)
        """
        response = self._handle_response(
            self._client.post(
                f"/api/v1/processes/{pid}/signal", json={"signal": signal}
            )
        )
        return SendSignalResponse.model_validate(response.json())

    # --- Process I/O ---

    def write_stdin(self, pid: int, data: bytes) -> None:
        """Write data to a process's stdin."""
        self._handle_response(
            self._client.post(f"/api/v1/processes/{pid}/stdin", content=data)
        )

    def close_stdin(self, pid: int) -> None:
        """Close a process's stdin."""
        self._handle_response(self._client.post(f"/api/v1/processes/{pid}/stdin/close"))

    def get_stdout(self, pid: int) -> OutputResponse:
        """Get all stdout output from a process."""
        response = self._handle_response(
            self._client.get(f"/api/v1/processes/{pid}/stdout")
        )
        return OutputResponse.model_validate(response.json())

    def get_stderr(self, pid: int) -> OutputResponse:
        """Get all stderr output from a process."""
        response = self._handle_response(
            self._client.get(f"/api/v1/processes/{pid}/stderr")
        )
        return OutputResponse.model_validate(response.json())

    def get_output(self, pid: int) -> OutputResponse:
        """Get all combined output from a process."""
        response = self._handle_response(
            self._client.get(f"/api/v1/processes/{pid}/output")
        )
        return OutputResponse.model_validate(response.json())

    def follow_stdout(self, pid: int) -> Iterator[OutputEvent]:
        """Stream stdout from a process via SSE. Replays existing output then streams live."""
        yield from self._follow_stream(f"/api/v1/processes/{pid}/stdout/follow")

    def follow_stderr(self, pid: int) -> Iterator[OutputEvent]:
        """Stream stderr from a process via SSE. Replays existing output then streams live."""
        yield from self._follow_stream(f"/api/v1/processes/{pid}/stderr/follow")

    def follow_output(self, pid: int) -> Iterator[OutputEvent]:
        """Stream combined output from a process via SSE. Replays existing output then streams live."""
        yield from self._follow_stream(f"/api/v1/processes/{pid}/output/follow")

    def _follow_stream(self, path: str) -> Iterator[OutputEvent]:
        with self._client.stream("GET", path, timeout=None) as response:
            if not response.is_success:
                response.read()
                try:
                    error_data = response.json()
                    message = error_data.get("error", response.text)
                except Exception:
                    message = response.text
                raise RemoteAPIError(response.status_code, message)

            for line in response.iter_lines():
                if line.startswith("data: "):
                    data = json.loads(line[6:])
                    yield OutputEvent.model_validate(data)

    # --- File operations ---

    def read_file(self, path: str) -> bytes:
        """Read a file from the sandbox.

        Args:
            path: Absolute path inside the sandbox

        Returns:
            File contents as bytes
        """
        response = self._handle_response(
            self._client.get("/api/v1/files", params={"path": path})
        )
        return response.content

    def write_file(self, path: str, content: bytes) -> None:
        """Write a file to the sandbox.

        Args:
            path: Absolute path inside the sandbox
            content: File contents as bytes
        """
        self._handle_response(
            self._client.put("/api/v1/files", params={"path": path}, content=content)
        )

    def delete_file(self, path: str) -> None:
        """Delete a file from the sandbox.

        Args:
            path: Absolute path inside the sandbox
        """
        self._handle_response(
            self._client.delete("/api/v1/files", params={"path": path})
        )

    def list_directory(self, path: str) -> ListDirectoryResponse:
        """List contents of a directory in the sandbox.

        Args:
            path: Absolute path inside the sandbox

        Returns:
            ListDirectoryResponse with path and entries
        """
        response = self._handle_response(
            self._client.get("/api/v1/files/list", params={"path": path})
        )
        return ListDirectoryResponse.model_validate(response.json())

    # --- Health and info ---

    def health(self) -> HealthResponse:
        """Check the container daemon health."""
        response = self._handle_response(self._client.get("/api/v1/health"))
        return HealthResponse.model_validate(response.json())

    def info(self) -> DaemonInfo:
        """Get container daemon info (version, uptime, process counts)."""
        response = self._handle_response(self._client.get("/api/v1/info"))
        return DaemonInfo.model_validate(response.json())
