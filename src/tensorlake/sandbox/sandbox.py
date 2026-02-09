"""Client for interacting with a running sandbox."""

from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional
from urllib.parse import urlparse

import httpx

from .exceptions import RemoteAPIError, SandboxError
from .models import (
    CommandResult,
    DaemonInfo,
    DirectoryEntry,
    HealthResponse,
    ListDirectoryResponse,
    ListProcessesResponse,
    OutputEvent,
    OutputResponse,
    ProcessInfo,
    ProcessStatus,
    SendSignalResponse,
)

if TYPE_CHECKING:
    from .client import SandboxClient

_SANDBOX_PROXY_URL_FROM_ENV: str = os.getenv(
    "TENSORLAKE_SANDBOX_PROXY_URL", "https://sandbox.tensorlake.ai"
)
_API_KEY_FROM_ENV: str | None = os.getenv("TENSORLAKE_API_KEY")


class Sandbox:
    """Client for interacting with a running sandbox.

    Provides process management, file operations, and I/O streaming
    through the sandbox proxy.

    Can be used as a context manager. If created via
    SandboxClient.create_and_connect(), exiting the context manager
    automatically terminates the sandbox.
    """

    def __init__(
        self,
        sandbox_id: str,
        proxy_url: str = _SANDBOX_PROXY_URL_FROM_ENV,
        api_key: str | None = _API_KEY_FROM_ENV,
        organization_id: str | None = None,
        project_id: str | None = None,
    ):
        self._sandbox_id = sandbox_id
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

        self._client: httpx.Client = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=30.0,
        )

    @property
    def sandbox_id(self) -> str:
        return self._sandbox_id

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lifecycle_client is not None:
            self.terminate()
        else:
            self.close()

    def close(self):
        """Close the HTTP connection. The sandbox keeps running."""
        self._client.close()

    def terminate(self):
        """Terminate the sandbox and close the connection."""
        sandbox_id = self._sandbox_id
        lifecycle_client = self._lifecycle_client
        self._lifecycle_client = None
        self.close()
        if lifecycle_client is not None:
            lifecycle_client.delete(sandbox_id)

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
        args: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        working_dir: Optional[str] = None,
        timeout: Optional[float] = None,
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
                raise SandboxError(
                    f"Command timed out after {timeout}s"
                )
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
        args: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        working_dir: Optional[str] = None,
        stdin_mode: str = "closed",
        stdout_mode: str = "capture",
        stderr_mode: str = "capture",
    ) -> ProcessInfo:
        """Start a new process in the sandbox.

        Args:
            command: Command to execute
            args: Command arguments
            env: Environment variables
            working_dir: Working directory
            stdin_mode: "closed" or "pipe"
            stdout_mode: "capture" or "discard"
            stderr_mode: "capture" or "discard"

        Returns:
            ProcessInfo with pid and status
        """
        payload: dict = {"command": command}
        if args:
            payload["args"] = args
        if env:
            payload["env"] = env
        if working_dir is not None:
            payload["working_dir"] = working_dir
        if stdin_mode != "closed":
            payload["stdin_mode"] = stdin_mode
        if stdout_mode != "capture":
            payload["stdout_mode"] = stdout_mode
        if stderr_mode != "capture":
            payload["stderr_mode"] = stderr_mode

        response = self._handle_response(
            self._client.post("/api/v1/processes", json=payload)
        )
        return ProcessInfo(**response.json())

    def list_processes(self) -> List[ProcessInfo]:
        """List all processes in the sandbox."""
        response = self._handle_response(self._client.get("/api/v1/processes"))
        data = ListProcessesResponse(**response.json())
        return data.processes

    def get_process(self, pid: int) -> ProcessInfo:
        """Get information about a specific process."""
        response = self._handle_response(
            self._client.get(f"/api/v1/processes/{pid}")
        )
        return ProcessInfo(**response.json())

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
        return SendSignalResponse(**response.json())

    # --- Process I/O ---

    def write_stdin(self, pid: int, data: bytes) -> None:
        """Write data to a process's stdin."""
        self._handle_response(
            self._client.post(f"/api/v1/processes/{pid}/stdin", content=data)
        )

    def close_stdin(self, pid: int) -> None:
        """Close a process's stdin."""
        self._handle_response(
            self._client.post(f"/api/v1/processes/{pid}/stdin/close")
        )

    def get_stdout(self, pid: int) -> OutputResponse:
        """Get all stdout output from a process."""
        response = self._handle_response(
            self._client.get(f"/api/v1/processes/{pid}/stdout")
        )
        return OutputResponse(**response.json())

    def get_stderr(self, pid: int) -> OutputResponse:
        """Get all stderr output from a process."""
        response = self._handle_response(
            self._client.get(f"/api/v1/processes/{pid}/stderr")
        )
        return OutputResponse(**response.json())

    def get_output(self, pid: int) -> OutputResponse:
        """Get all combined output from a process."""
        response = self._handle_response(
            self._client.get(f"/api/v1/processes/{pid}/output")
        )
        return OutputResponse(**response.json())

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
                    yield OutputEvent(**data)

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
        return ListDirectoryResponse(**response.json())

    # --- Health and info ---

    def health(self) -> HealthResponse:
        """Check the container daemon health."""
        response = self._handle_response(self._client.get("/api/v1/health"))
        return HealthResponse(**response.json())

    def info(self) -> DaemonInfo:
        """Get container daemon info (version, uptime, process counts)."""
        response = self._handle_response(self._client.get("/api/v1/info"))
        return DaemonInfo(**response.json())
