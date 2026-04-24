"""Client for interacting with a running sandbox."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Iterator
from urllib.parse import urlparse

import httpx

from . import _defaults
from .exceptions import (
    RemoteAPIError,
    SandboxConnectionError,
    SandboxError,
)
from .models import (
    CommandResult,
    CreateSnapshotResponse,
    DaemonInfo,
    HealthResponse,
    ListDirectoryResponse,
    ListProcessesResponse,
    OutputEvent,
    OutputMode,
    OutputResponse,
    ProcessInfo,
    SandboxInfo,
    SandboxStatus,
    SendSignalResponse,
    SnapshotContentMode,
    SnapshotInfo,
    StdinMode,
)

if TYPE_CHECKING:
    from .client import SandboxClient

try:
    from tensorlake._cloud_sdk import (
        CloudSandboxClientError as RustCloudSandboxClientError,
    )
    from tensorlake._cloud_sdk import (
        CloudSandboxDesktopClient as RustCloudSandboxDesktopClient,
    )
    from tensorlake._cloud_sdk import (
        CloudSandboxProxyClient as RustCloudSandboxProxyClient,
    )

    _RUST_SANDBOX_PROXY_CLIENT_AVAILABLE = True
except Exception:
    try:
        from _cloud_sdk import CloudSandboxClientError as RustCloudSandboxClientError
        from _cloud_sdk import (
            CloudSandboxDesktopClient as RustCloudSandboxDesktopClient,
        )
        from _cloud_sdk import CloudSandboxProxyClient as RustCloudSandboxProxyClient

        _RUST_SANDBOX_PROXY_CLIENT_AVAILABLE = True
    except Exception:
        RustCloudSandboxDesktopClient = None
        RustCloudSandboxProxyClient = None
        RustCloudSandboxClientError = None
        _RUST_SANDBOX_PROXY_CLIENT_AVAILABLE = False


def _parse_rust_client_error_fields(
    e: Exception,
) -> tuple[str | None, int | None, str]:
    kind: str | None = None
    status_code: int | None = None
    message = str(e)

    if len(e.args) == 3:
        kind, status_code, message = e.args
    elif len(e.args) == 1 and isinstance(e.args[0], tuple) and len(e.args[0]) == 3:
        kind, status_code, message = e.args[0]

    return kind, status_code, message


def _raise_as_sandbox_error(e: Exception) -> None:
    if isinstance(e, SandboxError):
        raise

    if (
        RustCloudSandboxClientError is not None
        and isinstance(e, RustCloudSandboxClientError)
        and len(e.args) > 0
    ):
        kind, status_code, message = _parse_rust_client_error_fields(e)
        if kind == "connection":
            raise SandboxConnectionError(message) from None
        if status_code is not None:
            raise RemoteAPIError(status_code, message) from None
        raise SandboxError(message) from None

    raise SandboxError(str(e)) from e


class Sandbox:
    """Client for interacting with a running sandbox.

    Provides process management, file operations, and I/O streaming
    through the sandbox proxy.

    Can be used as a context manager. If created via
    ``SandboxClient.create_and_connect()``, exiting the context manager
    automatically terminates the sandbox. Otherwise, it only closes the
    client while the sandbox continues running.
    """

    def __init__(
        self,
        identifier: str | None = None,
        proxy_url: str = _defaults.SANDBOX_PROXY_URL,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
        *,
        sandbox_id: str | None = None,
        routing_hint: str | None = None,
        _proxy_rust_client: object | None = None,
    ):
        if identifier and sandbox_id and identifier != sandbox_id:
            raise SandboxError(
                "Provide only one of `identifier` or `sandbox_id`, not both."
            )
        sandbox_identifier = identifier or sandbox_id
        if not sandbox_identifier:
            raise SandboxError(
                "`identifier` is required. `sandbox_id` is accepted as a deprecated alias."
            )

        self._identifier = sandbox_identifier
        self._sandbox_id: str | None = None
        self._name: str | None = None
        self._name_loaded: bool = False
        self._cached_info: SandboxInfo | None = None
        self._owns_sandbox: bool = False
        self._lifecycle_client: SandboxClient | None = None
        self._owns_lifecycle_client: bool = False
        self._proxy_url = proxy_url
        self._api_key = api_key
        self._organization_id = organization_id
        self._project_id = project_id
        parsed_proxy = urlparse(proxy_url)
        self._host_header = None
        if parsed_proxy.hostname in ("localhost", "127.0.0.1"):
            self._host_header = f"{sandbox_identifier}.local"
        self._proxy_headers: dict[str, str] = {}
        if api_key:
            self._proxy_headers["Authorization"] = f"Bearer {api_key}"
        if organization_id:
            self._proxy_headers["X-Forwarded-Organization-Id"] = organization_id
        if project_id:
            self._proxy_headers["X-Forwarded-Project-Id"] = project_id
        if self._host_header:
            self._proxy_headers["Host"] = self._host_header

        if _proxy_rust_client is not None:
            self._rust_client = _proxy_rust_client
            self._base_url = self._rust_client.base_url()
            return

        if not _RUST_SANDBOX_PROXY_CLIENT_AVAILABLE:
            raise SandboxError(
                "Rust Cloud SDK sandbox proxy client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        try:
            self._rust_client = RustCloudSandboxProxyClient(
                proxy_url=proxy_url,
                sandbox_id=sandbox_identifier,
                api_key=api_key,
                organization_id=organization_id,
                project_id=project_id,
                routing_hint=routing_hint,
            )
            self._base_url = self._rust_client.base_url()
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- Class-level factories ---

    @staticmethod
    def _resolve_lifecycle_client(
        _client: "SandboxClient | None",
        *,
        api_url: str,
        api_key: str | None,
        organization_id: str | None,
        project_id: str | None,
        namespace: str | None,
    ) -> "SandboxClient":
        if _client is not None:
            return _client
        from .client import SandboxClient

        return SandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            _internal=True,
        )

    @classmethod
    def create(
        cls,
        *,
        image: str | None = None,
        cpus: float = 1.0,
        memory_mb: int = 1024,
        ephemeral_disk_mb: int = 1024,
        secret_names: list[str] | None = None,
        timeout_secs: int | None = None,
        entrypoint: list[str] | None = None,
        allow_internet_access: bool = True,
        allow_out: list[str] | None = None,
        deny_out: list[str] | None = None,
        pool_id: str | None = None,
        snapshot_id: str | None = None,
        name: str | None = None,
        proxy_url: str | None = None,
        startup_timeout: float = 60,
        api_url: str = _defaults.API_URL,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
        _client: "SandboxClient | None" = None,
    ) -> Sandbox:
        """Create a new sandbox (or restore from ``snapshot_id``) and return a ready handle.

        Blocks until the sandbox reaches Running status.
        """
        owns_client = _client is None
        client = cls._resolve_lifecycle_client(
            _client,
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
        )
        sandbox = client.create_and_connect(
            image=image,
            cpus=cpus,
            memory_mb=memory_mb,
            ephemeral_disk_mb=ephemeral_disk_mb,
            secret_names=secret_names,
            timeout_secs=timeout_secs,
            entrypoint=entrypoint,
            allow_internet_access=allow_internet_access,
            allow_out=allow_out,
            deny_out=deny_out,
            pool_id=pool_id,
            snapshot_id=snapshot_id,
            proxy_url=proxy_url,
            startup_timeout=startup_timeout,
            name=name,
        )
        if owns_client:
            sandbox._owns_lifecycle_client = True
        return sandbox

    @classmethod
    def connect(
        cls,
        *,
        sandbox_id: str | None = None,
        identifier: str | None = None,
        proxy_url: str | None = None,
        routing_hint: str | None = None,
        api_url: str = _defaults.API_URL,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
        _client: "SandboxClient | None" = None,
    ) -> Sandbox:
        """Attach to an existing sandbox by ID or name without changing its state."""
        owns_client = _client is None
        client = cls._resolve_lifecycle_client(
            _client,
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
        )
        sandbox = client.connect(
            identifier=identifier,
            proxy_url=proxy_url,
            sandbox_id=sandbox_id,
            routing_hint=routing_hint,
        )
        if owns_client:
            sandbox._owns_lifecycle_client = True
        return sandbox

    @classmethod
    def get_snapshot(
        cls,
        snapshot_id: str,
        *,
        api_url: str = _defaults.API_URL,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
        _client: "SandboxClient | None" = None,
    ) -> SnapshotInfo:
        """Get a snapshot by ID."""
        client = cls._resolve_lifecycle_client(
            _client,
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
        )
        try:
            return client.get_snapshot(snapshot_id)
        finally:
            if _client is None:
                client.close()

    @classmethod
    def delete_snapshot(
        cls,
        snapshot_id: str,
        *,
        api_url: str = _defaults.API_URL,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
        _client: "SandboxClient | None" = None,
    ) -> None:
        """Delete a snapshot by ID."""
        client = cls._resolve_lifecycle_client(
            _client,
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
        )
        try:
            client.delete_snapshot(snapshot_id)
        finally:
            if _client is None:
                client.close()

    def _fetch_info(self) -> SandboxInfo:
        if self._cached_info is None:
            if self._lifecycle_client is None:
                raise SandboxError(
                    "Cannot resolve sandbox info: no lifecycle client available. "
                    "Connect via SandboxClient.connect() to enable sandbox_id and name lookup."
                )
            self._cached_info = self._lifecycle_client.get(self._identifier)
        return self._cached_info

    @property
    def sandbox_id(self) -> str:
        """The server-assigned UUID for this sandbox."""
        if self._sandbox_id is not None:
            return self._sandbox_id
        self._sandbox_id = self._fetch_info().sandbox_id
        return self._sandbox_id

    @property
    def name(self) -> str | None:
        """The human-readable name for this sandbox, or None if unnamed."""
        if self._name_loaded:
            return self._name
        self._name = self._fetch_info().name
        self._name_loaded = True
        return self._name

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager.

        If this sandbox was created via ``create_and_connect()``, the
        sandbox is terminated (deleted from the server). Otherwise only
        the client is closed and the sandbox keeps running.
        """
        if self._owns_sandbox:
            self.terminate()
        else:
            self.close()
            self._release_owned_lifecycle_client()

    def close(self):
        """Close the proxy connection. The sandbox keeps running.

        The lifecycle client (if any) is preserved so that subsequent
        ``terminate()`` calls on this handle still work.
        """
        self._rust_client.close()

    def terminate(self):
        """Terminate the sandbox and release all owned resources."""
        self._owns_sandbox = False
        try:
            if self._lifecycle_client is not None:
                self._lifecycle_client.delete(self._identifier)
        finally:
            self.close()
            self._release_owned_lifecycle_client()

    def _release_owned_lifecycle_client(self):
        if self._owns_lifecycle_client and self._lifecycle_client is not None:
            self._lifecycle_client.close()
        self._owns_lifecycle_client = False
        self._lifecycle_client = None

    # --- Lifecycle: suspend / resume / checkpoint ---

    def _require_lifecycle_client(self, operation: str) -> "SandboxClient":
        if self._lifecycle_client is None:
            raise SandboxError(
                f"Cannot {operation}: this Sandbox was constructed without a "
                "lifecycle client. Use Sandbox.create() / Sandbox.connect() "
                "(or SandboxClient.create_and_connect() / SandboxClient.connect())."
            )
        return self._lifecycle_client

    def _wait_for_status(
        self,
        client: "SandboxClient",
        target: SandboxStatus,
        *,
        operation: str,
        timeout: float,
        poll_interval: float,
        initial_info: SandboxInfo | None = None,
    ) -> None:
        deadline = time.time() + timeout
        info = initial_info
        while True:
            if info is None:
                info = client.get(self._identifier)
            if info.status == target:
                self._cached_info = info
                return
            if info.status == SandboxStatus.TERMINATED:
                raise SandboxError(
                    f"Sandbox {self._identifier} became terminated while awaiting {operation}"
                )
            remaining = deadline - time.time()
            if remaining <= 0:
                raise SandboxError(
                    f"Sandbox {self._identifier} did not reach {target.value.capitalize()} within {timeout}s"
                )
            time.sleep(min(poll_interval, remaining))
            info = None

    def suspend(
        self,
        *,
        timeout: float = 300,
        poll_interval: float = 1.0,
        wait: bool = True,
    ) -> None:
        """Suspend this sandbox, blocking until Suspended when ``wait`` is true."""
        client = self._require_lifecycle_client("suspend")
        client.suspend(self._identifier)
        if not wait:
            return
        self._wait_for_status(
            client,
            SandboxStatus.SUSPENDED,
            operation="suspend",
            timeout=timeout,
            poll_interval=poll_interval,
        )

    def resume(
        self,
        *,
        timeout: float = 300,
        poll_interval: float = 1.0,
        wait: bool = True,
    ) -> None:
        """Resume this sandbox, blocking until Running when ``wait`` is true. No-op if already Running."""
        client = self._require_lifecycle_client("resume")

        initial_info: SandboxInfo | None = None
        if wait:
            try:
                initial_info = client.get(self._identifier)
            except (SandboxError, RemoteAPIError, SandboxConnectionError):
                initial_info = None
            if (
                initial_info is not None
                and initial_info.status == SandboxStatus.RUNNING
            ):
                self._cached_info = initial_info
                return

        client.resume(self._identifier)
        if not wait:
            return
        self._wait_for_status(
            client,
            SandboxStatus.RUNNING,
            operation="resume",
            timeout=timeout,
            poll_interval=poll_interval,
        )

    def checkpoint(
        self,
        *,
        content_mode: SnapshotContentMode | None = None,
        timeout: float = 300,
        poll_interval: float = 1.0,
        wait: bool = True,
    ) -> SnapshotInfo | CreateSnapshotResponse:
        """Take a snapshot of this sandbox.

        Returns :class:`SnapshotInfo` when ``wait`` is true, else the immediate
        :class:`CreateSnapshotResponse`.
        """
        client = self._require_lifecycle_client("checkpoint")
        if wait:
            return client.snapshot_and_wait(
                self._identifier,
                timeout=timeout,
                poll_interval=poll_interval,
                content_mode=content_mode,
            )
        return client.snapshot(self._identifier, content_mode=content_mode)

    def list_snapshots(self) -> list[SnapshotInfo]:
        """List snapshots taken from this sandbox."""
        client = self._require_lifecycle_client("list_snapshots")
        sid = self.sandbox_id
        return [s for s in client.list_snapshots() if s.sandbox_id == sid]

    @staticmethod
    def _build_command_payload(
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        **extra: object,
    ) -> dict:
        """Build a process command payload dict with common fields."""
        payload: dict = {"command": command}
        if args is not None:
            payload["args"] = args
        if env is not None:
            payload["env"] = env
        if working_dir is not None:
            payload["working_dir"] = working_dir
        for key, value in extra.items():
            if value is not None:
                payload[key] = value
        return payload

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

        Uses a single streaming ``POST /api/v1/processes/run`` request that
        starts the process, streams output, and delivers the exit code — all
        over one HTTP connection.

        Args:
            command: Command to execute
            args: Command arguments
            env: Environment variables
            working_dir: Working directory
            timeout: Maximum seconds to wait (enforced server-side; None = no limit)

        Returns:
            CommandResult with exit_code, stdout, and stderr
        """
        payload = self._build_command_payload(
            command,
            args,
            env,
            working_dir,
            timeout=timeout,
        )

        try:
            events_json = self._rust_client.run_process_json(json.dumps(payload))
        except Exception as e:
            _raise_as_sandbox_error(e)

        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        exit_code: int | None = None

        for event_json in events_json:
            event = json.loads(event_json)
            if "line" in event:
                if event.get("stream") == "stderr":
                    stderr_lines.append(event["line"])
                else:
                    stdout_lines.append(event["line"])
            elif "exit_code" in event or "signal" in event:
                if event.get("exit_code") is not None:
                    exit_code = event["exit_code"]
                elif event.get("signal") is not None:
                    exit_code = -event["signal"]

        if exit_code is None:
            raise SandboxConnectionError(
                "sandbox process stream ended without an exit event"
            )

        return CommandResult(
            exit_code=exit_code,
            stdout="\n".join(stdout_lines),
            stderr="\n".join(stderr_lines),
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
        payload = self._build_command_payload(
            command,
            args,
            env,
            working_dir,
            stdin_mode=stdin_mode if stdin_mode != StdinMode.CLOSED else None,
            stdout_mode=stdout_mode if stdout_mode != OutputMode.CAPTURE else None,
            stderr_mode=stderr_mode if stderr_mode != OutputMode.CAPTURE else None,
        )

        try:
            response_json = self._rust_client.start_process_json(json.dumps(payload))
            return ProcessInfo.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def list_processes(self) -> list[ProcessInfo]:
        """List all processes in the sandbox."""
        try:
            response_json = self._rust_client.list_processes_json()
            data = ListProcessesResponse.model_validate_json(response_json)
            return data.processes
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_process(self, pid: int) -> ProcessInfo:
        """Get information about a specific process."""
        try:
            response_json = self._rust_client.get_process_json(pid=pid)
            return ProcessInfo.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def kill_process(self, pid: int) -> None:
        """Kill a process."""
        try:
            self._rust_client.kill_process(pid=pid)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def send_signal(self, pid: int, signal: int) -> SendSignalResponse:
        """Send a signal to a process.

        Args:
            pid: Process ID
            signal: Signal number (e.g. 15 for SIGTERM, 9 for SIGKILL)
        """
        try:
            response_json = self._rust_client.send_signal_json(pid=pid, signal=signal)
            return SendSignalResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- Process I/O ---

    def write_stdin(self, pid: int, data: bytes) -> None:
        """Write data to a process's stdin."""
        try:
            self._rust_client.write_stdin(pid=pid, data=data)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def close_stdin(self, pid: int) -> None:
        """Close a process's stdin."""
        try:
            self._rust_client.close_stdin(pid=pid)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_stdout(self, pid: int) -> OutputResponse:
        """Get all stdout output from a process."""
        try:
            response_json = self._rust_client.get_stdout_json(pid=pid)
            return OutputResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_stderr(self, pid: int) -> OutputResponse:
        """Get all stderr output from a process."""
        try:
            response_json = self._rust_client.get_stderr_json(pid=pid)
            return OutputResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_output(self, pid: int) -> OutputResponse:
        """Get all combined output from a process."""
        try:
            response_json = self._rust_client.get_output_json(pid=pid)
            return OutputResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def follow_stdout(self, pid: int) -> Iterator[OutputEvent]:
        """Stream stdout from a process via SSE. Replays existing output then streams live."""
        try:
            events_json = self._rust_client.follow_stdout_json(pid=pid)
            for event_json in events_json:
                yield OutputEvent.model_validate_json(event_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def follow_stderr(self, pid: int) -> Iterator[OutputEvent]:
        """Stream stderr from a process via SSE. Replays existing output then streams live."""
        try:
            events_json = self._rust_client.follow_stderr_json(pid=pid)
            for event_json in events_json:
                yield OutputEvent.model_validate_json(event_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def follow_output(self, pid: int) -> Iterator[OutputEvent]:
        """Stream combined output from a process via SSE. Replays existing output then streams live."""
        try:
            events_json = self._rust_client.follow_output_json(pid=pid)
            for event_json in events_json:
                yield OutputEvent.model_validate_json(event_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- File operations ---

    def read_file(self, path: str) -> bytes:
        """Read a file from the sandbox.

        Args:
            path: Absolute path inside the sandbox

        Returns:
            File contents as bytes
        """
        try:
            return self._rust_client.read_file_bytes(path=path)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def write_file(self, path: str, content: bytes) -> None:
        """Write a file to the sandbox.

        Args:
            path: Absolute path inside the sandbox
            content: File contents as bytes
        """
        try:
            self._rust_client.write_file(path=path, content=content)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def delete_file(self, path: str) -> None:
        """Delete a file from the sandbox.

        Args:
            path: Absolute path inside the sandbox
        """
        try:
            self._rust_client.delete_file(path=path)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def list_directory(self, path: str) -> ListDirectoryResponse:
        """List contents of a directory in the sandbox.

        Args:
            path: Absolute path inside the sandbox

        Returns:
            ListDirectoryResponse with path and entries
        """
        try:
            response_json = self._rust_client.list_directory_json(path=path)
            return ListDirectoryResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- PTY sessions ---

    def create_pty_session(
        self,
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        rows: int = 24,
        cols: int = 80,
    ) -> dict:
        """Create an interactive PTY session.

        Returns a dict with ``session_id`` and ``token`` for WebSocket
        connection via :meth:`pty_ws_url`.
        """
        payload: dict = {"command": command, "rows": rows, "cols": cols}
        if args is not None:
            payload["args"] = args
        if env is not None:
            payload["env"] = env
        if working_dir is not None:
            payload["working_dir"] = working_dir

        try:
            response_json = self._rust_client.create_pty_session_json(
                json.dumps(payload)
            )
            return json.loads(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def pty_ws_url(self, session_id: str, token: str) -> str:
        """Construct the WebSocket URL for a PTY session.

        The token is NOT included in the URL query string to avoid leaking it
        into proxy/CDN access logs. Callers should send the token via the
        ``X-PTY-Token`` header on the WebSocket upgrade request instead.
        """
        base = self._base_url.rstrip("/")
        if base.startswith("https://"):
            ws_base = "wss://" + base[8:]
        elif base.startswith("http://"):
            ws_base = "ws://" + base[7:]
        else:
            ws_base = base
        return f"{ws_base}/api/v1/pty/{session_id}/ws"

    def connect_pty(
        self,
        session_id: str,
        token: str,
        *,
        on_data=None,
        on_exit=None,
        connect_timeout: float = 10.0,
    ):
        """Attach to an existing PTY session and return a high-level handle."""
        from .pty import build_pty_connection

        pty = build_pty_connection(
            session_id=session_id,
            token=token,
            ws_url=self.pty_ws_url(session_id, token),
            http_url=f"{self._base_url.rstrip('/')}/api/v1/pty/{session_id}",
            ws_headers=self._proxy_headers,
            http_headers=self._proxy_headers,
            connect_timeout=connect_timeout,
        )
        if on_data is not None:
            pty.on_data(on_data)
        if on_exit is not None:
            pty.on_exit(on_exit)
        return pty.connect()

    def _delete_pty_session(self, session_id: str, *, timeout: float = 10.0) -> None:
        response = httpx.delete(
            f"{self._base_url.rstrip('/')}/api/v1/pty/{session_id}",
            headers=self._proxy_headers,
            timeout=timeout,
        )
        if response.is_success or response.status_code == 404:
            return
        raise RemoteAPIError(response.status_code, response.text)

    def create_pty(
        self,
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        rows: int = 24,
        cols: int = 80,
        *,
        on_data=None,
        on_exit=None,
        connect_timeout: float = 10.0,
    ):
        """Create a PTY session, connect immediately, and return its handle."""
        session = self.create_pty_session(
            command=command,
            args=args,
            env=env,
            working_dir=working_dir,
            rows=rows,
            cols=cols,
        )
        try:
            return self.connect_pty(
                session["session_id"],
                session["token"],
                on_data=on_data,
                on_exit=on_exit,
                connect_timeout=connect_timeout,
            )
        except Exception:
            try:
                self._delete_pty_session(session["session_id"], timeout=connect_timeout)
            except Exception:
                pass
            raise

    def connect_desktop(
        self,
        port: int = 5901,
        password: str | None = None,
        shared: bool = True,
        connect_timeout: float = 10.0,
    ):
        """Connect to a sandbox VNC session for programmatic desktop control."""
        from .desktop import Desktop

        if RustCloudSandboxDesktopClient is None:
            raise SandboxError(
                "Rust Cloud SDK desktop client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        try:
            rust_client = RustCloudSandboxDesktopClient(
                proxy_url=self._proxy_url,
                sandbox_id=self._identifier,
                port=port,
                password=password,
                shared=shared,
                connect_timeout_sec=connect_timeout,
                api_key=self._api_key,
                organization_id=self._organization_id,
                project_id=self._project_id,
            )
            return Desktop(rust_client)
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- Health and info ---

    def health(self) -> HealthResponse:
        """Check the container daemon health."""
        try:
            response_json = self._rust_client.health_json()
            return HealthResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def info(self) -> DaemonInfo:
        """Get container daemon info (version, uptime, process counts)."""
        try:
            response_json = self._rust_client.info_json()
            return DaemonInfo.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)
