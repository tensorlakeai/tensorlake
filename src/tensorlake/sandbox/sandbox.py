"""Client for interacting with a running sandbox."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx

from tensorlake._tracing import USER_AGENT, Traced, TracedIterator, inject_traceparent

from . import _defaults
from .exceptions import (
    RemoteAPIError,
    SandboxConnectionError,
    SandboxError,
)
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
    SandboxInfo,
    SendSignalResponse,
    SnapshotContentMode,
    SnapshotInfo,
    StdinMode,
)

# Avoid circular import: sandbox.py ↔ client.py.  With ``from __future__
# import annotations`` the type string is never evaluated at runtime, but
# the import itself would still execute and trigger a cycle.
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

        if not _RUST_SANDBOX_PROXY_CLIENT_AVAILABLE:
            raise SandboxError(
                "Rust Cloud SDK sandbox proxy client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        if _proxy_rust_client is not None:
            self._rust_client = _proxy_rust_client
            self._base_url = self._rust_client.base_url()
        else:
            try:
                self._rust_client = RustCloudSandboxProxyClient(
                    proxy_url=proxy_url,
                    sandbox_id=sandbox_identifier,
                    api_key=api_key,
                    organization_id=organization_id,
                    project_id=project_id,
                    routing_hint=routing_hint,
                    user_agent=USER_AGENT,
                )
                self._base_url = self._rust_client.base_url()
            except Exception as e:
                _raise_as_sandbox_error(e)

    # --- Class-level factory methods ---

    @classmethod
    def create(
        cls,
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
        proxy_url: str | None = None,
        startup_timeout: float = 60,
        name: str | None = None,
        api_key: str | None = _defaults.API_KEY,
        api_url: str = _defaults.API_URL,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
    ) -> "Sandbox":
        """Create a new sandbox and return a connected, running handle.

        Covers both fresh sandbox creation and restore-from-snapshot (set
        ``snapshot_id``). Blocks until the sandbox is ``Running``.

        Args:
            image: Sandbox image name. When omitted, Tensorlake uses the
                default managed environment.
            cpus: Number of CPUs to allocate.
            memory_mb: Memory in megabytes.
            ephemeral_disk_mb: Ephemeral disk space in megabytes.
            secret_names: List of secret names to inject.
            timeout_secs: Sandbox timeout in seconds.
            entrypoint: Custom entrypoint command.
            allow_internet_access: If True (default), outbound traffic is allowed.
            allow_out: Destination IPs/CIDRs to allow.
            deny_out: Destination IPs/CIDRs to deny.
            pool_id: Pool ID to claim a warm container from.
            snapshot_id: Restore from this snapshot ID.
            proxy_url: Override the sandbox proxy URL.
            startup_timeout: Max seconds to wait for Running status (default 60).
            name: Optional name; named sandboxes support suspend/resume.
            api_key: Tensorlake API key (defaults to TENSORLAKE_API_KEY env var).
            api_url: API server URL (defaults to TENSORLAKE_API_URL env var).
            organization_id: Organization ID for multi-tenant access.
            project_id: Project ID for scoping resources.
            namespace: Namespace for local-server deployments.

        Returns:
            Connected Sandbox handle (auto-terminates in context manager).

        Raises:
            SandboxError: If sandbox fails to start or times out.
        """
        from .client import SandboxClient

        client = SandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            _internal=True,
        )
        return client.create_and_connect(
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

    @classmethod
    def connect(
        cls,
        sandbox_id: str,
        *,
        proxy_url: str | None = None,
        routing_hint: str | None = None,
        api_key: str | None = _defaults.API_KEY,
        api_url: str = _defaults.API_URL,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
    ) -> "Sandbox":
        """Attach to an existing sandbox and return a connected handle.

        Verifies the sandbox exists via a server GET call, then returns a
        handle in whatever state the sandbox is in. Does **not** auto-resume
        a suspended sandbox — call ``sandbox.resume()`` explicitly.

        Args:
            sandbox_id: ID or name of the sandbox to attach to.
            proxy_url: Override the sandbox proxy URL.
            routing_hint: Optional routing hint for the proxy.
            api_key: Tensorlake API key (defaults to TENSORLAKE_API_KEY env var).
            api_url: API server URL (defaults to TENSORLAKE_API_URL env var).
            organization_id: Organization ID for multi-tenant access.
            project_id: Project ID for scoping resources.
            namespace: Namespace for local-server deployments.

        Returns:
            Connected Sandbox handle (does not auto-terminate on context exit).

        Raises:
            SandboxNotFoundError: If the sandbox does not exist.
        """
        from .client import SandboxClient

        client = SandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            _internal=True,
        )
        client.get(sandbox_id)  # raises SandboxNotFoundError if not found
        return client.connect(
            sandbox_id, proxy_url=proxy_url, routing_hint=routing_hint
        )

    # --- Class-level snapshot management ---

    @classmethod
    def get_snapshot(
        cls,
        snapshot_id: str,
        api_key: str | None = _defaults.API_KEY,
        api_url: str = _defaults.API_URL,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
    ) -> SnapshotInfo:
        """Get information about a snapshot by ID.

        No sandbox handle is needed — snapshots are looked up directly.

        Args:
            snapshot_id: ID of the snapshot.

        Returns:
            SnapshotInfo with full snapshot details.
        """
        from .client import SandboxClient

        client = SandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            _internal=True,
        )
        return client.get_snapshot(snapshot_id).value

    @classmethod
    def delete_snapshot(
        cls,
        snapshot_id: str,
        api_key: str | None = _defaults.API_KEY,
        api_url: str = _defaults.API_URL,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
    ) -> None:
        """Delete a snapshot by ID.

        No sandbox handle is needed — snapshots are deleted directly.

        Args:
            snapshot_id: ID of the snapshot to delete.
        """
        from .client import SandboxClient

        client = SandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            _internal=True,
        )
        client.delete_snapshot(snapshot_id)

    # --- Instance lifecycle methods ---

    def _require_lifecycle_client(self, operation: str) -> None:
        if self._lifecycle_client is None:
            raise SandboxError(
                f"Cannot {operation}: no lifecycle client available. "
                "Use Sandbox.create() or Sandbox.connect() to get a lifecycle-aware handle."
            )

    def suspend(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> None:
        """Suspend this sandbox.

        By default blocks until the sandbox is fully ``Suspended``.
        Pass ``wait=False`` for fire-and-return (the server queues the
        suspend but this method returns immediately).

        Args:
            wait: If True (default), poll until Suspended.
            timeout: Max seconds to wait when wait=True (default 300).
            poll_interval: Seconds between polls when wait=True (default 1.0).

        Raises:
            SandboxError: If the sandbox does not suspend within timeout,
                or terminates unexpectedly.
        """
        self._require_lifecycle_client("suspend")
        self._lifecycle_client.suspend(
            self.sandbox_id, wait=wait, timeout=timeout, poll_interval=poll_interval
        )

    def resume(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> None:
        """Resume this sandbox.

        By default blocks until the sandbox is ``Running`` and routable.
        Pass ``wait=False`` for fire-and-return. No-op (no error) if the
        sandbox is already Running.

        Args:
            wait: If True (default), poll until Running.
            timeout: Max seconds to wait when wait=True (default 300).
            poll_interval: Seconds between polls when wait=True (default 1.0).

        Raises:
            SandboxError: If the sandbox does not resume within timeout,
                or terminates unexpectedly.
        """
        self._require_lifecycle_client("resume")
        self._lifecycle_client.resume(
            self.sandbox_id, wait=wait, timeout=timeout, poll_interval=poll_interval
        )

    def checkpoint(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
        content_mode: SnapshotContentMode | None = None,
    ) -> SnapshotInfo | None:
        """Create a snapshot of this sandbox's filesystem.

        By default blocks until the snapshot artifact is committed and
        returns the completed ``SnapshotInfo``. Pass ``wait=False`` to
        fire-and-return (returns ``None``).

        Args:
            wait: If True (default), poll until the snapshot is committed.
            timeout: Max seconds to wait when wait=True (default 300).
            poll_interval: Seconds between polls when wait=True (default 1.0).
            content_mode: Optional content mode for the snapshot.

        Returns:
            Completed SnapshotInfo when wait=True; None when wait=False.

        Raises:
            SandboxError: If the snapshot fails or times out.
        """
        self._require_lifecycle_client("checkpoint")
        if not wait:
            self._lifecycle_client.snapshot(self.sandbox_id, content_mode=content_mode)
            return None
        return self._lifecycle_client.snapshot_and_wait(
            self.sandbox_id,
            timeout=timeout,
            poll_interval=poll_interval,
            content_mode=content_mode,
        ).value

    def list_snapshots(self) -> TracedIterator[SnapshotInfo]:
        """List snapshots taken from this sandbox.

        Returns:
            TracedIterator[SnapshotInfo] — iterable over this sandbox's snapshots.
        """
        self._require_lifecycle_client("list_snapshots")
        all_snaps = self._lifecycle_client.list_snapshots()
        my_id = self.sandbox_id
        filtered = [s for s in all_snaps if s.sandbox_id == my_id]
        return TracedIterator(all_snaps.trace_id, filtered)

    def _fetch_info(self) -> SandboxInfo:
        """Fetch and cache sandbox info from the server (lazy, once per instance)."""
        if self._cached_info is None:
            if self._lifecycle_client is None:
                raise SandboxError(
                    "Cannot resolve sandbox info: no lifecycle client available. "
                    "Connect via SandboxClient.connect() to enable sandbox_id and name lookup."
                )
            self._cached_info = self._lifecycle_client.get(self._identifier).value
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

    def close(self):
        """Close the client connection. The sandbox keeps running."""
        self._rust_client.close()

    def terminate(self):
        """Terminate the sandbox and close the connection."""
        lifecycle_client = self._lifecycle_client
        self._owns_sandbox = False
        self._lifecycle_client = None
        self.close()
        if lifecycle_client is not None:
            lifecycle_client.delete(self._identifier)

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
    ) -> Traced[CommandResult]:
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
            Traced[CommandResult] — access ``.trace_id`` for the W3C trace ID
            and ``.exit_code`` / ``.stdout`` / ``.stderr`` directly (or via
            ``.value``).
        """
        payload = self._build_command_payload(
            command,
            args,
            env,
            working_dir,
            timeout=timeout,
        )

        try:
            trace_id, events_json = self._rust_client.run_process_json(
                json.dumps(payload)
            )
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

        return Traced(
            trace_id,
            CommandResult(
                exit_code=exit_code,
                stdout="\n".join(stdout_lines),
                stderr="\n".join(stderr_lines),
            ),
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
    ) -> Traced[ProcessInfo]:
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
            Traced[ProcessInfo] — access ``.trace_id`` for the W3C trace ID
            and ``.pid`` / ``.status`` directly (or via ``.value``).
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
            trace_id, response_json = self._rust_client.start_process_json(
                json.dumps(payload)
            )
            return Traced(trace_id, ProcessInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    def list_processes(self) -> Traced[list[ProcessInfo]]:
        """List all processes in the sandbox."""
        try:
            trace_id, response_json = self._rust_client.list_processes_json()
            data = ListProcessesResponse.model_validate_json(response_json)
            return Traced(trace_id, data.processes)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_process(self, pid: int) -> Traced[ProcessInfo]:
        """Get information about a specific process."""
        try:
            trace_id, response_json = self._rust_client.get_process_json(pid=pid)
            return Traced(trace_id, ProcessInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    def kill_process(self, pid: int) -> Traced[None]:
        """Kill a process."""
        try:
            trace_id = self._rust_client.kill_process(pid=pid)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def send_signal(self, pid: int, signal: int) -> Traced[SendSignalResponse]:
        """Send a signal to a process.

        Args:
            pid: Process ID
            signal: Signal number (e.g. 15 for SIGTERM, 9 for SIGKILL)
        """
        try:
            trace_id, response_json = self._rust_client.send_signal_json(
                pid=pid, signal=signal
            )
            return Traced(
                trace_id, SendSignalResponse.model_validate_json(response_json)
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- Process I/O ---

    def write_stdin(self, pid: int, data: bytes) -> Traced[None]:
        """Write data to a process's stdin."""
        try:
            trace_id = self._rust_client.write_stdin(pid=pid, data=data)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def close_stdin(self, pid: int) -> Traced[None]:
        """Close a process's stdin."""
        try:
            trace_id = self._rust_client.close_stdin(pid=pid)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_stdout(self, pid: int) -> Traced[OutputResponse]:
        """Get all stdout output from a process."""
        try:
            trace_id, response_json = self._rust_client.get_stdout_json(pid=pid)
            return Traced(trace_id, OutputResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_stderr(self, pid: int) -> Traced[OutputResponse]:
        """Get all stderr output from a process."""
        try:
            trace_id, response_json = self._rust_client.get_stderr_json(pid=pid)
            return Traced(trace_id, OutputResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_output(self, pid: int) -> Traced[OutputResponse]:
        """Get all combined output from a process."""
        try:
            trace_id, response_json = self._rust_client.get_output_json(pid=pid)
            return Traced(trace_id, OutputResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    def follow_stdout(self, pid: int) -> TracedIterator[OutputEvent]:
        """Collect all stdout output events from a process and return them as an iterable.

        Blocks until the process exits and all output has been received."""
        try:
            trace_id, events_json = self._rust_client.follow_stdout_json(pid=pid)
            return TracedIterator(
                trace_id,
                [OutputEvent.model_validate_json(ej) for ej in events_json],
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    def follow_stderr(self, pid: int) -> TracedIterator[OutputEvent]:
        """Collect all stderr output events from a process and return them as an iterable.

        Blocks until the process exits and all output has been received."""
        try:
            trace_id, events_json = self._rust_client.follow_stderr_json(pid=pid)
            return TracedIterator(
                trace_id,
                [OutputEvent.model_validate_json(ej) for ej in events_json],
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    def follow_output(self, pid: int) -> TracedIterator[OutputEvent]:
        """Collect all combined output events from a process and return them as an iterable.

        Blocks until the process exits and all output has been received."""
        try:
            trace_id, events_json = self._rust_client.follow_output_json(pid=pid)
            return TracedIterator(
                trace_id,
                [OutputEvent.model_validate_json(ej) for ej in events_json],
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- File operations ---

    def read_file(self, path: str) -> Traced[bytes]:
        """Read a file from the sandbox.

        Args:
            path: Absolute path inside the sandbox

        Returns:
            Traced[bytes] — access ``.trace_id`` for the W3C trace ID and
            ``.value`` for the raw file bytes.
        """
        try:
            trace_id, data = self._rust_client.read_file_bytes(path=path)
            return Traced(trace_id, data)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def write_file(self, path: str, content: bytes) -> Traced[None]:
        """Write a file to the sandbox.

        Args:
            path: Absolute path inside the sandbox
            content: File contents as bytes
        """
        try:
            trace_id = self._rust_client.write_file(path=path, content=content)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def delete_file(self, path: str) -> Traced[None]:
        """Delete a file from the sandbox.

        Args:
            path: Absolute path inside the sandbox
        """
        try:
            trace_id = self._rust_client.delete_file(path=path)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def list_directory(self, path: str) -> Traced[ListDirectoryResponse]:
        """List contents of a directory in the sandbox.

        Args:
            path: Absolute path inside the sandbox

        Returns:
            Traced[ListDirectoryResponse] — access ``.trace_id`` for the W3C
            trace ID and ``.path`` / ``.entries`` directly (or via ``.value``).
        """
        try:
            trace_id, response_json = self._rust_client.list_directory_json(path=path)
            return Traced(
                trace_id, ListDirectoryResponse.model_validate_json(response_json)
            )
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
    ) -> Traced[dict]:
        """Create an interactive PTY session.

        Returns a Traced dict with ``session_id`` and ``token`` for WebSocket
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
            trace_id, response_json = self._rust_client.create_pty_session_json(
                json.dumps(payload)
            )
            return Traced(trace_id, json.loads(response_json))
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
            headers=inject_traceparent(self._proxy_headers),
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
        traced_session = self.create_pty_session(
            command=command,
            args=args,
            env=env,
            working_dir=working_dir,
            rows=rows,
            cols=cols,
        )
        session = traced_session.value
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
                user_agent=USER_AGENT,
            )
            return Desktop(rust_client)
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- Health and info ---

    def health(self) -> Traced[HealthResponse]:
        """Check the container daemon health."""
        try:
            trace_id, response_json = self._rust_client.health_json()
            return Traced(trace_id, HealthResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    def info(self) -> Traced[DaemonInfo]:
        """Get container daemon info (version, uptime, process counts)."""
        try:
            trace_id, response_json = self._rust_client.info_json()
            return Traced(trace_id, DaemonInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)
