"""Async client for interacting with a running sandbox.

Mirrors :class:`Sandbox` but exposes coroutine methods that await the Rust
``*_async`` bindings directly, so each call yields control to the active
asyncio event loop instead of blocking a worker thread.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx

from tensorlake._tracing import USER_AGENT, Traced, TracedIterator, inject_traceparent

from . import _defaults
from .exceptions import RemoteAPIError, SandboxConnectionError, SandboxError
from .models import (
    CheckpointType,
    CommandResult,
    CopySandboxResponse,
    DaemonInfo,
    HealthResponse,
    ListDirectoryResponse,
    ListProcessesResponse,
    OutputEvent,
    OutputMode,
    OutputResponse,
    ProcessHealthCheck,
    ProcessInfo,
    ProcessUser,
    RestartPolicyConfig,
    SandboxInfo,
    SandboxStatus,
    SendSignalResponse,
    SnapshotInfo,
    SnapshotType,
    SnapshotWaitCondition,
    StdinMode,
)
from .sandbox import (
    _RUST_SANDBOX_PROXY_CLIENT_AVAILABLE,
    RustCloudSandboxProxyClient,
    Sandbox,
    _raise_as_sandbox_error,
)

if TYPE_CHECKING:
    from .async_client import AsyncSandboxClient


class AsyncSandbox:
    """Async client for interacting with a running sandbox.

    Use :class:`AsyncSandboxClient` to create or connect, then call methods
    on the returned :class:`AsyncSandbox` to drive the sandbox without
    blocking the event loop.
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
        request_timeout: float | None = None,
        _proxy_rust_client: object | None = None,
    ) -> None:
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
        self._trace_id: str | None = None
        self._cached_info: SandboxInfo | None = None
        self._owns_sandbox: bool = False
        self._lifecycle_client: "AsyncSandboxClient | None" = None
        self._proxy_url = proxy_url
        self._api_key = api_key
        self._organization_id = organization_id
        self._project_id = project_id
        self._request_timeout = request_timeout
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
        else:
            self._proxy_headers["X-Tensorlake-Sandbox-Id"] = sandbox_identifier

        if _proxy_rust_client is not None:
            self._rust_client = _proxy_rust_client
            self._base_url = self._rust_client.base_url()
        elif not _RUST_SANDBOX_PROXY_CLIENT_AVAILABLE:
            raise SandboxError(
                "Rust Cloud SDK sandbox proxy client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )
        else:
            try:
                kwargs = {
                    "proxy_url": proxy_url,
                    "sandbox_id": sandbox_identifier,
                    "api_key": api_key,
                    "organization_id": organization_id,
                    "project_id": project_id,
                    "routing_hint": routing_hint,
                    "user_agent": USER_AGENT,
                }
                if request_timeout is not None:
                    kwargs["request_timeout_sec"] = request_timeout
                self._rust_client = RustCloudSandboxProxyClient(**kwargs)
                self._base_url = self._rust_client.base_url()
            except Exception as e:
                _raise_as_sandbox_error(e)

    # --- Class-level factory methods ---

    @classmethod
    async def create(
        cls,
        image: str | None = None,
        cpus: float = 1.0,
        memory_mb: int = 1024,
        disk_mb: int | None = None,
        timeout_secs: int | None = None,
        entrypoint: list[str] | None = None,
        allow_internet_access: bool = True,
        allow_out: list[str] | None = None,
        deny_out: list[str] | None = None,
        pool_id: str | None = None,
        snapshot_id: str | None = None,
        proxy_url: str | None = None,
        request_timeout: float | None = None,
        startup_timeout: float | None = None,
        name: str | None = None,
        cloud_init: str | os.PathLike[str] | None = None,
        api_key: str | None = _defaults.API_KEY,
        api_url: str = _defaults.API_URL,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
    ) -> "AsyncSandbox":
        from .async_client import AsyncSandboxClient

        effective_request_timeout = (
            startup_timeout
            if startup_timeout is not None
            else (
                request_timeout
                if request_timeout is not None
                else _defaults.DEFAULT_HTTP_TIMEOUT_SEC
            )
        )
        client = AsyncSandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            request_timeout=effective_request_timeout,
            _internal=True,
        )
        return await client.create_and_connect(
            image=image,
            cpus=cpus,
            memory_mb=memory_mb,
            disk_mb=disk_mb,
            timeout_secs=timeout_secs,
            entrypoint=entrypoint,
            allow_internet_access=allow_internet_access,
            allow_out=allow_out,
            deny_out=deny_out,
            pool_id=pool_id,
            snapshot_id=snapshot_id,
            proxy_url=proxy_url,
            request_timeout=effective_request_timeout,
            name=name,
            cloud_init=cloud_init,
        )

    @classmethod
    async def connect(
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
        request_timeout: float | None = None,
    ) -> "AsyncSandbox":
        from .async_client import AsyncSandboxClient

        client = AsyncSandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            request_timeout=(
                request_timeout
                if request_timeout is not None
                else _defaults.DEFAULT_HTTP_TIMEOUT_SEC
            ),
            _internal=True,
        )
        return await client.connect(
            sandbox_id,
            proxy_url=proxy_url,
            routing_hint=routing_hint,
            request_timeout=request_timeout,
        )

    # --- Lifecycle ---

    def _require_lifecycle_client(self, operation: str) -> None:
        if self._lifecycle_client is None:
            raise SandboxError(
                f"Cannot {operation}: no lifecycle client available. "
                "Use AsyncSandbox.create() or AsyncSandbox.connect() to get a "
                "lifecycle-aware handle."
            )

    async def suspend(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> None:
        self._require_lifecycle_client("suspend")
        await self._lifecycle_client.suspend(
            self._lifecycle_identifier(),
            wait=wait,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    async def resume(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> None:
        self._require_lifecycle_client("resume")
        await self._lifecycle_client.resume(
            self._lifecycle_identifier(),
            wait=wait,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    async def copy(
        self,
        *,
        times: int = 1,
        request_timeout: float | None = None,
    ) -> Traced[CopySandboxResponse]:
        self._require_lifecycle_client("copy")
        return await self._lifecycle_client.copy(
            self._lifecycle_identifier(),
            times=times,
            request_timeout=request_timeout,
        )

    async def checkpoint(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
        checkpoint_type: CheckpointType | None = None,
        wait_until: SnapshotWaitCondition | str = SnapshotWaitCondition.LOCAL_READY,
    ) -> SnapshotInfo | None:
        self._require_lifecycle_client("checkpoint")
        snapshot_type = (
            SnapshotType(checkpoint_type.value) if checkpoint_type is not None else None
        )
        if not wait:
            await self._lifecycle_client.snapshot(
                self._lifecycle_identifier(), snapshot_type=snapshot_type
            )
            return None
        traced = await self._lifecycle_client.snapshot_and_wait(
            self._lifecycle_identifier(),
            timeout=timeout,
            poll_interval=poll_interval,
            snapshot_type=snapshot_type,
            wait_until=wait_until,
        )
        return traced.value

    async def list_snapshots(self) -> TracedIterator[SnapshotInfo]:
        self._require_lifecycle_client("list_snapshots")
        my_id = self._sandbox_id or (await self._fetch_info()).sandbox_id
        all_snaps = await self._lifecycle_client.list_snapshots()
        filtered = [s for s in all_snaps if s.sandbox_id == my_id]
        return TracedIterator(all_snaps.trace_id, filtered)

    async def _fetch_info(self) -> SandboxInfo:
        if self._cached_info is None:
            if self._lifecycle_client is None:
                raise SandboxError(
                    "Cannot resolve sandbox info: no lifecycle client available."
                )
            lookup_id = self._sandbox_id or self._identifier
            self._cached_info = (await self._lifecycle_client.get(lookup_id)).value
        return self._cached_info

    async def info(self) -> SandboxInfo:
        return await self._fetch_info()

    def _lifecycle_identifier(self) -> str:
        if self._sandbox_id is not None:
            return self._sandbox_id
        if self._cached_info is not None:
            self._sandbox_id = self._cached_info.sandbox_id
            return self._sandbox_id
        return self._identifier

    @property
    def sandbox_id(self) -> str:
        if self._sandbox_id is not None:
            return self._sandbox_id
        if self._cached_info is None:
            raise SandboxError(
                "sandbox_id is not yet known; call `await sandbox.info()` first."
            )
        self._sandbox_id = self._cached_info.sandbox_id
        return self._sandbox_id

    @property
    def trace_id(self) -> str | None:
        return self._trace_id

    async def status(self) -> SandboxStatus:
        self._require_lifecycle_client("read_status")
        info = (await self._lifecycle_client.get(self._lifecycle_identifier())).value
        self._sandbox_id = info.sandbox_id
        self._cached_info = info
        return info.status

    async def update(
        self,
        name: str | None = None,
        *,
        allow_unauthenticated_access: bool | None = None,
        exposed_ports: list[int] | None = None,
    ) -> Traced[SandboxInfo]:
        self._require_lifecycle_client("update")
        traced = await self._lifecycle_client.update_sandbox(
            self._lifecycle_identifier(),
            name=name,
            allow_unauthenticated_access=allow_unauthenticated_access,
            exposed_ports=exposed_ports,
        )
        self._sandbox_id = traced.sandbox_id
        self._cached_info = traced.value
        return traced

    async def __aenter__(self) -> "AsyncSandbox":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._owns_sandbox:
            await self.terminate()
        else:
            self.close()

    def close(self) -> None:
        self._rust_client.close()

    async def terminate(self) -> None:
        lifecycle_client = self._lifecycle_client
        delete_identifier = self._sandbox_id or self._identifier
        self._owns_sandbox = False
        self._lifecycle_client = None
        self.close()
        if lifecycle_client is not None:
            await lifecycle_client.delete(delete_identifier)

    # --- High-level convenience ---

    async def run(
        self,
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        timeout: float | None = None,
        user: ProcessUser = "tl-user",
    ) -> Traced[CommandResult]:
        process_user = Sandbox._normalize_process_user(user)
        payload = Sandbox._build_command_payload(
            command,
            args,
            env,
            working_dir,
            timeout=timeout,
            user=process_user,
        )
        try:
            trace_id, events_json = await self._rust_client.run_process_json_async(
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

    async def start_process(
        self,
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        stdin_mode: StdinMode = StdinMode.CLOSED,
        stdout_mode: OutputMode = OutputMode.CAPTURE,
        stderr_mode: OutputMode = OutputMode.CAPTURE,
        user: ProcessUser = "tl-user",
        name: str | None = None,
        restart: RestartPolicyConfig | Mapping[str, object] | None = None,
        health_check: ProcessHealthCheck | Mapping[str, object] | None = None,
    ) -> Traced[ProcessInfo]:
        process_user = Sandbox._normalize_process_user(user)
        restart_payload = Sandbox._normalize_restart_config(restart)
        health_check_payload = Sandbox._normalize_health_check(health_check)
        payload = Sandbox._build_command_payload(
            command,
            args,
            env,
            working_dir,
            stdin_mode=stdin_mode if stdin_mode != StdinMode.CLOSED else None,
            stdout_mode=stdout_mode if stdout_mode != OutputMode.CAPTURE else None,
            stderr_mode=stderr_mode if stderr_mode != OutputMode.CAPTURE else None,
            user=process_user,
            name=name,
            restart=restart_payload,
            health_check=health_check_payload,
        )
        try:
            trace_id, response_json = await self._rust_client.start_process_json_async(
                json.dumps(payload)
            )
            return Traced(trace_id, ProcessInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def list_processes(self) -> TracedIterator[ProcessInfo]:
        try:
            trace_id, response_json = (
                await self._rust_client.list_processes_json_async()
            )
            data = ListProcessesResponse.model_validate_json(response_json)
            return TracedIterator(trace_id, data.processes)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def get_process(self, pid: int) -> Traced[ProcessInfo]:
        try:
            trace_id, response_json = await self._rust_client.get_process_json_async(
                pid=pid
            )
            return Traced(trace_id, ProcessInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def kill_process(self, pid: int) -> Traced[None]:
        try:
            trace_id = await self._rust_client.kill_process_async(pid=pid)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def restart_process(self, pid: int) -> Traced[ProcessInfo]:
        try:
            trace_id, response_json = (
                await self._rust_client.restart_process_json_async(pid=pid)
            )
            return Traced(trace_id, ProcessInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def send_signal(self, pid: int, signal: int) -> Traced[SendSignalResponse]:
        try:
            trace_id, response_json = await self._rust_client.send_signal_json_async(
                pid=pid, signal=signal
            )
            return Traced(
                trace_id, SendSignalResponse.model_validate_json(response_json)
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- Process I/O ---

    async def write_stdin(self, pid: int, data: bytes) -> Traced[None]:
        try:
            trace_id = await self._rust_client.write_stdin_async(pid=pid, data=data)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def close_stdin(self, pid: int) -> Traced[None]:
        try:
            trace_id = await self._rust_client.close_stdin_async(pid=pid)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def get_stdout(self, pid: int) -> Traced[OutputResponse]:
        try:
            trace_id, response_json = await self._rust_client.get_stdout_json_async(
                pid=pid
            )
            return Traced(trace_id, OutputResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def get_stderr(self, pid: int) -> Traced[OutputResponse]:
        try:
            trace_id, response_json = await self._rust_client.get_stderr_json_async(
                pid=pid
            )
            return Traced(trace_id, OutputResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def get_output(self, pid: int) -> Traced[OutputResponse]:
        try:
            trace_id, response_json = await self._rust_client.get_output_json_async(
                pid=pid
            )
            return Traced(trace_id, OutputResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def follow_stdout(self, pid: int) -> TracedIterator[OutputEvent]:
        try:
            trace_id, events_json = await self._rust_client.follow_stdout_json_async(
                pid=pid
            )
            return TracedIterator(
                trace_id, [OutputEvent.model_validate_json(ej) for ej in events_json]
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def follow_stderr(self, pid: int) -> TracedIterator[OutputEvent]:
        try:
            trace_id, events_json = await self._rust_client.follow_stderr_json_async(
                pid=pid
            )
            return TracedIterator(
                trace_id, [OutputEvent.model_validate_json(ej) for ej in events_json]
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def follow_output(self, pid: int) -> TracedIterator[OutputEvent]:
        try:
            trace_id, events_json = await self._rust_client.follow_output_json_async(
                pid=pid
            )
            return TracedIterator(
                trace_id, [OutputEvent.model_validate_json(ej) for ej in events_json]
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- File operations ---

    async def read_file(self, path: str) -> Traced[bytes]:
        try:
            trace_id, data = await self._rust_client.read_file_bytes_async(path=path)
            return Traced(trace_id, data)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def write_file(self, path: str, content: bytes) -> Traced[None]:
        try:
            trace_id = await self._rust_client.write_file_async(
                path=path, content=content
            )
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def upload_file(
        self, local_path: str | os.PathLike[str], path: str
    ) -> Traced[None]:
        try:
            trace_id = await self._rust_client.upload_file_async(
                path=path, local_path=os.fspath(local_path)
            )
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def delete_file(self, path: str) -> Traced[None]:
        try:
            trace_id = await self._rust_client.delete_file_async(path=path)
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def list_directory(self, path: str) -> Traced[ListDirectoryResponse]:
        try:
            trace_id, response_json = await self._rust_client.list_directory_json_async(
                path=path
            )
            return Traced(
                trace_id, ListDirectoryResponse.model_validate_json(response_json)
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    # --- PTY sessions ---

    async def create_pty_session(
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
            trace_id, response_json = (
                await self._rust_client.create_pty_session_json_async(
                    json.dumps(payload)
                )
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

    async def connect_pty(
        self,
        session_id: str,
        token: str,
        *,
        on_data=None,
        on_exit=None,
        connect_timeout: float = 10.0,
    ):
        """Attach to an existing PTY session and return a connected async handle."""
        from .pty import build_async_pty_connection

        pty = build_async_pty_connection(
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
        return await pty.connect()

    async def _delete_pty_session(
        self, session_id: str, *, timeout: float = 10.0
    ) -> None:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.delete(
                f"{self._base_url.rstrip('/')}/api/v1/pty/{session_id}",
                headers=inject_traceparent(self._proxy_headers),
            )
        if response.is_success or response.status_code == 404:
            return
        raise RemoteAPIError(response.status_code, response.text)

    async def create_pty(
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
        traced_session = await self.create_pty_session(
            command=command,
            args=args,
            env=env,
            working_dir=working_dir,
            rows=rows,
            cols=cols,
        )
        session = traced_session.value
        try:
            return await self.connect_pty(
                session["session_id"],
                session["token"],
                on_data=on_data,
                on_exit=on_exit,
                connect_timeout=connect_timeout,
            )
        except Exception:
            try:
                await self._delete_pty_session(
                    session["session_id"], timeout=connect_timeout
                )
            except Exception:
                pass
            raise

    # --- Health and info ---

    async def health(self) -> Traced[HealthResponse]:
        try:
            trace_id, response_json = await self._rust_client.health_json_async()
            return Traced(trace_id, HealthResponse.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def daemon_info(self) -> Traced[DaemonInfo]:
        try:
            trace_id, response_json = await self._rust_client.info_json_async()
            return Traced(trace_id, DaemonInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)
