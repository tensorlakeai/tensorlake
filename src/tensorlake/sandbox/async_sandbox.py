"""Async client for interacting with a running sandbox.

Mirrors :class:`Sandbox` but exposes coroutine methods that await the Rust
``*_async`` bindings directly, so each call yields control to the active
asyncio event loop instead of blocking a worker thread.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from tensorlake._tracing import USER_AGENT, Traced, TracedIterator

from . import _defaults
from .exceptions import SandboxConnectionError, SandboxError
from .models import (
    CheckpointType,
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
    SandboxStatus,
    SendSignalResponse,
    SnapshotInfo,
    SnapshotType,
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
    async def create(
        cls,
        image: str | None = None,
        cpus: float = 1.0,
        memory_mb: int = 1024,
        disk_mb: int | None = None,
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
    ) -> "AsyncSandbox":
        from .async_client import AsyncSandboxClient

        client = AsyncSandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            _internal=True,
        )
        return await client.create_and_connect(
            image=image,
            cpus=cpus,
            memory_mb=memory_mb,
            disk_mb=disk_mb,
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
    ) -> "AsyncSandbox":
        from .async_client import AsyncSandboxClient

        client = AsyncSandboxClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
            _internal=True,
        )
        return await client.connect(
            sandbox_id, proxy_url=proxy_url, routing_hint=routing_hint
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
            self.sandbox_id, wait=wait, timeout=timeout, poll_interval=poll_interval
        )

    async def resume(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> None:
        self._require_lifecycle_client("resume")
        await self._lifecycle_client.resume(
            self.sandbox_id, wait=wait, timeout=timeout, poll_interval=poll_interval
        )

    async def checkpoint(
        self,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
        checkpoint_type: CheckpointType | None = None,
    ) -> SnapshotInfo | None:
        self._require_lifecycle_client("checkpoint")
        snapshot_type = (
            SnapshotType(checkpoint_type.value) if checkpoint_type is not None else None
        )
        if not wait:
            await self._lifecycle_client.snapshot(
                self.sandbox_id, snapshot_type=snapshot_type
            )
            return None
        traced = await self._lifecycle_client.snapshot_and_wait(
            self.sandbox_id,
            timeout=timeout,
            poll_interval=poll_interval,
            snapshot_type=snapshot_type,
        )
        return traced.value

    async def list_snapshots(self) -> TracedIterator[SnapshotInfo]:
        self._require_lifecycle_client("list_snapshots")
        all_snaps = await self._lifecycle_client.list_snapshots()
        my_id = self.sandbox_id
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
    ) -> Traced[CommandResult]:
        payload = Sandbox._build_command_payload(
            command, args, env, working_dir, timeout=timeout
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
            from .exceptions import SandboxConnectionError

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
    ) -> Traced[ProcessInfo]:
        payload = Sandbox._build_command_payload(
            command,
            args,
            env,
            working_dir,
            stdin_mode=stdin_mode if stdin_mode != StdinMode.CLOSED else None,
            stdout_mode=stdout_mode if stdout_mode != OutputMode.CAPTURE else None,
            stderr_mode=stderr_mode if stderr_mode != OutputMode.CAPTURE else None,
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
            trace_id, response_json = await self._rust_client.list_processes_json_async()
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
