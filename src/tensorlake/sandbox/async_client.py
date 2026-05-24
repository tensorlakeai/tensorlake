"""Async client SDK for managing Tensorlake sandboxes.

Mirrors :class:`SandboxClient` but exposes coroutine methods backed by the
Rust pyo3 ``*_async`` bindings (``future_into_py``) so each call is a true
non-blocking awaitable in the active asyncio event loop.
"""

from __future__ import annotations

import asyncio
import json
import warnings
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from tensorlake._tracing import USER_AGENT, Traced, TracedIterator

if TYPE_CHECKING:
    from .async_sandbox import AsyncSandbox

from . import _defaults
from .client import (
    _RUST_SANDBOX_CLIENT_AVAILABLE,
    RustCloudSandboxClient,
    _normalize_user_ports,
    _parse_rust_client_error_fields,
    _raise_as_sandbox_error,
    _resolve_sandbox_identifier,
    _rust_status_code,
    _startup_failure_message,
)
from .exceptions import (
    PoolInUseError,
    PoolNotFoundError,
    SandboxConnectionError,
    SandboxError,
    SandboxNotFoundError,
)
from .models import (
    ArchivedSandboxInfo,
    ContainerResourcesInfo,
    CreateSandboxPoolResponse,
    CreateSandboxRequest,
    CreateSandboxResources,
    CreateSandboxResponse,
    CreateSnapshotResponse,
    ListArchivedSandboxesResponse,
    ListSandboxesResponse,
    ListSandboxPoolsResponse,
    ListSnapshotsResponse,
    NetworkConfig,
    SandboxInfo,
    SandboxPoolInfo,
    SandboxPoolRequest,
    SandboxPortAccess,
    SandboxStatus,
    SnapshotInfo,
    SnapshotStatus,
    SnapshotType,
    SnapshotWaitCondition,
    UpdateSandboxRequest,
    snapshot_satisfies_wait_condition,
)


class AsyncSandboxClient:
    """Async client for managing Tensorlake sandboxes and pools.

    Use :meth:`for_cloud` or :meth:`for_localhost` for clearer construction.
    Every method is a coroutine that awaits the Rust async binding directly.
    """

    def __init__(
        self,
        api_url: str = _defaults.API_URL,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _defaults.NAMESPACE,
        max_retries: int = _defaults.MAX_RETRIES,
        retry_backoff_sec: float = _defaults.RETRY_BACKOFF_SEC,
        _internal: bool = False,
    ) -> None:
        if not _internal:
            warnings.warn(
                "AsyncSandboxClient is in preview; the surface may change.",
                FutureWarning,
                stacklevel=2,
            )
        self._api_url = api_url
        self._api_key = api_key
        self._organization_id = organization_id
        self._project_id = project_id
        self._namespace = namespace
        self._max_retries = max_retries
        self._retry_backoff_sec = retry_backoff_sec
        if not _RUST_SANDBOX_CLIENT_AVAILABLE:
            raise SandboxError(
                "Rust Cloud SDK sandbox client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )
        try:
            self._rust_client = RustCloudSandboxClient(
                api_url=api_url,
                api_key=api_key,
                organization_id=organization_id,
                project_id=project_id,
                namespace=namespace,
                user_agent=USER_AGENT,
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    @classmethod
    def for_cloud(
        cls,
        api_key: str | None = _defaults.API_KEY,
        organization_id: str | None = None,
        project_id: str | None = None,
        api_url: str = "https://api.tensorlake.ai",
    ) -> "AsyncSandboxClient":
        return cls(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            _internal=True,
        )

    @classmethod
    def for_localhost(
        cls,
        api_url: str = "http://localhost:8900",
        namespace: str = "default",
    ) -> "AsyncSandboxClient":
        return cls(api_url=api_url, namespace=namespace, _internal=True)

    async def __aenter__(self) -> "AsyncSandboxClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        self._rust_client.close()

    def _is_localhost(self) -> bool:
        parsed = urlparse(self._api_url)
        return parsed.hostname in ("localhost", "127.0.0.1")

    def _resolve_proxy_url(self) -> str:
        import os

        explicit = os.getenv("TENSORLAKE_SANDBOX_PROXY_URL")
        if explicit:
            return explicit
        if self._is_localhost():
            return "http://localhost:9443"
        parsed = urlparse(self._api_url)
        host = parsed.hostname or ""
        if host.startswith("api."):
            return f"{parsed.scheme}://sandbox.{host[4:]}"
        return _defaults.SANDBOX_PROXY_URL

    # --- Sandbox lifecycle ---

    async def create(
        self,
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
        snapshot_id: str | None = None,
        name: str | None = None,
    ) -> Traced[CreateSandboxResponse]:
        network = None
        if not allow_internet_access or allow_out is not None or deny_out is not None:
            network = NetworkConfig(
                allow_internet_access=allow_internet_access,
                allow_out=allow_out or [],
                deny_out=deny_out or [],
            )
        request_model = CreateSandboxRequest(
            image=image,
            resources=CreateSandboxResources(
                cpus=cpus, memory_mb=memory_mb, disk_mb=disk_mb
            ),
            secret_names=secret_names,
            timeout_secs=timeout_secs,
            entrypoint=entrypoint,
            network=network,
            snapshot_id=snapshot_id,
            name=name,
        )
        try:
            trace_id, response_json = await self._rust_client.create_sandbox_async(
                request_json=request_model.model_dump_json(exclude_none=True)
            )
            return Traced(
                trace_id, CreateSandboxResponse.model_validate_json(response_json)
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def claim(self, pool_id: str) -> Traced[CreateSandboxResponse]:
        try:
            trace_id, response_json = await self._rust_client.claim_sandbox_async(
                pool_id=pool_id
            )
            return Traced(
                trace_id, CreateSandboxResponse.model_validate_json(response_json)
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def get(self, sandbox_id: str) -> Traced[SandboxInfo]:
        try:
            trace_id, response_json = await self._rust_client.get_sandbox_json_async(
                sandbox_id=sandbox_id
            )
            return Traced(trace_id, SandboxInfo.model_validate_json(response_json))
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    async def list(self) -> TracedIterator[SandboxInfo]:
        try:
            trace_id, response_json = (
                await self._rust_client.list_sandboxes_json_async()
            )
            data = ListSandboxesResponse.model_validate(json.loads(response_json))
            return TracedIterator(trace_id, data.sandboxes)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def list_archived(
        self,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        direction: str | None = None,
    ) -> Traced[ListArchivedSandboxesResponse]:
        try:
            trace_id, response_json = (
                await self._rust_client.list_archived_sandboxes_json_async(
                    limit=limit,
                    cursor=cursor,
                    direction=direction,
                )
            )
            return Traced(
                trace_id,
                ListArchivedSandboxesResponse.model_validate_json(response_json),
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def get_archived(self, sandbox_id: str) -> Traced[ArchivedSandboxInfo]:
        try:
            trace_id, response_json = (
                await self._rust_client.get_archived_sandbox_json_async(
                    sandbox_id=sandbox_id
                )
            )
            return Traced(
                trace_id, ArchivedSandboxInfo.model_validate_json(response_json)
            )
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    async def update_sandbox(
        self,
        sandbox_id: str,
        *,
        allow_unauthenticated_access: bool | None = None,
        exposed_ports: list[int] | None = None,
    ) -> Traced[SandboxInfo]:
        normalized_ports = (
            _normalize_user_ports(exposed_ports) if exposed_ports is not None else None
        )
        if allow_unauthenticated_access is None and normalized_ports is None:
            raise SandboxError("At least one sandbox update field must be provided.")
        request = UpdateSandboxRequest(
            allow_unauthenticated_access=allow_unauthenticated_access,
            exposed_ports=normalized_ports,
        )
        try:
            trace_id, response_json = await self._rust_client.update_sandbox_async(
                sandbox_id=sandbox_id,
                request_json=request.model_dump_json(exclude_none=True),
            )
            return Traced(trace_id, SandboxInfo.model_validate_json(response_json))
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    async def get_port_access(self, sandbox_id: str) -> Traced[SandboxPortAccess]:
        traced = await self.get(sandbox_id)
        port_access = SandboxPortAccess(
            allow_unauthenticated_access=traced.allow_unauthenticated_access,
            exposed_ports=sorted(set(traced.exposed_ports or [])),
            sandbox_url=traced.sandbox_url,
        )
        return Traced(traced.trace_id, port_access)

    async def expose_ports(
        self,
        sandbox_id: str,
        ports: list[int],
        *,
        allow_unauthenticated_access: bool | None = None,
    ) -> Traced[SandboxInfo]:
        current = await self.get_port_access(sandbox_id)
        desired_ports = sorted(
            set(current.exposed_ports + _normalize_user_ports(ports))
        )
        return await self.update_sandbox(
            sandbox_id,
            allow_unauthenticated_access=(
                current.allow_unauthenticated_access
                if allow_unauthenticated_access is None
                else allow_unauthenticated_access
            ),
            exposed_ports=desired_ports,
        )

    async def unexpose_ports(
        self, sandbox_id: str, ports: list[int]
    ) -> Traced[SandboxInfo]:
        current = await self.get_port_access(sandbox_id)
        ports_to_remove = set(_normalize_user_ports(ports))
        desired_ports = [
            port for port in current.exposed_ports if port not in ports_to_remove
        ]
        return await self.update_sandbox(
            sandbox_id,
            allow_unauthenticated_access=(
                current.allow_unauthenticated_access if desired_ports else False
            ),
            exposed_ports=desired_ports,
        )

    async def delete(self, sandbox_id: str) -> Traced[None]:
        try:
            trace_id = await self._rust_client.delete_sandbox_async(
                sandbox_id=sandbox_id
            )
            return Traced(trace_id, None)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    async def suspend(
        self,
        sandbox_id: str,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> Traced[None]:
        try:
            trace_id = await self._rust_client.suspend_sandbox_async(
                sandbox_id=sandbox_id
            )
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)
        if not wait:
            return Traced(trace_id, None)
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            info = (await self.get(sandbox_id)).value
            if info.status == SandboxStatus.SUSPENDED:
                return Traced(trace_id, None)
            if info.status == SandboxStatus.TERMINATED:
                raise SandboxError(
                    f"Sandbox {sandbox_id!r} terminated while waiting for suspend"
                )
            await asyncio.sleep(poll_interval)
        raise SandboxError(f"Sandbox {sandbox_id!r} did not suspend within {timeout}s")

    async def resume(
        self,
        sandbox_id: str,
        wait: bool = True,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> Traced[None]:
        try:
            trace_id = await self._rust_client.resume_sandbox_async(
                sandbox_id=sandbox_id
            )
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)
        if not wait:
            return Traced(trace_id, None)
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            info = (await self.get(sandbox_id)).value
            if info.status == SandboxStatus.RUNNING:
                return Traced(trace_id, None)
            if info.status == SandboxStatus.TERMINATED:
                raise SandboxError(
                    f"Sandbox {sandbox_id!r} terminated while waiting for resume"
                )
            await asyncio.sleep(poll_interval)
        raise SandboxError(f"Sandbox {sandbox_id!r} did not resume within {timeout}s")

    # --- Snapshots ---

    async def snapshot(
        self,
        sandbox_id: str,
        snapshot_type: SnapshotType | None = None,
    ) -> Traced[CreateSnapshotResponse]:
        try:
            trace_id, response_json = await self._rust_client.create_snapshot_async(
                sandbox_id=sandbox_id,
                snapshot_type=(
                    snapshot_type.value if snapshot_type is not None else None
                ),
            )
            return Traced(
                trace_id, CreateSnapshotResponse.model_validate_json(response_json)
            )
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    async def get_snapshot(self, snapshot_id: str) -> Traced[SnapshotInfo]:
        try:
            trace_id, response_json = await self._rust_client.get_snapshot_json_async(
                snapshot_id=snapshot_id
            )
            return Traced(trace_id, SnapshotInfo.model_validate_json(response_json))
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def list_snapshots(self) -> TracedIterator[SnapshotInfo]:
        try:
            trace_id, response_json = (
                await self._rust_client.list_snapshots_json_async()
            )
            data = ListSnapshotsResponse.model_validate(json.loads(response_json))
            return TracedIterator(trace_id, data.snapshots)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def delete_snapshot(self, snapshot_id: str) -> Traced[None]:
        try:
            trace_id = await self._rust_client.delete_snapshot_async(
                snapshot_id=snapshot_id
            )
            return Traced(trace_id, None)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def snapshot_and_wait(
        self,
        sandbox_id: str,
        timeout: float = 300,
        poll_interval: float = 1.0,
        snapshot_type: SnapshotType | None = None,
        wait_until: SnapshotWaitCondition | str = SnapshotWaitCondition.LOCAL_READY,
    ) -> Traced[SnapshotInfo]:
        try:
            wait_condition = SnapshotWaitCondition(wait_until)
        except ValueError as e:
            raise SandboxError("wait_until must be 'local_ready' or 'completed'") from e

        traced_create = await self.snapshot(sandbox_id, snapshot_type=snapshot_type)
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            traced_info = await self.get_snapshot(traced_create.snapshot_id)
            if snapshot_satisfies_wait_condition(traced_info.status, wait_condition):
                return traced_info
            if traced_info.status == SnapshotStatus.FAILED:
                raise SandboxError(
                    f"Snapshot {traced_create.snapshot_id} failed: {traced_info.error}"
                )
            await asyncio.sleep(poll_interval)
        raise SandboxError(
            f"Snapshot {traced_create.snapshot_id} did not reach {wait_condition.value} within {timeout}s"
        )

    # --- Pools ---

    async def create_pool(
        self,
        image: str | None = None,
        cpus: float = 1.0,
        memory_mb: int = 1024,
        ephemeral_disk_mb: int = 1024,
        secret_names: list[str] | None = None,
        timeout_secs: int = 0,
        entrypoint: list[str] | None = None,
        max_containers: int | None = None,
        warm_containers: int | None = None,
    ) -> Traced[CreateSandboxPoolResponse]:
        request_model = SandboxPoolRequest(
            image=image,
            resources=ContainerResourcesInfo(
                cpus=cpus, memory_mb=memory_mb, ephemeral_disk_mb=ephemeral_disk_mb
            ),
            secret_names=secret_names,
            timeout_secs=timeout_secs,
            entrypoint=entrypoint,
            max_containers=max_containers,
            warm_containers=warm_containers,
        )
        try:
            trace_id, response_json = await self._rust_client.create_pool_async(
                request_json=request_model.model_dump_json(exclude_none=True)
            )
            return Traced(
                trace_id, CreateSandboxPoolResponse.model_validate_json(response_json)
            )
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def get_pool(self, pool_id: str) -> Traced[SandboxPoolInfo]:
        try:
            trace_id, response_json = await self._rust_client.get_pool_json_async(
                pool_id=pool_id
            )
            return Traced(trace_id, SandboxPoolInfo.model_validate_json(response_json))
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise PoolNotFoundError(pool_id) from None
            _raise_as_sandbox_error(e)

    async def list_pools(self) -> TracedIterator[SandboxPoolInfo]:
        try:
            trace_id, response_json = await self._rust_client.list_pools_json_async()
            data = ListSandboxPoolsResponse.model_validate(json.loads(response_json))
            return TracedIterator(trace_id, data.pools)
        except Exception as e:
            _raise_as_sandbox_error(e)

    async def update_pool(
        self,
        pool_id: str,
        image: str,
        cpus: float = 1.0,
        memory_mb: int = 1024,
        ephemeral_disk_mb: int = 1024,
        secret_names: list[str] | None = None,
        timeout_secs: int = 0,
        entrypoint: list[str] | None = None,
        max_containers: int | None = None,
        warm_containers: int | None = None,
    ) -> Traced[SandboxPoolInfo]:
        request_model = SandboxPoolRequest(
            image=image,
            resources=ContainerResourcesInfo(
                cpus=cpus, memory_mb=memory_mb, ephemeral_disk_mb=ephemeral_disk_mb
            ),
            secret_names=secret_names,
            timeout_secs=timeout_secs,
            entrypoint=entrypoint,
            max_containers=max_containers,
            warm_containers=warm_containers,
        )
        try:
            trace_id, response_json = await self._rust_client.update_pool_async(
                pool_id=pool_id,
                request_json=request_model.model_dump_json(exclude_none=True),
            )
            return Traced(trace_id, SandboxPoolInfo.model_validate_json(response_json))
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise PoolNotFoundError(pool_id) from None
            _raise_as_sandbox_error(e)

    async def delete_pool(self, pool_id: str) -> Traced[None]:
        try:
            trace_id = await self._rust_client.delete_pool_async(pool_id=pool_id)
            return Traced(trace_id, None)
        except Exception as e:
            kind, status_code, message = _parse_rust_client_error_fields(e)
            if status_code == 404:
                raise PoolNotFoundError(pool_id) from None
            if status_code == 409:
                raise PoolInUseError(pool_id, message) from None
            if kind == "connection":
                raise SandboxConnectionError(message) from None
            _raise_as_sandbox_error(e)

    # --- Connect ---

    async def connect(
        self,
        identifier: str | None = None,
        *,
        proxy_url: str | None = None,
        sandbox_id: str | None = None,
        routing_hint: str | None = None,
    ) -> "AsyncSandbox":
        from .async_sandbox import AsyncSandbox

        sandbox_identifier = _resolve_sandbox_identifier(
            identifier, sandbox_id, parameter_name="identifier"
        )
        if proxy_url is None:
            proxy_url = self._resolve_proxy_url()
        proxy_rust_client = self._rust_client.connect_proxy(
            proxy_url=proxy_url,
            sandbox_id=sandbox_identifier,
            routing_hint=routing_hint,
        )
        sandbox = AsyncSandbox(
            identifier=sandbox_identifier,
            proxy_url=proxy_url,
            api_key=self._api_key,
            organization_id=self._organization_id,
            project_id=self._project_id,
            routing_hint=routing_hint,
            _proxy_rust_client=proxy_rust_client,
        )
        sandbox._lifecycle_client = self
        return sandbox

    async def create_and_connect(
        self,
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
    ) -> "AsyncSandbox":
        requested_name = None if pool_id is not None else name
        if pool_id is not None:
            result = await self.claim(pool_id)
        else:
            result = await self.create(
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
                snapshot_id=snapshot_id,
                name=name,
            )

        if result.status == SandboxStatus.RUNNING:
            sandbox = await self.connect(
                result.sandbox_id,
                proxy_url=proxy_url,
                routing_hint=result.routing_hint,
            )
            sandbox._sandbox_id = result.sandbox_id
            sandbox._owns_sandbox = True
            sandbox._lifecycle_client = self
            sandbox._trace_id = result.trace_id
            sandbox._cached_info = SandboxInfo.model_construct(
                sandbox_id=result.sandbox_id,
                status=result.status,
                name=result.name or requested_name,
            )
            return sandbox
        if result.status in (SandboxStatus.SUSPENDED, SandboxStatus.TERMINATED):
            raise SandboxError(
                _startup_failure_message(
                    result.sandbox_id,
                    result.status,
                    error_details=result.error_details,
                    termination_reason=result.termination_reason,
                )
            )

        deadline = asyncio.get_running_loop().time() + startup_timeout
        while asyncio.get_running_loop().time() < deadline:
            info = await self.get(result.sandbox_id)
            if info.status == SandboxStatus.RUNNING:
                sandbox = await self.connect(
                    result.sandbox_id,
                    proxy_url=proxy_url,
                    routing_hint=info.routing_hint,
                )
                sandbox._sandbox_id = info.sandbox_id
                sandbox._cached_info = info.value
                sandbox._owns_sandbox = True
                sandbox._lifecycle_client = self
                sandbox._trace_id = result.trace_id
                return sandbox
            if info.status in (SandboxStatus.SUSPENDED, SandboxStatus.TERMINATED):
                raise SandboxError(
                    _startup_failure_message(
                        result.sandbox_id,
                        info.status,
                        error_details=info.error_details,
                        termination_reason=info.termination_reason,
                    )
                )
            await asyncio.sleep(0.5)

        try:
            await self.delete(result.sandbox_id)
        except Exception:
            pass
        raise SandboxError(
            f"Sandbox {result.sandbox_id} did not start within {startup_timeout}s"
        )
