"""Client SDK for managing Tensorlake sandboxes."""

from __future__ import annotations

import json
import time
from urllib.parse import urlparse

from . import _defaults
from .exceptions import (
    PoolInUseError,
    PoolNotFoundError,
    RemoteAPIError,
    SandboxConnectionError,
    SandboxError,
    SandboxNotFoundError,
)
from .models import (
    ContainerResourcesInfo,
    CreateSandboxPoolResponse,
    CreateSandboxRequest,
    CreateSandboxResponse,
    CreateSnapshotResponse,
    ListSandboxesResponse,
    ListSandboxPoolsResponse,
    ListSnapshotsResponse,
    NetworkConfig,
    SandboxInfo,
    SandboxPoolInfo,
    SandboxPoolRequest,
    SandboxPortAccess,
    SandboxStatus,
    SnapshotContentMode,
    SnapshotInfo,
    SnapshotStatus,
    UpdateSandboxRequest,
)

try:
    from tensorlake._cloud_sdk import CloudSandboxClient as RustCloudSandboxClient
    from tensorlake._cloud_sdk import (
        CloudSandboxClientError as RustCloudSandboxClientError,
    )

    _RUST_SANDBOX_CLIENT_AVAILABLE = True
except Exception:
    try:
        from _cloud_sdk import CloudSandboxClient as RustCloudSandboxClient
        from _cloud_sdk import CloudSandboxClientError as RustCloudSandboxClientError

        _RUST_SANDBOX_CLIENT_AVAILABLE = True
    except Exception:
        RustCloudSandboxClient = None
        RustCloudSandboxClientError = None
        _RUST_SANDBOX_CLIENT_AVAILABLE = False


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


def _rust_status_code(e: Exception) -> int | None:
    if (
        RustCloudSandboxClientError is not None
        and isinstance(e, RustCloudSandboxClientError)
        and len(e.args) > 0
    ):
        _, status_code, _ = _parse_rust_client_error_fields(e)
        return status_code
    return None


def _resolve_sandbox_identifier(
    identifier: str | None,
    sandbox_id: str | None,
    *,
    parameter_name: str,
) -> str:
    if identifier and sandbox_id and identifier != sandbox_id:
        raise SandboxError(
            f"Provide only one of `{parameter_name}` or `sandbox_id`, not both."
        )

    resolved = identifier or sandbox_id
    if not resolved:
        raise SandboxError(
            f"`{parameter_name}` is required. `sandbox_id` is accepted as a deprecated alias."
        )
    return resolved


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


_RESERVED_SANDBOX_MANAGEMENT_PORT = 9501


def _normalize_user_ports(ports: list[int]) -> list[int]:
    normalized: set[int] = set()
    for port in ports:
        if isinstance(port, bool) or not isinstance(port, int):
            raise SandboxError(f"invalid port '{port}'")
        if port < 1 or port > 65535:
            raise SandboxError(f"invalid port '{port}'")
        if port == _RESERVED_SANDBOX_MANAGEMENT_PORT:
            raise SandboxError("port 9501 is reserved for sandbox management")
        normalized.add(port)
    return sorted(normalized)


class SandboxClient:
    """Client for managing Tensorlake sandboxes and sandbox pools.

    Use the ``for_cloud`` or ``for_localhost`` class methods for
    clearer construction depending on your deployment target.
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
    ):
        self._api_url: str = api_url
        self._api_key: str | None = api_key
        self._organization_id: str | None = organization_id
        self._project_id: str | None = project_id
        self._namespace: str | None = namespace
        self._max_retries = max_retries
        self._retry_backoff_sec = retry_backoff_sec
        if not _RUST_SANDBOX_CLIENT_AVAILABLE:
            raise SandboxError(
                "Rust Cloud SDK sandbox client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        try:
            self._rust_client = RustCloudSandboxClient(
                api_url=self._api_url,
                api_key=self._api_key,
                organization_id=self._organization_id,
                project_id=self._project_id,
                namespace=self._namespace,
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
    ) -> "SandboxClient":
        """Create a client for the Tensorlake cloud platform.

        In the cloud, resources are scoped by *organization_id* and
        *project_id* (sent as headers). The *namespace* parameter is
        not used.

        Args:
            api_key: Tensorlake API key (defaults to TENSORLAKE_API_KEY env var)
            organization_id: Organization ID for multi-tenant access
            project_id: Project ID for scoping resources
            api_url: Cloud API URL override
        """
        return cls(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
        )

    @classmethod
    def for_localhost(
        cls,
        api_url: str = "http://localhost:8900",
        namespace: str = "default",
    ) -> "SandboxClient":
        """Create a client for a local Indexify server.

        Locally, resources are scoped by *namespace* which is embedded
        in the URL path (``/v1/namespaces/{ns}/...``).

        Args:
            api_url: Local server URL
            namespace: Namespace for resource scoping
        """
        return cls(api_url=api_url, namespace=namespace)

    def __enter__(self) -> "SandboxClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def close(self):
        """Close the HTTP client."""
        self._rust_client.close()

    def _is_localhost(self) -> bool:
        """Check whether the API URL points to a local server.

        This determines URL routing: local servers use namespace-scoped
        paths (``/v1/namespaces/{ns}/...``), while the cloud API uses
        flat paths with auth headers for scoping.
        """
        parsed = urlparse(self._api_url)
        return parsed.hostname in ("localhost", "127.0.0.1")

    def _resolve_proxy_url(self) -> str:
        """Derive the sandbox proxy URL from the API URL.

        Checks the ``TENSORLAKE_SANDBOX_PROXY_URL`` env var first, then
        infers the proxy domain from the API URL so that
        ``api.tensorlake.dev`` → ``sandbox.tensorlake.dev``, etc.
        """
        import os

        explicit = os.getenv("TENSORLAKE_SANDBOX_PROXY_URL")
        if explicit:
            return explicit
        if self._is_localhost():
            return "http://localhost:9443"
        parsed = urlparse(self._api_url)
        host = parsed.hostname or ""
        if host.startswith("api."):
            proxy_host = "sandbox." + host[4:]
            return f"{parsed.scheme}://{proxy_host}"
        return _defaults.SANDBOX_PROXY_URL

    def create(
        self,
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
        snapshot_id: str | None = None,
        name: str | None = None,
    ) -> CreateSandboxResponse:
        """Create a new standalone sandbox.

        Args:
            image: Sandbox image name to boot from, such as
                ``tensorlake/ubuntu-minimal`` or a registered Sandbox Image name.
                When omitted, Tensorlake uses the default managed environment.
            cpus: Number of CPUs to allocate
            memory_mb: Memory in megabytes
            ephemeral_disk_mb: Ephemeral disk space in megabytes
            secret_names: List of secret names to inject
            timeout_secs: Timeout in seconds (optional)
            entrypoint: Custom entrypoint command (optional)
            allow_internet_access: If True (default), outbound traffic is
                allowed unless denied. If False, all outbound traffic is
                blocked unless explicitly allowed.
            allow_out: Destination IPs/CIDRs to allow
                (e.g. ``["8.8.8.8", "10.0.0.0/8"]``). Takes precedence
                over *deny_out*.
            deny_out: Destination IPs/CIDRs to deny
                (e.g. ``["192.168.1.0/24"]``).
            snapshot_id: ID of a completed snapshot to restore from.
                When set, image, resources, entrypoint, and secrets
                are inherited from the snapshot unless explicitly
                overridden.
            name: Optional name for the sandbox. Named sandboxes support
                suspend/resume. When absent the sandbox is ephemeral.

        Returns:
            CreateSandboxResponse with sandbox_id and status

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        network = None
        if not allow_internet_access or allow_out is not None or deny_out is not None:
            network = NetworkConfig(
                allow_internet_access=allow_internet_access,
                allow_out=allow_out or [],
                deny_out=deny_out or [],
            )

        request_model = CreateSandboxRequest(
            image=image,
            resources=ContainerResourcesInfo(
                cpus=cpus,
                memory_mb=memory_mb,
                ephemeral_disk_mb=ephemeral_disk_mb,
            ),
            secret_names=secret_names,
            timeout_secs=timeout_secs,
            entrypoint=entrypoint,
            network=network,
            snapshot_id=snapshot_id,
            name=name,
        )

        try:
            response_json = self._rust_client.create_sandbox(
                request_json=request_model.model_dump_json(exclude_none=True)
            )
            return CreateSandboxResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def claim(self, pool_id: str) -> CreateSandboxResponse:
        """Claim a sandbox from a pool.

        Claims a warm container from the pool, or creates a new one
        if no warm containers are available (subject to max_containers).

        Args:
            pool_id: ID of the pool to claim from

        Returns:
            CreateSandboxResponse with sandbox_id and status

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.claim_sandbox(pool_id=pool_id)
            return CreateSandboxResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get(self, sandbox_id: str) -> SandboxInfo:
        """Get information about a sandbox.

        Args:
            sandbox_id: ID of the sandbox

        Returns:
            SandboxInfo with full sandbox details

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.get_sandbox_json(sandbox_id=sandbox_id)
            return SandboxInfo.model_validate_json(response_json)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    def list(self) -> list[SandboxInfo]:
        """List all sandboxes in the namespace.

        Returns:
            List of SandboxInfo objects

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.list_sandboxes_json()
            data = ListSandboxesResponse.model_validate(json.loads(response_json))
            return data.sandboxes
        except Exception as e:
            _raise_as_sandbox_error(e)

    def update_sandbox(
        self,
        sandbox_id: str,
        name: str | None = None,
        *,
        allow_unauthenticated_access: bool | None = None,
        exposed_ports: list[int] | None = None,
    ) -> SandboxInfo:
        """Update a sandbox's properties.

        Supports updating the sandbox name and sandbox proxy access settings.

        Args:
            sandbox_id: ID or name of the sandbox to update
            name: New name for the sandbox. Naming an ephemeral sandbox makes it
                non-ephemeral and enables suspend/resume.
            allow_unauthenticated_access: Whether exposed user ports should be
                reachable without TensorLake auth.
            exposed_ports: User ports that should be routable through the sandbox
                proxy. Port ``9501`` is reserved for sandbox management and cannot
                be configured here.

        Returns:
            SandboxInfo with updated sandbox details

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        normalized_ports = (
            _normalize_user_ports(exposed_ports) if exposed_ports is not None else None
        )
        if (
            name is None
            and allow_unauthenticated_access is None
            and normalized_ports is None
        ):
            raise SandboxError("At least one sandbox update field must be provided.")

        request = UpdateSandboxRequest(
            name=name,
            allow_unauthenticated_access=allow_unauthenticated_access,
            exposed_ports=normalized_ports,
        )
        try:
            response_json = self._rust_client.update_sandbox(
                sandbox_id=sandbox_id,
                request_json=request.model_dump_json(exclude_none=True),
            )
            return SandboxInfo.model_validate_json(response_json)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    def get_port_access(self, sandbox_id: str) -> SandboxPortAccess:
        """Return the current sandbox proxy settings for user ports."""
        info = self.get(sandbox_id)
        return SandboxPortAccess(
            allow_unauthenticated_access=info.allow_unauthenticated_access,
            exposed_ports=sorted(set(info.exposed_ports or [])),
            sandbox_url=info.sandbox_url,
        )

    def expose_ports(
        self,
        sandbox_id: str,
        ports: list[int],
        *,
        allow_unauthenticated_access: bool | None = None,
    ) -> SandboxInfo:
        """Expose additional user ports through the sandbox proxy.

        By default this preserves the sandbox's current auth mode for user ports.
        Set ``allow_unauthenticated_access=True`` to make them publicly reachable.
        """
        current = self.get_port_access(sandbox_id)
        desired_ports = sorted(
            set(current.exposed_ports + _normalize_user_ports(ports))
        )
        return self.update_sandbox(
            sandbox_id,
            allow_unauthenticated_access=(
                current.allow_unauthenticated_access
                if allow_unauthenticated_access is None
                else allow_unauthenticated_access
            ),
            exposed_ports=desired_ports,
        )

    def unexpose_ports(self, sandbox_id: str, ports: list[int]) -> SandboxInfo:
        """Remove one or more user ports from the sandbox proxy allowlist."""
        current = self.get_port_access(sandbox_id)
        ports_to_remove = set(_normalize_user_ports(ports))
        desired_ports = [
            port for port in current.exposed_ports if port not in ports_to_remove
        ]
        return self.update_sandbox(
            sandbox_id,
            allow_unauthenticated_access=(
                current.allow_unauthenticated_access if desired_ports else False
            ),
            exposed_ports=desired_ports,
        )

    def delete(self, sandbox_id: str) -> None:
        """Terminate a sandbox.

        Args:
            sandbox_id: ID of the sandbox to terminate

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            self._rust_client.delete_sandbox(sandbox_id=sandbox_id)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    def suspend(self, sandbox_id: str) -> None:
        """Suspend a named sandbox.

        Only sandboxes created with a ``name`` can be suspended; ephemeral
        sandboxes cannot. The call returns as soon as the server accepts
        the request; poll :meth:`get` until
        :attr:`SandboxStatus.SUSPENDED` if you need to wait for the
        transition to complete.

        Args:
            sandbox_id: ID or name of the sandbox to suspend

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            self._rust_client.suspend_sandbox(sandbox_id=sandbox_id)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    def resume(self, sandbox_id: str) -> None:
        """Resume a suspended sandbox.

        The call returns as soon as the server accepts the request; poll
        :meth:`get` until :attr:`SandboxStatus.RUNNING` if you need to
        wait for the transition to complete.

        Args:
            sandbox_id: ID or name of the sandbox to resume

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            self._rust_client.resume_sandbox(sandbox_id=sandbox_id)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    # --- Snapshot operations ---

    def snapshot(
        self,
        sandbox_id: str,
        content_mode: SnapshotContentMode | None = None,
    ) -> CreateSnapshotResponse:
        """Create a snapshot of a running sandbox's filesystem.

        This is an asynchronous operation. Poll with :meth:`get_snapshot`
        until the status is ``completed`` or ``failed``.

        Args:
            sandbox_id: ID of the running sandbox to snapshot.
            content_mode: Optional content mode for the snapshot. When
                ``None`` (default) the server picks its default. Use
                :attr:`SnapshotContentMode.FILESYSTEM_ONLY` for snapshots
                that should be cold-booted by sandboxes restoring from
                them (e.g. sandbox image builds).

        Returns:
            CreateSnapshotResponse with snapshot_id and status

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.create_snapshot(
                sandbox_id=sandbox_id,
                content_mode=content_mode.value if content_mode is not None else None,
            )
            return CreateSnapshotResponse.model_validate_json(response_json)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise SandboxNotFoundError(sandbox_id) from None
            _raise_as_sandbox_error(e)

    def get_snapshot(self, snapshot_id: str) -> SnapshotInfo:
        """Get information about a snapshot.

        Args:
            snapshot_id: ID of the snapshot

        Returns:
            SnapshotInfo with full snapshot details

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.get_snapshot_json(snapshot_id=snapshot_id)
            return SnapshotInfo.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def list_snapshots(self) -> list[SnapshotInfo]:
        """List all snapshots in the namespace.

        Returns:
            List of SnapshotInfo objects

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.list_snapshots_json()
            data = ListSnapshotsResponse.model_validate(json.loads(response_json))
            return data.snapshots
        except Exception as e:
            _raise_as_sandbox_error(e)

    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete a snapshot.

        Args:
            snapshot_id: ID of the snapshot to delete

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            self._rust_client.delete_snapshot(snapshot_id=snapshot_id)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def snapshot_and_wait(
        self,
        sandbox_id: str,
        timeout: float = 300,
        poll_interval: float = 1.0,
        content_mode: SnapshotContentMode | None = None,
    ) -> SnapshotInfo:
        """Create a snapshot and wait for it to complete.

        Args:
            sandbox_id: ID of the running sandbox to snapshot
            timeout: Max seconds to wait for completion (default 300)
            poll_interval: Seconds between status polls (default 1)
            content_mode: Optional content mode for the snapshot. See
                :meth:`snapshot` for details.

        Returns:
            SnapshotInfo with completed snapshot details

        Raises:
            SandboxError: If snapshot fails or times out
        """
        result = self.snapshot(sandbox_id, content_mode=content_mode)
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.get_snapshot(result.snapshot_id)
            if info.status == SnapshotStatus.COMPLETED:
                return info
            if info.status == SnapshotStatus.FAILED:
                raise SandboxError(
                    f"Snapshot {result.snapshot_id} failed: {info.error}"
                )
            time.sleep(poll_interval)
        raise SandboxError(
            f"Snapshot {result.snapshot_id} did not complete within {timeout}s"
        )

    # --- Pool operations ---

    def create_pool(
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
    ) -> CreateSandboxPoolResponse:
        """Create a new sandbox pool.

        Args:
            image: Sandbox image name to boot from, such as
                ``tensorlake/ubuntu-minimal`` or a registered Sandbox Image name.
            cpus: Number of CPUs to allocate
            memory_mb: Memory in megabytes
            ephemeral_disk_mb: Ephemeral disk space in megabytes
            secret_names: List of secret names to inject
            timeout_secs: Timeout in seconds (default: 0 = no timeout)
            entrypoint: Custom entrypoint command (optional)
            max_containers: Maximum number of containers in pool
            warm_containers: Number of warm containers to maintain

        Returns:
            CreateSandboxPoolResponse with pool_id and namespace

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
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
            response_json = self._rust_client.create_pool(
                request_json=request_model.model_dump_json(exclude_none=True)
            )
            return CreateSandboxPoolResponse.model_validate_json(response_json)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def get_pool(self, pool_id: str) -> SandboxPoolInfo:
        """Get information about a sandbox pool.

        Args:
            pool_id: ID of the pool

        Returns:
            SandboxPoolInfo with full pool details

        Raises:
            PoolNotFoundError: If pool doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.get_pool_json(pool_id=pool_id)
            return SandboxPoolInfo.model_validate_json(response_json)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise PoolNotFoundError(pool_id) from None
            _raise_as_sandbox_error(e)

    def list_pools(self) -> list[SandboxPoolInfo]:
        """List all sandbox pools in the namespace.

        Returns:
            List of SandboxPoolInfo objects

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            response_json = self._rust_client.list_pools_json()
            data = ListSandboxPoolsResponse.model_validate(json.loads(response_json))
            return data.pools
        except Exception as e:
            _raise_as_sandbox_error(e)

    def update_pool(
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
    ) -> SandboxPoolInfo:
        """Update a sandbox pool configuration.

        Args:
            pool_id: ID of the pool to update
            image: Sandbox image name to boot from, such as
                ``tensorlake/ubuntu-minimal`` or a registered Sandbox Image name.
            cpus: Number of CPUs to allocate
            memory_mb: Memory in megabytes
            ephemeral_disk_mb: Ephemeral disk space in megabytes
            secret_names: List of secret names to inject
            timeout_secs: Timeout in seconds (default: 0 = no timeout)
            entrypoint: Custom entrypoint command (optional)
            max_containers: Maximum number of containers in pool
            warm_containers: Number of warm containers to maintain

        Returns:
            SandboxPoolInfo with updated pool details

        Raises:
            PoolNotFoundError: If pool doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
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
            response_json = self._rust_client.update_pool(
                pool_id=pool_id,
                request_json=request_model.model_dump_json(exclude_none=True),
            )
            return SandboxPoolInfo.model_validate_json(response_json)
        except Exception as e:
            if _rust_status_code(e) == 404:
                raise PoolNotFoundError(pool_id) from None
            _raise_as_sandbox_error(e)

    def delete_pool(self, pool_id: str) -> None:
        """Delete a sandbox pool.

        Args:
            pool_id: ID of the pool to delete

        Raises:
            PoolNotFoundError: If pool doesn't exist
            PoolInUseError: If pool has active containers
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        try:
            self._rust_client.delete_pool(pool_id=pool_id)
        except Exception as e:
            kind, status_code, message = _parse_rust_client_error_fields(e)
            if status_code == 404:
                raise PoolNotFoundError(pool_id) from None
            if status_code == 409:
                raise PoolInUseError(pool_id, message) from None
            if kind == "connection":
                raise SandboxConnectionError(message) from None
            _raise_as_sandbox_error(e)

    def connect(
        self,
        identifier: str | None = None,
        *,
        proxy_url: str | None = None,
        sandbox_id: str | None = None,
        routing_hint: str | None = None,
    ) -> "Sandbox":
        """Connect to a running sandbox for process and file operations.

        Args:
            identifier: Sandbox ID or name to connect to.
            proxy_url: Override the sandbox proxy URL. Auto-detected based on
                api_url when not provided. Can also be set via the
                TENSORLAKE_SANDBOX_PROXY_URL environment variable.
            sandbox_id: Deprecated alias for ``identifier``.

        Returns:
            Sandbox instance for interacting with the running sandbox
        """
        from .sandbox import Sandbox

        sandbox_identifier = _resolve_sandbox_identifier(
            identifier,
            sandbox_id,
            parameter_name="identifier",
        )

        if proxy_url is None:
            proxy_url = self._resolve_proxy_url()

        proxy_rust_client = self._rust_client.connect_proxy(
            proxy_url=proxy_url,
            sandbox_id=sandbox_identifier,
            routing_hint=routing_hint,
        )

        sandbox = Sandbox(
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

    def create_and_connect(
        self,
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
    ) -> "Sandbox":
        """Create a sandbox, wait for it to start, and return a connected Sandbox.

        This is a convenience method that combines create(), polling for
        Running status, and connect() into a single call. The returned
        Sandbox will auto-terminate when used as a context manager.

        Args:
            image: Sandbox image name to boot from, such as
                ``tensorlake/ubuntu-minimal`` or a registered Sandbox Image name
                (optional if using pool).
            cpus: Number of CPUs to allocate
            memory_mb: Memory in megabytes
            ephemeral_disk_mb: Ephemeral disk space in megabytes
            secret_names: List of secret names to inject
            timeout_secs: Timeout in seconds (optional)
            entrypoint: Custom entrypoint command (optional)
            allow_internet_access: If True (default), outbound traffic is
                allowed unless denied. If False, all outbound traffic is
                blocked unless explicitly allowed.
            allow_out: Destination IPs/CIDRs to allow
                (e.g. ``["8.8.8.8", "10.0.0.0/8"]``). Takes precedence
                over *deny_out*.
            deny_out: Destination IPs/CIDRs to deny
                (e.g. ``["192.168.1.0/24"]``).
            pool_id: Pool ID to use for warm containers (optional)
            snapshot_id: ID of a completed snapshot to restore from
            proxy_url: Override the sandbox proxy URL
            startup_timeout: Max seconds to wait for Running status (default 60)
            name: Optional name for the sandbox. Named sandboxes support
                suspend/resume. When absent the sandbox is ephemeral.

        Returns:
            Connected Sandbox instance (auto-terminates in context manager)

        Raises:
            SandboxError: If sandbox fails to start or times out
            SandboxConnectionError: If the server is unreachable
        """
        if pool_id is not None:
            result = self.claim(pool_id)
        else:
            result = self.create(
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
                snapshot_id=snapshot_id,
                name=name,
            )

        # Fast path: the blocking create/claim response already carries Running status
        # and a short-lived routing hint. Use it immediately to skip an extra poll RTT
        # and let the proxy route the first request without a placement lookup.
        if result.status == SandboxStatus.RUNNING:
            sandbox = self.connect(
                result.sandbox_id,
                proxy_url=proxy_url,
                routing_hint=result.routing_hint,
            )
            sandbox._sandbox_id = result.sandbox_id
            sandbox._name = result.name
            sandbox._name_loaded = True
            sandbox._owns_sandbox = True
            sandbox._lifecycle_client = self
            return sandbox

        deadline = time.time() + startup_timeout
        while time.time() < deadline:
            info = self.get(result.sandbox_id)
            if info.status == SandboxStatus.RUNNING:
                sandbox = self.connect(
                    result.sandbox_id,
                    proxy_url=proxy_url,
                    routing_hint=info.routing_hint,
                )
                sandbox._sandbox_id = info.sandbox_id
                sandbox._name = info.name
                sandbox._name_loaded = True
                sandbox._cached_info = info
                sandbox._owns_sandbox = True
                sandbox._lifecycle_client = self
                return sandbox
            if info.status in (SandboxStatus.SUSPENDED, SandboxStatus.TERMINATED):
                raise SandboxError(
                    f"Sandbox {result.sandbox_id} became {info.status.value} during startup"
                )
            # Poll at 0.5s — balances responsiveness against API load.
            time.sleep(0.5)

        # Timed out — clean up the pending sandbox
        try:
            self.delete(result.sandbox_id)
        except Exception:
            pass
        raise SandboxError(
            f"Sandbox {result.sandbox_id} did not start within {startup_timeout}s"
        )
