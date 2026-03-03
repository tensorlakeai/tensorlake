"""Client SDK for managing Tensorlake sandboxes."""

from __future__ import annotations

import json
import time
from urllib.parse import urlparse

import httpx

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
    SandboxStatus,
    SnapshotInfo,
    SnapshotStatus,
)

try:
    from tensorlake_rust_cloud_sdk import CloudSandboxClient as RustCloudSandboxClient
    from tensorlake_rust_cloud_sdk import (
        CloudSandboxClientError as RustCloudSandboxClientError,
    )

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

    if isinstance(e, (httpx.NetworkError, httpx.RemoteProtocolError)):
        raise SandboxConnectionError(str(e)) from e

    if isinstance(e, httpx.TimeoutException):
        raise SandboxConnectionError(str(e)) from e

    if isinstance(e, httpx.HTTPStatusError):
        raise RemoteAPIError(e.response.status_code, e.response.text) from e

    raise SandboxError(str(e)) from e


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
        self._client: httpx.Client = httpx.Client(
            timeout=_defaults.DEFAULT_HTTP_TIMEOUT_SEC
        )
        self._rust_client = None

        if _RUST_SANDBOX_CLIENT_AVAILABLE:
            try:
                self._rust_client = RustCloudSandboxClient(
                    api_url=self._api_url,
                    api_key=self._api_key,
                    organization_id=self._organization_id,
                    project_id=self._project_id,
                    namespace=self._namespace,
                )
            except Exception:
                # Fallback to pure-Python implementation when Rust backend is unavailable.
                self._rust_client = None

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
        self._client.close()
        if self._rust_client is not None:
            try:
                self._rust_client.close()
            except Exception:
                pass

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

    def _endpoint_url(self, endpoint: str) -> str:
        if self._is_localhost():
            return f"{self._api_url}/v1/namespaces/{self._namespace}/{endpoint}"
        return f"{self._api_url}/{endpoint}"

    def _add_auth_headers(self, headers: dict[str, str]) -> None:
        if self._api_key is not None:
            headers["Authorization"] = f"Bearer {self._api_key}"
        if self._organization_id is not None:
            headers["X-Forwarded-Organization-Id"] = self._organization_id
        if self._project_id is not None:
            headers["X-Forwarded-Project-Id"] = self._project_id

    def _run_request(self, request: httpx.Request) -> httpx.Response:
        """Send an HTTP request with auth headers and retry on transient errors.

        Retries on connection errors and 429/502/503/504 status codes
        with exponential backoff.

        Raises:
            httpx.HTTPStatusError: For non-retryable HTTP errors (callers
                handle mapping to specific exception types).
            SandboxConnectionError: When the server is unreachable after
                all retry attempts.
        """
        self._add_auth_headers(request.headers)
        last_exception: Exception | None = None

        for attempt in range(self._max_retries):
            if attempt > 0:
                time.sleep(self._retry_backoff_sec * (2 ** (attempt - 1)))

            try:
                response = self._client.send(request)
            except httpx.RequestError as e:
                last_exception = SandboxConnectionError(str(e))
                if attempt < self._max_retries - 1:
                    continue
                raise last_exception from e

            if (
                response.status_code in _defaults.RETRYABLE_STATUS_CODES
                and attempt < self._max_retries - 1
            ):
                continue

            response.raise_for_status()
            return response

        # Should not be reached, but guard against it.
        if last_exception is not None:
            raise last_exception
        raise SandboxError("Request failed after retries")

    def create(
        self,
        image: str | None = None,
        cpus: float = 1.0,
        memory_mb: int = 512,
        ephemeral_disk_mb: int = 1024,
        secret_names: list[str] | None = None,
        timeout_secs: int | None = None,
        entrypoint: list[str] | None = None,
        allow_internet_access: bool = True,
        allow_out: list[str] | None = None,
        deny_out: list[str] | None = None,
        snapshot_id: str | None = None,
    ) -> CreateSandboxResponse:
        """Create a new standalone sandbox.

        Args:
            image: Container image to use
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
        )

        if self._rust_client is not None:
            try:
                response_json = self._rust_client.create_sandbox(
                    request_json=request_model.model_dump_json(exclude_none=True)
                )
                return CreateSandboxResponse.model_validate_json(response_json)
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "POST",
                    url=self._endpoint_url("sandboxes"),
                    json=request_model.model_dump(exclude_none=True),
                )
            )
            return CreateSandboxResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

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
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.claim_sandbox(pool_id=pool_id)
                return CreateSandboxResponse.model_validate_json(response_json)
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "POST",
                    url=self._endpoint_url(f"sandbox-pools/{pool_id}/sandboxes"),
                )
            )
            return CreateSandboxResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

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
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.get_sandbox_json(
                    sandbox_id=sandbox_id
                )
                return SandboxInfo.model_validate_json(response_json)
            except Exception as e:
                if _rust_status_code(e) == 404:
                    raise SandboxNotFoundError(sandbox_id) from None
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url(f"sandboxes/{sandbox_id}"),
                )
            )
            return SandboxInfo.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise SandboxNotFoundError(sandbox_id) from e
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def list(self) -> list[SandboxInfo]:
        """List all sandboxes in the namespace.

        Returns:
            List of SandboxInfo objects

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.list_sandboxes_json()
                data = ListSandboxesResponse.model_validate(json.loads(response_json))
                return data.sandboxes
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url("sandboxes"),
                )
            )
            data = ListSandboxesResponse.model_validate(response.json())
            return data.sandboxes
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def delete(self, sandbox_id: str) -> None:
        """Terminate a sandbox.

        Args:
            sandbox_id: ID of the sandbox to terminate

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        if self._rust_client is not None:
            try:
                self._rust_client.delete_sandbox(sandbox_id=sandbox_id)
                return
            except Exception as e:
                if _rust_status_code(e) == 404:
                    raise SandboxNotFoundError(sandbox_id) from None
                _raise_as_sandbox_error(e)

        try:
            self._run_request(
                self._client.build_request(
                    "DELETE",
                    url=self._endpoint_url(f"sandboxes/{sandbox_id}"),
                )
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise SandboxNotFoundError(sandbox_id) from e
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    # --- Snapshot operations ---

    def snapshot(self, sandbox_id: str) -> CreateSnapshotResponse:
        """Create a snapshot of a running sandbox's filesystem.

        This is an asynchronous operation. Poll with :meth:`get_snapshot`
        until the status is ``completed`` or ``failed``.

        Args:
            sandbox_id: ID of the running sandbox to snapshot

        Returns:
            CreateSnapshotResponse with snapshot_id and status

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.create_snapshot(sandbox_id=sandbox_id)
                return CreateSnapshotResponse.model_validate_json(response_json)
            except Exception as e:
                if _rust_status_code(e) == 404:
                    raise SandboxNotFoundError(sandbox_id) from None
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "POST",
                    url=self._endpoint_url(f"sandboxes/{sandbox_id}/snapshot"),
                )
            )
            return CreateSnapshotResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise SandboxNotFoundError(sandbox_id) from e
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

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
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.get_snapshot_json(
                    snapshot_id=snapshot_id
                )
                return SnapshotInfo.model_validate_json(response_json)
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url(f"snapshots/{snapshot_id}"),
                )
            )
            return SnapshotInfo.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def list_snapshots(self) -> list[SnapshotInfo]:
        """List all snapshots in the namespace.

        Returns:
            List of SnapshotInfo objects

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.list_snapshots_json()
                data = ListSnapshotsResponse.model_validate(json.loads(response_json))
                return data.snapshots
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url("snapshots"),
                )
            )
            data = ListSnapshotsResponse.model_validate(response.json())
            return data.snapshots
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete a snapshot.

        Args:
            snapshot_id: ID of the snapshot to delete

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        if self._rust_client is not None:
            try:
                self._rust_client.delete_snapshot(snapshot_id=snapshot_id)
                return
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            self._run_request(
                self._client.build_request(
                    "DELETE",
                    url=self._endpoint_url(f"snapshots/{snapshot_id}"),
                )
            )
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def snapshot_and_wait(
        self,
        sandbox_id: str,
        timeout: float = 300,
        poll_interval: float = 1.0,
    ) -> SnapshotInfo:
        """Create a snapshot and wait for it to complete.

        Args:
            sandbox_id: ID of the running sandbox to snapshot
            timeout: Max seconds to wait for completion (default 300)
            poll_interval: Seconds between status polls (default 1)

        Returns:
            SnapshotInfo with completed snapshot details

        Raises:
            SandboxError: If snapshot fails or times out
        """
        result = self.snapshot(sandbox_id)
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
        image: str,
        cpus: float = 1.0,
        memory_mb: int = 512,
        ephemeral_disk_mb: int = 1024,
        secret_names: list[str] | None = None,
        timeout_secs: int = 0,
        entrypoint: list[str] | None = None,
        max_containers: int | None = None,
        warm_containers: int | None = None,
    ) -> CreateSandboxPoolResponse:
        """Create a new sandbox pool.

        Args:
            image: Container image to use
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

        if self._rust_client is not None:
            try:
                response_json = self._rust_client.create_pool(
                    request_json=request_model.model_dump_json(exclude_none=True)
                )
                return CreateSandboxPoolResponse.model_validate_json(response_json)
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "POST",
                    url=self._endpoint_url("sandbox-pools"),
                    json=request_model.model_dump(exclude_none=True),
                )
            )
            return CreateSandboxPoolResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

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
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.get_pool_json(pool_id=pool_id)
                return SandboxPoolInfo.model_validate_json(response_json)
            except Exception as e:
                if _rust_status_code(e) == 404:
                    raise PoolNotFoundError(pool_id) from None
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url(f"sandbox-pools/{pool_id}"),
                )
            )
            return SandboxPoolInfo.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise PoolNotFoundError(pool_id) from e
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def list_pools(self) -> list[SandboxPoolInfo]:
        """List all sandbox pools in the namespace.

        Returns:
            List of SandboxPoolInfo objects

        Raises:
            RemoteAPIError: If the API request fails
            SandboxConnectionError: If the server is unreachable
        """
        if self._rust_client is not None:
            try:
                response_json = self._rust_client.list_pools_json()
                data = ListSandboxPoolsResponse.model_validate(
                    json.loads(response_json)
                )
                return data.pools
            except Exception as e:
                _raise_as_sandbox_error(e)

        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url("sandbox-pools"),
                )
            )
            data = ListSandboxPoolsResponse.model_validate(response.json())
            return data.pools
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def update_pool(
        self,
        pool_id: str,
        image: str,
        cpus: float = 1.0,
        memory_mb: int = 512,
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
            image: Container image to use
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

        if self._rust_client is not None:
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

        try:
            response = self._run_request(
                self._client.build_request(
                    "PUT",
                    url=self._endpoint_url(f"sandbox-pools/{pool_id}"),
                    json=request_model.model_dump(exclude_none=True),
                )
            )
            return SandboxPoolInfo.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise PoolNotFoundError(pool_id) from e
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

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
        if self._rust_client is not None:
            try:
                self._rust_client.delete_pool(pool_id=pool_id)
                return
            except Exception as e:
                kind, status_code, message = _parse_rust_client_error_fields(e)
                if status_code == 404:
                    raise PoolNotFoundError(pool_id) from None
                if status_code == 409:
                    raise PoolInUseError(pool_id, message) from None
                if kind == "connection":
                    raise SandboxConnectionError(message) from None
                _raise_as_sandbox_error(e)

        try:
            self._run_request(
                self._client.build_request(
                    "DELETE",
                    url=self._endpoint_url(f"sandbox-pools/{pool_id}"),
                )
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise PoolNotFoundError(pool_id) from e
            elif e.response.status_code == 409:
                raise PoolInUseError(pool_id, e.response.text) from e
            raise RemoteAPIError(e.response.status_code, e.response.text) from e

    def connect(self, sandbox_id: str, proxy_url: str | None = None) -> "Sandbox":
        """Connect to a running sandbox for process and file operations.

        Args:
            sandbox_id: ID of the sandbox to connect to
            proxy_url: Override the sandbox proxy URL. Auto-detected based on
                api_url when not provided. Can also be set via the
                TENSORLAKE_SANDBOX_PROXY_URL environment variable.

        Returns:
            Sandbox instance for interacting with the running sandbox
        """
        from .sandbox import Sandbox

        if proxy_url is None:
            proxy_url = self._resolve_proxy_url()

        return Sandbox(
            sandbox_id=sandbox_id,
            proxy_url=proxy_url,
            api_key=self._api_key,
            organization_id=self._organization_id,
            project_id=self._project_id,
        )

    def create_and_connect(
        self,
        image: str | None = None,
        cpus: float = 1.0,
        memory_mb: int = 512,
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
    ) -> "Sandbox":
        """Create a sandbox, wait for it to start, and return a connected Sandbox.

        This is a convenience method that combines create(), polling for
        Running status, and connect() into a single call. The returned
        Sandbox will auto-terminate when used as a context manager.

        Args:
            image: Container image to use (optional if using pool)
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
            )

        deadline = time.time() + startup_timeout
        while time.time() < deadline:
            info = self.get(result.sandbox_id)
            if info.status == SandboxStatus.RUNNING:
                sandbox = self.connect(result.sandbox_id, proxy_url=proxy_url)
                sandbox._owns_sandbox = True
                sandbox._lifecycle_client = self
                return sandbox
            if info.status == SandboxStatus.TERMINATED:
                raise SandboxError(
                    f"Sandbox {result.sandbox_id} terminated during startup"
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
