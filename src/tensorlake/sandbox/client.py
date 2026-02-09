"""Client SDK for managing Tensorlake sandboxes."""

import os
import time
from typing import List, Optional
from urllib.parse import urlparse

import httpx

from .exceptions import (
    PoolInUseError,
    PoolNotFoundError,
    RemoteAPIError,
    SandboxError,
    SandboxNotFoundError,
)
from .models import (
    CreateSandboxPoolResponse,
    CreateSandboxResponse,
    ListSandboxesResponse,
    ListSandboxPoolsResponse,
    SandboxInfo,
    SandboxPoolInfo,
    SandboxStatus,
)

_DEFAULT_HTTP_REQUEST_TIMEOUT_SEC = 30.0

_API_URL_FROM_ENV: str = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
_API_KEY_ENVIRONMENT_VARIABLE_NAME = "TENSORLAKE_API_KEY"
_API_KEY_FROM_ENV: str | None = os.getenv(_API_KEY_ENVIRONMENT_VARIABLE_NAME)
_API_NAMESPACE_FROM_ENV: str | None = os.getenv("INDEXIFY_NAMESPACE", "default")


class SandboxClient:
    """Client for managing Tensorlake sandboxes and sandbox pools."""

    def __init__(
        self,
        api_url: str = _API_URL_FROM_ENV,
        api_key: str | None = _API_KEY_FROM_ENV,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _API_NAMESPACE_FROM_ENV,
    ):
        self._api_url: str = api_url
        self._api_key: str | None = api_key
        self._organization_id: str | None = organization_id
        self._project_id: str | None = project_id
        self._namespace: str | None = namespace
        self._client: httpx.Client = httpx.Client(
            timeout=_DEFAULT_HTTP_REQUEST_TIMEOUT_SEC
        )

    def __enter__(self) -> "SandboxClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def close(self):
        """Close the HTTP client."""
        self._client.close()

    def _is_localhost(self) -> bool:
        parsed = urlparse(self._api_url)
        return parsed.hostname in ("localhost", "127.0.0.1")

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
        self._add_auth_headers(request.headers)
        try:
            response = self._client.send(request)
            response.raise_for_status()
        except httpx.HTTPStatusError:
            raise
        except httpx.RequestError as e:
            raise RemoteAPIError(0, str(e))
        return response

    def create(
        self,
        image: Optional[str] = None,
        cpus: float = 1.0,
        memory_mb: int = 512,
        ephemeral_disk_mb: int = 1024,
        secret_names: Optional[List[str]] = None,
        timeout_secs: Optional[int] = None,
        entrypoint: Optional[List[str]] = None,
        allow_internet_access: bool = True,
        allow_out: Optional[List[str]] = None,
        deny_out: Optional[List[str]] = None,
        pool_id: Optional[str] = None,
    ) -> CreateSandboxResponse:
        """Create a new sandbox.

        Args:
            image: Container image to use (optional if using pool)
            cpus: Number of CPUs to allocate
            memory_mb: Memory in megabytes
            ephemeral_disk_mb: Ephemeral disk space in megabytes
            secret_names: List of secret names to inject
            timeout_secs: Timeout in seconds (optional)
            entrypoint: Custom entrypoint command (optional)
            allow_internet_access: Allow internet access
            allow_out: List of allowed outbound destinations
            deny_out: List of denied outbound destinations
            pool_id: Pool ID to use for warm containers (optional)

        Returns:
            CreateSandboxResponse with sandbox_id and status

        Raises:
            RemoteAPIError: If the API request fails
        """
        payload = {
            "resources": {
                "cpus": cpus,
                "memory_mb": memory_mb,
                "ephemeral_disk_mb": ephemeral_disk_mb,
            }
        }

        if image:
            payload["image"] = image
        if secret_names:
            payload["secret_names"] = secret_names
        if timeout_secs is not None:
            payload["timeout_secs"] = timeout_secs
        if entrypoint:
            payload["entrypoint"] = entrypoint
        if not allow_internet_access or allow_out or deny_out:
            payload["network"] = {
                "allow_internet_access": allow_internet_access,
                "allow_out": allow_out or [],
                "deny_out": deny_out or [],
            }
        if pool_id:
            payload["pool_id"] = pool_id

        try:
            response = self._run_request(
                self._client.build_request(
                    "POST",
                    url=self._endpoint_url("sandboxes"),
                    json=payload,
                )
            )
            return CreateSandboxResponse(**response.json())
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def get(self, sandbox_id: str) -> SandboxInfo:
        """Get information about a sandbox.

        Args:
            sandbox_id: ID of the sandbox

        Returns:
            SandboxInfo with full sandbox details

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
        """
        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url(f"sandboxes/{sandbox_id}"),
                )
            )
            return SandboxInfo(**response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise SandboxNotFoundError(sandbox_id)
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def list(self) -> List[SandboxInfo]:
        """List all sandboxes in the namespace.

        Returns:
            List of SandboxInfo objects

        Raises:
            RemoteAPIError: If the API request fails
        """
        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url("sandboxes"),
                )
            )
            data = ListSandboxesResponse(**response.json())
            return data.sandboxes
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def delete(self, sandbox_id: str) -> None:
        """Terminate a sandbox.

        Args:
            sandbox_id: ID of the sandbox to terminate

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist
            RemoteAPIError: If the API request fails
        """
        try:
            self._run_request(
                self._client.build_request(
                    "DELETE",
                    url=self._endpoint_url(f"sandboxes/{sandbox_id}"),
                )
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise SandboxNotFoundError(sandbox_id)
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def create_pool(
        self,
        image: str,
        cpus: float = 1.0,
        memory_mb: int = 512,
        ephemeral_disk_mb: int = 1024,
        secret_names: Optional[List[str]] = None,
        timeout_secs: int = 0,
        entrypoint: Optional[List[str]] = None,
        max_containers: Optional[int] = None,
        warm_containers: Optional[int] = None,
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
        """
        payload = {
            "image": image,
            "resources": {
                "cpus": cpus,
                "memory_mb": memory_mb,
                "ephemeral_disk_mb": ephemeral_disk_mb,
            },
            "timeout_secs": timeout_secs,
        }

        if secret_names:
            payload["secret_names"] = secret_names
        if entrypoint:
            payload["entrypoint"] = entrypoint
        if max_containers is not None:
            payload["max_containers"] = max_containers
        if warm_containers is not None:
            payload["warm_containers"] = warm_containers

        try:
            response = self._run_request(
                self._client.build_request(
                    "POST",
                    url=self._endpoint_url("sandbox-pools"),
                    json=payload,
                )
            )
            return CreateSandboxPoolResponse(**response.json())
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def get_pool(self, pool_id: str) -> SandboxPoolInfo:
        """Get information about a sandbox pool.

        Args:
            pool_id: ID of the pool

        Returns:
            SandboxPoolInfo with full pool details

        Raises:
            PoolNotFoundError: If pool doesn't exist
            RemoteAPIError: If the API request fails
        """
        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url(f"sandbox-pools/{pool_id}"),
                )
            )
            return SandboxPoolInfo(**response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise PoolNotFoundError(pool_id)
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def list_pools(self) -> List[SandboxPoolInfo]:
        """List all sandbox pools in the namespace.

        Returns:
            List of SandboxPoolInfo objects

        Raises:
            RemoteAPIError: If the API request fails
        """
        try:
            response = self._run_request(
                self._client.build_request(
                    "GET",
                    url=self._endpoint_url("sandbox-pools"),
                )
            )
            data = ListSandboxPoolsResponse(**response.json())
            return data.pools
        except httpx.HTTPStatusError as e:
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def update_pool(
        self,
        pool_id: str,
        image: str,
        cpus: float = 1.0,
        memory_mb: int = 512,
        ephemeral_disk_mb: int = 1024,
        secret_names: Optional[List[str]] = None,
        timeout_secs: int = 0,
        entrypoint: Optional[List[str]] = None,
        max_containers: Optional[int] = None,
        warm_containers: Optional[int] = None,
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
        """
        payload = {
            "image": image,
            "resources": {
                "cpus": cpus,
                "memory_mb": memory_mb,
                "ephemeral_disk_mb": ephemeral_disk_mb,
            },
            "timeout_secs": timeout_secs,
        }

        if secret_names:
            payload["secret_names"] = secret_names
        if entrypoint:
            payload["entrypoint"] = entrypoint
        if max_containers is not None:
            payload["max_containers"] = max_containers
        if warm_containers is not None:
            payload["warm_containers"] = warm_containers

        try:
            response = self._run_request(
                self._client.build_request(
                    "PUT",
                    url=self._endpoint_url(f"sandbox-pools/{pool_id}"),
                    json=payload,
                )
            )
            return SandboxPoolInfo(**response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise PoolNotFoundError(pool_id)
            raise RemoteAPIError(e.response.status_code, e.response.text)

    def delete_pool(self, pool_id: str) -> None:
        """Delete a sandbox pool.

        Args:
            pool_id: ID of the pool to delete

        Raises:
            PoolNotFoundError: If pool doesn't exist
            PoolInUseError: If pool has active containers
            RemoteAPIError: If the API request fails
        """
        try:
            self._run_request(
                self._client.build_request(
                    "DELETE",
                    url=self._endpoint_url(f"sandbox-pools/{pool_id}"),
                )
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise PoolNotFoundError(pool_id)
            elif e.response.status_code == 409:
                raise PoolInUseError(pool_id, e.response.text)
            raise RemoteAPIError(e.response.status_code, e.response.text)

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
            if self._is_localhost():
                proxy_url = "http://localhost:9443"
            else:
                proxy_url = os.getenv(
                    "TENSORLAKE_SANDBOX_PROXY_URL", "https://sandbox.tensorlake.ai"
                )

        return Sandbox(
            sandbox_id=sandbox_id,
            proxy_url=proxy_url,
            api_key=self._api_key,
            organization_id=self._organization_id,
            project_id=self._project_id,
        )

    def create_and_connect(
        self,
        image: Optional[str] = None,
        cpus: float = 1.0,
        memory_mb: int = 512,
        ephemeral_disk_mb: int = 1024,
        secret_names: Optional[List[str]] = None,
        timeout_secs: Optional[int] = None,
        entrypoint: Optional[List[str]] = None,
        allow_internet_access: bool = True,
        allow_out: Optional[List[str]] = None,
        deny_out: Optional[List[str]] = None,
        pool_id: Optional[str] = None,
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
            allow_internet_access: Allow internet access
            allow_out: List of allowed outbound destinations
            deny_out: List of denied outbound destinations
            pool_id: Pool ID to use for warm containers (optional)
            proxy_url: Override the sandbox proxy URL
            startup_timeout: Max seconds to wait for Running status (default 60)

        Returns:
            Connected Sandbox instance (auto-terminates in context manager)

        Raises:
            SandboxError: If sandbox fails to start or times out
        """
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
            pool_id=pool_id,
        )

        deadline = time.time() + startup_timeout
        while time.time() < deadline:
            info = self.get(result.sandbox_id)
            if info.status == SandboxStatus.RUNNING:
                sandbox = self.connect(result.sandbox_id, proxy_url=proxy_url)
                sandbox._lifecycle_client = self
                return sandbox
            if info.status == SandboxStatus.TERMINATED:
                raise SandboxError(
                    f"Sandbox {result.sandbox_id} terminated during startup"
                )
            time.sleep(0.5)

        # Timed out — clean up the pending sandbox
        try:
            self.delete(result.sandbox_id)
        except Exception:
            pass
        raise SandboxError(
            f"Sandbox {result.sandbox_id} did not start within {startup_timeout}s"
        )
