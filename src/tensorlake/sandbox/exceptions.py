"""Exception hierarchy for sandbox operations."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import SandboxStatus


class SandboxException(Exception):
    """Base exception for all sandbox-related errors."""

    pass


class SandboxError(SandboxException):
    """General sandbox operation error."""

    pass


class SandboxConnectionError(SandboxError):
    """Raised when the client cannot connect to the API server."""

    def __init__(self, message: str):
        super().__init__(f"Connection error: {message}")


class SandboxNotFoundError(SandboxError):
    """Raised when a sandbox is not found."""

    def __init__(self, sandbox_id: str):
        self._sandbox_id = sandbox_id
        super().__init__(f"Sandbox not found: {sandbox_id}")

    @property
    def sandbox_id(self) -> str:
        return self._sandbox_id


class SandboxNotRoutableError(SandboxError):
    """Raised when a sandbox exists but has no proxy routing yet.

    A sandbox that is not ``Running`` (for example, one that is still
    starting) has no ``sandbox_url``, so a connected handle cannot be
    built for it. Wait for the sandbox to reach ``Running`` and connect
    again, or pass an explicit ``proxy_url``.
    """

    def __init__(self, sandbox_id: str, status: "SandboxStatus | None" = None):
        self._sandbox_id = sandbox_id
        self._status = status
        status_part = (
            f" (status: {getattr(status, 'value', status)})"
            if status is not None
            else ""
        )
        super().__init__(
            f"Sandbox {sandbox_id} did not include proxy routing "
            f"information{status_part}; it may still be starting. Wait for "
            "it to be Running and connect again, or pass an explicit proxy_url."
        )

    @property
    def sandbox_id(self) -> str:
        return self._sandbox_id

    @property
    def status(self) -> "SandboxStatus | None":
        return self._status


class PoolNotFoundError(SandboxError):
    """Raised when a sandbox pool is not found."""

    def __init__(self, pool_id: str):
        self._pool_id = pool_id
        super().__init__(f"Sandbox pool not found: {pool_id}")

    @property
    def pool_id(self) -> str:
        return self._pool_id


class PoolInUseError(SandboxError):
    """Raised when attempting to delete a pool that is in use."""

    def __init__(self, pool_id: str, message: str = ""):
        self._pool_id = pool_id
        error_msg = f"Cannot delete pool {pool_id}: pool is in use"
        if message:
            error_msg += f" - {message}"
        super().__init__(error_msg)

    @property
    def pool_id(self) -> str:
        return self._pool_id


class RemoteAPIError(SandboxError):
    """Raised when the remote API returns an error."""

    def __init__(self, status_code: int, message: str):
        self._status_code = status_code
        self._message = message
        super().__init__(f"API error (status {status_code}): {message}")

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def message(self) -> str:
        return self._message
