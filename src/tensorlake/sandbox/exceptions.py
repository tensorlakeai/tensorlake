"""Exception hierarchy for sandbox operations."""


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
