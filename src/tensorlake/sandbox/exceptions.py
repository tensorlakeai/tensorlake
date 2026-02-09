"""Exception hierarchy for sandbox operations."""


class SandboxException(Exception):
    """Base exception for all sandbox-related errors."""

    pass


class SandboxError(SandboxException):
    """General sandbox operation error."""

    pass


class SandboxNotFoundError(SandboxError):
    """Raised when a sandbox is not found."""

    def __init__(self, sandbox_id: str):
        self.sandbox_id = sandbox_id
        super().__init__(f"Sandbox not found: {sandbox_id}")


class PoolNotFoundError(SandboxError):
    """Raised when a sandbox pool is not found."""

    def __init__(self, pool_id: str):
        self.pool_id = pool_id
        super().__init__(f"Sandbox pool not found: {pool_id}")


class PoolInUseError(SandboxError):
    """Raised when attempting to delete a pool that is in use."""

    def __init__(self, pool_id: str, message: str = ""):
        self.pool_id = pool_id
        error_msg = f"Cannot delete pool {pool_id}: pool is in use"
        if message:
            error_msg += f" - {message}"
        super().__init__(error_msg)


class RemoteAPIError(SandboxError):
    """Raised when the remote API returns an error."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"API error (status {status_code}): {message}")
