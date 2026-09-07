"""Exception hierarchy for sandbox operations."""

import json
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


def _format_error_details(error_details: object | None) -> str | None:
    if error_details is None:
        return None
    if isinstance(error_details, str):
        detail = error_details.strip()
        return detail or None
    if isinstance(error_details, dict):
        for key in ("message", "detail", "error", "reason"):
            value = error_details.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        if error_details:
            return json.dumps(error_details, sort_keys=True)
        return None
    if isinstance(error_details, list):
        parts = [
            formatted
            for item in error_details
            if (formatted := _format_error_details(item)) is not None
        ]
        if parts:
            return "; ".join(parts)
        return json.dumps(error_details)
    return str(error_details)


class RemoteAPIError(SandboxError):
    """Raised when the remote API returns an error."""

    def __init__(self, status_code: int, message: str):
        self._status_code = status_code
        self._message = message
        self._sandbox_id: str | None = None
        self._reason: str | None = None
        self._error_details: object | None = None
        display_message = message
        try:
            payload = json.loads(message)
        except (ValueError, TypeError):
            payload = None
        # Only interpret sandbox lifecycle responses. Keep other API errors intact.
        if (
            isinstance(payload, dict)
            and isinstance(payload.get("sandbox_id"), str)
            and payload["sandbox_id"].strip()
            and payload.get("status") in ("failed", "terminated")
        ):
            self._sandbox_id = payload["sandbox_id"]
            reason = payload.get("reason") or payload.get("termination_reason")
            self._reason = reason if isinstance(reason, str) else None
            self._error_details = payload.get("error_details")
            display_message = f"Sandbox {self._sandbox_id} {payload['status']}"
            if self._reason:
                display_message += f" ({self._reason})"
            detail = _format_error_details(self._error_details)
            if detail:
                display_message += f": {detail}"
        super().__init__(f"API error (status {status_code}): {display_message}")

    @property
    def sandbox_id(self) -> str | None:
        """Sandbox that failed, when this is a sandbox lifecycle response."""
        return self._sandbox_id

    @property
    def reason(self) -> str | None:
        """Server reason, such as ConfigurationError; unknown reasons are preserved."""
        return self._reason

    @property
    def error_details(self) -> object | None:
        """Original diagnostic supplied by the server, without text parsing."""
        return self._error_details

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def message(self) -> str:
        return self._message
