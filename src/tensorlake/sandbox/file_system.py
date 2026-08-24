"""Compatibility helpers for Artifact Storage filesystems.

New code should use :class:`tensorlake.filesystem.FilesystemClient` directly.
These helpers retain the original create/list/delete API while routing it to
the current Artifact Storage implementation.

Once registered, mount a file system into a sandbox at boot via
``Sandbox.create(file_systems=[...])``, include it in a warm-pool claim, or
attach it to a running sandbox with
:meth:`tensorlake.sandbox.Sandbox.attach_file_system`.
"""

from __future__ import annotations

from .exceptions import SandboxError
from .models import FileSystem


def _filesystem_client():
    # Lazy to avoid a package-initialization cycle through tensorlake.sandbox.
    from tensorlake.filesystem import FilesystemClient

    return FilesystemClient()


def create_file_system(name: str, description: str | None = None) -> FileSystem:
    """Create an Artifact Storage filesystem for the API key's project.

    ``description`` is retained for source compatibility but Artifact Storage
    filesystems do not currently persist descriptions.

    Args:
        name: Human-readable file system name.
        description: Optional description.

    Returns:
        The registered :class:`FileSystem`.

    Raises:
        TypeError: ``name`` is not a non-empty string.
        SandboxError: Credentials or project context are missing, or the
            request failed.
    """
    if not isinstance(name, str) or not name:
        raise TypeError("name must be a non-empty string")

    try:
        filesystem = _filesystem_client().create(name)
    except Exception as e:
        raise SandboxError(f"{type(e).__name__}: {e}") from e
    return FileSystem(
        id=filesystem.name,
        name=filesystem.name,
        description=description,
        status="ready",
    )


def list_file_systems() -> list[FileSystem]:
    """List Artifact Storage filesystems for the API key's project.

    Returns:
        The registered file systems as a list of :class:`FileSystem`.

    Raises:
        SandboxError: Credentials or project context are missing, or the
            request failed.
    """
    try:
        filesystems = _filesystem_client().list()
    except Exception as e:
        raise SandboxError(f"{type(e).__name__}: {e}") from e
    return [
        FileSystem(id=filesystem.name, name=filesystem.name, status=filesystem.status)
        for filesystem in filesystems
    ]


def delete_file_system(file_system_id: str) -> None:
    """Delete an Artifact Storage filesystem by name.

    Args:
        file_system_id: The registered file system's id.

    Raises:
        TypeError: ``file_system_id`` is not a non-empty string.
        SandboxError: Credentials or project context are missing, or the
            request failed.
    """
    if not isinstance(file_system_id, str) or not file_system_id:
        raise TypeError("file_system_id must be a non-empty string")

    try:
        _filesystem_client().delete(file_system_id)
    except Exception as e:
        raise SandboxError(f"{type(e).__name__}: {e}") from e
