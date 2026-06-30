"""Project-scoped file system registry operations.

File systems are managed through the platform API (the same
environment-based Tensorlake auth and organization/project scope as sandbox
images). Register one with :func:`create_file_system`, list them with
:func:`list_file_systems`, and remove one with
:func:`delete_file_system`.

Once registered, mount a file system into a sandbox at boot via
``Sandbox.create(file_systems=[...])`` or attach it to a running sandbox
with :meth:`tensorlake.sandbox.Sandbox.attach_file_system`.
"""

from __future__ import annotations

import json
from typing import Any

from tensorlake._tracing import USER_AGENT
from tensorlake.cli._common import Context, build_context_from_env

from .exceptions import SandboxError
from .models import FileSystem


def _require_project_context() -> Context:
    """Resolve and validate the auth + project context, or raise."""
    ctx = build_context_from_env()
    token = ctx.api_key or ctx.personal_access_token
    if not token:
        raise SandboxError("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.")
    if not ctx.organization_id or not ctx.project_id:
        raise SandboxError(
            "File system operations require organization and project context "
            "(TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID)."
        )
    return ctx


def _cloud_api_client(ctx: Context) -> Any:
    try:
        from tensorlake._cloud_sdk import CloudApiClient
    except ImportError:
        from _cloud_sdk import CloudApiClient

    return CloudApiClient(
        api_url=ctx.api_url,
        api_key=ctx.api_key or ctx.personal_access_token,
        organization_id=ctx.organization_id,
        project_id=ctx.project_id,
        namespace=ctx.namespace,
        user_agent=USER_AGENT,
    )


def create_file_system(name: str, description: str | None = None) -> FileSystem:
    """Register a new file system for the current project.

    Uses the same environment-based Tensorlake auth as sandbox images, and
    requires organization/project context (``TENSORLAKE_ORGANIZATION_ID`` and
    ``TENSORLAKE_PROJECT_ID``).

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

    ctx = _require_project_context()
    client = _cloud_api_client(ctx)
    try:
        result_json = client.create_file_system(
            ctx.organization_id, ctx.project_id, name, description
        )
    except Exception as e:
        raise SandboxError(f"{type(e).__name__}: {e}") from e
    finally:
        client.close()

    return FileSystem.model_validate_json(result_json)


def list_file_systems() -> list[FileSystem]:
    """List all registered file systems for the current project.

    Uses the same environment-based Tensorlake auth as sandbox images, and
    requires organization/project context (``TENSORLAKE_ORGANIZATION_ID`` and
    ``TENSORLAKE_PROJECT_ID``).

    Returns:
        The registered file systems as a list of :class:`FileSystem`.

    Raises:
        SandboxError: Credentials or project context are missing, or the
            request failed.
    """
    ctx = _require_project_context()
    client = _cloud_api_client(ctx)
    try:
        result_json = client.list_file_systems(ctx.organization_id, ctx.project_id)
    except Exception as e:
        raise SandboxError(f"{type(e).__name__}: {e}") from e
    finally:
        client.close()

    if not result_json:
        return []
    return [FileSystem.model_validate(item) for item in json.loads(result_json)]


def delete_file_system(file_system_id: str) -> None:
    """Delete a registered file system by its id (e.g. ``file_system_...``).

    Uses the same environment-based Tensorlake auth as sandbox images, and
    requires organization/project context (``TENSORLAKE_ORGANIZATION_ID`` and
    ``TENSORLAKE_PROJECT_ID``).

    Args:
        file_system_id: The registered file system's id.

    Raises:
        TypeError: ``file_system_id`` is not a non-empty string.
        SandboxError: Credentials or project context are missing, or the
            request failed.
    """
    if not isinstance(file_system_id, str) or not file_system_id:
        raise TypeError("file_system_id must be a non-empty string")

    ctx = _require_project_context()
    client = _cloud_api_client(ctx)
    try:
        client.delete_file_system(ctx.organization_id, ctx.project_id, file_system_id)
    except Exception as e:
        raise SandboxError(f"{type(e).__name__}: {e}") from e
    finally:
        client.close()
