"""Pydantic models for filesystem operations."""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

# Git tree entry mode for a directory (0o40000 rendered by the server as an int).
_GIT_MODE_DIR = 0o40000
_GIT_MODE_SYMLINK = 0o120000


class FilesystemInfo(BaseModel):
    """One filesystem as returned by listing/point-read endpoints."""

    model_config = ConfigDict(extra="ignore")

    name: str
    full_name: str = ""
    default_branch: str = "main"
    status: str = ""
    kind: str = "filesystem"


class FilesystemStatus(BaseModel):
    """Remote status of a filesystem: identity plus current head."""

    model_config = ConfigDict(extra="ignore")

    name: str
    status: str = ""
    default_branch: str = "main"
    #: Commit hash the default branch currently points at (None for an empty
    #: filesystem that has never been written to).
    head_commit: Optional[str] = None
    #: Server-side movement counter for the default branch; bumps whenever the
    #: head advances. None on servers that do not report it.
    generation: Optional[int] = None


class FileEntry(BaseModel):
    """One directory entry from a filesystem listing."""

    model_config = ConfigDict(extra="ignore")

    name: str
    #: Git blob/tree object id.
    oid: str = ""
    #: Raw git mode (0o100644 file, 0o100755 executable, 0o120000 symlink,
    #: 0o40000 directory).
    mode: int = 0o100644
    #: Blob size in bytes when cheaply known server-side.
    size: Optional[int] = None
    #: Path of the entry relative to the filesystem root.
    path: str = ""

    @property
    def is_dir(self) -> bool:
        return self.mode == _GIT_MODE_DIR

    @property
    def is_symlink(self) -> bool:
        return self.mode == _GIT_MODE_SYMLINK


class Snapshot(BaseModel):
    """A durable version of the filesystem (a commit)."""

    model_config = ConfigDict(extra="ignore")

    #: Commit hash — pass as ``version=`` to read the filesystem at this point.
    commit: str
    tree: str = ""
    ref_name: str = ""
    parent: Optional[str] = None
    #: False when the write was a no-op (content identical to the parent).
    created: bool = True
    message: str = ""


class MountStatus(BaseModel):
    """Status of a local mount as reported by ``tl fs status --json``.

    The mount daemon's JSON is versioned independently of this SDK, so only
    stable fields are typed; the full payload is preserved in :attr:`raw`.
    """

    model_config = ConfigDict(extra="ignore")

    #: Local mount path.
    path: str = ""
    #: Filesystem name this mount serves, when reported.
    filesystem: Optional[str] = None
    #: Whether the daemon reports the mount as healthy/active.
    mounted: bool = False
    #: Complete parsed JSON payload from the CLI.
    raw: Dict[str, Any] = Field(default_factory=dict)
