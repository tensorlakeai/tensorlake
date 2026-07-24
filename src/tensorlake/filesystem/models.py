"""Pydantic models for filesystem operations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class FilesystemInfo(BaseModel):
    """One filesystem as returned by listing/point-read endpoints."""

    model_config = ConfigDict(extra="ignore")

    name: str
    full_name: str = ""
    status: str = ""
    kind: str = "filesystem"


class FilesystemStatus(BaseModel):
    """Remote status of a filesystem: identity plus current head."""

    model_config = ConfigDict(extra="ignore")

    name: str
    status: str = ""
    #: Current native version id (None for an empty filesystem that has never
    #: been written to).
    version_id: Optional[str] = None
    #: Server-side movement counter for the live version; bumps whenever it
    #: advances. None on servers that do not report it.
    generation: Optional[int] = None


class FileEntry(BaseModel):
    """One directory entry from a filesystem listing."""

    model_config = ConfigDict(extra="ignore")

    name: str
    #: Stable native content identity for the file or directory.
    content_id: str = ""
    #: Native entry kind.
    kind: Literal["file", "directory", "symlink"] = "file"
    #: Whether a regular file is executable.
    executable: bool = False
    #: Blob size in bytes when cheaply known server-side.
    size: Optional[int] = None
    #: Path of the entry relative to the filesystem root.
    path: str = ""

    @property
    def is_dir(self) -> bool:
        return self.kind == "directory"

    @property
    def is_symlink(self) -> bool:
        return self.kind == "symlink"


class FilesystemVersion(BaseModel):
    """The durable live version produced by one atomic publication."""

    model_config = ConfigDict(extra="ignore")

    #: Native version id — pass as ``version=`` to read this point in time.
    version_id: str
    #: Version replaced by this publication, or None for the first publication.
    previous_version_id: Optional[str] = None
    #: False when the write was a no-op (content identical to the parent).
    created: bool = True
    message: str = ""


class FilesystemSnapshot(BaseModel):
    """One newly retained native filesystem snapshot."""

    model_config = ConfigDict(extra="ignore")

    id: str
    message: str = ""


class FilesystemSnapshotInfo(BaseModel):
    """One explicitly retained native filesystem snapshot."""

    model_config = ConfigDict(extra="ignore")

    id: str
    created_at: datetime
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
