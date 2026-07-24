"""Client for Tensorlake filesystems.

A filesystem is a durable, versioned file tree that lives in Tensorlake
Cloud. Every write produces a
:class:`~tensorlake.filesystem.models.FilesystemVersion`, files can be read
at any version, and a filesystem can
be mounted to a local path through the ``tl`` CLI's FUSE/FSKit daemon.

Reads and writes are served by the shared Rust cloud-sdk core. Writes split
large files into bounded checksum-addressed parts, upload missing bytes
directly to object storage, and atomically publish the resulting metadata.
Payload bytes never pass through the Tensorlake API service.

Example::

    from tensorlake.filesystem import FilesystemClient

    client = FilesystemClient()          # env-based auth
    fs = client.create("my-data")
    fs.write_file("docs/hello.txt", b"hi")
    print(fs.read_file("docs/hello.txt"))
    snapshot = fs.snapshot("after first write")

    mount = fs.mount("/mnt/my-data")     # requires the `tl` CLI
    ...                                   # use it as a normal directory
    mount.unmount()
"""

from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone
from os import PathLike
from typing import Dict, Iterable, List, Optional, Union

from tensorlake.cli._common import build_context_from_env

from ._cli import FsCli
from ._native import NativeFilesystems
from .exceptions import FilesystemAPIError, FilesystemError, FilesystemNotFoundError
from .models import (
    FileEntry,
    FilesystemInfo,
    FilesystemSnapshot,
    FilesystemSnapshotInfo,
    FilesystemStatus,
    FilesystemVersion,
    MountStatus,
)

_FILESYSTEM_KIND = "filesystem"
_FileData = Union[bytes, str]


def _to_bytes(data: _FileData) -> bytes:
    return data.encode("utf-8") if isinstance(data, str) else data


def mount_status_from_raw(raw: dict, local_path: Optional[str] = None) -> MountStatus:
    """Map one ``tl fs status --json`` payload to a :class:`MountStatus`.

    ``mounted`` honors key presence (an explicit null means "not mounted"),
    and path/filesystem fall through empty strings, not just missing keys.
    The TypeScript SDK mirrors these exact semantics.
    """
    return MountStatus(
        path=str(raw.get("path") or raw.get("mount_path") or local_path or ""),
        filesystem=raw.get("filesystem") or raw.get("file_system") or None,
        mounted=bool(raw.get("mounted", raw.get("active", True))),
        raw=raw,
    )


class FilesystemClient:
    """Manages the filesystems of one Tensorlake project."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """Create a client.

        Any argument left as ``None`` is resolved from the environment
        (``TENSORLAKE_API_KEY`` / ``TENSORLAKE_PAT``, ``TENSORLAKE_API_URL``,
        ``TENSORLAKE_ORGANIZATION_ID``, ``TENSORLAKE_PROJECT_ID``).
        """
        ctx = build_context_from_env()
        token = api_key or ctx.api_key or ctx.personal_access_token
        if not token:
            raise FilesystemError(
                "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
            )
        organization_id = organization_id or ctx.organization_id
        project_id = project_id or ctx.project_id
        if not organization_id or not project_id:
            raise FilesystemError(
                "Filesystem operations require organization and project context "
                "(TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID)."
            )
        self._native = NativeFilesystems(
            api_url=api_url or ctx.api_url,
            bearer_token=token,
            organization_id=organization_id,
            project_id=project_id,
        )
        self._cli = FsCli(
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            api_url=api_url or ctx.api_url,
        )

    # -- lifecycle -----------------------------------------------------------

    def create(self, name: str) -> "Filesystem":
        """Create a new filesystem and return a handle to it."""
        if not name:
            raise FilesystemError("filesystem name must not be empty")
        default_branch = self._native.create_filesystem(name)
        return Filesystem(self, name, default_branch=default_branch)

    def get(self, name: str) -> "Filesystem":
        """Return a handle to an existing filesystem (verifies it exists)."""
        meta = self._native.filesystem_meta(name)
        if meta.get("kind", _FILESYSTEM_KIND) != _FILESYSTEM_KIND:
            raise FilesystemNotFoundError(name)
        return Filesystem(
            self, name, default_branch=meta.get("default_branch") or "main"
        )

    def fork(
        self, name: str, base_filesystem: str, snapshot: Optional[str] = None
    ) -> "Filesystem":
        """Create a metadata-only fork at a live head or retained snapshot."""
        if not name or not base_filesystem:
            raise FilesystemError("fork and base filesystem names must not be empty")
        default_branch = self._native.fork_filesystem(name, base_filesystem, snapshot)
        return Filesystem(self, name, default_branch=default_branch)

    def list(self) -> List[FilesystemInfo]:
        """List all filesystems in the project."""
        return [
            FilesystemInfo.model_validate(repo)
            for repo in self._native.list_filesystems()
        ]

    def delete(self, name: str) -> None:
        """Permanently delete a filesystem and all its snapshots."""
        self._native.delete_filesystem(name)

    # -- local mounts ----------------------------------------------------------

    def mount(self, name: str, local_path: str, readonly: bool = False) -> "Mount":
        """Mount a filesystem to a local path (requires the ``tl`` CLI)."""
        self._cli.mount(name, local_path, readonly)
        return Mount(self, name, local_path, readonly)

    def unmount(self, local_path: str, discard: bool = False) -> None:
        """Unmount a locally mounted filesystem.

        ``discard=True`` drops local changes that were not yet uploaded.
        """
        self._cli.unmount(local_path, discard=discard)

    def mount_status(self, local_path: Optional[str] = None) -> MountStatus:
        """Status of a local mount (defaults to the mount containing CWD)."""
        return mount_status_from_raw(self._cli.status(local_path), local_path)


class Filesystem:
    """Handle to one filesystem; reads/writes go through the Rust core."""

    def __init__(
        self,
        client: FilesystemClient,
        name: str,
        default_branch: Optional[str] = None,
    ):
        self._client = client
        self._native = client._native
        self._name = name
        self._default_branch = default_branch

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"Filesystem(name={self._name!r})"

    def _branch(self) -> str:
        """The filesystem's default branch — the target of every write and
        the default version of every read, so writes and ``status()`` can
        never silently disagree on a non-"main" filesystem."""
        if not self._default_branch:
            meta = self._native.filesystem_meta(self._name)
            self._default_branch = meta.get("default_branch") or "main"
        return self._default_branch

    # -- writes ---------------------------------------------------------------

    def write_file(
        self, path: str, data: _FileData, message: Optional[str] = None
    ) -> FilesystemVersion:
        """Write one file. Returns the durable live version produced."""
        return self.write_files({path: data}, message=message)

    def write_files(
        self,
        files: Dict[str, _FileData],
        message: Optional[str] = None,
        deletes: Iterable[str] = (),
    ) -> FilesystemVersion:
        """Write several files (and/or delete paths) in one atomic publication."""
        writes = [(path, _to_bytes(data)) for path, data in files.items()]
        delete_paths = list(deletes)
        if not writes and not delete_paths:
            raise FilesystemError("nothing to write: no files or deletions given")
        resolved_message = message or f"write {len(writes)} file(s) via SDK"
        return self._publish_changes(
            writes=writes,
            deletes=delete_paths,
            moves=[],
            copies=[],
            message=resolved_message,
        )

    def move_file(
        self, source: str, destination: str, message: Optional[str] = None
    ) -> FilesystemVersion:
        """Atomically move a file or directory without transferring its bytes."""
        return self._publish_changes(
            writes=[],
            deletes=[],
            moves=[(source, destination)],
            copies=[],
            message=message or f"move {source} to {destination} via SDK",
        )

    def write_file_from_path(
        self,
        path: str,
        local_path: Union[str, PathLike[str]],
        message: Optional[str] = None,
    ) -> FilesystemVersion:
        """Stream one local file directly to object storage in bounded parts."""
        return self.write_files_from_paths(
            {path: local_path},
            message=message,
        )

    def write_files_from_paths(
        self,
        files: Dict[str, Union[str, PathLike[str]]],
        message: Optional[str] = None,
    ) -> FilesystemVersion:
        """Atomically publish local files without loading them fully into memory."""
        writes = [(path, os.fspath(local_path)) for path, local_path in files.items()]
        if not writes:
            raise FilesystemError("nothing to write: no local files given")
        resolved_message = message or f"write {len(writes)} local file(s) via SDK"
        report = self._native.push_paths(
            self._name,
            files=writes,
            message=resolved_message,
            idempotency_key=secrets.token_hex(16),
            branch=self._branch(),
        )
        version_id = str(report.get("version_id") or "")
        previous_version_id = report.get("previous_version_id")
        return FilesystemVersion(
            version_id=version_id,
            previous_version_id=(
                str(previous_version_id) if previous_version_id is not None else None
            ),
            created=previous_version_id != version_id,
            message=resolved_message,
        )

    def copy_file(
        self, source: str, destination: str, message: Optional[str] = None
    ) -> FilesystemVersion:
        """Copy a file or directory by reusing its immutable content references."""
        return self._publish_changes(
            writes=[],
            deletes=[],
            moves=[],
            copies=[(source, destination)],
            message=message or f"copy {source} to {destination} via SDK",
        )

    def _publish_changes(
        self,
        *,
        writes: List[tuple[str, bytes]],
        deletes: List[str],
        moves: List[tuple[str, str]],
        copies: List[tuple[str, str]],
        message: str,
    ) -> FilesystemVersion:
        report = self._native.push_files(
            self._name,
            files=writes,
            deletes=deletes,
            moves=moves,
            copies=copies,
            message=message,
            # One key per logical write: a retried submit reattaches to the
            # same durable commit job instead of double-committing.
            idempotency_key=secrets.token_hex(16),
            branch=self._branch(),
        )
        version_id = str(report.get("version_id") or "")
        previous_version_id = report.get("previous_version_id")
        return FilesystemVersion(
            version_id=version_id,
            previous_version_id=(
                str(previous_version_id) if previous_version_id is not None else None
            ),
            created=previous_version_id != version_id,
            message=message,
        )

    def delete_file(
        self, path: str, message: Optional[str] = None
    ) -> FilesystemVersion:
        """Delete one file. Returns the durable live version produced."""
        return self.write_files(
            {}, message=message or f"delete {path} via SDK", deletes=[path]
        )

    def snapshot(self, message: str = "snapshot via SDK") -> FilesystemSnapshot:
        """Permanently retain the filesystem's current version.

        Writes publish durable live versions. This retains the current head
        permanently in one metadata-only server operation without changing
        or copying any content.
        """
        resolved_message = message or "snapshot via SDK"
        retained = self._native.retain_snapshot(
            self._name,
            resolved_message,
            secrets.token_hex(16),
        )
        return FilesystemSnapshot(
            id=retained.get("snapshot_id") or "",
            message=retained.get("message") or resolved_message,
        )

    def list_snapshots(self) -> List[FilesystemSnapshotInfo]:
        """List explicitly retained snapshots without reading file content."""
        return [
            FilesystemSnapshotInfo(
                id=str(snapshot.get("snapshot_id") or ""),
                created_at=datetime.fromtimestamp(
                    float(snapshot.get("created_at_ms") or 0) / 1000,
                    tz=timezone.utc,
                ),
                message=str(snapshot.get("message") or ""),
            )
            for snapshot in self._native.list_snapshots(self._name)
        ]

    def delete_snapshot(self, snapshot: str) -> None:
        """Delete one permanent retention point."""
        self._native.delete_snapshot(self._name, snapshot)

    # -- reads ------------------------------------------------------------------

    def read_file(self, path: str, version: Optional[str] = None) -> bytes:
        """Read a file's bytes at ``version`` (the current ``main`` head or
        a retained snapshot id); defaults to the filesystem's current head."""
        if not path.strip("/"):
            raise FilesystemError("file path must not be empty")
        return self._native.read_file(self._name, path, version or self._branch())

    def read_text(
        self, path: str, version: Optional[str] = None, encoding: str = "utf-8"
    ) -> str:
        """Read a file as text at ``version``."""
        return self.read_file(path, version).decode(encoding)

    def list_files(
        self, dir_path: str = "", version: Optional[str] = None
    ) -> List[FileEntry]:
        """List one directory (non-recursive) at ``version``."""
        prefix = dir_path.strip("/")
        entries = []
        for entry in self._native.list_tree(
            self._name, dir_path, version or self._branch()
        ):
            mode = int(entry.get("mode", 0o100644))
            name = str(entry["name"])
            entries.append(
                FileEntry(
                    name=name,
                    path=f"{prefix}/{name}" if prefix else name,
                    content_id=str(entry.get("oid") or ""),
                    kind=(
                        "directory"
                        if mode == 0o40000
                        else "symlink" if mode == 0o120000 else "file"
                    ),
                    executable=mode == 0o100755,
                    size=entry.get("size"),
                )
            )
        return entries

    # -- status -------------------------------------------------------------------

    def status(self) -> FilesystemStatus:
        """Remote status: identity plus the current native version."""
        meta = self._native.filesystem_meta(self._name)
        version_id: Optional[str] = None
        generation: Optional[int] = None
        default_branch = meta.get("default_branch") or "main"
        self._default_branch = default_branch
        try:
            ref = self._native.ref_status(self._name, default_branch)
            version_id = ref.get("resolved_commit") or ref.get("oid") or None
            generation = ref.get("generation")
        except FilesystemAPIError as e:
            # Only "no such ref yet" means an empty filesystem; anything else
            # (auth, 5xx) must not masquerade as one.
            if e.status_code != 404:
                raise
        return FilesystemStatus(
            name=self._name,
            status=meta.get("status", ""),
            version_id=version_id,
            generation=generation,
        )

    # -- mounts ---------------------------------------------------------------------

    def mount(self, local_path: str, readonly: bool = False) -> "Mount":
        """Mount this filesystem to a local path (requires the ``tl`` CLI)."""
        return self._client.mount(self._name, local_path, readonly)


class Mount:
    """A filesystem mounted to a local path via the ``tl`` CLI daemon."""

    def __init__(
        self,
        client: FilesystemClient,
        filesystem: str,
        local_path: str,
        readonly: bool,
    ):
        self._client = client
        self._filesystem = filesystem
        self._local_path = local_path
        self._readonly = readonly

    @property
    def filesystem(self) -> str:
        return self._filesystem

    @property
    def path(self) -> str:
        return self._local_path

    @property
    def readonly(self) -> bool:
        return self._readonly

    def __repr__(self) -> str:
        return f"Mount(filesystem={self._filesystem!r}, path={self._local_path!r})"

    def snapshot(self, message: Optional[str] = None) -> None:
        """Flush pending local changes into a durable snapshot."""
        self._client._cli.snapshot(self._local_path, message)

    def status(self) -> MountStatus:
        """Local mount status as reported by the mount daemon."""
        return self._client.mount_status(self._local_path)

    def unmount(self, discard: bool = False) -> None:
        """Unmount; ``discard=True`` drops changes not yet uploaded."""
        self._client.unmount(self._local_path, discard=discard)

    def __enter__(self) -> "Mount":
        return self

    def __exit__(self, *exc_info) -> None:
        self.unmount()
