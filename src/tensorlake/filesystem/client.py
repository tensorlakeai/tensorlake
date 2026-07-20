"""Client for Tensorlake filesystems.

A filesystem is a durable, versioned file tree that lives in Tensorlake
Cloud. Every write produces a :class:`~tensorlake.filesystem.models.Snapshot`
(a durable version), files can be read at any version, and a filesystem can
be mounted to a local path through the ``tl`` CLI's FUSE/FSKit daemon.

Reads and writes are served by the shared Rust cloud-sdk core (the same
engine behind the ``tl`` CLI), so uploads get content-defined chunking,
dedup, transient retries, and idempotent commit reattachment for free.

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

import secrets
from typing import Dict, Iterable, List, Optional, Union

from tensorlake.cli._common import build_context_from_env

from ._cli import FsCli
from ._native import NativeFilesystems
from .exceptions import FilesystemAPIError, FilesystemError, FilesystemNotFoundError
from .models import (
    FileEntry,
    FilesystemInfo,
    FilesystemStatus,
    MountStatus,
    Snapshot,
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
    ) -> Snapshot:
        """Write one file. Returns the snapshot (version) the write produced."""
        return self.write_files({path: data}, message=message)

    def write_files(
        self,
        files: Dict[str, _FileData],
        message: Optional[str] = None,
        deletes: Iterable[str] = (),
    ) -> Snapshot:
        """Write several files (and/or delete paths) in one atomic snapshot."""
        writes = [(path, _to_bytes(data)) for path, data in files.items()]
        delete_paths = list(deletes)
        if not writes and not delete_paths:
            raise FilesystemError("nothing to write: no files or deletions given")
        resolved_message = message or f"write {len(writes)} file(s) via SDK"
        report = self._native.push_files(
            self._name,
            files=writes,
            deletes=delete_paths,
            message=resolved_message,
            # One key per logical write: a retried submit reattaches to the
            # same durable commit job instead of double-committing.
            idempotency_key=secrets.token_hex(16),
            branch=self._branch(),
        )
        return Snapshot(
            commit=report.get("commit") or "",
            tree=report.get("tree") or "",
            ref_name=report.get("ref_name") or "",
            created=bool(report.get("created", True)),
            message=resolved_message,
        )

    def delete_file(self, path: str, message: Optional[str] = None) -> Snapshot:
        """Delete one file. Returns the snapshot the deletion produced."""
        return self.write_files(
            {}, message=message or f"delete {path} via SDK", deletes=[path]
        )

    def snapshot(self, message: str = "") -> Snapshot:
        """Return the filesystem's current version as a snapshot.

        Writes already create snapshots implicitly; this pins the current
        head without changing any content.
        """
        status = self.status()
        if not status.head_commit:
            raise FilesystemError(
                f"filesystem {self._name} is empty: write files first"
            )
        return Snapshot(
            commit=status.head_commit,
            ref_name=f"refs/heads/{status.default_branch}",
            created=False,
            message=message,
        )

    # -- reads ------------------------------------------------------------------

    def read_file(self, path: str, version: Optional[str] = None) -> bytes:
        """Read a file's bytes at ``version`` (branch, ref, or snapshot
        commit; defaults to the filesystem's default branch)."""
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
            model = FileEntry.model_validate(entry)
            model.path = f"{prefix}/{model.name}" if prefix else model.name
            entries.append(model)
        return entries

    # -- status -------------------------------------------------------------------

    def status(self) -> FilesystemStatus:
        """Remote status: identity plus the current head snapshot."""
        meta = self._native.filesystem_meta(self._name)
        head_commit: Optional[str] = None
        generation: Optional[int] = None
        default_branch = meta.get("default_branch") or "main"
        self._default_branch = default_branch
        try:
            ref = self._native.ref_status(self._name, default_branch)
            head_commit = ref.get("resolved_commit") or ref.get("oid") or None
            generation = ref.get("generation")
        except FilesystemAPIError as e:
            # Only "no such ref yet" means an empty filesystem; anything else
            # (auth, 5xx) must not masquerade as one.
            if e.status_code != 404:
                raise
        return FilesystemStatus(
            name=self._name,
            status=meta.get("status", ""),
            default_branch=default_branch,
            head_commit=head_commit,
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
