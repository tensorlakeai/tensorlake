"""Bridge to the Rust cloud-sdk core for filesystem operations.

All wire-protocol work (credential minting, chunked ingest with
content-defined chunking and dedup, commit-job polling, transient retries,
pagination) lives in the shared Rust `ArtifactStorageClient`, exposed through
the ``tensorlake._cloud_sdk`` native module. This bridge only builds the
native client and translates its exceptions into the filesystem exception
hierarchy.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple

from tensorlake._tracing import USER_AGENT

from .exceptions import (
    FileNotFoundInFilesystemError,
    FilesystemAPIError,
    FilesystemError,
    FilesystemNotFoundError,
)


def _native_module():
    try:
        from tensorlake import _cloud_sdk

        return _cloud_sdk
    except ImportError:
        import _cloud_sdk

        return _cloud_sdk


class NativeFilesystems:
    """Filesystem operations of one project, served by the Rust core."""

    def __init__(
        self,
        api_url: str,
        bearer_token: str,
        organization_id: str,
        project_id: str,
    ):
        module = _native_module()
        self._error_type = module.CloudApiClientError
        self._client = module.CloudApiClient(
            api_url=api_url,
            api_key=bearer_token,
            organization_id=organization_id,
            project_id=project_id,
            user_agent=USER_AGENT,
        )
        self._project_id = project_id

    @property
    def project_id(self) -> str:
        return self._project_id

    def _call(self, operation: Callable[[], Any], not_found: Optional[Exception]):
        """Run one native call, translating its exception on the way out.

        ``not_found`` is raised instead of the generic API error when the
        native call fails with a 404.
        """
        try:
            return operation()
        except self._error_type as e:
            args = getattr(e, "args", ())
            status = args[1] if len(args) > 1 else None
            message = str(args[2]) if len(args) > 2 else str(e)
            if status == 404 and not_found is not None:
                raise not_found from e
            if isinstance(status, int):
                raise FilesystemAPIError(status, message) from e
            raise FilesystemError(message) from e

    def create_filesystem(self, name: str) -> str:
        """Create the filesystem; returns its effective default branch.

        The branch differs from "main" only when a lost-response retry
        adopted a pre-existing filesystem inside the native binding.
        """
        raw = self._call(
            lambda: self._client.create_filesystem(self._project_id, name),
            not_found=None,
        )
        return str(json.loads(raw).get("default_branch") or "main")

    def filesystem_meta(self, name: str) -> Dict[str, Any]:
        raw = self._call(
            lambda: self._client.filesystem_meta(self._project_id, name),
            not_found=FilesystemNotFoundError(name),
        )
        return json.loads(raw)

    def list_filesystems(self) -> List[Dict[str, Any]]:
        raw = self._call(
            lambda: self._client.list_filesystems(self._project_id),
            not_found=None,
        )
        return json.loads(raw).get("repos", [])

    def delete_filesystem(self, name: str) -> None:
        self._call(
            lambda: self._client.delete_filesystem(self._project_id, name),
            not_found=FilesystemNotFoundError(name),
        )

    def ref_status(self, name: str, refspec: str) -> Dict[str, Any]:
        # A 404 here means "no such ref yet" (an empty filesystem), not a
        # missing filesystem — surface it as an API error for the caller to
        # interpret, never as FilesystemNotFoundError.
        raw = self._call(
            lambda: self._client.filesystem_ref_status(self._project_id, name, refspec),
            not_found=None,
        )
        return json.loads(raw)

    def read_file(self, name: str, path: str, version: str) -> bytes:
        return bytes(
            self._call(
                lambda: self._client.read_filesystem_file(
                    self._project_id, name, path, version
                ),
                not_found=FileNotFoundInFilesystemError(name, path),
            )
        )

    def list_tree(self, name: str, dir_path: str, version: str) -> List[Dict[str, Any]]:
        raw = self._call(
            lambda: self._client.list_filesystem_tree(
                self._project_id, name, dir_path, version
            ),
            not_found=FileNotFoundInFilesystemError(name, dir_path or "/"),
        )
        return json.loads(raw).get("entries", [])

    def push_files(
        self,
        name: str,
        files: List[Tuple[str, bytes]],
        deletes: List[str],
        message: str,
        idempotency_key: str,
        branch: str = "main",
    ) -> Dict[str, Any]:
        raw = self._call(
            lambda: self._client.push_filesystem_files(
                self._project_id,
                name,
                files,
                deletes,
                message,
                branch,
                idempotency_key,
            ),
            not_found=FilesystemNotFoundError(name),
        )
        return json.loads(raw)
