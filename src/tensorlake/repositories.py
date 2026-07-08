"""Git repository APIs backed by the Rust cloud SDK."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from tensorlake.cloud_client import CloudClient


class RepositoryError(RuntimeError):
    """Raised when repository SDK configuration or operations fail."""


@dataclass(frozen=True)
class _EnvContext:
    api_url: str
    api_key: str | None
    personal_access_token: str | None
    organization_id: str | None
    project_id: str | None
    namespace: str


class _Model(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class Repository(_Model):
    name: str
    full_name: str = Field(alias="full_name")
    default_branch: str = Field(alias="default_branch")
    status: str


class RepositoryHandle(_Model):
    repo: str
    url: str
    trace_id: str | None = Field(default=None, alias="trace_id")
    base_repo: str | None = Field(default=None, alias="base_repo")


class GitRef(_Model):
    name: str
    oid: str


class Branch(_Model):
    name: str
    ref_name: str = Field(alias="ref_name")
    oid: str


class RepositoryInfo(_Model):
    repo: str
    url: str
    branches: list[Branch]
    refs: list[GitRef]


class GitCredential(_Model):
    token: str
    token_type: str = Field(alias="tokenType")
    expires_at: str = Field(alias="expiresAt")
    git_username: str = Field(alias="gitUsername")
    repo_pattern: str = Field(alias="repoPattern")
    scopes: list[str]


class CommitJobReadBack(_Model):
    done: int
    total: int


class CommitJobError(_Model):
    kind: str = ""
    message: str = ""
    retryable: bool = False


class CommitJobStatus(_Model):
    job_id: str = Field(alias="job_id")
    state: str
    phase: str | None = None
    read_back: CommitJobReadBack | None = Field(default=None, alias="read_back")
    commit: str | None = None
    tree: str | None = None
    ref_name: str | None = Field(default=None, alias="ref_name")
    parent: str | None = None
    created: bool | None = None
    error: CommitJobError | None = None


class PushReport(_Model):
    commit: str
    tree: str
    ref_name: str = Field(alias="ref_name")
    created: bool
    files: int
    bytes_total: int = Field(alias="bytes_total")
    chunks_total: int = Field(alias="chunks_total")
    chunks_uploaded: int = Field(alias="chunks_uploaded")
    bytes_uploaded: int = Field(alias="bytes_uploaded")
    file_blob_oids: list[tuple[str, str]] = Field(alias="file_blob_oids")


class MergeEntry(_Model):
    mode: int
    oid: str


class MergeConflict(_Model):
    path: str
    kind: str
    potential: bool = False
    ours: MergeEntry | None = None
    base: MergeEntry | None = None
    theirs: MergeEntry | None = None


class MergeStats(_Model):
    trees_read: int = Field(alias="trees_read")
    entries_compared: int = Field(alias="entries_compared")
    blobs_merged: int = Field(alias="blobs_merged")
    wall_ms: float = Field(alias="wall_ms")


class MergeReport(_Model):
    ours: str
    theirs: str
    merge_base: str | None = Field(default=None, alias="merge_base")
    clean: bool
    fast_forward: bool = Field(alias="fast_forward")
    already_merged: bool = Field(alias="already_merged")
    changed_paths: int = Field(alias="changed_paths")
    conflicts: list[MergeConflict]
    stats: MergeStats
    commit: str | None = None
    fast_forwarded: bool = Field(default=False, alias="fast_forwarded")


class ConflictTerm(_Model):
    mode: int
    oid: str


class ConflictPath(_Model):
    path: str
    kind: str
    terms: list[ConflictTerm | None]


class MergeConflictRecord(_Model):
    version: int
    ours_commit: str = Field(alias="ours_commit")
    theirs_commit: str = Field(alias="theirs_commit")
    base_commit: str | None = Field(default=None, alias="base_commit")
    paths: list[ConflictPath]
    truncated_paths: int = Field(default=0, alias="truncated_paths")


class OperationRef(_Model):
    name: str
    old: str | None = None
    new: str | None = None


class Operation(_Model):
    op_id: str = Field(alias="op_id")
    repo: str
    network: str | None = None
    parent_op_id: str | None = Field(default=None, alias="parent_op_id")
    actor: str
    at_secs: int = Field(alias="at_secs")
    kind: str
    result: str
    refs: list[OperationRef]
    pack_ids: list[str] = Field(alias="pack_ids")
    old_pack_ids: list[str] = Field(alias="old_pack_ids")
    related_repo: str | None = Field(default=None, alias="related_repo")
    status: str | None = None
    old_pack_count: int = Field(alias="old_pack_count")
    object_count: int = Field(alias="object_count")
    pack_bytes: int = Field(alias="pack_bytes")


def _load_json(raw: str) -> Any:
    return json.loads(raw) if raw else {}


def _non_empty_env(name: str) -> str | None:
    value = os.environ.get(name)
    return value or None


def _build_context_from_env() -> _EnvContext:
    return _EnvContext(
        api_url=_non_empty_env("TENSORLAKE_API_URL") or "https://api.tensorlake.ai",
        api_key=_non_empty_env("TENSORLAKE_API_KEY"),
        personal_access_token=_non_empty_env("TENSORLAKE_PAT"),
        organization_id=_non_empty_env("TENSORLAKE_ORGANIZATION_ID"),
        project_id=_non_empty_env("TENSORLAKE_PROJECT_ID"),
        namespace=_non_empty_env("INDEXIFY_NAMESPACE") or "default",
    )


def _extract_project_id(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None

    for key in ("project_id", "projectId"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value

    project = payload.get("project")
    if isinstance(project, str) and project:
        return project
    if isinstance(project, dict):
        for key in ("id", "project_id", "projectId"):
            value = project.get(key)
            if isinstance(value, str) and value:
                return value

    for key in ("scope", "api_key", "apiKey", "key"):
        found = _extract_project_id(payload.get(key))
        if found:
            return found

    projects = payload.get("projects")
    if isinstance(projects, list) and len(projects) == 1:
        return _extract_project_id(projects[0])

    return None


class RepositoryClient:
    """Client for Tensorlake Git repositories."""

    def __init__(
        self,
        api_url: str | None = None,
        api_key: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
    ):
        ctx = _build_context_from_env()
        token = api_key or ctx.api_key
        if not token:
            if ctx.personal_access_token:
                raise RepositoryError(
                    "Repository SDKs require TENSORLAKE_API_KEY. "
                    "Personal access tokens are CLI-only."
                )
            raise RepositoryError(
                "Missing TENSORLAKE_API_KEY credentials."
            )

        self._client = CloudClient(
            api_url=api_url or ctx.api_url,
            api_key=token,
            organization_id=None,
            project_id=None,
            namespace=ctx.namespace,
        )
        self.project_id = project_id or ctx.project_id or self._project_id_from_api_key()

    def _project_id_from_api_key(self) -> str:
        payload = _load_json(self._client.introspect_api_key_json())
        project_id = _extract_project_id(payload)
        if not project_id:
            raise RepositoryError("Repository API key did not include project context.")
        return project_id

    @classmethod
    def from_env(cls) -> "RepositoryClient":
        return cls()

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "RepositoryClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def url(self, repo: str) -> str:
        return self._client.git_repo_url(self.project_id, repo)

    def create(
        self,
        repo: str,
        default_branch: str | None = None,
    ) -> RepositoryHandle:
        raw = self._client.create_git_repo(self.project_id, repo, default_branch)
        return RepositoryHandle.model_validate(_load_json(raw))

    def list(self) -> list[Repository]:
        raw = self._client.list_git_repos(self.project_id)
        payload = _load_json(raw)
        return [Repository.model_validate(item) for item in payload.get("repos", [])]

    def delete(self, repo: str) -> None:
        self._client.delete_git_repo(self.project_id, repo)

    def fork(self, repo: str, base_repo: str) -> RepositoryHandle:
        raw = self._client.fork_git_repo(self.project_id, repo, base_repo)
        return RepositoryHandle.model_validate(_load_json(raw))

    def archive(self, repo: str) -> None:
        self._client.archive_git_repo(self.project_id, repo)

    def restore(self, repo: str) -> None:
        self._client.restore_git_repo(self.project_id, repo)

    def info(self, repo: str) -> RepositoryInfo:
        raw = self._client.git_repo_info(self.project_id, repo)
        return RepositoryInfo.model_validate(_load_json(raw))

    def branches(self, repo: str) -> list[Branch]:
        raw = self._client.list_git_branches(self.project_id, repo)
        payload = _load_json(raw)
        return [Branch.model_validate(item) for item in payload.get("branches", [])]

    def refs(self, repo: str) -> list[GitRef]:
        raw = self._client.list_git_refs(self.project_id, repo)
        payload = _load_json(raw)
        return [GitRef.model_validate(item) for item in payload.get("refs", [])]

    def delete_branch(self, repo: str, branch: str) -> None:
        self._client.delete_git_branch(self.project_id, repo, branch)

    def operations(self, repo: str) -> list[Operation]:
        raw = self._client.list_git_operations(self.project_id, repo)
        payload = _load_json(raw)
        return [Operation.model_validate(item) for item in payload.get("operations", [])]

    def credential(self, repo: str | None = None) -> GitCredential:
        raw = self._client.git_credential(self.project_id, repo)
        return GitCredential.model_validate(_load_json(raw))

    def commit_status(self, repo: str, job_id: str) -> CommitJobStatus:
        raw = self._client.git_commit_status(self.project_id, repo, job_id)
        return CommitJobStatus.model_validate(_load_json(raw))

    def push_worktree(
        self,
        repo: str,
        root: str | Path = ".",
        branch: str = "main",
        message: str = "Update repository",
        expect_oid: str | None = None,
    ) -> PushReport:
        raw = self._client.push_git_worktree(
            self.project_id,
            repo,
            str(root),
            branch,
            message,
            expect_oid,
        )
        return PushReport.model_validate(_load_json(raw))

    def merge(
        self,
        repo: str,
        ours: str,
        theirs: str,
        *,
        preflight: bool = False,
        deep: bool = False,
        materialize: bool = False,
        message: str | None = None,
        base: str | None = None,
    ) -> MergeReport:
        raw = self._client.merge_git_repo(
            self.project_id,
            repo,
            ours,
            theirs,
            preflight,
            deep,
            materialize,
            message,
            base,
        )
        return MergeReport.model_validate(_load_json(raw))

    def commit_conflicts(
        self,
        repo: str,
        commit: str,
    ) -> MergeConflictRecord | None:
        raw = self._client.git_commit_conflicts(self.project_id, repo, commit)
        payload = _load_json(raw)
        if payload is None:
            return None
        return MergeConflictRecord.model_validate(payload)


def client_from_env() -> RepositoryClient:
    return RepositoryClient.from_env()
