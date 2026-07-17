//! Workspace client: private durable refs (`refs/workspaces/<id>`) on artifact storage.
//!
//! A workspace is the unit of ongoing work (artifact_storage issue #24): created from a base
//! commit, advanced by snapshots (ordinary commits on the workspace ref, uploaded as CDC chunks
//! through the resumable-ingest pipeline), and published by promoting a snapshot onto a real
//! branch (squash by default). Workspaces are durable scratch pads — they survive crashes,
//! timeouts, and unmounts, and die only by explicit deletion. All calls authenticate with a
//! minted git credential, like the repo/commit APIs.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::Traced;
use crate::error::SdkError;

use super::ingest::expect_json;
use super::merge::{MergeConflict, MergeReport, MergeStats, Signature, expect_json_or_conflict};
use super::{ArtifactStorageClient, decode_empty};

/// One workspace joined across its identity record, ref, and lease rows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceInfo {
    pub id: String,
    pub ref_name: String,
    /// Principal bound at creation; empty for open-mode workspaces.
    pub principal: String,
    /// Base commit (hex) the workspace was created from.
    pub base: String,
    /// The ref the base was resolved from, when created from one; display-only.
    pub base_ref: Option<String>,
    /// Current snapshot tip (hex); equals `base` until the first snapshot.
    pub head: String,
    pub created_at_secs: u64,
    pub lease_secs: u64,
    /// Epoch-ms lease expiry. `None` means pinned: never expires.
    pub lease_due_ms: Option<u64>,
    pub pinned: bool,
    /// Shared-rw mode: every snapshot is automatically reconciled into this branch (short
    /// name), in server-assigned order.
    #[serde(default)]
    pub shared_target: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateWorkspaceRequest {
    /// Base commit-ish (branch, full ref, tag, or commit hex). Defaults to the repo's HEAD.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// Shared-rw mode: reconcile every snapshot into this branch (short name) in server order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_target: Option<String>,
    /// Shared-rw conflict handling: `"materialize"` (default — three-way merge, diff3 markers
    /// on conflicts) or `"lww"` (legacy last-writer-wins overwrite).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconcile_policy: Option<String>,
    /// Which product surface this session belongs to: `"filesystem"` makes the SERVER apply
    /// filesystem semantics from the repo's authoritative kind (publish-on-save defaults;
    /// kind=repository rejected with the right command). Requires a server with repo
    /// surface support (issue #103); older servers ignore the field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface: Option<String>,
    /// The session will never write (a read-only mount's anchor): the surface kind check
    /// still applies, publish defaults are skipped.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkspaceHeartbeat {
    /// New lease expiry (epoch ms); absent when the workspace is pinned.
    pub lease_due_ms: Option<u64>,
    pub pinned: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PromoteWorkspaceRequest {
    /// Target branch (short name; `refs/heads/` implied).
    pub branch: String,
    /// Expected target branch head; defaults to the observed head (CAS either way).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expect_oid: Option<String>,
    /// Land the full checkpoint chain instead of the default squash.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub full_history: bool,
    /// `"squash"` (default) | `"full_history"` | `"merge"`. Takes precedence over the legacy
    /// `full_history` flag. `merge` lands a two-parent merge commit when the target moved
    /// since the workspace forked; conflicts come back as `PromoteOutcome::Conflicted`
    /// (`fail` policy — the workspace is the resolution surface).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<Signature>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromoteWorkspaceResponse {
    pub commit: String,
    pub ref_name: String,
    pub created: bool,
    pub squashed: bool,
    /// Merge-mode promote landed a merge (or found nothing to do).
    #[serde(default)]
    pub merged: bool,
    /// Merge mode advanced the ref to the workspace tip with no new commit object.
    #[serde(default)]
    pub fast_forwarded: bool,
}

/// A merge-mode promote either lands or reports why it can't — conflicts are a normal outcome,
/// not a transport error (the server's `409` report body).
#[derive(Clone, Debug)]
pub enum PromoteOutcome {
    Promoted(PromoteWorkspaceResponse),
    /// `fail`-policy conflicts: nothing was published; the target branch and the workspace are
    /// both untouched. Sync the workspace, resolve, and promote again.
    Conflicted(MergeReport),
}

/// Ref-level promote preflight: did the target move since the workspace forked?
/// (Tree-level conflict prediction is `repo_merge` in preflight mode.)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromotePreflight {
    pub target_ref: String,
    /// Absent when the target branch does not exist yet.
    pub target_head: Option<String>,
    pub workspace_base: String,
    pub workspace_head: String,
    pub moved: bool,
    pub fast_forward: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SyncWorkspaceRequest {
    /// Branch to pull from (short name). Defaults to the workspace's shared-rw target, else
    /// the ref it was created from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// `"materialize"` (default — conflicts land as diff3 markers in the workspace, with the
    /// commit's structured conflict record) or `"fail"` (a conflicted sync changes nothing and
    /// returns the report).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<Signature>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncWorkspaceResponse {
    /// The workspace ref after the sync (unchanged when `up_to_date`, or when a
    /// `fail`-policy sync hit conflicts).
    pub workspace_head: String,
    pub target_head: String,
    /// The workspace's fork point after the sync.
    pub base: String,
    pub clean: bool,
    pub up_to_date: bool,
    /// The workspace had no snapshots: its ref moved to the target head with no new commit.
    pub fast_forwarded: bool,
    pub changed_paths: u64,
    pub conflicts: Vec<MergeConflict>,
    pub stats: MergeStats,
}

/// Result of a non-history-rewriting view refresh or pristine-workspace target switch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewSyncWorkspaceResponse {
    pub workspace_head: String,
    pub target_head: String,
    pub base: String,
    pub target_ref: Option<String>,
    pub changed: bool,
}

/// Server-resolved source for a lazy repository mount. The canonical ref is returned by the
/// server so clients never reinterpret a tag as a branch while following it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitMountSource {
    pub format_ver: u16,
    /// `branch` | `tag` | `commit`.
    pub kind: String,
    /// `follow` for canonical branch/tag refs; `pinned` for full commits.
    pub follow_policy: String,
    #[serde(default)]
    pub canonical_ref: Option<String>,
    pub resolved_commit: String,
    #[serde(default)]
    pub subtree: Option<String>,
    pub root_tree: String,
}

/// Expiring liveness record for a read-only repository mount. Presence never roots repository
/// history; expiry is authoritative when a sandbox disappears without unmounting.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitMountPresence {
    pub format_ver: u16,
    pub session_id: String,
    pub principal: String,
    pub source: GitMountSource,
    pub mounted_on: String,
    pub started_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecordGitMountPresenceRequest<'a> {
    pub source: &'a GitMountSource,
    pub mounted_on: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitMountPresencePage {
    pub mounts: Vec<GitMountPresence>,
    pub truncated: bool,
    #[serde(default)]
    pub next_after: Option<String>,
}

/// The explicit history-rewriting workspace operation. `sync` is reserved for refreshing or
/// switching a view when no workspace snapshots would be rewritten.
pub type RebaseWorkspaceRequest = SyncWorkspaceRequest;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RebaseWorkspaceResponse {
    #[serde(flatten)]
    pub result: SyncWorkspaceResponse,
    /// Retained server ref for the replaced chain. Absent for an up-to-date rebase.
    #[serde(default)]
    pub recovery_ref: Option<String>,
}

/// One durable commit in a workspace's active or retained chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitWorkspaceLogEntry {
    pub oid: String,
    pub subject: String,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub operation: Option<String>,
    pub at_ms: u64,
    #[serde(default)]
    pub conflicted: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitRetainedChain {
    pub recovery_ref: String,
    pub head: String,
    pub base: String,
    pub created_at_ms: u64,
    pub reason: String,
    pub retention: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitWorkspaceLogPage {
    pub format_ver: u16,
    pub workspace_id: String,
    #[serde(default)]
    pub active_chain: Vec<GitWorkspaceLogEntry>,
    #[serde(default)]
    pub retained_chains: Vec<GitRetainedChain>,
    pub truncated: bool,
    #[serde(default)]
    pub next_after: Option<String>,
}

/// One bounded node/edge page used by repository and project-fleet smartlog views.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitSmartlogNode {
    pub id: String,
    pub kind: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub oid: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub timestamp_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitSmartlogEdge {
    pub from: String,
    pub to: String,
    pub kind: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitSmartlogPage {
    pub format_ver: u16,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub nodes: Vec<GitSmartlogNode>,
    #[serde(default)]
    pub edges: Vec<GitSmartlogEdge>,
    pub truncated: bool,
    #[serde(default)]
    pub next_after: Option<String>,
}

/// One ref's head and movement generation — the poll target for branch/workspace following.
#[derive(Clone, Debug, Deserialize)]
pub struct RefStatus {
    pub ref_name: String,
    /// Raw ref target (hex); for annotated tags this is the tag-object oid. Absent when deleted.
    pub oid: Option<String>,
    /// Commit reached by peeling the ref target. New servers populate this for every live ref;
    /// older servers omit it, so branch/lightweight-tag clients may fall back to `oid`.
    #[serde(default)]
    pub resolved_commit: Option<String>,
    /// Movement counter: bumps on every write or delete of the ref. 0 = never written.
    pub generation: u64,
}

/// One directory page from the paged tree listing.
#[derive(Clone, Debug, Deserialize)]
pub struct TreePage {
    pub entries: Vec<TreeEntry>,
    pub truncated: bool,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub oid: String,
    /// Raw git mode (`0o100644`, `0o100755`, `0o120000`, `0o40000` for directories).
    pub mode: u32,
    /// Blob size when cheaply known server-side.
    pub size: Option<u64>,
}

/// One row of the project-scope workspace fleet listing
/// (`GET /project/{project}/workspaces`, artifact_storage issue #56).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceFleetItem {
    pub id: String,
    /// Full repo id (`{project}/{repo}`).
    pub repo: String,
    /// `git` for repository workspaces, `native` for filesystem workspaces. Defaults to `git`
    /// against servers predating native fleet rows.
    #[serde(default = "default_workspace_storage")]
    pub storage: String,
    /// Derived server-side at read time: `live` | `gap` | `idle` | `detached`.
    pub status: String,
    /// `default` | `shared-rw`.
    pub mode: String,
    /// Creating principal; absent for unbound (open-mode) workspaces.
    #[serde(default)]
    pub created_by: Option<WorkspaceActor>,
    /// Base commit (hex) the workspace was created from.
    pub base: String,
    #[serde(default)]
    pub base_ref: Option<String>,
    /// Current snapshot tip (hex); equals `base` until the first snapshot.
    pub head: String,
    pub created_at_secs: u64,
    /// Lease duration from the identity record; 0 for durable (modern) workspaces. Defaults
    /// to 0 against servers that predate the field.
    #[serde(default)]
    pub lease_secs: u64,
    /// Epoch-ms lease expiry. `None` means pinned: never expires. Servers that predate the
    /// field omit it, which decodes as pinned — correct for every durable workspace.
    #[serde(default)]
    pub lease_due_ms: Option<u64>,
    #[serde(default)]
    pub shared_target: Option<String>,
    #[serde(default)]
    pub snapshot_count: u64,
    #[serde(default)]
    pub last_snapshot_ms: Option<u64>,
    #[serde(default)]
    pub last_heartbeat_ms: Option<u64>,
    /// Host reported by the currently-open mount session, if any.
    #[serde(default)]
    pub mounted_on: Option<String>,
}

fn default_workspace_storage() -> String {
    "git".to_string()
}

/// A workspace's creating principal: the authenticated `sub` and its human/agent classification.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceActor {
    pub name: String,
    /// `human` | `agent` | `unknown`.
    pub kind: String,
}

/// Per-status totals for the fleet page's filter set (status filter excluded).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct WorkspaceFleetCounts {
    pub total: u64,
    pub live: u64,
    pub detached: u64,
    pub gap: u64,
    pub idle: u64,
    /// Counts cover only the server's bounded scan prefix when the fleet exceeds its cap.
    pub truncated: bool,
}

/// One page of the project-scope workspace fleet.
#[derive(Clone, Debug, Deserialize)]
pub struct WorkspaceFleetPage {
    pub project: String,
    pub items: Vec<WorkspaceFleetItem>,
    pub counts: WorkspaceFleetCounts,
    /// More items exist past this page; resume with `after = next_after`.
    pub truncated: bool,
    #[serde(default)]
    pub next_after: Option<String>,
}

/// Filters for one fleet page. `repo` accepts the bare repo name (the server also matches the
/// full `{project}/{repo}` id); `q` is an id substring match.
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkspaceFleetQuery<'a> {
    pub repo: Option<&'a str>,
    pub q: Option<&'a str>,
    /// Ask the server for the caller's own + unbound workspaces only (`principal=self`).
    /// Servers that predate the param ignore it — callers relying on the narrowing must also
    /// filter client-side.
    pub principal_self: bool,
    pub after: Option<&'a str>,
    /// Page size; the server clamps to its own cap (200).
    pub limit: Option<usize>,
}

impl ArtifactStorageClient {
    /// Resolve a branch, tag, full commit, and optional subtree into a canonical lazy-mount
    /// source. The server owns namespace resolution and directory validation.
    pub async fn resolve_git_mount_source(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        source: Option<&str>,
        subtree: Option<&str>,
    ) -> Result<Traced<GitMountSource>, SdkError> {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        if let Some(source) = source {
            params.append_pair("source", source);
        }
        if let Some(subtree) = subtree {
            params.append_pair("subtree", subtree);
        }
        let params = params.finish();
        let suffix = if params.is_empty() {
            "mount-source".to_string()
        } else {
            format!("mount-source?{params}")
        };
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let source = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, source))
    }

    pub async fn record_git_mount_presence(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        session_id: &str,
        request: &RecordGitMountPresenceRequest<'_>,
    ) -> Result<Traced<GitMountPresence>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("mount-presence/{session_id}")),
            git_username,
            git_token,
        )?;
        let presence = expect_json(req.json(request).send().await?).await?;
        Ok(Traced::new(trace_id, presence))
    }

    pub async fn delete_git_mount_presence(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        session_id: &str,
    ) -> Result<Traced<()>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::DELETE,
            project_id,
            repo,
            Some(&format!("mount-presence/{session_id}")),
            git_username,
            git_token,
        )?;
        decode_empty(req.send().await?, trace_id).await
    }

    pub async fn list_git_mount_presence(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Traced<GitMountPresencePage>, SdkError> {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        params.append_pair("limit", &limit.to_string());
        if let Some(after) = after {
            params.append_pair("after", after);
        }
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&format!("mount-presence?{}", params.finish())),
            git_username,
            git_token,
        )?;
        let page = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, page))
    }

    pub async fn create_workspace(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        request: &CreateWorkspaceRequest,
    ) -> Result<Traced<WorkspaceInfo>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some("workspaces"),
            git_username,
            git_token,
        )?;
        let info = expect_json(req.json(request).send().await?).await?;
        Ok(Traced::new(trace_id, info))
    }

    pub async fn get_workspace(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
    ) -> Result<Traced<WorkspaceInfo>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}")),
            git_username,
            git_token,
        )?;
        let info = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, info))
    }

    /// One page of the project-scope workspace fleet: every repo in one call, joined with
    /// liveness (status, `mounted_on`). Requires a project-wide credential — repo-scoped mints
    /// carry no `project:read` and are refused. Page with `next_after` until `truncated` is
    /// false.
    pub async fn workspace_fleet(
        &self,
        project_id: &str,
        git_username: &str,
        git_token: &str,
        query: &WorkspaceFleetQuery<'_>,
    ) -> Result<Traced<WorkspaceFleetPage>, SdkError> {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        if let Some(repo) = query.repo {
            params.append_pair("repo", repo);
        }
        if let Some(q) = query.q {
            params.append_pair("q", q);
        }
        if query.principal_self {
            params.append_pair("principal", "self");
        }
        if let Some(after) = query.after {
            params.append_pair("after", after);
        }
        if let Some(limit) = query.limit {
            params.append_pair("limit", &limit.to_string());
        }
        let params = params.finish();
        let suffix = if params.is_empty() {
            "workspaces".to_string()
        } else {
            format!("workspaces?{params}")
        };
        let (req, trace_id) =
            self.project_git_request(Method::GET, project_id, &suffix, git_username, git_token);
        let page = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, page))
    }

    /// The complete fleet for the given filters: pages internally (like every other list
    /// helper in this crate) until the server reports no more rows, starting from `query.after`
    /// if set. Returns the items and the trace id of the final page.
    pub async fn workspace_fleet_all(
        &self,
        project_id: &str,
        git_username: &str,
        git_token: &str,
        query: &WorkspaceFleetQuery<'_>,
    ) -> Result<Traced<Vec<WorkspaceFleetItem>>, SdkError> {
        let mut items = Vec::new();
        let mut after: Option<String> = query.after.map(str::to_string);
        loop {
            let page = self
                .workspace_fleet(
                    project_id,
                    git_username,
                    git_token,
                    &WorkspaceFleetQuery {
                        after: after.as_deref(),
                        ..*query
                    },
                )
                .await?;
            let trace_id = page.trace_id.clone();
            let page = page.into_inner();
            items.extend(page.items);
            if !page.truncated {
                return Ok(Traced::new(trace_id, items));
            }
            let Some(next) = page.next_after else {
                return Ok(Traced::new(trace_id, items));
            };
            after = Some(next);
        }
    }

    pub async fn list_workspaces(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<Vec<WorkspaceInfo>>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("workspaces"),
            git_username,
            git_token,
        )?;
        let list = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, list))
    }

    /// Re-arm the workspace's activity lease. Pinned workspaces heartbeat as a no-op.
    pub async fn workspace_heartbeat(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
    ) -> Result<Traced<WorkspaceHeartbeat>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}/heartbeat")),
            git_username,
            git_token,
        )?;
        let hb = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, hb))
    }

    /// CAS-advance a real branch to the workspace's snapshot (squash by default). Merge mode
    /// (`mode: "merge"`) lands a two-parent merge commit when the target moved; its conflicts
    /// are returned as `PromoteOutcome::Conflicted`, not an error. A plain `409` (concurrent
    /// ref move) still surfaces as `SdkError::ServerError`.
    pub async fn workspace_promote(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
        request: &PromoteWorkspaceRequest,
    ) -> Result<Traced<PromoteOutcome>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}/promote")),
            git_username,
            git_token,
        )?;
        let outcome = expect_json_or_conflict::<PromoteWorkspaceResponse, MergeReport>(
            req.json(request).send().await?,
        )
        .await?
        .map(PromoteOutcome::Promoted)
        .unwrap_or_else(PromoteOutcome::Conflicted);
        Ok(Traced::new(trace_id, outcome))
    }

    /// Ref-level promote preflight: whether `target` moved since the workspace forked, and
    /// whether a promote would fast-forward.
    pub async fn workspace_promote_preflight(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
        target: &str,
    ) -> Result<Traced<PromotePreflight>, SdkError> {
        let suffix = format!(
            "workspaces/{workspace_id}/promote/preflight?target={}",
            url::form_urlencoded::byte_serialize(target.as_bytes()).collect::<String>()
        );
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let preflight = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, preflight))
    }

    /// Refresh the current workspace source or switch a pristine workspace to `target`. The
    /// server rejects a base-changing sync once snapshots exist; use `workspace_rebase` when
    /// rewriting the active chain is intentional.
    pub async fn workspace_sync(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
        request: &SyncWorkspaceRequest,
    ) -> Result<Traced<ViewSyncWorkspaceResponse>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}/sync")),
            git_username,
            git_token,
        )?;
        let resp = expect_json(req.json(request).send().await?).await?;
        Ok(Traced::new(trace_id, resp))
    }

    /// Rebase a snapshotted workspace onto `target`, retaining the replaced chain under the
    /// returned recovery ref. This is deliberately separate from pristine/view-only sync.
    pub async fn workspace_rebase(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
        request: &RebaseWorkspaceRequest,
    ) -> Result<Traced<RebaseWorkspaceResponse>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}/rebase")),
            git_username,
            git_token,
        )?;
        let resp = expect_json_or_conflict::<RebaseWorkspaceResponse, RebaseWorkspaceResponse>(
            req.json(request).send().await?,
        )
        .await?
        .unwrap_or_else(|conflicted| conflicted);
        Ok(Traced::new(trace_id, resp))
    }

    pub async fn workspace_log(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Traced<GitWorkspaceLogPage>, SdkError> {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        params.append_pair("limit", &limit.to_string());
        if let Some(after) = after {
            params.append_pair("after", after);
        }
        let suffix = format!("workspaces/{workspace_id}/log?{}", params.finish());
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let page = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, page))
    }

    pub async fn repo_smartlog(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Traced<GitSmartlogPage>, SdkError> {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        params.append_pair("limit", &limit.to_string());
        if let Some(after) = after {
            params.append_pair("after", after);
        }
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&format!("smartlog?{}", params.finish())),
            git_username,
            git_token,
        )?;
        let page = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, page))
    }

    pub async fn project_smartlog(
        &self,
        project_id: &str,
        git_username: &str,
        git_token: &str,
        repo: Option<&str>,
        workspace: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Traced<GitSmartlogPage>, SdkError> {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        params.append_pair("limit", &limit.to_string());
        if let Some(repo) = repo {
            params.append_pair("repo", repo);
        }
        if let Some(workspace) = workspace {
            params.append_pair("workspace", workspace);
        }
        if let Some(after) = after {
            params.append_pair("after", after);
        }
        let (req, trace_id) = self.project_git_request(
            Method::GET,
            project_id,
            &format!("smartlog?{}", params.finish()),
            git_username,
            git_token,
        );
        let page = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, page))
    }

    pub async fn delete_workspace(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
    ) -> Result<Traced<()>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::DELETE,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}")),
            git_username,
            git_token,
        )?;
        decode_empty(req.send().await?, trace_id).await
    }

    /// One ref's head + movement generation (`?ref=` accepts a short branch or full ref name).
    pub async fn ref_status(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        refspec: &str,
    ) -> Result<Traced<RefStatus>, SdkError> {
        let suffix = format!(
            "ref-status?ref={}",
            url::form_urlencoded::byte_serialize(refspec.as_bytes()).collect::<String>()
        );
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let status = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, status))
    }

    /// One page of a directory listing at `version` (commit hex, branch, or ref).
    pub async fn list_tree_page(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        version: &str,
        dir_path: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Traced<TreePage>, SdkError> {
        let enc = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
        let mut suffix = if dir_path.is_empty() {
            format!("tree?version={}&limit={limit}", enc(version))
        } else {
            format!("tree/{}?version={}&limit={limit}", dir_path, enc(version))
        };
        if let Some(after) = after {
            suffix.push_str(&format!("&after={}", enc(after)));
        }
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let page = expect_json(req.send().await?).await?;
        Ok(Traced::new(trace_id, page))
    }

    /// Raw file bytes at `version`.
    pub async fn get_file_bytes(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        version: &str,
        file_path: &str,
    ) -> Result<Traced<Vec<u8>>, SdkError> {
        let suffix = format!(
            "files/{file_path}?version={}",
            url::form_urlencoded::byte_serialize(version.as_bytes()).collect::<String>()
        );
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(SdkError::ServerError {
                status,
                message: body,
            });
        }
        let bytes = resp.bytes().await?.to_vec();
        Ok(Traced::new(trace_id, bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Promote responses from servers that predate merge mode decode with the new fields
    /// defaulting to false.
    #[test]
    fn promote_response_tolerates_old_servers() {
        let body = r#"{"commit": "aa", "ref_name": "refs/heads/main", "created": false, "squashed": true}"#;
        let resp: PromoteWorkspaceResponse = serde_json::from_str(body).unwrap();
        assert!(resp.squashed);
        assert!(!resp.merged);
        assert!(!resp.fast_forwarded);
    }

    /// The fleet page decodes gsvc-server's exact wire shape (`fleet_item_json` /
    /// `WorkspaceFleetResponse` in `crates/gsvc-server/src/http.rs`) — every field the CLI
    /// listing consumes. The server side of this contract is pinned by the fleet e2e test in
    /// artifact_storage; change either together.
    #[test]
    fn fleet_page_decodes_server_shape() {
        let body = r#"{
            "project": "p1",
            "items": [{
                "id": "aabbccdd", "repo": "p1/demo", "storage": "native", "status": "live", "mode": "shared-rw",
                "created_by": {"name": "user:u1", "kind": "human"},
                "base": "1111111111111111111111111111111111111111",
                "base_ref": "refs/heads/main",
                "head": "2222222222222222222222222222222222222222",
                "created_at_secs": 1751000000,
                "lease_secs": 3600,
                "lease_due_ms": 1751003600000,
                "shared_target": "main",
                "snapshot_count": 3,
                "last_snapshot_ms": 1751000500000,
                "last_heartbeat_ms": 1751000600000,
                "mounted_on": "sandbox-42"
            }],
            "counts": {"total": 1, "live": 1, "detached": 0, "gap": 0, "idle": 0, "truncated": false},
            "truncated": true,
            "next_after": "aabbccdd"
        }"#;
        let page: WorkspaceFleetPage = serde_json::from_str(body).unwrap();
        assert_eq!(page.project, "p1");
        assert_eq!(page.counts.total, 1);
        assert!(page.truncated);
        assert_eq!(page.next_after.as_deref(), Some("aabbccdd"));
        let item = &page.items[0];
        assert_eq!(item.id, "aabbccdd");
        assert_eq!(item.repo, "p1/demo");
        assert_eq!(item.storage, "native");
        assert_eq!(item.status, "live");
        assert_eq!(item.mode, "shared-rw");
        assert_eq!(item.created_by.as_ref().unwrap().name, "user:u1");
        assert_eq!(item.created_by.as_ref().unwrap().kind, "human");
        assert_eq!(item.base_ref.as_deref(), Some("refs/heads/main"));
        assert_eq!(item.lease_secs, 3600);
        assert_eq!(item.lease_due_ms, Some(1751003600000));
        assert_eq!(item.snapshot_count, 3);
        assert_eq!(item.mounted_on.as_deref(), Some("sandbox-42"));
    }

    /// Optional fleet fields are `skip_serializing_if` on the server: an unbound, never-mounted,
    /// never-snapshotted workspace omits them entirely and must still decode. Lease fields also
    /// default (0 / pinned) against servers that predate them.
    #[test]
    fn fleet_item_tolerates_omitted_optionals() {
        let body = r#"{
            "project": "p1",
            "items": [{
                "id": "ee", "repo": "p1/demo", "status": "detached", "mode": "default",
                "base": "1111111111111111111111111111111111111111",
                "head": "1111111111111111111111111111111111111111",
                "created_at_secs": 1751000000,
                "snapshot_count": 0
            }],
            "counts": {"total": 1, "live": 0, "detached": 1, "gap": 0, "idle": 0, "truncated": false},
            "truncated": false
        }"#;
        let page: WorkspaceFleetPage = serde_json::from_str(body).unwrap();
        let item = &page.items[0];
        assert!(item.created_by.is_none(), "unbound workspace");
        assert_eq!(item.storage, "git", "old servers default to git rows");
        assert!(item.base_ref.is_none());
        assert_eq!(item.lease_secs, 0, "old servers omit lease_secs");
        assert!(item.lease_due_ms.is_none(), "omitted lease row = pinned");
        assert!(item.shared_target.is_none());
        assert!(item.last_snapshot_ms.is_none());
        assert!(item.last_heartbeat_ms.is_none());
        assert!(item.mounted_on.is_none());
        assert!(page.next_after.is_none());
    }

    /// The sync response decodes the full server shape, conflicts included.
    #[test]
    fn sync_response_decodes_server_shape() {
        let body = r#"{
            "workspace_head": "ws1", "target_head": "t1", "base": "t1",
            "clean": false, "up_to_date": false, "fast_forwarded": false,
            "changed_paths": 2,
            "conflicts": [{"path": "f", "kind": "content", "potential": false,
                           "ours": {"mode": 33188, "oid": "o"}, "base": {"mode": 33188, "oid": "b"},
                           "theirs": {"mode": 33188, "oid": "t"}}],
            "stats": {"trees_read": 4, "entries_compared": 9, "blobs_merged": 1, "wall_ms": 0.7}
        }"#;
        let resp: SyncWorkspaceResponse = serde_json::from_str(body).unwrap();
        assert!(!resp.clean);
        assert_eq!(resp.base, "t1");
        assert_eq!(resp.conflicts.len(), 1);
        assert_eq!(resp.conflicts[0].kind, "content");

        let view: ViewSyncWorkspaceResponse = serde_json::from_value(serde_json::json!({
            "workspace_head": "t2", "target_head": "t2", "base": "t2",
            "target_ref": "refs/tags/v2", "changed": true
        }))
        .unwrap();
        assert!(view.changed);
        assert_eq!(view.target_ref.as_deref(), Some("refs/tags/v2"));
    }

    #[test]
    fn mount_source_and_presence_decode_versioned_wire() {
        let source: GitMountSource = serde_json::from_value(serde_json::json!({
            "format_ver": 1,
            "kind": "tag", "follow_policy": "follow",
            "canonical_ref": "refs/tags/v1",
            "resolved_commit": "1111111111111111111111111111111111111111",
            "subtree": "services/api",
            "root_tree": "2222222222222222222222222222222222222222"
        }))
        .unwrap();
        assert_eq!(source.canonical_ref.as_deref(), Some("refs/tags/v1"));
        assert_eq!(source.subtree.as_deref(), Some("services/api"));

        let presence: GitMountPresence = serde_json::from_value(serde_json::json!({
            "format_ver": 1,
            "session_id": "mount-1",
            "principal": "user:1",
            "source": source,
            "mounted_on": "sandbox-1",
            "started_at_ms": 1,
            "last_heartbeat_ms": 2,
            "expires_at_ms": 3
        }))
        .unwrap();
        assert_eq!(presence.source.kind, "tag");
        assert_eq!(presence.expires_at_ms, 3);

        let annotated: RefStatus = serde_json::from_value(serde_json::json!({
            "ref_name": "refs/tags/v1",
            "oid": "3333333333333333333333333333333333333333",
            "resolved_commit": "1111111111111111111111111111111111111111",
            "generation": 4
        }))
        .unwrap();
        assert_eq!(
            annotated.resolved_commit.as_deref(),
            Some("1111111111111111111111111111111111111111")
        );
        let legacy: RefStatus = serde_json::from_value(serde_json::json!({
            "ref_name": "refs/heads/main", "oid": "1111", "generation": 1
        }))
        .unwrap();
        assert!(legacy.resolved_commit.is_none());
    }

    #[test]
    fn rebase_log_and_smartlog_decode_server_shapes() {
        let response: RebaseWorkspaceResponse = serde_json::from_value(serde_json::json!({
            "workspace_head": "ws1", "target_head": "t1", "base": "t1",
            "clean": true, "up_to_date": false, "fast_forwarded": false,
            "changed_paths": 1, "conflicts": [],
            "stats": {"trees_read": 1, "entries_compared": 2, "blobs_merged": 0, "wall_ms": 0.1},
            "recovery_ref": "refs/recovery/workspaces/ws-1/rebase-1"
        }))
        .unwrap();
        assert_eq!(response.result.workspace_head, "ws1");
        assert!(response.recovery_ref.unwrap().contains("recovery"));

        let log: GitWorkspaceLogPage = serde_json::from_value(serde_json::json!({
            "format_ver": 1,
            "workspace_id": "ws-1",
            "active_chain": [{
                "oid": "abc", "subject": "checkpoint", "at_ms": 10,
                "actor": "agent:1", "operation": "op-1", "conflicted": false
            }],
            "retained_chains": [{
                "recovery_ref": "refs/recovery/ws-1/one", "head": "old", "base": "base",
                "created_at_ms": 9, "reason": "rebase", "retention": "retained"
            }],
            "truncated": false,
            "next_after": null
        }))
        .unwrap();
        assert_eq!(log.active_chain[0].subject, "checkpoint");
        assert_eq!(log.retained_chains[0].retention, "retained");

        let graph: GitSmartlogPage = serde_json::from_value(serde_json::json!({
            "format_ver": 1, "repo": "demo",
            "nodes": [{
                "id": "branch:main", "kind": "branch", "label": "main", "oid": "abc",
                "workspace_id": null, "actor": null, "timestamp_ms": null, "state": "active"
            }],
            "edges": [{"from": "branch:main", "to": "abc", "kind": "points_to"}],
            "truncated": true, "next_after": "opaque-cursor"
        }))
        .unwrap();
        assert_eq!(graph.nodes[0].kind, "branch");
        assert_eq!(graph.next_after.as_deref(), Some("opaque-cursor"));
    }
}
