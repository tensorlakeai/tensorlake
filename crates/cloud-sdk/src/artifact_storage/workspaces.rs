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

/// One ref's head and movement generation — the poll target for branch/workspace following.
#[derive(Clone, Debug, Deserialize)]
pub struct RefStatus {
    pub ref_name: String,
    /// Current head (hex); absent when the ref does not exist.
    pub oid: Option<String>,
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

impl ArtifactStorageClient {
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

    /// Pull the target branch into a behind workspace, rebase-style: one merge commit on the
    /// target head, workspace history kept linear, the pre-sync chain preserved under a
    /// presync ref. Under the default `materialize` policy conflicts land as diff3 markers in
    /// the workspace; under `fail` a conflicted sync changes nothing and the returned report
    /// has `clean: false` with `workspace_head` untouched. Snapshot uncommitted mount changes
    /// before syncing.
    pub async fn workspace_sync(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
        request: &SyncWorkspaceRequest,
    ) -> Result<Traced<SyncWorkspaceResponse>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}/sync")),
            git_username,
            git_token,
        )?;
        let resp = expect_json_or_conflict::<SyncWorkspaceResponse, SyncWorkspaceResponse>(
            req.json(request).send().await?,
        )
        .await?
        .unwrap_or_else(|conflicted| conflicted);
        Ok(Traced::new(trace_id, resp))
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
    }
}
