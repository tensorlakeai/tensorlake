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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PromoteWorkspaceResponse {
    pub commit: String,
    pub ref_name: String,
    pub created: bool,
    pub squashed: bool,
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

    /// CAS-advance a real branch to the workspace's snapshot (squash by default).
    pub async fn workspace_promote(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        workspace_id: &str,
        request: &PromoteWorkspaceRequest,
    ) -> Result<Traced<PromoteWorkspaceResponse>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("workspaces/{workspace_id}/promote")),
            git_username,
            git_token,
        )?;
        let resp = expect_json(req.json(request).send().await?).await?;
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
