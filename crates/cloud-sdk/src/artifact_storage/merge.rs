//! Server-side three-way merge client (gsvc `docs/merge-design.md` §9.3).
//!
//! `POST /repos/{repo}/merge` computes a jj-style three-way tree merge entirely server-side:
//! `preflight` mode is a pure reader returning the conflict report; `commit` mode mints the
//! merged tree plus a two-parent merge commit and CAS-advances the `ours` branch. A conflicted
//! commit under the default `fail` policy publishes nothing and the report rides a `409`;
//! `materialize` publishes diff3 marker content plus a structured conflict record readable at
//! `GET /repos/{repo}/commits/{oid}/conflicts`. Wire types here duplicate the gsvc-server
//! shapes (same rule as the ingest shapes).

use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::Traced;
use crate::error::SdkError;

use super::ArtifactStorageClient;
use super::ingest::expect_json;

/// Commit author/committer identity for server-minted commits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Signature {
    pub name: String,
    pub email: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MergeRequest {
    /// The side the merge lands on. Any commitish for preflight; a branch for commit mode
    /// (its ref is what the merge commit CAS-advances).
    pub ours: String,
    /// The side being merged in.
    pub theirs: String,
    /// Explicit merge base override; omitted = walk the commit graph for the best common
    /// ancestor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// Preflight only: run the text merges for same-file collisions instead of reporting them
    /// as potential conflicts. Commit mode always runs them.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub deep: bool,
    /// `"preflight"` (default) or `"commit"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Commit mode: `"fail"` (default — conflicts publish nothing) or `"materialize"`
    /// (conflicts publish marker content plus the structured conflict record).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    /// Commit mode: merge commit message; default `Merge {theirs} into {branch}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<Signature>,
}

/// One side's entry at a conflicted path; `None` on the wire = the side is absent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergeEntry {
    /// Raw git mode (`0o100644`, `0o100755`, `0o120000`, `0o40000` for directories).
    pub mode: u32,
    pub oid: String,
}

/// One conflicted path with its jj-shaped term list (`ours`, negative `base`, `theirs`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergeConflict {
    pub path: String,
    /// `content` | `delete_modify` | `add_add` | `kind_mismatch` | `mode` | `too_large`.
    pub kind: String,
    /// Shallow preflight only: the sides collide on content but the texts were not merged —
    /// a deep preflight (or commit mode) may still resolve them cleanly.
    #[serde(default)]
    pub potential: bool,
    pub ours: Option<MergeEntry>,
    pub base: Option<MergeEntry>,
    pub theirs: Option<MergeEntry>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MergeStats {
    pub trees_read: u64,
    pub entries_compared: u64,
    pub blobs_merged: u64,
    pub wall_ms: f64,
}

/// The merge report: preflight result, commit-mode success body, and the `409` body when a
/// `fail`-policy merge (or merge-mode promote) hit conflicts and published nothing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MergeReport {
    /// Resolved `ours` commit (hex).
    pub ours: String,
    /// Resolved `theirs` commit (hex).
    pub theirs: String,
    /// `None` = unrelated histories (the report then treats every path as added on both
    /// sides).
    pub merge_base: Option<String>,
    pub clean: bool,
    /// `ours` is an ancestor of `theirs`: the merge is a ref advance, no new commit needed.
    pub fast_forward: bool,
    /// `theirs` is already reachable from `ours`: nothing to do.
    pub already_merged: bool,
    pub changed_paths: u64,
    pub conflicts: Vec<MergeConflict>,
    pub stats: MergeStats,
    /// Commit mode only: the published merge commit (`theirs` itself for a fast-forward).
    #[serde(default)]
    pub commit: Option<String>,
    /// Commit mode only: the ref advanced to `theirs` with no new commit object.
    #[serde(default)]
    pub fast_forwarded: bool,
}

/// One term of a recorded conflict; index order is `[ours, base, theirs]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConflictTerm {
    pub mode: u32,
    pub oid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConflictPath {
    pub path: String,
    pub kind: String,
    /// `[ours, base, theirs]`; `None` = the side is absent.
    pub terms: Vec<Option<ConflictTerm>>,
}

/// The structured conflict record of a `materialize`-policy merge commit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MergeConflictRecord {
    pub version: u32,
    pub ours_commit: String,
    pub theirs_commit: String,
    pub base_commit: Option<String>,
    pub paths: Vec<ConflictPath>,
    /// Conflicted paths dropped to bound the stored record; the commit's marker content
    /// remains complete.
    #[serde(default)]
    pub truncated_paths: u32,
}

/// Decode a response whose `409` body may be a structured merge report (`fail`-policy
/// conflicts publish nothing and return the report). A `409` that isn't a report (e.g. a
/// concurrent ref move) stays a `ServerError`.
pub(super) async fn expect_json_or_conflict<T, R>(
    resp: reqwest::Response,
) -> Result<Result<T, R>, SdkError>
where
    T: serde::de::DeserializeOwned,
    R: serde::de::DeserializeOwned,
{
    let status = resp.status();
    if status == StatusCode::CONFLICT {
        let message = resp.text().await.unwrap_or_default();
        if let Ok(report) = serde_json::from_str::<R>(&message) {
            return Ok(Err(report));
        }
        return Err(SdkError::ServerError { status, message });
    }
    if !status.is_success() {
        let message = resp.text().await.unwrap_or_default();
        return Err(SdkError::ServerError { status, message });
    }
    resp.json::<T>()
        .await
        .map(Ok)
        .map_err(|e| SdkError::ClientError(format!("bad response body: {e}")))
}

impl ArtifactStorageClient {
    /// Server-side three-way merge between two commitishes. Preflight (the default mode) never
    /// writes; commit mode CAS-advances the `ours` branch. A `fail`-policy conflict returns
    /// `Ok` with `clean: false`, `commit: None`, and the conflict list (the server's `409`
    /// report body).
    pub async fn repo_merge(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        request: &MergeRequest,
    ) -> Result<Traced<MergeReport>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some("merge"),
            git_username,
            git_token,
        )?;
        let report =
            expect_json_or_conflict::<MergeReport, MergeReport>(req.json(request).send().await?)
                .await?
                .unwrap_or_else(|report| report);
        Ok(Traced::new(trace_id, report))
    }

    /// The structured conflict record of a `materialize`-policy merge commit. `Ok(None)` =
    /// the commit merged cleanly (or the server doesn't know it).
    pub async fn commit_conflicts(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        commit: &str,
    ) -> Result<Traced<Option<MergeConflictRecord>>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&format!("commits/{commit}/conflicts")),
            git_username,
            git_token,
        )?;
        let resp = req.send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(Traced::new(trace_id, None));
        }
        let record = expect_json(resp).await?;
        Ok(Traced::new(trace_id, Some(record)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gsvc-server merge report shape (both the 200 body and the fail-policy 409 body)
    /// decodes with every field the server serializes.
    #[test]
    fn merge_report_decodes_server_shape() {
        let body = r#"{
            "ours": "aaaa", "theirs": "bbbb", "merge_base": "cccc",
            "clean": false, "fast_forward": false, "already_merged": false,
            "changed_paths": 3,
            "conflicts": [{
                "path": "src/main.rs", "kind": "content", "potential": true,
                "ours": {"mode": 33188, "oid": "o1"}, "base": null,
                "theirs": {"mode": 33188, "oid": "t1"}
            }],
            "stats": {"trees_read": 10, "entries_compared": 100, "blobs_merged": 1, "wall_ms": 2.5},
            "commit": null, "fast_forwarded": false
        }"#;
        let report: MergeReport = serde_json::from_str(body).unwrap();
        assert!(!report.clean);
        assert_eq!(report.merge_base.as_deref(), Some("cccc"));
        assert_eq!(report.conflicts.len(), 1);
        let c = &report.conflicts[0];
        assert_eq!(c.kind, "content");
        assert!(c.potential);
        assert!(c.base.is_none());
        assert_eq!(c.theirs.as_ref().unwrap().oid, "t1");
        assert_eq!(report.stats.blobs_merged, 1);
        assert!(report.commit.is_none());
    }

    /// Optional request fields stay off the wire so older servers never see unknown keys.
    #[test]
    fn merge_request_skips_defaults() {
        let req = MergeRequest {
            ours: "refs/heads/main".into(),
            theirs: "dev".into(),
            ..Default::default()
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"ours": "refs/heads/main", "theirs": "dev"})
        );
    }

    /// The conflict-record terms ride as `[ours, base, theirs]` with `null` for absent sides.
    #[test]
    fn conflict_record_decodes_terms() {
        let body = r#"{
            "version": 1, "ours_commit": "aa", "theirs_commit": "bb", "base_commit": null,
            "paths": [{"path": "f", "kind": "delete_modify",
                       "terms": [null, {"mode": 33188, "oid": "b1"}, {"mode": 33188, "oid": "t1"}]}],
            "truncated_paths": 0
        }"#;
        let record: MergeConflictRecord = serde_json::from_str(body).unwrap();
        assert_eq!(record.paths.len(), 1);
        let terms = &record.paths[0].terms;
        assert_eq!(terms.len(), 3);
        assert!(terms[0].is_none());
        assert_eq!(terms[2].as_ref().unwrap().oid, "t1");
    }
}
