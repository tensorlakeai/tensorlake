//! Resumable git-history push (artifact_storage issue #279, phase 2 — the client half).
//!
//! `tl git push` ships the user's REAL git history — the same commits, trees, and blobs with
//! their original SHAs that `git push` would send — through the server's git-push upload
//! surface: stage the pack in bounded, retryable parts, then finalize through the server's
//! ordinary receive-pack pipeline. Identical artifacts, minus the single fragile hour-long
//! POST: a part that fails re-PUTs alone, the finalize is an idempotent reattach, and the
//! terminal report is receive-pack's own report-status.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::ArtifactStorageClient;
use crate::Traced;
use crate::error::SdkError;
use reqwest::Method;

/// One ref update, receive-pack CAS semantics: `old_oid: None` means "must not exist".
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushRefUpdate {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_oid: Option<String>,
    pub new_oid: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushRefResult {
    pub name: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushReport {
    pub unpack_ok: bool,
    pub refs: Vec<GitPushRefResult>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushError {
    pub kind: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushPart {
    pub part: u32,
    pub bytes: u64,
}

/// The upload session as the server reports it (mirrors the server's wire shape; unknown
/// fields are ignored so the surface can grow).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitPushUploadStatus {
    pub upload_id: String,
    pub state: String,
    #[serde(default)]
    pub parts: Vec<GitPushPart>,
    pub expires_at_ms: u64,
    pub part_max_bytes: usize,
    pub max_parts: u32,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub report: Option<GitPushReport>,
    #[serde(default)]
    pub error: Option<GitPushError>,
}

#[derive(Serialize)]
struct CompleteRequest<'a> {
    parts_total: u32,
    ref_updates: &'a [GitPushRefUpdate],
}

/// Progress callbacks for the long-running phases.
#[derive(Clone, Debug)]
pub enum GitPushProgress {
    /// One part landed: (parts uploaded, bytes uploaded so far).
    PartUploaded { parts: u32, bytes: u64 },
    /// The finalize is running server-side; `phase` is the server's phase note.
    Finalizing { phase: String },
}

const PART_PUT_ATTEMPTS: usize = 3;
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(750);
/// The finalize resolves kernel-scale packs in minutes; anything past this is genuinely stuck
/// (the server's own heartbeat/staleness machinery will let a retry re-claim).
const FINALIZE_POLL_MAX: std::time::Duration = std::time::Duration::from_secs(60 * 60);

impl ArtifactStorageClient {
    pub async fn git_push_upload_create(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<GitPushUploadStatus>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some("git-push/uploads"),
            git_username,
            git_token,
        )?;
        super::decode_json(request.send().await?, trace_id).await
    }

    pub async fn git_push_upload_status(
        &self,
        project_id: &str,
        repo: &str,
        upload_id: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<GitPushUploadStatus>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&format!("git-push/uploads/{upload_id}")),
            git_username,
            git_token,
        )?;
        super::decode_json(request.send().await?, trace_id).await
    }

    pub async fn git_push_upload_put_part(
        &self,
        project_id: &str,
        repo: &str,
        upload_id: &str,
        part: u32,
        data: Bytes,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<GitPushUploadStatus>, SdkError> {
        let mut last: Option<SdkError> = None;
        for attempt in 0..PART_PUT_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(500 << attempt)).await;
            }
            let (request, trace_id) = self.git_request(
                Method::PUT,
                project_id,
                repo,
                Some(&format!("git-push/uploads/{upload_id}/parts/{part}")),
                git_username,
                git_token,
            )?;
            match super::decode_json(request.body(data.clone()).send().await?, trace_id).await {
                Ok(status) => return Ok(status),
                // A 4xx is a contract rejection, not flakiness — surface it immediately.
                Err(SdkError::ServerError { status, message }) if status.is_client_error() => {
                    return Err(SdkError::ServerError { status, message });
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("at least one attempt ran"))
    }

    pub async fn git_push_upload_complete(
        &self,
        project_id: &str,
        repo: &str,
        upload_id: &str,
        parts_total: u32,
        ref_updates: &[GitPushRefUpdate],
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<GitPushUploadStatus>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("git-push/uploads/{upload_id}/complete")),
            git_username,
            git_token,
        )?;
        let body = CompleteRequest {
            parts_total,
            ref_updates,
        };
        super::decode_json(request.json(&body).send().await?, trace_id).await
    }

    /// The whole flow: open a session, stream `pack` up in bounded parts (each retried
    /// independently), finalize with `ref_updates`, and poll to the terminal report.
    ///
    /// `pack` is a blocking reader (typically `git pack-objects --stdout`'s pipe); each part is
    /// read on the blocking pool while the previous upload proceeds — the whole pack is never
    /// held in memory.
    #[allow(clippy::too_many_arguments)]
    pub async fn push_git_pack<R>(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        pack: R,
        ref_updates: Vec<GitPushRefUpdate>,
        mut progress: Option<Box<dyn FnMut(GitPushProgress) + Send>>,
    ) -> Result<GitPushReport, SdkError>
    where
        R: std::io::Read + Send + 'static,
    {
        let created = self
            .git_push_upload_create(project_id, repo, git_username, git_token)
            .await?
            .into_inner();
        let upload_id = created.upload_id.clone();
        let part_max = created.part_max_bytes.max(1);

        // Reader task: fills bounded parts on the blocking pool and hands them over a small
        // channel, so reading part N+1 overlaps uploading part N.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(1);
        let reader = tokio::task::spawn_blocking(move || {
            let mut pack = pack;
            loop {
                let mut buf = vec![0u8; part_max];
                let mut filled = 0usize;
                while filled < part_max {
                    match pack.read(&mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e));
                            return;
                        }
                    }
                }
                if filled == 0 {
                    return; // EOF on a part boundary — channel close signals the end.
                }
                buf.truncate(filled);
                if tx.blocking_send(Ok(Bytes::from(buf))).is_err() {
                    return;
                }
            }
        });

        let mut part: u32 = 0;
        let mut uploaded: u64 = 0;
        while let Some(chunk) = rx.recv().await {
            let chunk = chunk.map_err(SdkError::Io)?;
            if part >= created.max_parts {
                reader.abort();
                return Err(SdkError::ClientError(format!(
                    "pack exceeds {} parts of {} bytes",
                    created.max_parts, part_max
                )));
            }
            uploaded += chunk.len() as u64;
            self.git_push_upload_put_part(
                project_id,
                repo,
                &upload_id,
                part,
                chunk,
                git_username,
                git_token,
            )
            .await?;
            part += 1;
            if let Some(cb) = progress.as_mut() {
                cb(GitPushProgress::PartUploaded {
                    parts: part,
                    bytes: uploaded,
                });
            }
        }
        // Surface a reader panic rather than completing a truncated upload.
        reader
            .await
            .map_err(|e| SdkError::ClientError(format!("pack reader failed: {e}")))?;
        if part == 0 {
            return Err(SdkError::ClientError(
                "git produced an empty pack (nothing to push?)".to_string(),
            ));
        }

        let mut status = self
            .git_push_upload_complete(
                project_id,
                repo,
                &upload_id,
                part,
                &ref_updates,
                git_username,
                git_token,
            )
            .await?
            .into_inner();
        let deadline = std::time::Instant::now() + FINALIZE_POLL_MAX;
        loop {
            match status.state.as_str() {
                "done" => {
                    return status.report.ok_or_else(|| {
                        SdkError::ClientError("done state without a report".to_string())
                    });
                }
                "failed" => {
                    let err = status.error.unwrap_or(GitPushError {
                        kind: "internal".into(),
                        message: "finalize failed".into(),
                        retryable: false,
                    });
                    return Err(SdkError::ClientError(format!(
                        "push finalize failed ({}): {}{}",
                        err.kind,
                        err.message,
                        if err.retryable {
                            " — safe to re-run: staged parts are kept"
                        } else {
                            ""
                        }
                    )));
                }
                _ => {
                    if let (Some(cb), Some(phase)) = (progress.as_mut(), status.phase.clone()) {
                        cb(GitPushProgress::Finalizing { phase });
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(SdkError::ClientError(
                            "push finalize did not reach a terminal state in time; re-running \
                             will reattach or re-claim it"
                                .to_string(),
                        ));
                    }
                    tokio::time::sleep(POLL_INTERVAL).await;
                    status = self
                        .git_push_upload_status(
                            project_id,
                            repo,
                            &upload_id,
                            git_username,
                            git_token,
                        )
                        .await?
                        .into_inner();
                }
            }
        }
    }
}
