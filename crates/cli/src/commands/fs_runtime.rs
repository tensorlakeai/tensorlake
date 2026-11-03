//! Runtime-owned mounts use their existing authority, before the CLI's login guard.
use crate::commands::sbx::{
    ResolvedSandboxProxyTarget, resolve_sandbox_proxy_target, with_sandbox_headers,
};
use crate::{
    auth::context::CliContext,
    error::{CliError, Result},
};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

#[cfg(feature = "mount")]
const GUEST_SOCKET: &str = "/run/tensorlake/fs-control.sock";

pub enum Connection {
    #[cfg(feature = "mount")]
    Guest,
    Remote {
        client: reqwest::Client,
        target: ResolvedSandboxProxyTarget,
    },
}

impl Connection {
    pub async fn remote(ctx: &CliContext, sandbox: &str) -> Result<Self> {
        let client = ctx.client()?;
        let target = resolve_sandbox_proxy_target(ctx, sandbox).await?;
        Ok(Self::Remote { client, target })
    }

    async fn exchange(&self, body: Value) -> Result<Value> {
        let response = tokio::time::timeout(Duration::from_secs(20), async {
            match self {
                #[cfg(feature = "mount")]
                Self::Guest => guest_request(body).await,
                Self::Remote { client, target } => {
                    let response = with_sandbox_headers(
                        client
                            .post(format!("{}/api/v1/filesystems/control", target.proxy_base))
                            .json(&body),
                        target,
                    )
                    .send()
                    .await?;
                    let status = response.status();
                    let value: Value = response.json().await?;
                    if !status.is_success() {
                        return Err(CliError::usage(format!(
                            "filesystem control HTTP {status}: {value}"
                        )));
                    }
                    Ok(value)
                }
            }
        })
        .await
        .map_err(|_| CliError::usage("filesystem control timed out"))??;
        Ok(response)
    }

    pub async fn request(&self, body: Value) -> Result<Value> {
        checked_response(self.exchange(body).await?)
    }

    /// Ownership discovery must not turn authorization or transport failures into
    /// a local-mount fallback (and possibly an interactive login).
    #[cfg(feature = "mount")]
    pub async fn probe_status(&self, path: &Path) -> Result<Option<Value>> {
        runtime_status(
            self.exchange(json!({"operation":"status", "mount_path":path}))
                .await?,
        )
    }

    pub async fn status(&self, path: &Path, json_output: bool) -> Result<()> {
        let status = self
            .request(json!({"operation":"status", "mount_path":path}))
            .await?;
        print_status(&status, json_output)
    }

    pub async fn snapshot(
        &self,
        path: &Path,
        message: Option<&str>,
        request_id: Option<&str>,
        expected_mount_id: Option<&str>,
        json_output: bool,
    ) -> Result<()> {
        if request_id.is_some() != expected_mount_id.is_some() {
            return Err(CliError::usage(
                "replay requires both --request-id and --mount-id",
            ));
        }
        let status = self
            .request(json!({"operation":"status", "mount_path":path}))
            .await?;
        let mount_id = snapshot_mount_id(&status, expected_mount_id)?;
        let request_id = request_id
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let message = message.unwrap_or("tl fs snapshot");
        eprintln!(
            "Snapshot request {request_id}; retry with --request-id {request_id} --mount-id {mount_id} and the same message."
        );
        let body = json!({"operation":"snapshot_start", "mount_path":path, "mount_id":mount_id,
            "request_id":request_id, "message":message});
        let mut response = self.request(body).await?;
        // The daemon bounds both stalled progress and the whole operation. A large
        // snapshot can keep making durable progress beyond five minutes; leave room
        // for its one-hour lifetime and final receipt instead of abandoning it early.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3610);
        let mut last_progress = tokio::time::Instant::now();
        loop {
            match response["phase"].as_str() {
                Some("completed") => {
                    let snapshot = response["snapshot_id"]
                        .as_str()
                        .ok_or_else(|| CliError::usage("snapshot completed without a version"))?;
                    if json_output {
                        println!("{}", serde_json::to_string_pretty(&response)?);
                    } else {
                        println!("{snapshot}");
                    }
                    return Ok(());
                }
                Some("failed" | "retryable") => {
                    return Err(CliError::usage(format!(
                        "snapshot request {request_id}: {}",
                        response["error"]
                    )));
                }
                Some("queued" | "capturing" | "uploading" | "retaining") => {}
                _ => return Err(CliError::usage("unknown snapshot operation phase")),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(CliError::usage(format!(
                    "snapshot wait timed out; inspect or retry request {request_id}"
                )));
            }
            if last_progress.elapsed() >= Duration::from_secs(5) {
                eprintln!("Snapshot {request_id}: {}", response["phase"]);
                last_progress = tokio::time::Instant::now();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            response = self
                .request(json!({"operation":"snapshot_status", "mount_path":path,
                "mount_id":mount_id, "request_id":request_id}))
                .await?;
        }
    }
}

fn checked_response(response: Value) -> Result<Value> {
    if response["ok"] != true {
        return Err(CliError::usage(
            response["error"]
                .as_str()
                .unwrap_or("filesystem control failed"),
        ));
    }
    Ok(response)
}

#[cfg(feature = "mount")]
fn runtime_status(response: Value) -> Result<Option<Value>> {
    if response["ok"] == false && response["code"] == "not_runtime_mount" {
        return Ok(None);
    }
    checked_response(response).map(Some)
}

fn snapshot_mount_id<'a>(status: &'a Value, expected: Option<&str>) -> Result<&'a str> {
    let actual = status["mount_id"]
        .as_str()
        .ok_or_else(|| CliError::usage("guest runtime lacks mount snapshot support"))?;
    if expected.is_some_and(|expected| expected != actual) {
        return Err(CliError::usage(
            "mount_id changed; the original snapshot request cannot be replayed on this mount",
        ));
    }
    Ok(actual)
}

fn print_status(status: &Value, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(status)?);
    } else {
        println!(
            "Mount: {} ({})",
            status["mount_path"].as_str().unwrap_or("unknown"),
            status["mount_id"].as_str().unwrap_or("unknown")
        );
        println!(
            "Last saved version: {}",
            status["last_saved_version"].as_str().unwrap_or("unknown")
        );
        println!(
            "Unsaved writes pending: {}",
            status["unsaved_writes_pending"]
                .as_bool()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unknown".into())
        );
        println!("Snapshot supported: {}", status["snapshot_supported"]);
        if !status["publication"]["last_error"].is_null() {
            println!("Publication error: {}", status["publication"]["last_error"]);
        }
    }
    Ok(())
}

#[cfg(feature = "mount")]
pub async fn guest_available() -> Result<bool> {
    match tokio::fs::symlink_metadata(GUEST_SOCKET).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(unix, feature = "mount"))]
async fn guest_request(body: Value) -> Result<Value> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::UnixStream::connect(GUEST_SOCKET).await?;
    if stream.peer_cred()?.uid() != 0 {
        return Err(CliError::usage(
            "filesystem control peer is not the root runtime",
        ));
    }
    stream.write_all(&serde_json::to_vec(&body)?).await?;
    stream.write_all(b"\n").await?;
    let mut reader = tokio::io::BufReader::new(stream.take(16 * 1024));
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    if !line.ends_with('\n') {
        return Err(CliError::usage("incomplete filesystem control response"));
    }
    Ok(serde_json::from_str(&line)?)
}

#[cfg(all(not(unix), feature = "mount"))]
async fn guest_request(_body: Value) -> Result<Value> {
    Err(CliError::usage("guest filesystem control requires Unix"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_snapshot_replay_refuses_a_replacement_mount() {
        let original = json!({"ok":true,"mount_id":"original"});
        let replacement = json!({"ok":true,"mount_id":"replacement"});
        assert_eq!(
            snapshot_mount_id(&original, Some("original")).unwrap(),
            "original"
        );
        assert!(snapshot_mount_id(&replacement, Some("original")).is_err());
        assert_eq!(
            snapshot_mount_id(&replacement, None).unwrap(),
            "replacement"
        );
    }

    #[cfg(feature = "mount")]
    #[test]
    fn mount_snapshot_only_explicit_unmanaged_paths_allow_local_fallback() {
        assert!(
            runtime_status(json!({"ok":false,"code":"not_runtime_mount"}))
                .unwrap()
                .is_none()
        );
        assert!(
            runtime_status(json!({"ok":true,"mount_id":"root-owned"}))
                .unwrap()
                .is_some()
        );
        for code in ["unauthorized", "mount_changed", "runtime_not_ready", ""] {
            assert!(runtime_status(json!({"ok":false,"code":code,"error":"denied"})).is_err());
        }
    }
}
