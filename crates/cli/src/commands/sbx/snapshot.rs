use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub struct SnapshotDetails {
    pub snapshot_id: String,
    #[allow(dead_code)]
    pub snapshot_uri: String,
}

pub(crate) async fn fetch_snapshot_info(
    ctx: &CliContext,
    client: &reqwest::Client,
    snapshot_id: &str,
) -> Result<serde_json::Value> {
    let info_resp = client
        .get(sandbox_endpoint(ctx, &format!("snapshots/{}", snapshot_id)))
        .send()
        .await
        .map_err(CliError::Http)?;

    if info_resp.status().as_u16() == 404 {
        return Err(CliError::ExitCode(1));
    }

    if !info_resp.status().is_success() {
        let status = info_resp.status();
        let body = info_resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to fetch snapshot {} (HTTP {}): {}",
            snapshot_id,
            status,
            body
        )));
    }

    info_resp.json().await.map_err(CliError::Http)
}

pub async fn create_snapshot_with_details(
    ctx: &CliContext,
    sandbox_id: &str,
    timeout: f64,
    snapshot_type: Option<&str>,
) -> Result<SnapshotDetails> {
    let client = ctx.client()?;

    eprintln!("Snapshotting sandbox {}...", sandbox_id);

    let url = sandbox_endpoint(ctx, &format!("sandboxes/{}/snapshot", sandbox_id));
    let request = client.post(url);
    let resp = if let Some(t) = snapshot_type {
        request
            .json(&serde_json::json!({"snapshot_type": t}))
            .send()
            .await
            .map_err(CliError::Http)?
    } else {
        request.send().await.map_err(CliError::Http)?
    };

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to create snapshot (HTTP {}): {}",
            status,
            body
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let snapshot_id = result
        .get("snapshot_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let status = result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    eprintln!("Snapshot {} initiated ({})", snapshot_id, status);

    let spinner = crate::commands::sbx::new_spinner("Waiting for snapshot to complete...");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs_f64(timeout);

    loop {
        if tokio::time::Instant::now() > deadline {
            spinner.finish_with_message("Waiting for snapshot to complete... timed out");
            return Err(CliError::Other(anyhow::anyhow!(
                "Snapshot did not complete within {}s",
                timeout
            )));
        }

        let info = match fetch_snapshot_info(ctx, &client, &snapshot_id).await {
            Ok(info) => info,
            Err(CliError::ExitCode(1)) => {
                spinner.finish_with_message(format!("Snapshot not found: {}", snapshot_id));
                eprintln!("  The server may not support snapshot status polling.");
                return Err(CliError::ExitCode(1));
            }
            Err(err) => return Err(err),
        };

        let current_status = info.get("status").and_then(|v| v.as_str()).unwrap_or("");

        if current_status == "completed" {
            let size_bytes = info.get("size_bytes").and_then(|v| v.as_i64()).unwrap_or(0);
            let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
            let snapshot_uri = info
                .get("snapshot_uri")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    CliError::Other(anyhow::anyhow!(
                        "snapshot {} completed without snapshot_uri",
                        snapshot_id
                    ))
                })?
                .to_string();
            spinner.finish_with_message(format!("Snapshot completed ({:.1} MB)", size_mb));
            return Ok(SnapshotDetails {
                snapshot_id,
                snapshot_uri,
            });
        }

        if current_status == "failed" {
            let error = info
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            spinner.finish_with_message("Snapshot failed");
            return Err(CliError::Other(anyhow::anyhow!(
                "Snapshot failed: {}",
                error
            )));
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

pub async fn create_snapshot(
    ctx: &CliContext,
    sandbox_id: &str,
    timeout: f64,
    snapshot_type: Option<&str>,
) -> Result<String> {
    Ok(
        create_snapshot_with_details(ctx, sandbox_id, timeout, snapshot_type)
            .await?
            .snapshot_id,
    )
}

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    timeout: f64,
    snapshot_type: Option<&str>,
) -> Result<()> {
    let snapshot_id = create_snapshot(ctx, sandbox_id, timeout, snapshot_type).await?;
    println!("{}", snapshot_id);
    Ok(())
}
