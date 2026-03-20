use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub async fn create_snapshot(ctx: &CliContext, sandbox_id: &str, timeout: f64) -> Result<String> {
    let client = ctx.client()?;

    eprintln!("Snapshotting sandbox {}...", sandbox_id);

    let resp = client
        .post(sandbox_endpoint(
            ctx,
            &format!("sandboxes/{}/snapshot", sandbox_id),
        ))
        .send()
        .await
        .map_err(CliError::Http)?;

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
        .unwrap_or("unknown");
    let status = result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    eprintln!("Snapshot {} initiated ({})", snapshot_id, status);

    if status == "completed" {
        eprintln!("Snapshot completed");
        return Ok(snapshot_id.to_string());
    }

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

        let info_resp = client
            .get(sandbox_endpoint(ctx, &format!("snapshots/{}", snapshot_id)))
            .send()
            .await
            .map_err(CliError::Http)?;

        if info_resp.status().as_u16() == 404 {
            spinner.finish_with_message(format!("Snapshot not found: {}", snapshot_id));
            eprintln!("  The server may not support snapshot status polling.");
            return Err(CliError::ExitCode(1));
        }

        if info_resp.status().is_success() {
            let info: serde_json::Value = info_resp.json().await.map_err(CliError::Http)?;
            let current_status = info.get("status").and_then(|v| v.as_str()).unwrap_or("");

            if current_status == "completed" {
                let size_bytes = info.get("size_bytes").and_then(|v| v.as_i64()).unwrap_or(0);
                let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
                spinner.finish_with_message(format!(
                    "Snapshot completed ({:.1} MB)",
                    size_mb
                ));
                return Ok(snapshot_id.to_string());
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
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

pub async fn run(ctx: &CliContext, sandbox_id: &str, timeout: f64) -> Result<()> {
    let snapshot_id = create_snapshot(ctx, sandbox_id, timeout).await?;
    println!("{}", snapshot_id);
    Ok(())
}
