use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, format_sandbox_wait_termination_message, sandbox_endpoint,
    snapshot,
};
use crate::error::{CliError, Result};
use futures::future::try_join_all;
use tokio::time::{Duration, Instant};

pub async fn run(ctx: &CliContext, sandbox_id: &str, timeout: f64, times: usize) -> Result<()> {
    if times == 1 {
        eprintln!("Cloning sandbox {}...", sandbox_id);
    } else {
        eprintln!("Cloning sandbox {} into {} copies...", sandbox_id, times);
    }

    let snapshot_id = snapshot::create_snapshot(ctx, sandbox_id, timeout, None).await?;

    if times > 1 {
        eprintln!(
            "Launching {} clones in parallel from snapshot {}...",
            times, snapshot_id
        );
    }

    let clone_tasks = (0..times).map(|copy_index| {
        let ctx = ctx.clone();
        let snapshot_id = snapshot_id.clone();
        async move { create_clone_from_snapshot(&ctx, &snapshot_id, copy_index, times).await }
    });

    let mut cloned = try_join_all(clone_tasks).await?;
    cloned.sort_by_key(|(copy_index, _)| *copy_index);

    let cloned_ids: Vec<String> = cloned
        .into_iter()
        .map(|(_, sandbox_id)| sandbox_id)
        .collect();

    for sandbox_id in &cloned_ids {
        println!("{}", sandbox_id);
    }

    if times > 1 {
        eprintln!();
        for (i, id) in cloned_ids.iter().enumerate() {
            eprintln!("  Sandbox {}: {}", i + 1, id);
        }
    }

    Ok(())
}

async fn create_clone_from_snapshot(
    ctx: &CliContext,
    snapshot_id: &str,
    copy_index: usize,
    total_copies: usize,
) -> Result<(usize, String)> {
    if total_copies > 1 {
        eprintln!("Starting clone {}/{}...", copy_index + 1, total_copies);
    }

    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandboxes");

    let resp = client
        .post(&url)
        .json(&serde_json::json!({
            "snapshot_id": snapshot_id,
        }))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to create clone {}/{} from snapshot {} (HTTP {}): {}",
            copy_index + 1,
            total_copies,
            snapshot_id,
            status,
            body
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let sandbox_id = result
        .get("sandbox_id")
        .or_else(|| result.get("id"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_string();
    let status = result
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");

    if total_copies > 1 {
        eprintln!(
            "Clone {}/{} created: {} ({})",
            copy_index + 1,
            total_copies,
            sandbox_id,
            status
        );
    }

    if status != "running" {
        wait_for_clone_running(ctx, &sandbox_id, copy_index, total_copies).await?;
    }

    if total_copies > 1 {
        eprintln!(
            "Clone {}/{} is running: {}",
            copy_index + 1,
            total_copies,
            sandbox_id
        );
    }

    Ok((copy_index, sandbox_id))
}

async fn wait_for_clone_running(
    ctx: &CliContext,
    sandbox_id: &str,
    copy_index: usize,
    total_copies: usize,
) -> Result<()> {
    let client = ctx.client()?;
    let deadline = Instant::now() + DEFAULT_SANDBOX_WAIT_TIMEOUT;
    let poll_interval = Duration::from_secs(1);

    loop {
        if Instant::now() > deadline {
            return Err(CliError::Other(anyhow::anyhow!(
                "Clone {}/{} ({}) did not reach 'running' within {}s",
                copy_index + 1,
                total_copies,
                sandbox_id,
                DEFAULT_SANDBOX_WAIT_TIMEOUT.as_secs()
            )));
        }

        let info_resp = client
            .get(sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}")))
            .send()
            .await
            .map_err(CliError::Http)?;

        if info_resp.status().is_success() {
            let info: serde_json::Value = info_resp.json().await.map_err(CliError::Http)?;
            let current_status = info
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("");

            if current_status == "running" {
                return Ok(());
            }

            if current_status == "terminated" {
                let subject = format!("Clone {}/{} ({})", copy_index + 1, total_copies, sandbox_id);
                let message = format_sandbox_wait_termination_message(&subject, "running", &info);
                return Err(CliError::Other(anyhow::anyhow!(message)));
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}
