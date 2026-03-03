use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    ctx: &CliContext,
    image: Option<&str>,
    cpus: f64,
    memory: i64,
    disk: i64,
    timeout: Option<i64>,
    entrypoint: &[String],
    snapshot_id: Option<&str>,
    wait: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandboxes");

    let mut body = serde_json::json!({
        "resources": {
            "cpus": cpus,
            "memory_mb": memory,
            "ephemeral_disk_mb": disk,
        },
    });
    if let Some(img) = image {
        body["image"] = serde_json::Value::String(img.to_string());
    }
    if let Some(t) = timeout {
        body["timeout_secs"] = serde_json::Value::Number(t.into());
    }
    if !entrypoint.is_empty() {
        body["entrypoint"] = serde_json::json!(entrypoint);
    }
    if let Some(snap) = snapshot_id {
        body["snapshot_id"] = serde_json::Value::String(snap.to_string());
    }

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to create sandbox (HTTP {}): {}",
            status,
            body
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let sandbox_id = result
        .get("sandbox_id")
        .or_else(|| result.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let status = result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    eprintln!("Created sandbox {} ({})", sandbox_id, status);

    if wait && status != "running" {
        eprint!("Waiting for sandbox to start...");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            if tokio::time::Instant::now() > deadline {
                eprintln!(" timed out");
                return Err(CliError::Other(anyhow::anyhow!(
                    "Sandbox did not start within 120s"
                )));
            }

            let info_resp = client
                .get(sandbox_endpoint(ctx, &format!("sandboxes/{}", sandbox_id)))
                .send()
                .await
                .map_err(CliError::Http)?;

            if info_resp.status().is_success() {
                let info: serde_json::Value = info_resp.json().await.map_err(CliError::Http)?;
                let current_status = info.get("status").and_then(|v| v.as_str()).unwrap_or("");
                if current_status == "running" {
                    eprintln!(" running");
                    println!("{}", sandbox_id);
                    return Ok(());
                }
                if current_status == "terminated" {
                    eprintln!(" terminated");
                    return Err(CliError::Other(anyhow::anyhow!(
                        "Sandbox terminated during startup"
                    )));
                }
            }
            eprint!(".");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    } else {
        println!("{}", sandbox_id);
    }

    Ok(())
}
