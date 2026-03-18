use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, sandbox_endpoint, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};

pub async fn create_with_request(
    ctx: &CliContext,
    body: serde_json::Value,
    wait: bool,
) -> Result<String> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandboxes");

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
        .unwrap_or("unknown")
        .to_string();
    let status = result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    eprintln!("Created sandbox {} ({})", sandbox_id, status);

    if wait && status != "running" {
        wait_for_sandbox_status(
            ctx,
            &sandbox_id,
            "Waiting for sandbox to start",
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
    }

    Ok(sandbox_id)
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    ctx: &CliContext,
    image: Option<&str>,
    cpus: f64,
    memory: i64,
    timeout: Option<i64>,
    entrypoint: &[String],
    snapshot_id: Option<&str>,
    wait: bool,
) -> Result<()> {
    let mut body = serde_json::json!({
        "resources": {
            "cpus": cpus,
            "memory_mb": memory,
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

    let sandbox_id = create_with_request(ctx, body, wait).await?;
    println!("{}", sandbox_id);
    Ok(())
}
