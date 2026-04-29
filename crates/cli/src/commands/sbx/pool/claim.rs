use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, sandbox_endpoint, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, pool_id: &str, wait: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandbox-pools/{pool_id}/sandboxes"));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to claim sandbox from pool {} (HTTP {}): {}",
            pool_id,
            status,
            body
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let sandbox_id = result
        .get("sandbox_id")
        .or_else(|| result.get("id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!("claim response missing sandbox_id"))
        })?
        .to_string();
    let status = result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

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

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    if is_tty {
        eprintln!("Sandbox {} claimed from pool {}.", sandbox_id, pool_id);
    } else {
        println!("{}", sandbox_id);
    }
    Ok(())
}
