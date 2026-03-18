use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, sandbox_endpoint, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, sandbox_id: &str, wait: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/resume"));

    eprintln!("Resuming sandbox {}...", sandbox_id);

    let resp = client.post(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to resume sandbox (HTTP {}): {}",
            status,
            body
        )));
    }

    if wait {
        wait_for_sandbox_status(
            ctx,
            sandbox_id,
            "Waiting for sandbox to resume",
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
    }

    println!("{sandbox_id}");
    Ok(())
}
