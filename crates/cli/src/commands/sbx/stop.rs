use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, sandbox_ids: &[String]) -> Result<()> {
    let client = ctx.client()?;

    for sandbox_id in sandbox_ids {
        let resp = client
            .delete(sandbox_endpoint(ctx, &format!("sandboxes/{}", sandbox_id)))
            .send()
            .await
            .map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to stop sandbox {} (HTTP {}): {}",
                sandbox_id,
                status,
                body
            )));
        }

        println!("Stopped sandbox {}", sandbox_id);
    }

    Ok(())
}
