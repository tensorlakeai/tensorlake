use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, snapshot_ids: &[String]) -> Result<()> {
    let client = ctx.client()?;

    for snapshot_id in snapshot_ids {
        let resp = client
            .delete(sandbox_endpoint(ctx, &format!("snapshots/{}", snapshot_id)))
            .send()
            .await
            .map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to delete snapshot {} (HTTP {}): {}",
                snapshot_id,
                status,
                body
            )));
        }

        println!("Deleted snapshot {}", snapshot_id);
    }

    Ok(())
}
