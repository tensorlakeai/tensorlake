use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, sandbox_id: &str, new_name: &str) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let body = serde_json::json!({"name": new_name});
    let resp = client
        .patch(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to update sandbox name (HTTP {}): {}",
            status,
            text
        )));
    }

    println!("{sandbox_id}");
    Ok(())
}
