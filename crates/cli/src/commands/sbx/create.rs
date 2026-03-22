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

/// Resolve a template name to its snapshot ID by querying the Platform API.
async fn resolve_template(ctx: &CliContext, template_name: &str) -> Result<String> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required for --template"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required for --template"))?;

    let client = ctx.client()?;
    let url = format!(
        "{}/platform/v1/organizations/{}/projects/{}/sandbox-templates",
        ctx.api_url, org_id, proj_id
    );

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to list templates (HTTP {}): {}",
            status,
            body
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let items = result
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("unexpected template list response")))?;

    for item in items {
        let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name == template_name {
            let snapshot_id = item
                .get("snapshotId")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    CliError::Other(anyhow::anyhow!(
                        "template '{}' has no snapshotId",
                        template_name
                    ))
                })?;
            eprintln!(
                "Resolved template '{}' → snapshot {}",
                template_name, snapshot_id
            );
            return Ok(snapshot_id.to_string());
        }
    }

    Err(CliError::Other(anyhow::anyhow!(
        "template '{}' not found",
        template_name
    )))
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
    template_name: Option<&str>,
    wait: bool,
) -> Result<()> {
    // Resolve --template to a snapshot ID if provided.
    let resolved_snapshot = match template_name {
        Some(name) => Some(resolve_template(ctx, name).await?),
        None => None,
    };
    let effective_snapshot = snapshot_id.or(resolved_snapshot.as_deref());

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
    if let Some(snap) = effective_snapshot {
        body["snapshot_id"] = serde_json::Value::String(snap.to_string());
    }

    let sandbox_id = create_with_request(ctx, body, wait).await?;
    println!("{}", sandbox_id);
    Ok(())
}
