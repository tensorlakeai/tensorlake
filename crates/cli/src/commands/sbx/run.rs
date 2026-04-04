use crate::auth::context::CliContext;
use crate::commands::sbx::{apply_proxy_access_settings, sandbox_endpoint};
use crate::error::{CliError, Result};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    ctx: &CliContext,
    command: &str,
    args: &[String],
    image: Option<&str>,
    cpus: f64,
    memory: i64,
    disk: Option<i64>,
    timeout: Option<f64>,
    workdir: Option<&str>,
    env: &[String],
    keep: bool,
    ports: &[u16],
    allow_unauthenticated_access: bool,
    no_internet: bool,
    network_allow: &[String],
    network_deny: &[String],
) -> Result<()> {
    let client = ctx.client()?;

    // Create sandbox (lifecycle API)
    let label = image
        .map(|i| format!("image {}", i))
        .unwrap_or_else(|| "default image".to_string());
    eprintln!("Creating sandbox with {}...", label);

    let mut resources = serde_json::json!({
        "cpus": cpus,
        "memory_mb": memory,
    });
    if let Some(d) = disk {
        resources["ephemeral_disk_mb"] = serde_json::json!(d);
    }
    let mut create_body = serde_json::json!({
        "resources": resources,
    });
    if let Some(img) = image {
        create_body["image"] = serde_json::Value::String(img.to_string());
    }
    apply_proxy_access_settings(&mut create_body, ports, allow_unauthenticated_access);
    let has_network = no_internet || !network_allow.is_empty() || !network_deny.is_empty();
    if has_network {
        let mut network = serde_json::json!({});
        if no_internet {
            network["allow_internet_access"] = serde_json::Value::Bool(false);
        }
        if !network_allow.is_empty() {
            network["allow_out"] = serde_json::json!(network_allow);
        }
        if !network_deny.is_empty() {
            network["deny_out"] = serde_json::json!(network_deny);
        }
        create_body["network"] = network;
    }

    let create_resp = client
        .post(sandbox_endpoint(ctx, "sandboxes"))
        .json(&create_body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !create_resp.status().is_success() {
        let status = create_resp.status();
        let body = create_resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to create sandbox (HTTP {}): {}",
            status,
            body
        )));
    }

    let create_result: serde_json::Value = create_resp.json().await.map_err(CliError::Http)?;
    let sandbox_id = create_result
        .get("sandbox_id")
        .or_else(|| create_result.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    // Wait for sandbox to start (lifecycle API)
    crate::commands::sbx::wait_for_sandbox_status(
        ctx,
        &sandbox_id,
        "Waiting for sandbox to start",
        "running",
        crate::commands::sbx::DEFAULT_SANDBOX_WAIT_TIMEOUT,
    )
    .await?;

    // Run exec (proxy API)
    let result =
        crate::commands::sbx::exec::run(ctx, &sandbox_id, command, args, timeout, workdir, env)
            .await;

    // Cleanup (lifecycle API)
    if keep {
        eprintln!("Sandbox {} kept alive.", sandbox_id);
    } else {
        let _ = client
            .delete(sandbox_endpoint(ctx, &format!("sandboxes/{}", sandbox_id)))
            .send()
            .await;
    }

    result
}
