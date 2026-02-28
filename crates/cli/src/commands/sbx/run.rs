use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    ctx: &CliContext,
    command: &str,
    args: &[String],
    image: Option<&str>,
    cpus: f64,
    memory: i64,
    disk: i64,
    timeout: Option<f64>,
    workdir: Option<&str>,
    env: &[String],
    keep: bool,
) -> Result<()> {
    let client = ctx.client()?;

    // Create sandbox (lifecycle API)
    let label = image
        .map(|i| format!("image {}", i))
        .unwrap_or_else(|| "default image".to_string());
    eprintln!("Creating sandbox with {}...", label);

    let mut create_body = serde_json::json!({
        "resources": {
            "cpus": cpus,
            "memory_mb": memory,
            "ephemeral_disk_mb": disk,
        },
    });
    if let Some(img) = image {
        create_body["image"] = serde_json::Value::String(img.to_string());
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
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        if tokio::time::Instant::now() > deadline {
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
            let status = info.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if status == "running" {
                break;
            }
            if status == "terminated" {
                return Err(CliError::Other(anyhow::anyhow!(
                    "Sandbox terminated during startup"
                )));
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    eprintln!("Sandbox {} is running.", sandbox_id);

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
