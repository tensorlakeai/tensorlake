use crate::auth::context::CliContext;
use crate::commands::sbx::image::resolve_image;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, sandbox_endpoint, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};

const DEFAULT_SANDBOX_CPUS: f64 = 1.0;
const DEFAULT_SANDBOX_MEMORY_MB: i64 = 1024;

pub async fn create_with_request(
    ctx: &CliContext,
    body: serde_json::Value,
    wait: bool,
) -> Result<String> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandboxes");

    if ctx.debug {
        eprintln!("DEBUG sandbox create url: {}", url);
        eprintln!(
            "DEBUG sandbox create payload: {}",
            serde_json::to_string(&body).unwrap_or_else(|_| "<failed to serialize body>".to_string())
        );
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

pub async fn run(
    ctx: &CliContext,
    cpus: Option<f64>,
    memory: Option<i64>,
    timeout: Option<i64>,
    entrypoint: &[String],
    snapshot_id: Option<&str>,
    image_name: Option<&str>,
    wait: bool,
) -> Result<()> {
    // Resolve --image to a snapshot ID if provided.
    let resolved_snapshot = match image_name {
        Some(name) => Some(resolve_image(ctx, name).await?),
        None => None,
    };
    let effective_snapshot = snapshot_id.or(resolved_snapshot.as_deref());

    let body =
        build_create_request_body(cpus, memory, timeout, entrypoint, effective_snapshot);

    let sandbox_id = create_with_request(ctx, body, wait).await?;
    println!("{}", sandbox_id);
    Ok(())
}

fn build_create_request_body(
    cpus: Option<f64>,
    memory: Option<i64>,
    timeout: Option<i64>,
    entrypoint: &[String],
    snapshot_id: Option<&str>,
) -> serde_json::Value {
    let mut body = serde_json::json!({});

    if let Some(snapshot_id) = snapshot_id {
        let mut resources = serde_json::Map::new();
        if let Some(cpus) = cpus {
            resources.insert("cpus".to_string(), serde_json::json!(cpus));
        }
        if let Some(memory) = memory {
            resources.insert("memory_mb".to_string(), serde_json::json!(memory));
        }
        if !resources.is_empty() {
            body["resources"] = serde_json::Value::Object(resources);
        }
        body["snapshot_id"] = serde_json::Value::String(snapshot_id.to_string());
    } else {
        body["resources"] = serde_json::json!({
            "cpus": cpus.unwrap_or(DEFAULT_SANDBOX_CPUS),
            "memory_mb": memory.unwrap_or(DEFAULT_SANDBOX_MEMORY_MB),
        });
    }

    if let Some(t) = timeout {
        body["timeout_secs"] = serde_json::Value::Number(t.into());
    }
    if !entrypoint.is_empty() {
        body["entrypoint"] = serde_json::json!(entrypoint);
    }

    body
}

#[cfg(test)]
mod tests {
    use super::build_create_request_body;

    #[test]
    fn create_body_uses_defaults_without_snapshot() {
        let body = build_create_request_body(None, None, None, &[], None);

        assert_eq!(body["resources"]["cpus"], 1.0);
        assert_eq!(body["resources"]["memory_mb"], 1024);
        assert!(body.get("snapshot_id").is_none());
    }

    #[test]
    fn create_body_omits_resources_for_snapshot_without_overrides() {
        let body = build_create_request_body(None, None, None, &[], Some("snap-1"));

        assert_eq!(body["snapshot_id"], "snap-1");
        assert!(body.get("resources").is_none());
    }

    #[test]
    fn create_body_includes_only_explicit_snapshot_overrides() {
        let body = build_create_request_body(Some(2.5), None, None, &[], Some("snap-1"));

        assert_eq!(body["snapshot_id"], "snap-1");
        assert_eq!(body["resources"]["cpus"], 2.5);
        assert!(body["resources"].get("memory_mb").is_none());
    }
}
