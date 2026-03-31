use crate::auth::context::CliContext;
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

/// Resolve an image name to its snapshot ID by querying the Platform API.
///
/// Tries a direct GET by name first; falls back to paginated listing if the
/// server returns 404 (older Platform versions or ID-only endpoints).
async fn resolve_image(ctx: &CliContext, image_name: &str) -> Result<String> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required for --image"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required for --image"))?;

    let client = ctx.client()?;
    let direct_url = format!(
        "{}/platform/v1/organizations/{}/projects/{}/sandbox-templates/{}",
        ctx.api_url, org_id, proj_id, image_name
    );

    if ctx.debug {
        eprintln!("DEBUG resolve_image: trying direct lookup GET {}", direct_url);
    }

    let direct_resp = client.get(&direct_url).send().await.map_err(CliError::Http)?;
    if direct_resp.status().is_success() {
        let result: serde_json::Value = direct_resp.json().await.map_err(CliError::Http)?;
        if let Some(snapshot_id) = snapshot_id_from_item(&result, image_name)? {
            if ctx.debug {
                eprintln!("DEBUG resolve_image: direct lookup succeeded");
            }
            eprintln!("Resolved image '{}' → snapshot {}", image_name, snapshot_id);
            return Ok(snapshot_id);
        }
        return Err(CliError::Other(anyhow::anyhow!(
            "image '{}' has no snapshotId",
            image_name
        )));
    }
    if direct_resp.status().as_u16() != 404 {
        let status = direct_resp.status();
        let body = direct_resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to fetch image '{}' (HTTP {}): {}",
            image_name,
            status,
            body
        )));
    }

    if ctx.debug {
        eprintln!(
            "DEBUG resolve_image: direct lookup returned 404, falling back to paginated list"
        );
    }

    let snapshot_id = find_image_in_paginated_list(ctx, &client, &org_id, &proj_id, image_name)
        .await?
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("image '{}' not found", image_name)))?;

    if ctx.debug {
        eprintln!("DEBUG resolve_image: paginated list lookup succeeded");
    }
    eprintln!("Resolved image '{}' → snapshot {}", image_name, snapshot_id);
    Ok(snapshot_id)
}

/// Page through the sandbox-templates list endpoint looking for an image by
/// name or ID. Returns `Ok(Some(snapshot_id))` if found, `Ok(None)` if
/// exhausted without a match.
async fn find_image_in_paginated_list(
    ctx: &CliContext,
    client: &reqwest::Client,
    org_id: &str,
    proj_id: &str,
    image_ref: &str,
) -> Result<Option<String>> {
    let mut url = format!(
        "{}/platform/v1/organizations/{}/projects/{}/sandbox-templates?pageSize=100",
        ctx.api_url, org_id, proj_id
    );

    let mut page = 0u32;
    loop {
        page += 1;
        if ctx.debug {
            eprintln!("DEBUG find_image_in_paginated_list: page {} GET {}", page, url);
        }

        let resp = client.get(&url).send().await.map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to list images (HTTP {}): {}",
                status,
                body
            )));
        }

        let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;

        if let Some(snapshot_id) = find_image_snapshot_id(&result, image_ref)? {
            return Ok(Some(snapshot_id));
        }

        let next = result
            .get("pagination")
            .and_then(|v| v.get("next"))
            .and_then(|v| v.as_str());
        let Some(next) = next else {
            break;
        };

        url = absolute_api_url(&ctx.api_url, next);
    }

    Ok(None)
}

fn find_image_snapshot_id(
    result: &serde_json::Value,
    image_ref: &str,
) -> Result<Option<String>> {
    let items = result
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("unexpected image list response")))?;

    for item in items {
        if item_matches_image_ref(item, image_ref) {
            if let Some(snapshot_id) = snapshot_id_from_item(item, image_ref)? {
                return Ok(Some(snapshot_id));
            }
        }
    }

    Ok(None)
}

fn item_matches_image_ref(item: &serde_json::Value, image_ref: &str) -> bool {
    let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
    id == image_ref || name == image_ref
}

fn snapshot_id_from_item(
    item: &serde_json::Value,
    image_ref: &str,
) -> Result<Option<String>> {
    let snapshot_id = item.get("snapshotId").and_then(|v| v.as_str());
    match snapshot_id {
        Some(snapshot_id) => Ok(Some(snapshot_id.to_string())),
        None => Err(CliError::Other(anyhow::anyhow!(
            "image '{}' has no snapshotId",
            image_ref
        ))),
    }
}

fn absolute_api_url(api_url: &str, next: &str) -> String {
    if next.starts_with("http://") || next.starts_with("https://") {
        next.to_string()
    } else {
        format!("{}{}", api_url.trim_end_matches('/'), next)
    }
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
    use super::{
        absolute_api_url, build_create_request_body, find_image_snapshot_id,
        item_matches_image_ref, snapshot_id_from_item,
    };
    use serde_json::json;

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

    #[test]
    fn find_image_snapshot_id_matches_name() {
        let payload = json!({
            "items": [
                {"id": "sandbox_template_123", "name": "k3s-base", "snapshotId": "snap-1"}
            ]
        });

        let snapshot_id = find_image_snapshot_id(&payload, "k3s-base")
            .expect("lookup should succeed");
        assert_eq!(snapshot_id.as_deref(), Some("snap-1"));
    }

    #[test]
    fn find_image_snapshot_id_matches_id() {
        let payload = json!({
            "items": [
                {"id": "sandbox_template_123", "name": "k3s-base", "snapshotId": "snap-1"}
            ]
        });

        let snapshot_id = find_image_snapshot_id(&payload, "sandbox_template_123")
            .expect("lookup should succeed");
        assert_eq!(snapshot_id.as_deref(), Some("snap-1"));
    }

    #[test]
    fn item_matches_image_ref_matches_name_or_id() {
        let item = json!({
            "id": "sandbox_template_123",
            "name": "k3s-base",
            "snapshotId": "snap-1"
        });

        assert!(item_matches_image_ref(&item, "sandbox_template_123"));
        assert!(item_matches_image_ref(&item, "k3s-base"));
        assert!(!item_matches_image_ref(&item, "other"));
    }

    #[test]
    fn snapshot_id_from_item_reads_single_template_response() {
        let item = json!({
            "id": "sandbox_template_123",
            "name": "k3s-base",
            "snapshotId": "snap-1"
        });

        let snapshot_id = snapshot_id_from_item(&item, "sandbox_template_123")
            .expect("single item lookup should succeed");
        assert_eq!(snapshot_id.as_deref(), Some("snap-1"));
    }

    #[test]
    fn absolute_api_url_resolves_relative_next_link() {
        let next = "/platform/v1/organizations/org/projects/proj/sandbox-templates?pageSize=100&next=abc";
        assert_eq!(
            absolute_api_url("https://api.tensorlake.dev", next),
            format!("https://api.tensorlake.dev{}", next)
        );
    }
}
