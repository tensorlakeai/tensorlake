pub mod create;
pub mod describe;
pub mod import;
pub mod ls;
pub mod register;
pub mod rm;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use indicatif::{ProgressBar, ProgressStyle};
use tensorlake::{
    sandbox_images::{SandboxImageBuildEvent, cas_image_service_url},
    sandbox_templates::SandboxTemplatesClient,
};

const BUILD_CONTEXT_PROGRESS_PREFIX: &str = "Creating build context archive:";
const UPLOAD_CONTEXT_PROGRESS_PREFIX: &str = "Uploading build context archive:";

pub struct ImageBuildEventRenderer {
    progress: Option<ProgressBar>,
}

impl ImageBuildEventRenderer {
    pub fn new() -> Self {
        Self { progress: None }
    }

    pub fn render(&mut self, event: SandboxImageBuildEvent) {
        match event {
            SandboxImageBuildEvent::Status(message) if is_progress_status(&message) => {
                self.render_progress(&message);
            }
            SandboxImageBuildEvent::Status(message) => {
                self.finish_progress_line();
                eprintln!("⚙️  {message}");
            }
            SandboxImageBuildEvent::BuildLog { message, .. } => {
                self.finish_progress_line();
                eprintln!("{message}");
            }
            SandboxImageBuildEvent::Warning(message) => {
                self.finish_progress_line();
                eprintln!("⚠️  {message}");
            }
        }
    }

    fn render_progress(&mut self, message: &str) {
        let progress = self.progress.get_or_insert_with(|| {
            let progress = ProgressBar::new_spinner();
            progress.set_style(ProgressStyle::with_template("{msg}").unwrap());
            progress
        });
        progress.set_message(format!("⚙️  {message}"));
        progress.tick();
    }

    fn finish_progress_line(&mut self) {
        if let Some(progress) = self.progress.take() {
            progress.finish_and_clear();
        }
    }
}

impl Drop for ImageBuildEventRenderer {
    fn drop(&mut self) {
        self.finish_progress_line();
    }
}

fn is_progress_status(message: &str) -> bool {
    message.starts_with(BUILD_CONTEXT_PROGRESS_PREFIX)
        || message.starts_with(UPLOAD_CONTEXT_PROGRESS_PREFIX)
}

/// Build the sandbox-templates API base URL. API keys use the token-scoped
/// root route; PATs keep the explicitly addressed compatibility route.
pub fn templates_base_url(ctx: &CliContext) -> Result<String> {
    if ctx.api_key.is_some() {
        return Ok(format!(
            "{}/platform/v1/sandbox-templates",
            ctx.api_url.trim_end_matches('/')
        ));
    }
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required for --image"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required for --image"))?;
    let base = format!(
        "{}/platform/v1/organizations/{}/projects/{}/sandbox-templates",
        ctx.api_url.trim_end_matches('/'),
        org_id,
        proj_id
    );
    Ok(base)
}

pub fn org_and_project(ctx: &CliContext) -> Result<(String, String)> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required for --image"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required for --image"))?;
    Ok((org_id, proj_id))
}

pub fn sandbox_templates_client(ctx: &CliContext) -> Result<SandboxTemplatesClient> {
    let client = ctx.scoped_cloud_client()?;
    if ctx.api_key.is_some() {
        return Ok(SandboxTemplatesClient::new_api_key_scoped(client));
    }
    let (org_id, proj_id) = org_and_project(ctx)?;
    Ok(SandboxTemplatesClient::new(client, org_id, proj_id))
}

/// Page through the list, returning the full JSON item if found.
pub async fn find_image_item_in_paginated_list(
    ctx: &CliContext,
    client: &reqwest::Client,
    base_url: &str,
    image_ref: &str,
) -> Result<Option<serde_json::Value>> {
    let mut url = format!("{}?pageSize=100", base_url);

    let mut page = 0u32;
    loop {
        page += 1;
        if ctx.debug {
            eprintln!(
                "DEBUG find_image_in_paginated_list: page {} GET {}",
                page, url
            );
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

        if let Some(item) = find_image_item_in_page(&result, image_ref) {
            return Ok(Some(item));
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

/// Collect all items across all pages.
pub async fn list_all_images(
    ctx: &CliContext,
    client: &reqwest::Client,
    base_url: &str,
) -> Result<Vec<serde_json::Value>> {
    let mut url = format!("{}?pageSize=100", base_url);
    let mut all_items: Vec<serde_json::Value> = Vec::new();

    loop {
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

        if let Some(items) = result.get("items").and_then(|v| v.as_array()) {
            all_items.extend(items.iter().cloned());
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

    Ok(all_items)
}

/// Collect all project-owned CAS image names from Image Service. The service
/// returns an opaque cursor rather than a URL so the authenticated ingress
/// remains the only public routing contract.
pub async fn list_all_cas_images(
    ctx: &CliContext,
    client: &reqwest::Client,
) -> Result<Vec<serde_json::Value>> {
    let base_url = format!("{}/images", cas_image_service_url(&ctx.api_url));
    let mut cursor: Option<String> = None;
    let mut all_items = Vec::new();

    loop {
        let mut request = client.get(&base_url).query(&[("page_size", "100")]);
        if let Some(cursor) = cursor.as_deref() {
            request = request.query(&[("cursor", cursor)]);
        }
        let response = request.send().await.map_err(CliError::Http)?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to list CAS images from Image Service (HTTP {}): {}",
                status,
                body
            )));
        }
        let result: serde_json::Value = response.json().await.map_err(CliError::Http)?;
        if let Some(items) = result.get("items").and_then(serde_json::Value::as_array) {
            all_items.extend(items.iter().cloned());
        }
        cursor = result
            .get("pagination")
            .and_then(|pagination| pagination.get("next"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if cursor.is_none() {
            return Ok(all_items);
        }
    }
}

fn find_image_item_in_page(
    result: &serde_json::Value,
    image_ref: &str,
) -> Option<serde_json::Value> {
    let items = result.get("items").and_then(|v| v.as_array())?;
    for item in items {
        if item_matches_image_ref(item, image_ref) {
            return Some(item.clone());
        }
    }
    None
}

pub fn item_matches_image_ref(item: &serde_json::Value, image_ref: &str) -> bool {
    let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
    id == image_ref || name == image_ref
}

pub fn absolute_api_url(api_url: &str, next: &str) -> String {
    if next.starts_with("http://") || next.starts_with("https://") {
        next.to_string()
    } else {
        format!("{}{}", api_url.trim_end_matches('/'), next)
    }
}

#[cfg(test)]
mod tests {
    use super::{absolute_api_url, item_matches_image_ref, templates_base_url};
    use crate::auth::context::CliContext;
    use crate::config::resolver::ResolvedConfig;
    use serde_json::json;

    fn test_context(api_key: Option<&str>, personal_access_token: Option<&str>) -> CliContext {
        CliContext::from_resolved(ResolvedConfig {
            api_url: "https://api.tensorlake.ai/".to_string(),
            cloud_url: "https://cloud.tensorlake.ai".to_string(),
            namespace: "default".to_string(),
            api_key: api_key.map(str::to_string),
            personal_access_token: personal_access_token.map(str::to_string),
            organization_id: Some("org-1".to_string()),
            project_id: Some("proj-1".to_string()),
            debug: false,
        })
    }

    #[test]
    fn api_key_templates_url_uses_token_scoped_root_route() {
        let ctx = test_context(Some("tl_apiKey_test"), Some("tl_pat_test"));
        assert_eq!(
            templates_base_url(&ctx).unwrap(),
            "https://api.tensorlake.ai/platform/v1/sandbox-templates"
        );
    }

    #[test]
    fn pat_templates_url_keeps_explicit_project_route() {
        let ctx = test_context(None, Some("tl_pat_test"));
        assert_eq!(
            templates_base_url(&ctx).unwrap(),
            "https://api.tensorlake.ai/platform/v1/organizations/org-1/projects/proj-1/sandbox-templates"
        );
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
    fn absolute_api_url_resolves_relative_next_link() {
        let next =
            "/platform/v1/organizations/org/projects/proj/sandbox-templates?pageSize=100&next=abc";
        assert_eq!(
            absolute_api_url("https://api.tensorlake.dev", next),
            format!("https://api.tensorlake.dev{}", next)
        );
    }
}
