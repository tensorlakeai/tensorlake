use std::path::Path;

use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_sandbox_path, sandbox_proxy_base};
use crate::error::{CliError, Result};

/// Build a reqwest client with auth headers and optional Host override for the sandbox proxy.
fn build_proxy_client(ctx: &CliContext, host_override: Option<&str>) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Ok(token) = ctx.bearer_token() {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
    }
    if let Some(org_id) = ctx.effective_organization_id() {
        headers.insert("X-Forwarded-Organization-Id", org_id.parse().unwrap());
    }
    if let Some(proj_id) = ctx.effective_project_id() {
        headers.insert("X-Forwarded-Project-Id", proj_id.parse().unwrap());
    }
    if let Some(host) = host_override {
        headers.insert(reqwest::header::HOST, host.parse().unwrap());
    }
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .map_err(|e| CliError::Other(anyhow::anyhow!("{}", e)))
}

pub async fn run(ctx: &CliContext, src: &str, dest: &str) -> Result<()> {
    let (src_sbx, src_path) = parse_sandbox_path(src);
    let (dest_sbx, dest_path) = parse_sandbox_path(dest);

    if src_sbx.is_some() && dest_sbx.is_some() {
        return Err(CliError::usage(
            "Cannot copy between two sandboxes. One side must be local.",
        ));
    }
    if src_sbx.is_none() && dest_sbx.is_none() {
        return Err(CliError::usage(
            "One of src or dest must be a sandbox path (sandbox_id:/path).",
        ));
    }

    if let Some(sandbox_id) = src_sbx {
        // Download: sandbox -> local
        let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
        let client = build_proxy_client(ctx, host_override.as_deref())?;

        let resp = client
            .get(format!(
                "{}/api/v1/files?path={}",
                proxy_base,
                urlencoding::encode(src_path)
            ))
            .send()
            .await
            .map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to download file (HTTP {}): {}",
                status,
                body
            )));
        }

        let data = resp.bytes().await.map_err(CliError::Http)?;
        let mut final_dest = dest_path.to_string();
        if Path::new(dest_path).is_dir() {
            let filename = Path::new(src_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file");
            final_dest = Path::new(dest_path).join(filename).display().to_string();
        }
        std::fs::write(&final_dest, &data)?;
        println!("{} -> {} ({} bytes)", src, final_dest, data.len());
    } else if let Some(sandbox_id) = dest_sbx {
        // Upload: local -> sandbox
        if !Path::new(src_path).is_file() {
            return Err(CliError::usage(format!(
                "Local file not found: {}",
                src_path
            )));
        }

        let data = std::fs::read(src_path)?;
        let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
        let client = build_proxy_client(ctx, host_override.as_deref())?;

        let resp = client
            .put(format!(
                "{}/api/v1/files?path={}",
                proxy_base,
                urlencoding::encode(dest_path)
            ))
            .body(data.clone())
            .send()
            .await
            .map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to upload file (HTTP {}): {}",
                status,
                body
            )));
        }
        println!("{} -> {} ({} bytes)", src_path, dest, data.len());
    }

    Ok(())
}
