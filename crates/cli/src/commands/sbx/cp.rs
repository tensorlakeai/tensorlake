use std::path::Path;

use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_sandbox_path, sandbox_proxy_base, with_host};
use crate::error::{CliError, Result};

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
            "One of src or dest must be a sandbox path (identifier:/path).",
        ));
    }

    if let Some(sandbox_id) = src_sbx {
        // Download: sandbox -> local
        let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
        let client = ctx.client()?;

        let resp = with_host(
            client.get(format!(
                "{}/api/v1/files?path={}",
                proxy_base,
                urlencoding::encode(src_path)
            )),
            host_override,
        )
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
        let client = ctx.client()?;

        let resp = with_host(
            client
                .put(format!(
                    "{}/api/v1/files?path={}",
                    proxy_base,
                    urlencoding::encode(dest_path)
                ))
                .body(data.clone()),
            host_override,
        )
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
