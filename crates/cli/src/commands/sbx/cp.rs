use std::path::Path;

use futures::StreamExt;
use reqwest::header::CONTENT_LENGTH;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use crate::auth::context::CliContext;
use crate::commands::sbx::{
    parse_sandbox_path, resolve_sandbox_proxy_target, with_sandbox_headers,
};
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
        let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
        let client = ctx.client()?;

        let resp = with_sandbox_headers(
            client.get(format!(
                "{}/api/v1/files?path={}",
                target.proxy_base,
                urlencoding::encode(src_path)
            )),
            &target,
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

        let mut final_dest = dest_path.to_string();
        if Path::new(dest_path).is_dir() {
            let filename = Path::new(src_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file");
            final_dest = Path::new(dest_path).join(filename).display().to_string();
        }

        let mut file = tokio::fs::File::create(&final_dest).await?;
        let mut downloaded = 0u64;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(CliError::Http)?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
        }
        file.flush().await?;
        println!("{} -> {} ({} bytes)", src, final_dest, downloaded);
    } else if let Some(sandbox_id) = dest_sbx {
        // Upload: local -> sandbox
        if !Path::new(src_path).is_file() {
            return Err(CliError::usage(format!(
                "Local file not found: {}",
                src_path
            )));
        }

        let file = tokio::fs::File::open(src_path).await?;
        let size = file.metadata().await?.len();
        let stream = ReaderStream::new(file);
        let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
        let client = ctx.client()?;

        let resp = with_sandbox_headers(
            client
                .put(format!(
                    "{}/api/v1/files?path={}",
                    target.proxy_base,
                    urlencoding::encode(dest_path)
                ))
                .header(CONTENT_LENGTH, size)
                .body(reqwest::Body::wrap_stream(stream)),
            &target,
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
        println!("{} -> {} ({} bytes)", src_path, dest, size);
    }

    Ok(())
}
