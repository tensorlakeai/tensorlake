//! `tl sbx shared-fs` — attach/detach/list shared file systems on a specific
//! sandbox.
//!
//! These are sandbox-lifecycle operations: they hit the sandbox lifecycle API
//! (`{sandbox_endpoint}/sandboxes/{id}/file_systems`), not the platform
//! shared-file-system registry. Registering and deleting shared file systems
//! themselves lives under the top-level `tl shared-fs` command.

use comfy_table::Cell;
use reqwest::Response;
use tensorlake::sandboxes::models::{SandboxInfo, SharedFileSystemMount};

use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

/// Parse a sandbox lifecycle response into typed [`SandboxInfo`], surfacing a
/// useful error on non-2xx.
async fn parse_sandbox_response(resp: Response, action: &str) -> Result<SandboxInfo> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to {action} (HTTP {}): {}",
            status,
            body
        )));
    }
    resp.json().await.map_err(CliError::Http)
}

/// Render a sandbox's currently-mounted shared file systems as a table.
fn print_mounts_table(mounts: &[SharedFileSystemMount]) {
    if mounts.is_empty() {
        println!("No shared file systems mounted.");
        return;
    }

    let mut table = new_table(&["Shared File System ID", "Mount Path"]);
    for mount in mounts {
        table.add_row(vec![
            Cell::new(mount.file_system_id.as_str()),
            Cell::new(mount.mount_path.as_str()),
        ]);
    }
    println!("{table}");
}

pub async fn attach(
    ctx: &CliContext,
    sandbox_id: &str,
    file_system_id: &str,
    mount_path: &str,
    output_json: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/file_systems"));
    let body = serde_json::json!({
        "file_system_id": file_system_id,
        "mount_path": mount_path,
    });

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;
    let info = parse_sandbox_response(resp, "attach shared file system").await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info.shared_file_systems)?);
        return Ok(());
    }

    println!(
        "Attached shared file system '{}' at '{}' on sandbox {}.",
        file_system_id, mount_path, sandbox_id
    );
    print_mounts_table(&info.shared_file_systems);
    Ok(())
}

pub async fn detach(
    ctx: &CliContext,
    sandbox_id: &str,
    mount_path: &str,
    output_json: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/file_systems"));
    let body = serde_json::json!({ "mount_path": mount_path });

    let resp = client
        .delete(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;
    let info = parse_sandbox_response(resp, "detach shared file system").await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info.shared_file_systems)?);
        return Ok(());
    }

    println!(
        "Detached shared file system at '{}' from sandbox {}.",
        mount_path, sandbox_id
    );
    print_mounts_table(&info.shared_file_systems);
    Ok(())
}

pub async fn list(ctx: &CliContext, sandbox_id: &str, output_json: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;
    let info = parse_sandbox_response(resp, "list sandbox shared file systems").await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info.shared_file_systems)?);
        return Ok(());
    }

    print_mounts_table(&info.shared_file_systems);
    Ok(())
}
