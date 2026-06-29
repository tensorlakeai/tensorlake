//! `tl sbx fs` — attach/detach/list ZeroFS file systems on a specific sandbox.
//!
//! These are sandbox-lifecycle operations: they hit the sandbox lifecycle API
//! (`{sandbox_endpoint}/sandboxes/{id}/file_systems`), not the platform
//! file-system registry. Registering and deleting file systems themselves lives
//! under the top-level `tl fs` command.

use comfy_table::Cell;
use reqwest::Response;

use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

/// Parse a sandbox lifecycle response, surfacing a useful error on non-2xx.
async fn parse_sandbox_response(resp: Response, action: &str) -> Result<serde_json::Value> {
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

/// Render the sandbox's currently-mounted file systems as a table.
fn print_mounts_table(info: &serde_json::Value) {
    let mounts = info
        .get("file_systems")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    if mounts.is_empty() {
        println!("No file systems mounted.");
        return;
    }

    let mut table = new_table(&["File System ID", "Mount Path"]);
    for mount in &mounts {
        let id = mount
            .get("file_system_id")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let path = mount
            .get("mount_path")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        table.add_row(vec![Cell::new(id), Cell::new(path)]);
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
    let info = parse_sandbox_response(resp, "attach file system").await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info)?);
        return Ok(());
    }

    println!(
        "Attached file system '{}' at '{}' on sandbox {}.",
        file_system_id, mount_path, sandbox_id
    );
    print_mounts_table(&info);
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
    let info = parse_sandbox_response(resp, "detach file system").await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info)?);
        return Ok(());
    }

    println!(
        "Detached file system at '{}' from sandbox {}.",
        mount_path, sandbox_id
    );
    print_mounts_table(&info);
    Ok(())
}

pub async fn list(ctx: &CliContext, sandbox_id: &str, output_json: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;
    let info = parse_sandbox_response(resp, "list sandbox file systems").await?;

    if output_json {
        let mounts = info
            .get("file_systems")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        println!("{}", serde_json::to_string_pretty(&mounts)?);
        return Ok(());
    }

    print_mounts_table(&info);
    Ok(())
}
