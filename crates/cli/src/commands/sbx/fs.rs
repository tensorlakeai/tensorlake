//! `tl sbx fs` — attach/detach/list file systems on a specific
//! sandbox.
//!
//! These are sandbox-lifecycle operations: they hit the sandbox lifecycle API
//! (`{sandbox_endpoint}/sandboxes/{id}/file_systems`), not the platform
//! file-system registry. Registering and deleting file systems
//! themselves lives under the top-level `tl fs` command.

use comfy_table::Cell;
use reqwest::Response;
use tensorlake::sandboxes::models::{FileSystemMount, SandboxInfo};

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

/// Render a sandbox's currently-mounted file systems as a table. A
/// snapshot-pinned mount shows its pin as `<name>@<snapshot_id>`, mirroring
/// the `--filesystem <name>[@<snapshot_id>]:<path>[:<opts>]` input syntax.
fn print_mounts_table(mounts: &[FileSystemMount]) {
    if mounts.is_empty() {
        println!("No file systems mounted.");
        return;
    }

    let mut table = new_table(&["File System ID", "Source Path", "Mount Path", "Options"]);
    for mount in mounts {
        let mut options = Vec::new();
        if mount.read_only {
            options.push("ro".to_string());
        }
        if mount.prefetch {
            options.push("prefetch".to_string());
        }
        if let Some(owner) = &mount.owner {
            options.push(format!("owner={owner}"));
        }
        let source = match &mount.snapshot_id {
            Some(snapshot_id) => format!("{}@{snapshot_id}", mount.file_system_id),
            None => mount.file_system_id.clone(),
        };
        table.add_row(vec![
            Cell::new(source),
            Cell::new(mount.source_path.as_deref().unwrap_or("/")),
            Cell::new(mount.mount_path.as_str()),
            Cell::new(options.join(",")),
        ]);
    }
    println!("{table}");
}

/// Reject noncanonical subtree selections before sending a request. Keep the
/// path intact: normalizing `..` could select a different directory than intended.
pub(super) fn validate_source_path(source_path: Option<&str>) -> Result<()> {
    let Some(path) = source_path else {
        return Ok(());
    };
    if path.len() > 4096 || path.chars().any(char::is_control) {
        return Err(CliError::usage(
            "source_path must be at most 4096 bytes with no control characters".to_string(),
        ));
    }
    if path != "/"
        && (!path.starts_with('/')
            || path.ends_with('/')
            || path.contains("//")
            || path.split('/').any(|part| matches!(part, "." | "..")))
    {
        return Err(CliError::usage(format!(
            "source_path must be an absolute canonical path: {path:?}"
        )));
    }
    Ok(())
}

/// Build the attach request body. The option keys are present only when set:
/// older servers deserialize attach bodies with `deny_unknown_fields` and
/// reject an explicit `false` (or an unknown pin field). A snapshot pin
/// without `--read-only` is rejected here, mirroring the server's 400 so
/// users fail fast offline.
fn build_attach_body(
    file_system_id: &str,
    mount_path: &str,
    read_only: bool,
    prefetch: bool,
    snapshot_id: Option<&str>,
    owner: Option<&str>,
    source_path: Option<&str>,
) -> Result<serde_json::Value> {
    if snapshot_id.is_some() && !read_only {
        return Err(CliError::usage(
            "--snapshot requires --read-only: snapshot-pinned mounts are read-only".to_string(),
        ));
    }
    validate_source_path(source_path)?;
    let mount = FileSystemMount {
        file_system_id: file_system_id.to_string(),
        mount_path: mount_path.to_string(),
        source_path: source_path.map(str::to_string),
        read_only,
        prefetch,
        snapshot_id: snapshot_id.map(str::to_string),
        owner: owner.map(str::to_string),
    };
    Ok(serde_json::to_value(mount)?)
}

#[allow(clippy::too_many_arguments)]
pub async fn attach(
    ctx: &CliContext,
    sandbox_id: &str,
    file_system_id: &str,
    mount_path: &str,
    read_only: bool,
    prefetch: bool,
    snapshot_id: Option<&str>,
    owner: Option<&str>,
    source_path: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/file_systems"));
    let body = build_attach_body(
        file_system_id,
        mount_path,
        read_only,
        prefetch,
        snapshot_id,
        owner,
        source_path,
    )?;

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;
    let info = parse_sandbox_response(resp, "attach file system").await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info.file_systems)?);
        return Ok(());
    }

    println!(
        "Attached file system '{}' at '{}' on sandbox {}.",
        file_system_id, mount_path, sandbox_id
    );
    print_mounts_table(&info.file_systems);
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
        println!("{}", serde_json::to_string_pretty(&info.file_systems)?);
        return Ok(());
    }

    println!(
        "Detached file system at '{}' from sandbox {}.",
        mount_path, sandbox_id
    );
    print_mounts_table(&info.file_systems);
    Ok(())
}

pub async fn list(ctx: &CliContext, sandbox_id: &str, output_json: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;
    let info = parse_sandbox_response(resp, "list sandbox file systems").await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info.file_systems)?);
        return Ok(());
    }

    print_mounts_table(&info.file_systems);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attach_body_omits_unset_mount_modes() {
        let body =
            build_attach_body("skills", "/mnt/skills", false, false, None, None, None).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
            })
        );
    }

    #[test]
    fn attach_body_includes_snapshot_pin_with_read_only() {
        let body = build_attach_body(
            "skills",
            "/mnt/skills",
            true,
            false,
            Some("0abc123def"),
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
                "snapshot_id": "0abc123def",
            })
        );
    }

    #[test]
    fn attach_body_rejects_snapshot_pin_without_read_only() {
        let error = build_attach_body(
            "skills",
            "/mnt/skills",
            false,
            true,
            Some("0abc123def"),
            None,
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("snapshot-pinned mounts are read-only"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn attach_body_includes_owner_only_when_set() {
        let body = build_attach_body(
            "skills",
            "/mnt/skills",
            false,
            false,
            None,
            Some("agent"),
            None,
        )
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "owner": "agent",
            })
        );
    }

    #[test]
    fn attach_body_preserves_subtree_with_pin_modes_and_owner() {
        let body = build_attach_body(
            "data",
            "/mnt/data",
            true,
            true,
            Some("snap"),
            Some("agent"),
            Some("/a,b with spaces"),
        )
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "file_system_id": "data", "mount_path": "/mnt/data",
                "source_path": "/a,b with spaces", "snapshot_id": "snap",
                "read_only": true, "prefetch": true, "owner": "agent"
            })
        );
    }

    #[test]
    fn attach_body_rejects_noncanonical_source_paths() {
        for path in [
            "", "relative", "/a/", "//a", "/a//b", "/a/./b", "/a/../b", "/a\n", "/a\0",
        ] {
            assert!(
                build_attach_body("data", "/mnt/data", false, false, None, None, Some(path))
                    .is_err(),
                "{path:?}"
            );
        }
        assert!(validate_source_path(Some(&format!("/{}", "x".repeat(4096)))).is_err());
        assert!(validate_source_path(Some("/")).is_ok());
    }
}
