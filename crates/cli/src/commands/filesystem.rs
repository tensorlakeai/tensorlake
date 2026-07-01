//! `tl filesystem` — manage the project-scoped filesystem registry.
//!
//! These commands hit the platform API
//! (`/platform/v1/organizations/{org}/projects/{project}/file-systems`) through
//! the cloud SDK's [`FilesystemsClient`], mirroring how `tl sbx image`
//! talks to the sandbox-templates registry. Attaching/detaching a filesystem
//! to a specific sandbox lives under `tl sbx filesystem` instead, because
//! that is a sandbox-lifecycle operation rather than registry management.

use comfy_table::Cell;
use tensorlake::filesystems::FilesystemsClient;
use tensorlake::filesystems::models::CreateFilesystemRequest;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

fn org_and_project(ctx: &CliContext) -> Result<(String, String)> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required to manage filesystems"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required to manage filesystems"))?;
    Ok((org_id, proj_id))
}

fn filesystems_client(ctx: &CliContext) -> Result<FilesystemsClient> {
    let client = ctx.scoped_cloud_client()?;
    let (org_id, proj_id) = org_and_project(ctx)?;
    Ok(FilesystemsClient::new(client, org_id, proj_id))
}

pub async fn create(
    ctx: &CliContext,
    name: &str,
    description: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let client = filesystems_client(ctx)?;
    let request = CreateFilesystemRequest {
        name: name.to_string(),
        description: description.map(str::to_string),
    };
    let filesystem = client.create(&request).await?.into_inner();

    if output_json {
        println!("{}", serde_json::to_string_pretty(&filesystem)?);
        return Ok(());
    }

    println!(
        "Created filesystem '{}' ({}).",
        filesystem.name.as_deref().unwrap_or(name),
        filesystem.id.as_deref().unwrap_or("-"),
    );
    Ok(())
}

pub async fn list(ctx: &CliContext, output_json: bool) -> Result<()> {
    let client = filesystems_client(ctx)?;
    let items = client.list().await?.into_inner();

    if output_json {
        println!("{}", serde_json::to_string_pretty(&items)?);
        return Ok(());
    }

    if items.is_empty() {
        println!("No filesystems found.");
        return Ok(());
    }

    let mut table = new_table(&["Name", "ID", "Region", "Status"]);
    for fs in &items {
        table.add_row(vec![
            Cell::new(fs.name.as_deref().unwrap_or("-")),
            Cell::new(fs.id.as_deref().unwrap_or("-")),
            Cell::new(fs.region.as_deref().unwrap_or("-")),
            Cell::new(fs.status.as_deref().unwrap_or("-")),
        ]);
    }
    println!("{table}");

    let count = items.len();
    println!(
        "{} filesystem{}",
        count,
        if count != 1 { "s" } else { "" }
    );
    Ok(())
}

pub async fn remove(ctx: &CliContext, file_system_id: &str) -> Result<()> {
    let client = filesystems_client(ctx)?;
    client.delete(file_system_id).await?;
    println!("Deleted filesystem '{}'.", file_system_id);
    Ok(())
}
