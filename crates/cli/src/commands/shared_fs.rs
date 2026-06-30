//! `tl shared-fs` — manage the project-scoped shared-file-system registry.
//!
//! These commands hit the platform API
//! (`/platform/v1/organizations/{org}/projects/{project}/file-systems`) through
//! the cloud SDK's [`SharedFileSystemsClient`], mirroring how `tl sbx image`
//! talks to the sandbox-templates registry. Attaching/detaching a shared file
//! system to a specific sandbox lives under `tl sbx shared-fs` instead, because
//! that is a sandbox-lifecycle operation rather than registry management.

use comfy_table::Cell;
use tensorlake::shared_file_systems::SharedFileSystemsClient;
use tensorlake::shared_file_systems::models::CreateSharedFileSystemRequest;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

fn org_and_project(ctx: &CliContext) -> Result<(String, String)> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required to manage shared file systems"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required to manage shared file systems"))?;
    Ok((org_id, proj_id))
}

fn shared_file_systems_client(ctx: &CliContext) -> Result<SharedFileSystemsClient> {
    let client = ctx.scoped_cloud_client()?;
    let (org_id, proj_id) = org_and_project(ctx)?;
    Ok(SharedFileSystemsClient::new(client, org_id, proj_id))
}

pub async fn create(
    ctx: &CliContext,
    name: &str,
    description: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let client = shared_file_systems_client(ctx)?;
    let request = CreateSharedFileSystemRequest {
        name: name.to_string(),
        description: description.map(str::to_string),
    };
    let shared_file_system = client.create(&request).await?.into_inner();

    if output_json {
        println!("{}", serde_json::to_string_pretty(&shared_file_system)?);
        return Ok(());
    }

    println!(
        "Created shared file system '{}' ({}).",
        shared_file_system.name.as_deref().unwrap_or(name),
        shared_file_system.id.as_deref().unwrap_or("-"),
    );
    Ok(())
}

pub async fn list(ctx: &CliContext, output_json: bool) -> Result<()> {
    let client = shared_file_systems_client(ctx)?;
    let items = client.list().await?.into_inner();

    if output_json {
        println!("{}", serde_json::to_string_pretty(&items)?);
        return Ok(());
    }

    if items.is_empty() {
        println!("No shared file systems found.");
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
        "{} shared file system{}",
        count,
        if count != 1 { "s" } else { "" }
    );
    Ok(())
}

pub async fn remove(ctx: &CliContext, file_system_id: &str) -> Result<()> {
    let client = shared_file_systems_client(ctx)?;
    client.delete(file_system_id).await?;
    println!("Deleted shared file system '{}'.", file_system_id);
    Ok(())
}
