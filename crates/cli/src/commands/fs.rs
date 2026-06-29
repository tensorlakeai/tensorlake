//! `tl fs` — manage the project-scoped ZeroFS file-system registry.
//!
//! These commands hit the platform API
//! (`/platform/v1/organizations/{org}/projects/{project}/file-systems`) through
//! the cloud SDK's [`FileSystemsClient`], mirroring how `tl sbx image` talks to
//! the sandbox-templates registry. Attaching/detaching a file system to a
//! specific sandbox lives under `tl sbx fs` instead, because that is a
//! sandbox-lifecycle operation rather than registry management.

use comfy_table::Cell;
use tensorlake::filesystems::FileSystemsClient;
use tensorlake::filesystems::models::CreateFileSystemRequest;
use tensorlake::{Client, ClientBuilder};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

fn org_and_project(ctx: &CliContext) -> Result<(String, String)> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required to manage file systems"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required to manage file systems"))?;
    Ok((org_id, proj_id))
}

/// Build a cloud SDK client scoped to the current org/project.
///
/// Mirrors `commands::sbx::image::scoped_cloud_client`: scope headers are only
/// attached for personal-access-token auth, since project API keys are already
/// implicitly scoped.
fn scoped_cloud_client(ctx: &CliContext) -> Result<Client> {
    let token = ctx.bearer_token()?;
    let mut builder = ClientBuilder::new(&ctx.api_url).bearer_token(&token);
    let use_scope_headers = ctx.personal_access_token.is_some() && ctx.api_key.is_none();

    if use_scope_headers
        && let (Some(organization_id), Some(project_id)) =
            (ctx.effective_organization_id(), ctx.effective_project_id())
    {
        builder = builder.scope(&organization_id, &project_id);
    }

    builder.build().map_err(Into::into)
}

fn file_systems_client(ctx: &CliContext) -> Result<FileSystemsClient> {
    let client = scoped_cloud_client(ctx)?;
    let (org_id, proj_id) = org_and_project(ctx)?;
    Ok(FileSystemsClient::new(client, org_id, proj_id))
}

pub async fn create(
    ctx: &CliContext,
    name: &str,
    description: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let client = file_systems_client(ctx)?;
    let request = CreateFileSystemRequest {
        name: name.to_string(),
        description: description.map(str::to_string),
    };
    let file_system = client.create(&request).await?.into_inner();

    if output_json {
        println!("{}", serde_json::to_string_pretty(&file_system)?);
        return Ok(());
    }

    println!(
        "Created file system '{}' ({}).",
        file_system.name.as_deref().unwrap_or(name),
        file_system.id.as_deref().unwrap_or("-"),
    );
    Ok(())
}

pub async fn list(ctx: &CliContext, output_json: bool) -> Result<()> {
    let client = file_systems_client(ctx)?;
    let items = client.list().await?.into_inner();

    if output_json {
        println!("{}", serde_json::to_string_pretty(&items)?);
        return Ok(());
    }

    if items.is_empty() {
        println!("No file systems found.");
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
    println!("{} file system{}", count, if count != 1 { "s" } else { "" });
    Ok(())
}

pub async fn remove(ctx: &CliContext, file_system_id: &str) -> Result<()> {
    let client = file_systems_client(ctx)?;
    client.delete(file_system_id).await?;
    println!("Deleted file system '{}'.", file_system_id);
    Ok(())
}
