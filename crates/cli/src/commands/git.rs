use comfy_table::Cell;
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::models::{
    ListBranchesResponse, ListOperationsResponse, ListRefsResponse, ListReposResponse,
};
use tensorlake::{ClientBuilder, Sdk};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub fn repo_url(ctx: &CliContext, repo: &str) -> Result<String> {
    let client = artifact_storage_client(ctx)?;
    let project_id = project_id(ctx)?;
    Ok(client.git_repo_url(&project_id, repo))
}

pub async fn mint_token(
    ctx: &CliContext,
    repo: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let project_id = project_id(ctx)?;
    let credential = artifact_storage_client(ctx)?
        .mint_token_for_repo(&project_id, repo)
        .await?
        .into_inner();
    if output_json {
        println!("{}", serde_json::to_string_pretty(&credential)?);
        return Ok(());
    }

    println!("project: {project_id}");
    println!("repo: {}", credential.repo_pattern);
    println!("username: {}", credential.git_username);
    println!("password: {}", credential.token);
    println!("expires: {}", credential.expires_at);
    println!("scopes: {}", credential.scopes.join(", "));

    if credential.repo_pattern != "*" {
        let remote_url =
            artifact_storage_client(ctx)?.git_repo_url(&project_id, &credential.repo_pattern);
        println!();
        println!("Use this as a Git credential for:");
        println!("  {remote_url}");
        println!();
        println!("When Git asks for credentials:");
        println!("  username: {}", credential.git_username);
        println!("  password: the token above");
    }
    Ok(())
}

pub async fn create_repo(
    ctx: &CliContext,
    repo: &str,
    default_branch: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    client
        .create_repo(&project_id, repo, default_branch)
        .await
        .map_err(map_sdk_error)?;
    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "repo": repo,
                "url": client.git_repo_url(&project_id, repo),
            }))?
        );
    } else {
        println!("created {}", client.git_repo_url(&project_id, repo));
    }
    Ok(())
}

pub async fn list_repos(ctx: &CliContext, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_repos(&project_id)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_repos_table(&response);
    Ok(())
}

pub async fn delete_repo(ctx: &CliContext, repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    artifact_storage_client(ctx)?
        .delete_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    println!("deleted {repo}");
    Ok(())
}

pub async fn fork_repo(ctx: &CliContext, repo: &str, base_repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    client
        .fork_repo(&project_id, repo, base_repo)
        .await
        .map_err(map_sdk_error)?;
    println!(
        "forked {} from {}",
        client.git_repo_url(&project_id, repo),
        base_repo
    );
    Ok(())
}

pub async fn archive_repo(ctx: &CliContext, repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    artifact_storage_client(ctx)?
        .archive_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    println!("archived {repo}");
    Ok(())
}

pub async fn restore_repo(ctx: &CliContext, repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    artifact_storage_client(ctx)?
        .restore_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    println!("restored {repo}");
    Ok(())
}

pub async fn list_branches(ctx: &CliContext, repo: &str, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_branches(&project_id, repo)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_branches_table(&response);
    Ok(())
}

pub async fn list_refs(ctx: &CliContext, repo: &str, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_refs(&project_id, repo)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_refs_table(&response);
    Ok(())
}

pub async fn list_operations(
    ctx: &CliContext,
    repo: &str,
    git_username: &str,
    git_token: &str,
    output_json: bool,
) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_operations_with_credential(&project_id, repo, git_username, git_token)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_operations_table(&response);
    Ok(())
}

fn artifact_storage_client(ctx: &CliContext) -> Result<ArtifactStorageClient> {
    let token = ctx.bearer_token()?;
    let mut builder = ClientBuilder::new(&ctx.api_url).bearer_token(&token);
    let use_scope_headers = ctx.personal_access_token.is_some() && ctx.api_key.is_none();
    if use_scope_headers
        && let (Some(organization_id), Some(project_id)) =
            (ctx.effective_organization_id(), ctx.effective_project_id())
    {
        builder = builder.scope(&organization_id, &project_id);
    }
    let sdk = Sdk::with_client_builder(builder)?;
    sdk.artifact_storage().map_err(Into::into)
}

fn project_id(ctx: &CliContext) -> Result<String> {
    ctx.effective_project_id()
        .ok_or_else(|| CliError::auth("missing project ID; run `tl init`"))
}

fn print_repos_table(response: &ListReposResponse) {
    if response.repos.is_empty() {
        println!("no repos found");
        return;
    }
    let mut table = new_table(&["Name", "Default Branch", "Status"]);
    for repo in &response.repos {
        table.add_row(vec![
            Cell::new(&repo.name),
            Cell::new(&repo.default_branch),
            Cell::new(&repo.status),
        ]);
    }
    println!("{table}");
    println!("{} repos", response.repos.len());
}

fn print_branches_table(response: &ListBranchesResponse) {
    if response.branches.is_empty() {
        println!("no branches found");
        return;
    }
    let mut table = new_table(&["Branch", "OID"]);
    for branch in &response.branches {
        table.add_row(vec![Cell::new(&branch.name), Cell::new(&branch.oid)]);
    }
    println!("{table}");
}

fn print_refs_table(response: &ListRefsResponse) {
    if response.refs.is_empty() {
        println!("no refs found");
        return;
    }
    let mut table = new_table(&["Ref", "OID"]);
    for git_ref in &response.refs {
        table.add_row(vec![Cell::new(&git_ref.name), Cell::new(&git_ref.oid)]);
    }
    println!("{table}");
}

fn print_operations_table(response: &ListOperationsResponse) {
    if response.operations.is_empty() {
        println!("no operations found");
        return;
    }
    let mut table = new_table(&["Op ID", "Kind", "Actor", "Refs", "Time"]);
    for op in &response.operations {
        table.add_row(vec![
            Cell::new(&op.op_id),
            Cell::new(&op.kind),
            Cell::new(&op.actor),
            Cell::new(op.refs.len()),
            Cell::new(op.at_secs),
        ]);
    }
    println!("{table}");
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn map_sdk_error(error: tensorlake::error::SdkError) -> CliError {
    match error {
        tensorlake::error::SdkError::Authentication(_) => {
            CliError::auth("authentication failed. set TENSORLAKE_API_KEY or run 'tl login'.")
        }
        tensorlake::error::SdkError::Authorization(_) => CliError::auth(
            "permission denied. set TENSORLAKE_API_KEY with required permissions, or run 'tl init'.",
        ),
        tensorlake::error::SdkError::ServerError { status, message } => {
            if status == reqwest::StatusCode::CONFLICT {
                CliError::usage(parse_remote_message(&message))
            } else if status == reqwest::StatusCode::NOT_FOUND {
                CliError::usage(parse_remote_message(&message))
            } else {
                CliError::from(tensorlake::error::SdkError::ServerError { status, message })
            }
        }
        other => CliError::from(other),
    }
}

fn parse_remote_message(raw: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|body| {
            body.get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| raw.to_string())
}
