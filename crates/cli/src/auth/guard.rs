use crate::auth::context::CliContext;
use crate::auth::login::run_login_flow;
use crate::commands::init::run_init_flow;
use crate::config::resolver;
use crate::error::{CliError, Result};
use crate::project::detection::find_project_root;

/// Ensure authentication and org/project are available.
/// Triggers login and/or init flows as needed.
pub async fn ensure_auth_and_project(ctx: &mut CliContext) -> Result<()> {
    if !ctx.has_authentication() {
        eprintln!("It seems like you're not logged in. Let's log you in...\n");
        let login_result = run_login_flow(ctx, true).await?;

        // Reload context with new credentials and org/project from login flow
        let resolved = resolver::resolve(
            Some(&ctx.api_url),
            Some(&ctx.cloud_url),
            None,
            Some(&login_result.token),
            Some(&ctx.namespace),
            login_result.organization_id.as_deref(),
            login_result.project_id.as_deref(),
            ctx.debug,
        );
        *ctx = CliContext::from_resolved(resolved);

        if !ctx.has_authentication() {
            return Err(CliError::auth(
                "Authentication failed. Please try running 'tl login' manually.",
            ));
        }
        if !ctx.has_org_and_project() {
            return Err(CliError::auth(
                "Organization and project configuration missing. Please run 'tl init'.",
            ));
        }
        return Ok(());
    }

    // If using API key, introspect to get org/project
    if ctx.api_key.is_some() {
        ctx.introspect().await?;
    }

    if ctx.has_org_and_project() {
        return Ok(());
    }

    // Have PAT but no org/project
    if ctx.api_key.is_some() {
        return Err(CliError::auth(
            "API key is set but could not determine organization and project. \
             Please check your API key or provide --organization and --project flags.",
        ));
    }

    eprintln!("Organization and project IDs are required for this command.");
    eprintln!("Running initialization flow to set up your project...\n");

    let project_root = find_project_root(None);
    let (org_id, proj_id) = run_init_flow(ctx, true, true, false, &project_root).await?;

    // Update context with new org/project
    let resolved = resolver::resolve(
        Some(&ctx.api_url),
        Some(&ctx.cloud_url),
        ctx.api_key.as_deref(),
        ctx.personal_access_token.as_deref(),
        Some(&ctx.namespace),
        Some(&org_id),
        Some(&proj_id),
        ctx.debug,
    );
    *ctx = CliContext::from_resolved(resolved);

    Ok(())
}
