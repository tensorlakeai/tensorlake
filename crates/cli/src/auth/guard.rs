use crate::auth::context::CliContext;
use crate::auth::login::run_login_flow;
use crate::commands::init::run_init_flow;
use crate::config::contexts::{load_contexts, login_name_for_url};
use crate::config::resolver::{self, UnknownContext};
use crate::error::{CliError, Result};
use crate::project::detection::find_project_root;

/// Ensure the caller is authenticated, but do not require an
/// organization/project context. Used by user-scoped commands like
/// `ssh-keys` that act on the user's profile rather than a project.
pub async fn ensure_auth(ctx: &mut CliContext) -> Result<()> {
    if ctx.has_authentication() {
        return Ok(());
    }
    eprintln!("It seems like you're not logged in. Let's log you in...\n");
    let context_name = login_context_name(ctx)?;
    match run_login_flow(ctx, true, &context_name).await {
        Ok(_) => {}
        Err(CliError::Cancelled) => {
            return Err(CliError::auth(
                "Login cancelled. Set TENSORLAKE_API_KEY or run 'tl login' to authenticate.",
            ));
        }
        Err(e) => return Err(e),
    }
    reload_from_saved_context(ctx, &context_name)?;
    if !ctx.has_authentication() {
        return Err(CliError::auth(
            "Authentication failed. Please try running 'tl login' manually.",
        ));
    }
    Ok(())
}

/// Ensure credentials are available for a Platform API route that derives
/// project scope directly from an API key. PAT callers still need explicit
/// organization/project context.
pub async fn ensure_auth_for_api_key_scoped_project(ctx: &mut CliContext) -> Result<()> {
    ensure_auth(ctx).await?;
    if ctx.api_key.is_some() {
        return Ok(());
    }
    ensure_auth_and_project(ctx).await
}

/// Ensure authentication and org/project are available.
/// Triggers login and/or init flows as needed.
pub async fn ensure_auth_and_project(ctx: &mut CliContext) -> Result<()> {
    // Sandbox attach path (issue #103): a pre-provisioned repo-scoped git credential IS the
    // authentication for the fs surface, and its JWT carries the project claim. Never start
    // an interactive login/init flow here — the guest is headless, and the browser-approval
    // poll would hang forever.
    if std::env::var("TENSORLAKE_GIT_TOKEN").is_ok() {
        if ctx.effective_project_id().is_none()
            && let Some(project) = crate::auth::context::project_from_git_token()
        {
            ctx.project_id = Some(project);
        }
        if ctx.effective_project_id().is_some() {
            return Ok(());
        }
        return Err(CliError::auth(
            "TENSORLAKE_GIT_TOKEN is set but carries no project claim; \
             re-mint it with `tl fs token <filesystem>` or configure a project",
        ));
    }
    if !ctx.has_authentication() {
        eprintln!("It seems like you're not logged in. Let's log you in...\n");
        let context_name = login_context_name(ctx)?;
        match run_login_flow(ctx, true, &context_name).await {
            Ok(_) => {}
            Err(CliError::Cancelled) => {
                return Err(CliError::auth(
                    "Login cancelled. Set TENSORLAKE_API_KEY or run 'tl login' to authenticate.",
                ));
            }
            Err(e) => return Err(e),
        }
        reload_from_saved_context(ctx, &context_name)?;

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

    // Rebuild `ctx` with the chosen organization and project. The run stays in its context:
    // the context supplies the token by name, so `tl --context staging git setup` bakes
    // `--context staging` into the credential helper even when setup had to init first.
    // A PAT beside a named context is an error in the resolver, so the PAT is passed only
    // when no context is in use.
    let context_name = ctx.context_name.clone();
    let pat = match context_name {
        Some(_) => None,
        None => ctx.personal_access_token.as_deref(),
    };
    let resolved = resolver::resolve(
        Some(&ctx.api_url),
        Some(&ctx.cloud_url),
        ctx.api_key.as_deref(),
        pat,
        Some(&ctx.namespace),
        Some(&org_id),
        Some(&proj_id),
        context_name.as_deref(),
        UnknownContext::Error,
        ctx.debug,
    )?;
    // The name was passed as if by `--context`; keep where it really came from.
    let context_source = ctx.context_source;
    *ctx = CliContext::from_resolved(resolved);
    if ctx.context_name == context_name {
        ctx.context_source = context_source;
    }

    Ok(())
}

/// The context an automatic login saves into: the selected one, else a name that fits the
/// API URL of this run (see [`login_name_for_url`]).
///
/// With no selected context, `default` is not a safe guess: the run may be for another API
/// URL than the saved `default`, and saving over it would log the user out of production.
fn login_context_name(ctx: &CliContext) -> Result<String> {
    Ok(match ctx.context_name.as_deref() {
        Some(name) => name.to_string(),
        None => login_name_for_url(&load_contexts()?, &ctx.api_url),
    })
}

/// Rebuild `ctx` from the context that the login flow saved as `name`.
///
/// The login saved its token under `name`, so the resolver finds the token, organization,
/// and project by that name. Resolving by name instead of by the new token keeps the context
/// identity: `tl --context staging git setup` then bakes `--context staging` into the
/// credential helper even when setup had to log in first. A context token works for one
/// project only, so a helper without the name would use whichever context is current later.
fn reload_from_saved_context(ctx: &mut CliContext, name: &str) -> Result<()> {
    let resolved = resolver::resolve(
        Some(&ctx.api_url),
        Some(&ctx.cloud_url),
        None,
        None,
        Some(&ctx.namespace),
        None,
        None,
        Some(name),
        UnknownContext::Error,
        ctx.debug,
    )?;
    *ctx = CliContext::from_resolved(resolved);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ensure_auth_for_api_key_scoped_project;
    use crate::auth::context::CliContext;
    use crate::config::resolver::ResolvedConfig;

    #[tokio::test]
    async fn api_key_scoped_guard_does_not_require_introspection_or_local_scope() {
        let mut ctx = CliContext::from_resolved(ResolvedConfig {
            api_url: "http://127.0.0.1:1".to_string(),
            cloud_url: "https://cloud.tensorlake.ai".to_string(),
            namespace: "default".to_string(),
            api_key: Some("tl_apiKey_test".to_string()),
            personal_access_token: None,
            organization_id: None,
            project_id: None,
            debug: false,
            context_name: None,
            context_source: None,
        });

        ensure_auth_for_api_key_scoped_project(&mut ctx)
            .await
            .expect("API-key-scoped routes do not need introspection");
    }
}
