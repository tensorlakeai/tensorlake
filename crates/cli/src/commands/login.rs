use crate::auth::context::CliContext;
use crate::auth::login::{forget_organization_tokens, run_login_flow};
use crate::auth::mint::{RevokeOutcome, revoke_token};
use crate::config::contexts::{DEFAULT_CONTEXT_NAME, load_contexts, validate_context_name};
use crate::config::files::{load_context_token, remove_credentials};
use crate::error::{CliError, Result};

/// `tl login [--context <name>]`: browser login, saved as context `<name>` (default: `default`).
pub async fn run(ctx: &CliContext, context: Option<&str>) -> Result<()> {
    let name = context.unwrap_or(DEFAULT_CONTEXT_NAME);
    validate_context_name(name)?;
    run_login_flow(ctx, true, name).await?;
    Ok(())
}

/// `tl logout`: revoke the login token of the current organization and forget the tokens of
/// its contexts. The contexts stay in `contexts.toml`; `tl login` fills them again.
pub async fn logout(ctx: &CliContext) -> Result<()> {
    let contexts = load_contexts();
    let Some(name) = ctx.context_name.as_deref() else {
        return Err(CliError::auth("not logged in: no context is selected"));
    };
    let entry = contexts
        .get(name)
        .ok_or_else(|| CliError::config(format!("unknown context '{name}'")))?;
    let organization = entry.organization.as_deref();

    // Each login token (a parent) of this organization is revoked. The server then revokes
    // the tokens minted from it. One organization can hold more than one parent: for
    // example, when `tl context create` fell back to the browser login because the server
    // could not mint tokens. Revoke them all, since all of them are forgotten below.
    let mut to_revoke: Vec<(String, String)> = contexts
        .for_api_url(&entry.api_url)
        .filter(|(_, e)| e.organization.as_deref() == organization)
        .filter_map(|(n, _)| {
            load_context_token(n)
                .filter(|t| t.parent)
                .map(|t| (n.to_string(), t.token))
        })
        .collect();
    if to_revoke.is_empty()
        && let Some(t) = load_context_token(name)
    {
        to_revoke.push((name.to_string(), t.token));
    }

    if to_revoke.is_empty() {
        eprintln!("no saved token for context '{name}'.");
    }
    for (revoked_name, token) in to_revoke {
        match revoke_token(&entry.api_url, &token).await {
            Ok(RevokeOutcome::Revoked) => {
                eprintln!("revoked the login token of context '{revoked_name}' on the server.")
            }
            Ok(RevokeOutcome::Unsupported) => eprintln!(
                "the server could not revoke the token of context '{revoked_name}' (not supported yet). tokens are removed from this machine only."
            ),
            Err(e) => {
                eprintln!("warning: could not revoke the token of context '{revoked_name}': {e}")
            }
        }
    }

    let forgotten = forget_organization_tokens(&entry.api_url, organization)?;
    remove_credentials(&entry.api_url)?;
    println!(
        "logged out of {} at {}. removed the tokens of: {}",
        organization.unwrap_or("the organization"),
        entry.api_url,
        if forgotten.is_empty() {
            "(none)".to_string()
        } else {
            forgotten.join(", ")
        }
    );
    eprintln!("run: tl login --context <name> to log in again.");
    Ok(())
}
