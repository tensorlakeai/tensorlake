use crate::auth::context::CliContext;
use crate::auth::login::run_login_flow;
use crate::commands::context::forget_token;
use crate::config::contexts::{DEFAULT_CONTEXT_NAME, load_contexts, validate_context_name};
use crate::config::files::{load_context_token, remove_all_url_credentials};
use crate::error::{CliError, Result};

/// `tl login [--context <name>]`: browser login, saved as a context.
///
/// `name` is the subcommand's own `--context`. `named_context` is the context named by the
/// global `--context` flag or `TENSORLAKE_CONTEXT`, saved or not.
pub async fn run(ctx: &CliContext, name: Option<&str>, named_context: Option<&str>) -> Result<()> {
    let name = login_context_name(name, named_context);
    validate_context_name(name)?;
    run_login_flow(ctx, true, name).await?;
    Ok(())
}

/// The context a login is saved as: the subcommand's `--context`, else the context named for
/// this run, else `default`.
///
/// A missing context blocks every command that runs in it. Saving the login under the named
/// context repairs that in one step, so `TENSORLAKE_CONTEXT=staging tl login` saves `staging`.
fn login_context_name<'a>(name: Option<&'a str>, named_context: Option<&'a str>) -> &'a str {
    name.or(named_context).unwrap_or(DEFAULT_CONTEXT_NAME)
}

/// `tl logout [--all]`: forget the token of the active context. With `--all`, do this for
/// every saved context. The contexts stay in `contexts.toml` without a token;
/// `tl login --context <name>` fills them again.
pub fn logout(ctx: &CliContext, all: bool) -> Result<()> {
    let contexts = load_contexts();
    let names: Vec<String> = if all {
        contexts.contexts.keys().cloned().collect()
    } else {
        let Some(name) = ctx.context_name.as_deref() else {
            return Err(CliError::auth(
                "not logged in: no context is selected. run: tl logout --all to remove every saved token",
            ));
        };
        vec![name.to_string()]
    };
    if names.is_empty() {
        eprintln!("no contexts are saved.");
    }

    let mut forgotten = Vec::new();
    for name in &names {
        let entry = contexts
            .get(name)
            .ok_or_else(|| CliError::config(format!("unknown context '{name}'")))?;
        let Some(token) = load_context_token(name) else {
            if !all {
                eprintln!("no saved token for context '{name}'.");
            }
            continue;
        };
        forget_token(name, entry, &token)?;
        forgotten.push(name.clone());
    }

    // A per-URL login can outlive its context: a login to another API URL replaces the
    // context but keeps the old URL's table. `--all` means every saved token, so sweep
    // the per-URL tables that the contexts did not account for.
    let mut orphan_urls = Vec::new();
    if all {
        orphan_urls = remove_all_url_credentials()?;
    }
    if names.is_empty() && orphan_urls.is_empty() {
        return Ok(());
    }
    if !orphan_urls.is_empty() {
        eprintln!(
            "also removed the saved login with no context for: {}",
            orphan_urls.join(", ")
        );
    }

    println!(
        "logged out. removed the tokens of: {}",
        if forgotten.is_empty() {
            "(none)".to_string()
        } else {
            forgotten.join(", ")
        }
    );
    eprintln!("run: tl login --context <name> to log in again.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_name_prefers_the_subcommand_flag_then_the_named_context() {
        assert_eq!(login_context_name(Some("new"), Some("staging")), "new");
        assert_eq!(login_context_name(None, Some("staging")), "staging");
        assert_eq!(login_context_name(None, None), "default");
    }
}
