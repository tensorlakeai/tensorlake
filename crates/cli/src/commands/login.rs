use crate::auth::context::CliContext;
use crate::auth::login::run_login_flow;
use crate::commands::context::forget_token;
use crate::config::contexts::{
    ContextsFile, check_no_case_clash, load_contexts, login_name_for_url, validate_context_name,
};
use crate::config::files::{remove_all_context_tokens, remove_all_url_credentials};
use crate::config::token_store::load_context_token;
use crate::error::{CliError, Result};

/// `tl login [--context <name>]`: browser login, saved as a context.
///
/// `name` is the subcommand's own `--context`. `named_context` is the context named by the
/// global `--context` flag or `TENSORLAKE_CONTEXT`, saved or not.
pub async fn run(ctx: &CliContext, name: Option<&str>, named_context: Option<&str>) -> Result<()> {
    let contexts = load_contexts()?;
    let name = login_context_name(name, named_context, &contexts, &ctx.api_url);
    validate_context_name(&name)?;
    check_no_case_clash(&contexts, &name)?;
    run_login_flow(ctx, true, &name).await?;
    Ok(())
}

/// The context a login is saved as: the subcommand's `--context`, else the context named for
/// this run, else a name that fits the API URL (`default` unless that is for another URL).
///
/// A missing context blocks every command that runs in it. Saving the login under the named
/// context repairs that in one step, so `TENSORLAKE_CONTEXT=staging tl login` saves `staging`.
fn login_context_name(
    name: Option<&str>,
    named_context: Option<&str>,
    contexts: &ContextsFile,
    api_url: &str,
) -> String {
    match name.or(named_context) {
        Some(name) => name.to_string(),
        None => login_name_for_url(contexts, api_url),
    }
}

/// `tl logout [--all]`: forget the token of the active context. With `--all`, do this for
/// every saved context. The contexts stay in `contexts.toml` without a token;
/// `tl login --context <name>` fills them again.
pub fn logout(ctx: &CliContext, all: bool) -> Result<()> {
    let contexts = load_contexts()?;
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
        let Some(token) = load_context_token(name, entry.storage)? else {
            if !all {
                eprintln!("no saved token for context '{name}'.");
            }
            continue;
        };
        forget_token(name, entry, &token)?;
        forgotten.push(name.clone());
    }

    // `--all` means every saved token, so sweep what the contexts did not account for. A
    // per-URL login outlives its context when a login to another API URL replaces the
    // context but keeps the old URL's table. A context token outlives its context when the
    // context is removed from `contexts.toml` by hand or by another CLI version.
    let mut orphan_urls = Vec::new();
    let mut orphan_tokens = Vec::new();
    if all {
        orphan_urls = remove_all_url_credentials()?;
        orphan_tokens = remove_all_context_tokens()?;
        orphan_tokens.retain(|name| contexts.get(name).is_none());
    }
    if names.is_empty() && orphan_urls.is_empty() && orphan_tokens.is_empty() {
        return Ok(());
    }
    if !orphan_urls.is_empty() {
        eprintln!(
            "also removed the saved login with no context for: {}",
            orphan_urls.join(", ")
        );
    }
    if !orphan_tokens.is_empty() {
        eprintln!(
            "also removed the token of a context that contexts.toml does not list: {}",
            orphan_tokens.join(", ")
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

    use crate::config::contexts::ContextEntry;

    const PROD: &str = "https://api.tensorlake.ai";
    const DEV: &str = "https://api.tensorlake.dev";

    fn saved(name: &str, api_url: &str) -> ContextsFile {
        let mut file = ContextsFile::default();
        file.contexts.insert(
            name.into(),
            ContextEntry {
                api_url: api_url.into(),
                organization: None,
                project: None,
                storage: None,
            },
        );
        file
    }

    #[test]
    fn login_name_prefers_the_subcommand_flag_then_the_named_context() {
        let none = ContextsFile::default();
        assert_eq!(
            login_context_name(Some("new"), Some("staging"), &none, PROD),
            "new"
        );
        assert_eq!(
            login_context_name(None, Some("staging"), &none, PROD),
            "staging"
        );
        assert_eq!(login_context_name(None, None, &none, PROD), "default");
    }

    #[test]
    fn an_unnamed_login_does_not_replace_the_default_of_another_url() {
        let prod_default = saved("default", PROD);
        assert_eq!(
            login_context_name(None, None, &prod_default, PROD),
            "default"
        );
        assert_eq!(
            login_context_name(None, None, &prod_default, DEV),
            "api-tensorlake-dev"
        );
    }
}
