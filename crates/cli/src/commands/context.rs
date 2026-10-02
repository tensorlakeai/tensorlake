//! `tl context`: switch between saved projects without a new browser login.
//!
//! A context is one API URL, one organization, one project, and one token. Each token comes
//! from its own browser login and works for that one project only. Contexts live in
//! `~/.config/tensorlake/contexts.toml` (no secrets). The token for each context lives in
//! `credentials.toml`. The per-URL table in `credentials.toml` always holds a copy of the
//! current context's token, so older CLI versions keep working.

use comfy_table::Cell;
use serde::Serialize;

use crate::config::contexts::{
    ContextEntry, ContextsFile, load_contexts, load_contexts_for_update, save_contexts,
    validate_context_name,
};
use crate::config::files::{
    ContextToken, load_context_token, load_stored_credentials, purge_git_credentials,
    remove_context_token, remove_credentials, rename_context_token, save_credentials,
};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

/// One context as shown by `list` and `show`.
#[derive(Debug, Serialize)]
struct ContextView<'a> {
    name: &'a str,
    current: bool,
    api_url: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project: Option<&'a str>,
    /// `saved` when the context has a token, `none` when it has none (for example after
    /// `tl logout`).
    token: &'static str,
}

fn token_kind(token: Option<&ContextToken>) -> &'static str {
    match token {
        Some(_) => "saved",
        None => "none",
    }
}

fn view<'a>(contexts: &'a ContextsFile, name: &'a str, entry: &'a ContextEntry) -> ContextView<'a> {
    let token = load_context_token(name);
    ContextView {
        name,
        current: contexts.current.as_deref() == Some(name),
        api_url: &entry.api_url,
        organization: entry.organization.as_deref(),
        project: entry.project.as_deref(),
        token: token_kind(token.as_ref()),
    }
}

fn get_entry<'a>(contexts: &'a ContextsFile, name: &str) -> Result<&'a ContextEntry> {
    contexts.get(name).ok_or_else(|| {
        let known: Vec<&str> = contexts.contexts.keys().map(String::as_str).collect();
        if known.is_empty() {
            CliError::config(format!(
                "unknown context '{name}'. no contexts are saved. run: tl login"
            ))
        } else {
            CliError::config(format!(
                "unknown context '{name}'. saved contexts: {}",
                known.join(", ")
            ))
        }
    })
}

pub fn list(output_json: bool) -> Result<()> {
    let contexts = load_contexts();
    if output_json {
        let views: Vec<ContextView> = contexts
            .contexts
            .iter()
            .map(|(name, entry)| view(&contexts, name, entry))
            .collect();
        println!("{}", serde_json::to_string_pretty(&views)?);
        return Ok(());
    }
    if contexts.contexts.is_empty() {
        eprintln!("no contexts. run: tl login");
        return Ok(());
    }
    let mut table = new_table(&["", "Name", "Organization", "Project", "API URL", "Token"]);
    for (name, entry) in &contexts.contexts {
        let v = view(&contexts, name, entry);
        table.add_row(vec![
            Cell::new(if v.current { "*" } else { "" }),
            Cell::new(v.name),
            Cell::new(v.organization.unwrap_or("-")),
            Cell::new(v.project.unwrap_or("-")),
            Cell::new(v.api_url),
            Cell::new(v.token),
        ]);
    }
    println!("{table}");
    Ok(())
}

pub fn current() -> Result<()> {
    let contexts = load_contexts();
    match contexts.current_entry() {
        Some((name, _)) => {
            println!("{name}");
            Ok(())
        }
        None => Err(CliError::config(
            "no current context. run: tl context use <name>, or tl login",
        )),
    }
}

pub fn show(name: &str, output_json: bool) -> Result<()> {
    let contexts = load_contexts();
    let entry = get_entry(&contexts, name)?;
    let v = view(&contexts, name, entry);
    if output_json {
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }
    println!(
        "Name         : {}{}",
        v.name,
        if v.current { " (current)" } else { "" }
    );
    println!("Organization : {}", v.organization.unwrap_or("-"));
    println!("Project      : {}", v.project.unwrap_or("-"));
    println!("API URL      : {}", v.api_url);
    println!("Token        : {}", v.token);
    Ok(())
}

/// Make `name` current. Local only: no network.
pub fn use_context(name: &str) -> Result<()> {
    let mut contexts = load_contexts_for_update()?;
    let entry = get_entry(&contexts, name)?.clone();
    contexts.current = Some(name.to_string());
    save_contexts(&contexts)?;
    copy_token_to_url_table(name, &entry)?;
    println!(
        "switched to context '{name}' ({} / {})",
        entry.organization.as_deref().unwrap_or("-"),
        entry.project.as_deref().unwrap_or("-")
    );
    Ok(())
}

/// Copy the token of `name` into the per-URL table that older CLI versions read.
fn copy_token_to_url_table(name: &str, entry: &ContextEntry) -> Result<()> {
    match load_context_token(name) {
        Some(token) => save_credentials(
            &entry.api_url,
            &token.token,
            entry.organization.as_deref(),
            entry.project.as_deref(),
        ),
        None => {
            eprintln!("warning: context '{name}' has no token. run: tl login --context {name}");
            Ok(())
        }
    }
}

pub fn rename(old: &str, new: &str) -> Result<()> {
    validate_context_name(new)?;
    let mut contexts = load_contexts_for_update()?;
    let entry = get_entry(&contexts, old)?.clone();
    if contexts.get(new).is_some() {
        return Err(CliError::usage(format!("context '{new}' exists")));
    }
    contexts.contexts.remove(old);
    contexts.contexts.insert(new.to_string(), entry);
    if contexts.current.as_deref() == Some(old) {
        contexts.current = Some(new.to_string());
    }
    rename_context_token(old, new)?;
    save_contexts(&contexts)?;
    println!("renamed context '{old}' to '{new}'");
    Ok(())
}

/// Remove the token of context `name` from `credentials.toml`. The per-URL table loses the
/// token too when it holds the same one. The cache of minted git credentials goes as well:
/// git reads it before the login token, so a cached entry would keep git authenticated
/// after the logout until it expires. The next `git fetch` mints a new one.
///
/// Used by `tl context delete` and `tl logout`.
pub(crate) fn forget_token(name: &str, entry: &ContextEntry, token: &ContextToken) -> Result<()> {
    remove_context_token(name)?;
    purge_git_credentials();
    let url_table_has_it =
        load_stored_credentials(&entry.api_url).is_some_and(|stored| stored.token == token.token);
    if url_table_has_it {
        remove_credentials(&entry.api_url)?;
    }
    Ok(())
}

/// `tl context delete <name>`: forget the token, then remove the context.
pub fn delete(name: &str) -> Result<()> {
    let mut contexts = load_contexts_for_update()?;
    let entry = get_entry(&contexts, name)?.clone();

    if let Some(token) = load_context_token(name) {
        forget_token(name, &entry, &token)?;
    }

    contexts.contexts.remove(name);
    let was_current = contexts.current.as_deref() == Some(name);
    if was_current {
        contexts.current = None;
    }
    save_contexts(&contexts)?;
    println!("deleted context '{name}'");
    if was_current && !contexts.contexts.is_empty() {
        eprintln!(
            "no current context. run: tl context use <name> (saved: {})",
            contexts
                .contexts
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_kinds() {
        assert_eq!(token_kind(None), "none");
        assert_eq!(
            token_kind(Some(&ContextToken { token: "t".into() })),
            "saved"
        );
    }
}
