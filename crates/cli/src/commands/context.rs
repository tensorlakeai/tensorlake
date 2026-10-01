//! `tl context`: switch between projects without a new browser login.
//!
//! Contexts live in `~/.config/tensorlake/contexts.toml` (no secrets). The token for each
//! context lives in `credentials.toml`. The per-URL table in `credentials.toml` always holds a
//! copy of the current context's token, so older CLI versions keep working.

use std::io::IsTerminal;

use comfy_table::Cell;
use serde::{Deserialize, Serialize};

use crate::auth::context::CliContext;
use crate::auth::login::{BrowserLogin, browser_login};
use crate::auth::mint::{MintOutcome, RevokeOutcome, mint_token, revoke_token};
use crate::config::contexts::{
    ContextEntry, ContextsFile, load_contexts, load_contexts_for_update, save_contexts,
    validate_context_name,
};
use crate::config::files::{
    ContextToken, load_context_token, load_stored_credentials, remove_context_token,
    remove_credentials, rename_context_token, save_context_token, save_credentials,
};
use crate::config::resolver::{validate_organization_id, validate_project_id};
use crate::error::{CliError, Result};
use crate::http;
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
    /// `login` for a browser login token, `minted` for a token minted from one, `none`
    /// when the context has no token (for example after `tl logout`).
    token: &'static str,
}

fn token_kind(token: Option<&ContextToken>) -> &'static str {
    match token {
        Some(t) if t.parent => "login",
        Some(_) => "minted",
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
            eprintln!(
                "warning: context '{name}' has no token. run: tl login --context {name}, or tl context create"
            );
            Ok(())
        }
    }
}

/// A parent (login) token that can mint tokens for `organization`.
struct Parent {
    name: String,
    organization: String,
    token: String,
}

/// Find a login token for the organization, preferring the selected context, then the
/// current one, then any context for the same API URL.
fn find_parent(
    ctx: &CliContext,
    contexts: &ContextsFile,
    organization: Option<&str>,
) -> Option<Parent> {
    let ordered = ctx
        .context_name
        .iter()
        .map(String::as_str)
        .chain(contexts.current.as_deref())
        .chain(contexts.for_api_url(&ctx.api_url).map(|(name, _)| name));
    for name in ordered {
        let Some(entry) = contexts.get(name) else {
            continue;
        };
        if !entry.matches(&ctx.api_url, None, None) {
            continue;
        }
        let Some(org) = entry.organization.as_deref() else {
            continue;
        };
        if let Some(wanted) = organization
            && wanted != org
        {
            continue;
        }
        if let Some(token) = load_context_token(name)
            && token.parent
        {
            return Some(Parent {
                name: name.to_string(),
                organization: org.to_string(),
                token: token.token,
            });
        }
    }
    None
}

#[derive(Debug, Deserialize)]
struct ProjectSummary {
    id: String,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListProjectsResponse {
    items: Vec<ProjectSummary>,
}

async fn pick_project(api_url: &str, parent: &Parent) -> Result<String> {
    if !std::io::stdin().is_terminal() {
        return Err(CliError::usage(
            "pass --project <id>: there is no terminal to pick a project in",
        ));
    }
    let client = http::client_builder().build().map_err(CliError::Http)?;
    let resp = client
        .get(format!(
            "{api_url}/platform/v1/organizations/{}/projects",
            urlencoding::encode(&parent.organization)
        ))
        .bearer_auth(&parent.token)
        .send()
        .await
        .map_err(|e| CliError::auth(format!("cannot reach {api_url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(CliError::auth(format!(
            "could not list the projects of {} (HTTP {}). run: tl login",
            parent.organization,
            resp.status()
        )));
    }
    let body: ListProjectsResponse = resp.json().await.map_err(CliError::Http)?;
    if body.items.is_empty() {
        return Err(CliError::config(format!(
            "organization {} has no projects. create one in the dashboard first.",
            parent.organization
        )));
    }
    let labels: Vec<String> = body
        .items
        .iter()
        .map(
            |p| match p.name.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
                Some(name) => format!("{name} ({})", p.id),
                None => p.id.clone(),
            },
        )
        .collect();
    let selection = dialoguer::Select::new()
        .with_prompt(format!("Select a project in {}", parent.organization))
        .items(&labels)
        .default(0)
        .interact()
        .map_err(|_| CliError::Cancelled)?;
    Ok(body.items[selection].id.clone())
}

/// `tl context create <name> [--project P] [--organization O]`.
///
/// With a login token for the organization, mints a token for the project (no browser).
/// Without one, or when the server has no mint route, runs the browser login and checks
/// that the browser approved the wanted organization and project.
pub async fn create(
    ctx: &CliContext,
    name: &str,
    project: Option<&str>,
    organization: Option<&str>,
) -> Result<()> {
    validate_context_name(name)?;
    if let Some(project) = project {
        validate_project_id(project)?;
    }
    if let Some(organization) = organization {
        validate_organization_id(organization)?;
    }
    // Strict: a broken file must stop the command before a token is minted.
    let contexts = load_contexts_for_update()?;
    if contexts.get(name).is_some() {
        return Err(CliError::usage(format!(
            "context '{name}' exists. run: tl context delete {name}, or tl login --context {name}"
        )));
    }

    // The project to ask the browser for: the flag, or the one picked from the parent's list.
    let mut wanted_project: Option<String> = project.map(str::to_string);
    if let Some(parent) = find_parent(ctx, &contexts, organization) {
        let project_id = match project {
            Some(p) => p.to_string(),
            None => pick_project(&ctx.api_url, &parent).await?,
        };
        match mint_token(&ctx.api_url, &parent.token, &project_id).await? {
            MintOutcome::Minted(minted) => {
                let entry = ContextEntry {
                    api_url: ctx.api_url.clone(),
                    organization: Some(minted.organization_id.clone()),
                    project: Some(minted.project_id.clone()),
                };
                save_new_context(name, entry, &minted.token, false)?;
                eprintln!(
                    "minted a token for project {} with the login of context '{}'.",
                    minted.project_id, parent.name
                );
                println!("saved context '{name}'. run: tl context use {name}");
                return Ok(());
            }
            MintOutcome::Unsupported => {
                eprintln!("the server cannot mint tokens yet. using the browser login instead.");
                // Keep the picked project so the browser login asks for it and checks it.
                wanted_project = Some(project_id);
            }
        }
    } else {
        eprintln!(
            "no login token for this organization. using the browser login.\n\
             (run: tl login first to mint tokens without a browser.)"
        );
    }

    let login = browser_login(ctx, wanted_project.as_deref()).await?;
    check_browser_login(&login, organization, wanted_project.as_deref())?;
    let entry = ContextEntry {
        api_url: ctx.api_url.clone(),
        organization: login.organization_id.clone(),
        project: login.project_id.clone(),
    };
    save_new_context(name, entry, &login.token, true)?;
    println!("saved context '{name}'. run: tl context use {name}");
    Ok(())
}

/// Refuse a browser login for an organization or project other than the one asked for.
pub(crate) fn check_browser_login(
    login: &BrowserLogin,
    wanted_organization: Option<&str>,
    wanted_project: Option<&str>,
) -> Result<()> {
    check_approved(
        "organization",
        login.organization_id.as_deref(),
        wanted_organization,
    )?;
    check_approved("project", login.project_id.as_deref(), wanted_project)
}

fn check_approved(what: &str, approved: Option<&str>, wanted: Option<&str>) -> Result<()> {
    let Some(wanted) = wanted else {
        return Ok(());
    };
    match approved {
        Some(approved) if approved == wanted => Ok(()),
        approved => Err(CliError::auth(format!(
            "browser approved {}, but you asked for {wanted}. run the command again and pick {wanted} in the browser.",
            approved
                .map(str::to_string)
                .unwrap_or_else(|| format!("no {what}"))
        ))),
    }
}

fn save_new_context(name: &str, entry: ContextEntry, token: &str, parent: bool) -> Result<()> {
    let mut contexts = load_contexts_for_update()?;
    contexts.contexts.insert(name.to_string(), entry);
    save_context_token(name, token, parent)?;
    save_contexts(&contexts)
}

/// `tl context set <name> key=value...` for `api_url`, `organization`, and `project`.
pub fn set(name: &str, pairs: &[String]) -> Result<()> {
    let mut contexts = load_contexts_for_update()?;
    let mut entry = get_entry(&contexts, name)?.clone();
    if pairs.is_empty() {
        return Err(CliError::usage(
            "give at least one key=value: api_url, organization, or project",
        ));
    }
    for pair in pairs {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(CliError::usage(format!("expected key=value, got '{pair}'")));
        };
        let value = value.trim();
        match key.trim() {
            "api_url" => entry.api_url = crate::config::files::normalize_api_url(value),
            "organization" => {
                validate_organization_id(value)?;
                entry.organization = Some(value.to_string());
            }
            "project" => {
                validate_project_id(value)?;
                entry.project = Some(value.to_string());
            }
            other => {
                return Err(CliError::usage(format!(
                    "unknown key '{other}': use api_url, organization, or project"
                )));
            }
        }
    }
    let scope_changed = contexts.get(name) != Some(&entry);
    contexts.contexts.insert(name.to_string(), entry.clone());
    save_contexts(&contexts)?;
    if scope_changed && load_context_token(name).is_some() {
        eprintln!(
            "warning: the saved token of '{name}' may not match its new scope. \
             run: tl context delete {name} && tl context create {name} --project <id>"
        );
    }
    if contexts.current.as_deref() == Some(name) {
        copy_token_to_url_table(name, &entry)?;
    }
    println!("updated context '{name}'");
    Ok(())
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

/// `tl context delete <name>`: revoke the token on the server, then remove the context.
pub async fn delete(name: &str, yes: bool) -> Result<()> {
    let mut contexts = load_contexts_for_update()?;
    let entry = get_entry(&contexts, name)?.clone();
    let token = load_context_token(name);

    if let Some(token) = &token
        && token.parent
    {
        let children: Vec<&str> = contexts
            .for_api_url(&entry.api_url)
            .filter(|(other, e)| {
                *other != name
                    && e.organization == entry.organization
                    && load_context_token(other).is_some_and(|t| !t.parent)
            })
            .map(|(other, _)| other)
            .collect();
        if !children.is_empty() {
            eprintln!(
                "context '{name}' holds the login token for {}. revoking it also revokes the tokens minted from it: {}",
                entry.organization.as_deref().unwrap_or("its organization"),
                children.join(", ")
            );
            if !yes {
                if !std::io::stdin().is_terminal() {
                    return Err(CliError::usage(
                        "pass --yes to delete a login context without a prompt",
                    ));
                }
                let confirm = dialoguer::Confirm::new()
                    .with_prompt("Continue?")
                    .default(false)
                    .interact()
                    .map_err(|_| CliError::Cancelled)?;
                if !confirm {
                    return Err(CliError::Cancelled);
                }
            }
        }
    }

    if let Some(token) = &token {
        match revoke_token(&entry.api_url, &token.token).await {
            Ok(RevokeOutcome::Revoked) => eprintln!("revoked the token of '{name}' on the server."),
            Ok(RevokeOutcome::Unsupported) => eprintln!(
                "the server could not revoke the token of '{name}' (not supported yet). it is removed from this machine only."
            ),
            Err(e) => eprintln!("warning: could not revoke the token of '{name}': {e}"),
        }
        remove_context_token(name)?;
        let url_table_has_it = load_stored_credentials(&entry.api_url)
            .is_some_and(|stored| stored.token == token.token);
        if url_table_has_it {
            remove_credentials(&entry.api_url)?;
        }
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

    fn login(project: Option<&str>) -> BrowserLogin {
        BrowserLogin {
            token: "tl_x".into(),
            organization_id: Some("org_1".into()),
            project_id: project.map(str::to_string),
        }
    }

    #[test]
    fn browser_project_must_match_the_wanted_one() {
        assert!(check_browser_login(&login(Some("project_a")), None, None).is_ok());
        assert!(check_browser_login(&login(Some("project_a")), None, Some("project_a")).is_ok());
        let err = check_browser_login(&login(Some("project_xyz")), None, Some("project_abc"))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "browser approved project_xyz, but you asked for project_abc. run the command again and pick project_abc in the browser."
        );
        let err = check_browser_login(&login(None), None, Some("project_abc")).unwrap_err();
        assert!(
            err.to_string().starts_with("browser approved no project,"),
            "{err}"
        );
    }

    #[test]
    fn browser_organization_must_match_the_wanted_one() {
        assert!(check_browser_login(&login(None), Some("org_1"), None).is_ok());
        let err = check_browser_login(&login(None), Some("org_2"), None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "browser approved org_1, but you asked for org_2. run the command again and pick org_2 in the browser."
        );
        let no_org = BrowserLogin {
            organization_id: None,
            ..login(None)
        };
        let err = check_browser_login(&no_org, Some("org_2"), None).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("browser approved no organization,"),
            "{err}"
        );
    }

    #[test]
    fn token_kinds() {
        assert_eq!(token_kind(None), "none");
        assert_eq!(
            token_kind(Some(&ContextToken {
                token: "t".into(),
                parent: true
            })),
            "login"
        );
        assert_eq!(
            token_kind(Some(&ContextToken {
                token: "t".into(),
                parent: false
            })),
            "minted"
        );
    }
}
