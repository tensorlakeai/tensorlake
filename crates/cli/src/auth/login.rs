use crate::auth::context::CliContext;
use crate::auth::mint::{MintOutcome, mint_token, revoke_token};
use crate::commands::init::run_init_flow;
use crate::config::contexts::{ContextEntry, ContextsFile, load_contexts, save_contexts};
use crate::config::files::{
    ContextToken, load_context_token, remove_context_token, save_context_token, save_credentials,
};
use crate::config::resolver;
use crate::error::{CliError, Result};
use crate::http;
use crate::project::detection::find_project_root;
use std::io::{IsTerminal, Write};

/// Result of a successful login flow.
pub struct LoginResult {
    pub token: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
}

/// What the browser login approved. Not yet saved anywhere.
#[derive(Debug, Clone)]
pub struct BrowserLogin {
    pub token: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
}

/// Build the optional device-fingerprint body we send on /cli/login/start.
///
/// The server tolerates any subset of these fields (see platform-api
/// /cli/login/start — pickString trims + truncates at 512 chars). We send
/// what we can resolve locally so the browser approve screen can show the
/// user what terminal is asking for access: hostname, OS + arch, and CLI
/// version.
fn device_fingerprint() -> serde_json::Value {
    let hostname = gethostname::gethostname()
        .into_string()
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let os = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    let client_version = format!("tensorlake-cli/{}", env!("CARGO_PKG_VERSION"));

    let mut body = serde_json::Map::new();
    if let Some(h) = hostname {
        body.insert("device_name".into(), serde_json::Value::String(h));
    }
    body.insert("os".into(), serde_json::Value::String(os));
    body.insert(
        "client_version".into(),
        serde_json::Value::String(client_version),
    );
    serde_json::Value::Object(body)
}

async fn countdown_before_open(seconds: u64) {
    let interactive = std::io::stderr().is_terminal();

    for remaining in (1..=seconds).rev() {
        let unit = if remaining == 1 { "second" } else { "seconds" };
        if interactive {
            eprint!("\r\x1b[2KOpening the browser in {remaining} {unit}...");
            let _ = std::io::stderr().flush();
        } else {
            eprintln!("Opening the browser in {remaining} {unit}...");
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    if interactive {
        eprintln!("\r\x1b[2KOpening browser...");
    }
}

fn update_status_line(interactive: bool, message: &str) {
    if interactive {
        eprint!("\r\x1b[2K{message}");
        let _ = std::io::stderr().flush();
    } else {
        eprintln!("{message}");
    }
}

fn finish_status_line(interactive: bool, message: &str) {
    if interactive {
        eprintln!("\r\x1b[2K{message}");
    } else {
        eprintln!("{message}");
    }
}

async fn wait_before_next_login_poll(attempt: u64, seconds: u64, interactive: bool) {
    for remaining in (1..=seconds).rev() {
        let unit = if remaining == 1 { "second" } else { "seconds" };
        update_status_line(
            interactive,
            &format!(
                "Waiting for browser approval. Check #{attempt}; checking again in {remaining} {unit}..."
            ),
        );
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// Run the interactive device code login flow in the browser. Saves nothing.
///
/// `wanted_project` is shown to the user before the browser opens, because the browser
/// picks the project and the CLI cannot preselect it.
pub async fn browser_login(ctx: &CliContext, wanted_project: Option<&str>) -> Result<BrowserLogin> {
    let login_start_url = format!("{}/platform/cli/login/start", ctx.api_url);

    let http = http::client_builder()
        .build()
        .map_err(|e| CliError::auth(format!("failed to initialize HTTP client: {}", e)))?;
    let resp = http
        .post(&login_start_url)
        .json(&device_fingerprint())
        .send()
        .await
        .map_err(|e| CliError::auth(format!("cannot reach {}: {}", ctx.api_url, e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::auth(format!(
            "login service returned an error ({}): {}",
            status, body
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::auth(e.to_string()))?;
    let device_code = body
        .get("device_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::auth("unexpected response from login service"))?
        .to_string();
    let user_code = body
        .get("user_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::auth("unexpected response from login service"))?
        .to_string();

    eprintln!("we're going to open a web browser for you to enter a one-time code.");
    eprintln!("Your code is: {}", user_code);
    if let Some(project) = wanted_project {
        eprintln!("In the browser, pick project {project}.");
    }

    // Embed the code in the URL so the browser can pre-fill the field.
    // The user still verifies the code matches what's in their terminal —
    // pre-fill is a UX improvement, not a security downgrade.
    let verification_uri = format!(
        "{}/cli/login?user_code={}",
        ctx.cloud_url,
        urlencoding::encode(&user_code),
    );
    eprintln!("URL: {}", verification_uri);

    // Give user time to read
    countdown_before_open(5).await;

    if open::that(&verification_uri).is_err() {
        eprintln!(
            "failed to open web browser. please open the URL above manually and enter the code."
        );
    }

    let poll_url = format!(
        "{}/platform/cli/login/poll?device_code={}",
        ctx.api_url, device_code
    );

    let mut poll_attempt = 1;
    let interactive_stderr = std::io::stderr().is_terminal();
    update_status_line(
        interactive_stderr,
        "Waiting for browser approval. Complete the flow in your browser.",
    );
    loop {
        let poll_resp = http
            .get(&poll_url)
            .send()
            .await
            .map_err(|e| CliError::auth(format!("failed to poll login status: {}", e)))?;

        if !poll_resp.status().is_success() {
            let status = poll_resp.status();
            let body = poll_resp.text().await.unwrap_or_default();
            return Err(CliError::auth(format!(
                "login service returned an error ({}): {}",
                status, body
            )));
        }

        let poll_body: serde_json::Value = poll_resp
            .json()
            .await
            .map_err(|e| CliError::auth(format!("unexpected response while polling: {}", e)))?;

        let status = poll_body
            .get("status")
            .and_then(|v| v.as_str())
            .ok_or_else(|| CliError::auth("unexpected response while polling login status"))?;

        match status {
            "pending" => {
                poll_attempt += 1;
                wait_before_next_login_poll(poll_attempt, 5, interactive_stderr).await;
            }
            "expired" => {
                finish_status_line(interactive_stderr, "Login request expired.");
                return Err(CliError::auth(
                    "login request has expired. run 'tl login' to start a new one.",
                ));
            }
            "failed" => {
                finish_status_line(interactive_stderr, "Login request denied.");
                return Err(CliError::auth(
                    "login request was denied. run 'tl login' to try again.",
                ));
            }
            "approved" => {
                finish_status_line(
                    interactive_stderr,
                    "Browser approval received. Requesting your PAT...",
                );
                break;
            }
            other => {
                return Err(CliError::auth(format!(
                    "got unexpected login status '{}'. run 'tl login' again.",
                    other
                )));
            }
        }

        if poll_attempt % 6 == 0 {
            update_status_line(
                interactive_stderr,
                "Still waiting. If the browser did not open, use the URL above.",
            );
        }
    }

    // Exchange device code for access token
    let exchange_url = format!("{}/platform/cli/login/exchange", ctx.api_url);
    let exchange_resp = http
        .post(&exchange_url)
        .json(&serde_json::json!({"device_code": device_code}))
        .send()
        .await
        .map_err(|e| CliError::auth(format!("failed to exchange token: {}", e)))?;

    if !exchange_resp.status().is_success() {
        let status = exchange_resp.status();
        let body = exchange_resp.text().await.unwrap_or_default();
        return Err(CliError::auth(format!(
            "login service returned an error ({}): {}",
            status, body
        )));
    }

    let exchange_body: serde_json::Value = exchange_resp
        .json()
        .await
        .map_err(|e| CliError::auth(format!("unexpected response during token exchange: {}", e)))?;

    let access_token = exchange_body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::auth("unexpected response during token exchange"))?
        .to_string();
    let exchange_org_id = exchange_body
        .get("organization_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let exchange_project_id = exchange_body
        .get("project_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(BrowserLogin {
        token: access_token,
        organization_id: exchange_org_id,
        project_id: exchange_project_id,
    })
}

/// What `save_login_context` changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedLogin {
    pub name: String,
    /// `Some(old)` when a context with this name existed before.
    pub replaced: Option<ContextEntry>,
    pub entry: ContextEntry,
}

impl SavedLogin {
    /// One line for each field that changed, for the user.
    pub fn changes(&self) -> Vec<String> {
        let Some(old) = &self.replaced else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let fields = [
            (
                "api_url",
                Some(old.api_url.as_str()),
                Some(self.entry.api_url.as_str()),
            ),
            (
                "organization",
                old.organization.as_deref(),
                self.entry.organization.as_deref(),
            ),
            (
                "project",
                old.project.as_deref(),
                self.entry.project.as_deref(),
            ),
        ];
        for (field, before, after) in fields {
            if before != after {
                out.push(format!(
                    "  {field}: {} -> {}",
                    before.unwrap_or("(none)"),
                    after.unwrap_or("(none)")
                ));
            }
        }
        out
    }
}

/// Save a login token as context `name`, make it current, and copy it to the per-URL
/// table that older CLI versions read.
pub fn save_login_context(api_url: &str, name: &str, login: &BrowserLogin) -> Result<SavedLogin> {
    let mut contexts = load_contexts();
    let saved = apply_login_to_contexts(&mut contexts, api_url, name, login);
    save_context_token(name, &login.token, true)?;
    save_credentials(
        api_url,
        &login.token,
        login.organization_id.as_deref(),
        login.project_id.as_deref(),
    )?;
    save_contexts(&contexts)?;
    Ok(saved)
}

pub(crate) fn apply_login_to_contexts(
    contexts: &mut ContextsFile,
    api_url: &str,
    name: &str,
    login: &BrowserLogin,
) -> SavedLogin {
    let entry = ContextEntry {
        api_url: api_url.to_string(),
        organization: login.organization_id.clone(),
        project: login.project_id.clone(),
    };
    let replaced = contexts.contexts.insert(name.to_string(), entry.clone());
    contexts.current = Some(name.to_string());
    SavedLogin {
        name: name.to_string(),
        replaced,
        entry,
    }
}

/// After a login, mint new tokens for the other saved contexts in the same organization,
/// so that they keep working when the old parent token expires or is revoked.
///
/// Contexts that hold their own login token are left alone. Such a token works on its own,
/// and `tl logout` revokes it only while it is still saved as a parent. Replacing it with a
/// minted token would leave it valid on the server but forgotten on this machine.
///
/// Stops quietly when the server has no mint route. Other failures are reported for each
/// context and do not fail the login.
pub async fn refresh_child_tokens(api_url: &str, parent_name: &str, parent: &BrowserLogin) {
    let Some(organization) = parent.organization_id.as_deref() else {
        return;
    };
    let contexts = load_contexts();
    let siblings = contexts_to_refresh(
        &contexts,
        api_url,
        parent_name,
        organization,
        load_context_token,
    );
    if siblings.is_empty() {
        return;
    }

    for (name, project) in siblings {
        match mint_token(api_url, &parent.token, &project).await {
            Ok(MintOutcome::Minted(minted)) => {
                if let Some(old) = load_context_token(&name) {
                    let _ = revoke_token(api_url, &old.token).await;
                }
                if let Err(e) = save_context_token(&name, &minted.token, false) {
                    eprintln!("warning: could not save a new token for context '{name}': {e}");
                } else {
                    eprintln!("minted a new token for context '{name}' ({project}).");
                }
            }
            Ok(MintOutcome::Unsupported) => return,
            Err(e) => {
                eprintln!("warning: could not mint a new token for context '{name}': {e}");
            }
        }
    }
}

/// The contexts whose token is minted again after a login as `parent_name`: every other
/// context of `organization` at `api_url` that has a project and does not hold its own
/// login token. Returns `(context name, project)` pairs.
pub(crate) fn contexts_to_refresh(
    contexts: &ContextsFile,
    api_url: &str,
    parent_name: &str,
    organization: &str,
    token_of: impl Fn(&str) -> Option<ContextToken>,
) -> Vec<(String, String)> {
    contexts
        .for_api_url(api_url)
        .filter(|(name, entry)| {
            *name != parent_name
                && entry.organization.as_deref() == Some(organization)
                && entry.project.is_some()
                && !token_of(name).is_some_and(|t| t.parent)
        })
        .map(|(name, entry)| (name.to_string(), entry.project.clone().unwrap_or_default()))
        .collect()
}

/// Remove the saved tokens of every context for `api_url` in `organization`. Used by
/// `tl logout`, after the parent token is revoked on the server.
///
/// `None` selects only the contexts that have no organization (a migrated legacy login).
/// This is the same selection that `tl logout` revokes, so that no context loses its local
/// token while its server token stays active.
pub fn forget_organization_tokens(
    api_url: &str,
    organization: Option<&str>,
) -> Result<Vec<String>> {
    let contexts = load_contexts();
    let names = contexts_to_forget(&contexts, api_url, organization);
    for name in &names {
        remove_context_token(name)?;
    }
    Ok(names)
}

/// The contexts of `organization` at `api_url`, by exact match: `None` matches only
/// contexts without an organization.
pub(crate) fn contexts_to_forget(
    contexts: &ContextsFile,
    api_url: &str,
    organization: Option<&str>,
) -> Vec<String> {
    contexts
        .for_api_url(api_url)
        .filter(|(_, entry)| entry.organization.as_deref() == organization)
        .map(|(name, _)| name.to_string())
        .collect()
}

/// Run the interactive device code login flow and save the token as context `context_name`.
pub async fn run_login_flow(
    ctx: &CliContext,
    auto_init: bool,
    context_name: &str,
) -> Result<LoginResult> {
    let login = browser_login(ctx, None).await?;
    let saved = save_login_context(&ctx.api_url, context_name, &login)?;
    eprintln!("login successful!");
    match &saved.replaced {
        Some(_) => {
            let changes = saved.changes();
            if changes.is_empty() {
                eprintln!("replaced the token of context '{context_name}'.");
            } else {
                eprintln!("replaced context '{context_name}':");
                for line in changes {
                    eprintln!("{line}");
                }
            }
        }
        None => eprintln!("saved context '{context_name}' and made it current."),
    }
    refresh_child_tokens(&ctx.api_url, context_name, &login).await;

    let access_token = login.token.clone();
    let mut org_id = login.organization_id.clone();
    let mut proj_id = login.project_id.clone();

    if auto_init {
        // Recreate context with new PAT
        let resolved = resolver::resolve(
            Some(&ctx.api_url),
            Some(&ctx.cloud_url),
            None,
            Some(&access_token),
            Some(&ctx.namespace),
            org_id.as_deref().or(ctx.organization_id.as_deref()),
            proj_id.as_deref().or(ctx.project_id.as_deref()),
            None,
            ctx.debug,
        )?;
        let updated_ctx = CliContext::from_resolved(resolved);

        if updated_ctx.has_org_and_project() {
            org_id = updated_ctx.effective_organization_id();
            proj_id = updated_ctx.effective_project_id();
        } else {
            eprintln!(
                "\nNo organization and project configuration found. Let's set up your project.\n"
            );
            let project_root = find_project_root(None);
            match run_init_flow(&updated_ctx, true, true, false, false, &project_root).await {
                Ok((o, p)) => {
                    org_id = Some(o);
                    proj_id = Some(p);
                }
                Err(e) => {
                    eprintln!("\nYou can run 'tl init' later to complete the setup.");
                    if ctx.debug {
                        eprintln!("Error: {}", e);
                    }
                }
            }
        }
    }

    Ok(LoginResult {
        token: access_token,
        organization_id: org_id,
        project_id: proj_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn login(org: &str, project: &str) -> BrowserLogin {
        BrowserLogin {
            token: "tl_new".into(),
            organization_id: Some(org.into()),
            project_id: Some(project.into()),
        }
    }

    #[test]
    fn login_adds_a_context_and_makes_it_current() {
        let mut contexts = ContextsFile::default();
        let saved = apply_login_to_contexts(
            &mut contexts,
            "https://api.tensorlake.ai",
            "staging",
            &login("org_1", "project_s"),
        );
        assert_eq!(saved.replaced, None);
        assert!(saved.changes().is_empty());
        assert_eq!(contexts.current.as_deref(), Some("staging"));
        assert_eq!(
            contexts.get("staging").unwrap().project.as_deref(),
            Some("project_s")
        );
    }

    #[test]
    fn login_replaces_an_existing_context_and_reports_the_change() {
        let mut contexts = ContextsFile::default();
        apply_login_to_contexts(
            &mut contexts,
            "https://api.tensorlake.ai",
            "default",
            &login("org_1", "project_a"),
        );
        contexts.current = Some("other".into());
        let saved = apply_login_to_contexts(
            &mut contexts,
            "https://api.tensorlake.ai",
            "default",
            &login("org_1", "project_b"),
        );
        assert!(saved.replaced.is_some());
        assert_eq!(
            saved.changes(),
            vec!["  project: project_a -> project_b".to_string()]
        );
        assert_eq!(contexts.current.as_deref(), Some("default"));
        assert_eq!(contexts.contexts.len(), 1);
    }

    #[test]
    fn forget_matches_the_organization_exactly() {
        let url = "https://api.tensorlake.ai";
        let mut contexts = ContextsFile::default();
        for (name, org, project) in [
            ("default", "org_1", "project_a"),
            ("staging", "org_1", "project_b"),
            ("other-org", "org_2", "project_c"),
        ] {
            apply_login_to_contexts(&mut contexts, url, name, &login(org, project));
        }
        contexts.contexts.insert(
            "legacy".into(),
            ContextEntry {
                api_url: url.into(),
                organization: None,
                project: None,
            },
        );
        apply_login_to_contexts(
            &mut contexts,
            "https://other.example",
            "other-url",
            &login("org_1", "project_d"),
        );

        let mut forgotten = contexts_to_forget(&contexts, url, Some("org_1"));
        forgotten.sort();
        assert_eq!(
            forgotten,
            vec!["default".to_string(), "staging".to_string()]
        );

        // A migrated legacy login has no organization. Logging out of it must not drop
        // the tokens of other organizations: their server tokens stay active.
        assert_eq!(
            contexts_to_forget(&contexts, url, None),
            vec!["legacy".to_string()]
        );
    }

    #[test]
    fn refresh_skips_contexts_that_hold_their_own_login_token() {
        let url = "https://api.tensorlake.ai";
        let mut contexts = ContextsFile::default();
        for (name, org, project) in [
            ("default", "org_1", "project_a"),
            ("staging", "org_1", "project_b"),
            ("child", "org_1", "project_c"),
            ("fresh", "org_1", "project_d"),
            ("other-org", "org_2", "project_e"),
        ] {
            apply_login_to_contexts(&mut contexts, url, name, &login(org, project));
        }
        apply_login_to_contexts(
            &mut contexts,
            "https://other.example",
            "other-url",
            &login("org_1", "project_f"),
        );
        contexts.contexts.insert(
            "no-project".into(),
            ContextEntry {
                api_url: url.into(),
                organization: Some("org_1".into()),
                project: None,
            },
        );

        let token_of = |name: &str| match name {
            "default" => Some(ContextToken {
                token: "tl_default".into(),
                parent: true,
            }),
            "child" => Some(ContextToken {
                token: "tl_child".into(),
                parent: false,
            }),
            _ => None,
        };

        let mut got = contexts_to_refresh(&contexts, url, "staging", "org_1", token_of);
        got.sort();
        assert_eq!(
            got,
            vec![
                ("child".to_string(), "project_c".to_string()),
                ("fresh".to_string(), "project_d".to_string()),
            ]
        );
    }
}
