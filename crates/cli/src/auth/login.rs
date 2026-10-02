use crate::auth::context::CliContext;
use crate::commands::init::run_init_flow;
use crate::config::contexts::{
    ContextEntry, ContextsFile, check_no_case_clash, load_contexts_for_update, save_contexts,
};
use crate::config::resolver::{self, UnknownContext};
use crate::config::token_store::{TokenStorage, save_context_token};
use crate::error::{CliError, Result};
use crate::http;
use crate::project::detection::find_project_root;
use std::io::{IsTerminal, Write};

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
pub async fn browser_login(ctx: &CliContext) -> Result<BrowserLogin> {
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

/// Save a login token as context `name` and make it current.
///
/// The token goes to the OS keychain, or to `credentials.toml` where there is none, and the
/// context records which. The token is saved first: a context whose token failed to save
/// would say "logged in" and then fail at the first command.
pub fn save_login_context(api_url: &str, name: &str, login: &BrowserLogin) -> Result<SavedLogin> {
    let mut contexts = load_contexts_for_update()?;
    check_no_case_clash(&contexts, name)?;
    let previous = contexts.get(name).and_then(|entry| entry.storage);
    let storage = save_context_token(name, &login.token, previous)?;
    let saved = apply_login_to_contexts(&mut contexts, api_url, name, login, storage);
    save_contexts(&contexts)?;
    Ok(saved)
}

pub(crate) fn apply_login_to_contexts(
    contexts: &mut ContextsFile,
    api_url: &str,
    name: &str,
    login: &BrowserLogin,
    storage: TokenStorage,
) -> SavedLogin {
    let entry = ContextEntry {
        api_url: api_url.to_string(),
        organization: login.organization_id.clone(),
        project: login.project_id.clone(),
        storage: Some(storage),
    };
    let replaced = contexts.contexts.insert(name.to_string(), entry.clone());
    contexts.current = Some(name.to_string());
    SavedLogin {
        name: name.to_string(),
        replaced,
        entry,
    }
}

/// Run the interactive device code login flow and save the token as context `context_name`.
///
/// Callers that go on in this run rebuild their `CliContext` from the saved context by name;
/// the resolver reads the token, organization, and project from the files this flow wrote.
pub async fn run_login_flow(ctx: &CliContext, auto_init: bool, context_name: &str) -> Result<()> {
    // A `contexts.toml` that does not parse stops the save. Find that out now, before the
    // user approves the login in the browser, or the approved token would be thrown away.
    load_contexts_for_update()?;
    let login = browser_login(ctx).await?;
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

    if auto_init {
        // Recreate context with new PAT
        let resolved = resolver::resolve(
            Some(&ctx.api_url),
            Some(&ctx.cloud_url),
            None,
            Some(&login.token),
            Some(&ctx.namespace),
            login
                .organization_id
                .as_deref()
                .or(ctx.organization_id.as_deref()),
            login.project_id.as_deref().or(ctx.project_id.as_deref()),
            None,
            // `tl login` runs with a missing named context ignored; stay that way here.
            UnknownContext::Ignore,
            ctx.debug,
        )?;
        let updated_ctx = CliContext::from_resolved(resolved);

        if !updated_ctx.has_org_and_project() {
            eprintln!(
                "\nNo organization and project configuration found. Let's set up your project.\n"
            );
            let project_root = find_project_root(None);
            if let Err(e) = run_init_flow(&updated_ctx, true, true, false, &project_root).await {
                eprintln!("\nYou can run 'tl init' later to complete the setup.");
                if ctx.debug {
                    eprintln!("Error: {}", e);
                }
            }
        }
    }

    Ok(())
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
            TokenStorage::Keychain,
        );
        assert_eq!(saved.replaced, None);
        assert_eq!(
            contexts.get("staging").unwrap().storage,
            Some(TokenStorage::Keychain)
        );
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
            TokenStorage::File,
        );
        contexts.current = Some("other".into());
        let saved = apply_login_to_contexts(
            &mut contexts,
            "https://api.tensorlake.ai",
            "default",
            &login("org_1", "project_b"),
            TokenStorage::File,
        );
        assert!(saved.replaced.is_some());
        assert_eq!(
            saved.changes(),
            vec!["  project: project_a -> project_b".to_string()]
        );
        assert_eq!(contexts.current.as_deref(), Some("default"));
        assert_eq!(contexts.contexts.len(), 1);
    }
}
