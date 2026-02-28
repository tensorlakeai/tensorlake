use crate::auth::context::CliContext;
use crate::commands::init::run_init_flow;
use crate::config::files::save_credentials;
use crate::config::resolver;
use crate::error::{CliError, Result};
use crate::project::detection::find_project_root;

/// Result of a successful login flow.
pub struct LoginResult {
    pub token: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
}

/// Run the interactive device code login flow.
pub async fn run_login_flow(ctx: &CliContext, auto_init: bool) -> Result<LoginResult> {
    let login_start_url = format!("{}/platform/cli/login/start", ctx.api_url);

    let http = reqwest::Client::new();
    let resp = http
        .post(&login_start_url)
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

    let body: serde_json::Value = resp.json().await.map_err(|e| CliError::auth(e.to_string()))?;
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

    let verification_uri = format!("{}/cli/login", ctx.cloud_url);
    eprintln!("URL: {}", verification_uri);
    eprintln!("opening web browser...");

    // Give user time to read
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    if let Err(_) = open::that(&verification_uri) {
        eprintln!("failed to open web browser. please open the URL above manually and enter the code.");
    }

    eprintln!("waiting for the code to be processed...");

    let poll_url = format!(
        "{}/platform/cli/login/poll?device_code={}",
        ctx.api_url, device_code
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
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            "expired" => {
                return Err(CliError::auth(
                    "login request has expired. run 'tensorlake login' to start a new one.",
                ));
            }
            "failed" => {
                return Err(CliError::auth(
                    "login request was denied. run 'tensorlake login' to try again.",
                ));
            }
            "approved" => break,
            other => {
                return Err(CliError::auth(format!(
                    "got unexpected login status '{}'. run 'tensorlake login' again.",
                    other
                )));
            }
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

    save_credentials(&ctx.api_url, &access_token)?;
    eprintln!("login successful!");

    let mut org_id = None;
    let mut proj_id = None;

    if auto_init {
        // Recreate context with new PAT
        let resolved = resolver::resolve(
            Some(&ctx.api_url),
            Some(&ctx.cloud_url),
            None,
            Some(&access_token),
            Some(&ctx.namespace),
            ctx.organization_id.as_deref(),
            ctx.project_id.as_deref(),
            ctx.debug,
        );
        let updated_ctx = CliContext::from_resolved(resolved);

        if updated_ctx.has_org_and_project() {
            org_id = updated_ctx.effective_organization_id();
            proj_id = updated_ctx.effective_project_id();
        } else {
            eprintln!("\nNo organization and project configuration found. Let's set up your project.\n");
            let project_root = find_project_root(None);
            match run_init_flow(&updated_ctx, true, true, false, &project_root).await {
                Ok((o, p)) => {
                    org_id = Some(o);
                    proj_id = Some(p);
                }
                Err(e) => {
                    eprintln!("\nYou can run 'tensorlake init' later to complete the setup.");
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
