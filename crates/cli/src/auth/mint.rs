//! Mint and revoke CLI tokens.
//!
//! `POST /platform/cli/tokens/mint` takes a login token (the parent) and gives a token for
//! another project in the same organization. `POST /platform/cli/tokens/revoke` revokes the
//! bearer token; for a parent, the server also revokes its children. A server that does not
//! have these routes yet answers 404, and callers fall back to the browser login.

use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};
use crate::http;

#[derive(Debug, Serialize)]
struct MintRequest<'a> {
    #[serde(rename = "projectId")]
    project_id: &'a str,
}

/// A token minted for one project. The server returns the token one time only.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct MintedToken {
    pub token: String,
    #[serde(rename = "organizationId")]
    pub organization_id: String,
    #[serde(rename = "projectId")]
    pub project_id: String,
    #[serde(rename = "expiresAt", default)]
    pub expires_at: Option<String>,
}

/// The error body platform-api sends. Unknown shapes fall back to the raw text.
#[derive(Debug, Deserialize, Default)]
struct ApiErrorBody {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

fn error_message(status: reqwest::StatusCode, body: &str) -> String {
    let parsed: ApiErrorBody = serde_json::from_str(body).unwrap_or_default();
    let detail = parsed
        .message
        .or(parsed.error)
        .unwrap_or_else(|| body.trim().to_string());
    if detail.is_empty() {
        format!("HTTP {status}")
    } else {
        format!("HTTP {status}: {detail}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MintOutcome {
    Minted(MintedToken),
    /// The server has no mint route (404). Use the browser login instead.
    Unsupported,
}

pub fn mint_url(api_url: &str) -> String {
    format!("{api_url}/platform/cli/tokens/mint")
}

pub fn revoke_url(api_url: &str) -> String {
    format!("{api_url}/platform/cli/tokens/revoke")
}

/// Ask the server for a token for `project_id`, using `parent_token` as the bearer.
pub async fn mint_token(
    api_url: &str,
    parent_token: &str,
    project_id: &str,
) -> Result<MintOutcome> {
    let client = http::client_builder().build().map_err(CliError::Http)?;
    let resp = client
        .post(mint_url(api_url))
        .bearer_auth(parent_token)
        .json(&MintRequest { project_id })
        .send()
        .await
        .map_err(|e| CliError::auth(format!("cannot reach {api_url}: {e}")))?;

    let status = resp.status();
    if status.as_u16() == 404 {
        return Ok(MintOutcome::Unsupported);
    }
    let body = resp.text().await.unwrap_or_default();
    if status.as_u16() == 401 {
        return Err(CliError::auth(format!(
            "the login token was rejected ({}). run: tl login",
            error_message(status, &body)
        )));
    }
    if status.as_u16() == 403 {
        return Err(CliError::auth(format!(
            "cannot mint a token for project {project_id} ({})",
            error_message(status, &body)
        )));
    }
    if !status.is_success() {
        return Err(CliError::auth(format!(
            "mint failed ({})",
            error_message(status, &body)
        )));
    }
    let minted: MintedToken = serde_json::from_str(&body)
        .map_err(|e| CliError::auth(format!("unexpected response from the mint endpoint: {e}")))?;
    Ok(MintOutcome::Minted(minted))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevokeOutcome {
    Revoked,
    /// The server has no revoke route (404), or the token was already gone (401).
    Unsupported,
}

/// Revoke `token` on the server. Best effort: a missing route is not an error.
pub async fn revoke_token(api_url: &str, token: &str) -> Result<RevokeOutcome> {
    let client = http::client_builder().build().map_err(CliError::Http)?;
    let resp = client
        .post(revoke_url(api_url))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| CliError::auth(format!("cannot reach {api_url}: {e}")))?;
    let status = resp.status();
    if status.as_u16() == 404 || status.as_u16() == 401 {
        return Ok(RevokeOutcome::Unsupported);
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::auth(format!(
            "revoke failed ({})",
            error_message(status, &body)
        )));
    }
    Ok(RevokeOutcome::Revoked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Serve one HTTP response and return the request that arrived.
    async fn one_shot(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 65536];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });
        (url, server)
    }

    #[tokio::test]
    async fn mint_parses_a_token() {
        let (url, server) = one_shot(
            200,
            r#"{"token":"tl_child","organizationId":"org_1","projectId":"project_2","expiresAt":"2027-01-01T00:00:00Z"}"#,
        )
        .await;
        let out = mint_token(&url, "tl_parent", "project_2").await.unwrap();
        let request = server.await.unwrap();
        assert!(
            request.starts_with("POST /platform/cli/tokens/mint "),
            "{request}"
        );
        assert!(
            request.contains("authorization: Bearer tl_parent"),
            "{request}"
        );
        assert!(
            request.ends_with(r#"{"projectId":"project_2"}"#),
            "{request}"
        );
        assert_eq!(
            out,
            MintOutcome::Minted(MintedToken {
                token: "tl_child".into(),
                organization_id: "org_1".into(),
                project_id: "project_2".into(),
                expires_at: Some("2027-01-01T00:00:00Z".into()),
            })
        );
    }

    #[tokio::test]
    async fn mint_reports_a_missing_route() {
        let (url, server) = one_shot(404, r#"{"message":"Not Found"}"#).await;
        let out = mint_token(&url, "tl_parent", "project_2").await.unwrap();
        server.await.unwrap();
        assert_eq!(out, MintOutcome::Unsupported);
    }

    #[tokio::test]
    async fn mint_passes_the_server_reason_through() {
        let (url, server) = one_shot(
            403,
            r#"{"message":"project is not in the token's organization"}"#,
        )
        .await;
        let err = mint_token(&url, "tl_parent", "project_2")
            .await
            .unwrap_err();
        server.await.unwrap();
        let msg = err.to_string();
        assert!(msg.contains("project_2"), "{msg}");
        assert!(msg.contains("not in the token's organization"), "{msg}");
    }

    #[tokio::test]
    async fn revoke_tolerates_a_missing_route() {
        let (url, server) = one_shot(404, "").await;
        assert_eq!(
            revoke_token(&url, "tl_x").await.unwrap(),
            RevokeOutcome::Unsupported
        );
        server.await.unwrap();
        let (url, server) = one_shot(204, "").await;
        assert_eq!(
            revoke_token(&url, "tl_x").await.unwrap(),
            RevokeOutcome::Revoked
        );
        let request = server.await.unwrap();
        assert!(
            request.starts_with("POST /platform/cli/tokens/revoke "),
            "{request}"
        );
    }
}
