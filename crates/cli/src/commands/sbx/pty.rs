use comfy_table::Cell;
use serde_json::Value;

use crate::auth::context::CliContext;
use crate::cache::KvCache;
use crate::commands::sbx::{sandbox_proxy_base, with_sandbox_headers};
use crate::config::files::normalize_api_url;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PtySessionSummary {
    session_id: String,
    command: Option<String>,
    status: Option<String>,
    clients: Option<String>,
    created: Option<String>,
    token: Option<String>,
}

const PTY_TOKEN_CACHE_NAMESPACE: &str = "pty_tokens";

pub async fn list(
    ctx: &CliContext,
    sandbox_id: &str,
    show_token: bool,
    output_json: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
    let resp = with_sandbox_headers(
        client.get(format!("{proxy_base}/api/v1/pty")),
        sandbox_id,
        host_override,
    )
    .send()
    .await
    .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to list PTY sessions (HTTP {}): {}",
            status,
            body
        )));
    }

    let response: Value = resp.json().await.map_err(CliError::Http)?;
    let mut sessions = parse_session_listing(&response)?;

    hydrate_session_tokens(ctx, sandbox_id, &mut sessions).await;

    if sessions.is_empty() {
        if output_json {
            println!("[]");
            return Ok(());
        }
        println!("No PTY sessions for sandbox {}.", sandbox_id);
        return Ok(());
    }

    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&sessions_to_json_value(&sessions))?
        );
        return Ok(());
    }

    let mut table = new_table(&[
        "Session ID",
        "Command",
        "Status",
        "Clients",
        "Created",
        "Token",
    ]);
    for session in sessions {
        table.add_row(vec![
            Cell::new(session.session_id),
            Cell::new(display_value(session.command.as_deref())),
            Cell::new(display_value(session.status.as_deref())),
            Cell::new(display_value(session.clients.as_deref())),
            Cell::new(display_value(session.created.as_deref())),
            Cell::new(display_token(session.token.as_deref(), show_token)),
        ]);
    }

    println!("{table}");
    Ok(())
}

pub async fn attach(
    ctx: &CliContext,
    sandbox_id: &str,
    session_id: &str,
    token: &str,
) -> Result<()> {
    cache_pty_token(ctx, sandbox_id, session_id, token).await;
    super::ssh::attach_to_session(ctx, sandbox_id, session_id, token, "pty attach").await
}

pub async fn remove(ctx: &CliContext, sandbox_id: &str, session_ids: &[String]) -> Result<()> {
    let client = ctx.client()?;
    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);

    for session_id in session_ids {
        let resp = with_sandbox_headers(
            client.delete(format!("{proxy_base}/api/v1/pty/{session_id}")),
            sandbox_id,
            host_override.clone(),
        )
        .send()
        .await
        .map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to remove PTY session {} (HTTP {}): {}",
                session_id,
                status,
                body
            )));
        }

        delete_cached_pty_token(ctx, sandbox_id, session_id).await;
    }

    if session_ids.len() == 1 {
        println!(
            "Removed PTY session {} from sandbox {}.",
            session_ids[0], sandbox_id
        );
    } else {
        println!(
            "Removed {} PTY sessions from sandbox {}.",
            session_ids.len(),
            sandbox_id
        );
    }

    Ok(())
}

fn parse_session_listing(value: &Value) -> Result<Vec<PtySessionSummary>> {
    let entries = match value {
        Value::Array(items) => items.iter().collect::<Vec<_>>(),
        Value::Object(map) => {
            if let Some(items) = ["sessions", "items", "pty_sessions", "ptySessions"]
                .into_iter()
                .find_map(|key| map.get(key))
            {
                let Value::Array(items) = items else {
                    return Err(CliError::Other(anyhow::anyhow!(
                        "unexpected PTY session listing shape: expected an array"
                    )));
                };
                items.iter().collect::<Vec<_>>()
            } else if map.contains_key("session_id") || map.contains_key("sessionId") {
                vec![value]
            } else {
                return Err(CliError::Other(anyhow::anyhow!(
                    "unexpected PTY session listing response shape"
                )));
            }
        }
        _ => {
            return Err(CliError::Other(anyhow::anyhow!(
                "unexpected PTY session listing response type"
            )));
        }
    };

    entries.into_iter().map(parse_session_summary).collect()
}

fn parse_session_summary(value: &Value) -> Result<PtySessionSummary> {
    let Value::Object(map) = value else {
        return Err(CliError::Other(anyhow::anyhow!(
            "unexpected PTY session entry shape"
        )));
    };

    let session_id = string_field(map, &["session_id", "sessionId"]).ok_or_else(|| {
        CliError::Other(anyhow::anyhow!("PTY session entry is missing session_id"))
    })?;

    Ok(PtySessionSummary {
        session_id,
        command: command_field(map),
        status: string_field(map, &["status", "state"]),
        clients: string_field(
            map,
            &["client_count", "clientCount", "clients", "attached_clients"],
        ),
        created: string_field(map, &["created_at", "createdAt", "started_at", "startedAt"]),
        token: string_field(
            map,
            &[
                "token",
                "pty_token",
                "ptyToken",
                "attach_token",
                "attachToken",
            ],
        ),
    })
}

fn command_field(map: &serde_json::Map<String, Value>) -> Option<String> {
    let command = string_field(map, &["command", "cmd"]);
    let args = array_field(map, &["args", "argv"]);

    match (command, args) {
        (Some(command), Some(args)) if !args.is_empty() => Some(format!("{command} {args}")),
        (Some(command), _) => Some(command),
        (None, Some(args)) if !args.is_empty() => Some(args),
        _ => None,
    }
}

fn array_field(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    for key in keys {
        let Some(value) = map.get(*key) else {
            continue;
        };
        let Value::Array(items) = value else {
            continue;
        };
        let values: Vec<String> = items.iter().filter_map(value_to_string).collect();
        if !values.is_empty() {
            return Some(values.join(" "));
        }
    }
    None
}

fn string_field(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| map.get(*key))
        .and_then(value_to_string)
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Bool(boolean) => Some(boolean.to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => Some(value.to_string()),
    }
}

fn display_value(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}

fn display_token(token: Option<&str>, show_token: bool) -> String {
    match token {
        Some(token) if show_token => token.to_string(),
        Some(token) => mask_token(token),
        None => "-".to_string(),
    }
}

fn mask_token(token: &str) -> String {
    match token.len() {
        0 => String::new(),
        1..=4 => "****".to_string(),
        5..=8 => format!("{}...{}", &token[..2], &token[token.len() - 2..]),
        _ => format!("{}...{}", &token[..4], &token[token.len() - 4..]),
    }
}

fn sessions_to_json_value(sessions: &[PtySessionSummary]) -> Value {
    Value::Array(
        sessions
            .iter()
            .map(|session| {
                serde_json::json!({
                    "session_id": session.session_id.clone(),
                    "command": session.command.clone(),
                    "status": session.status.clone(),
                    "clients": session.clients.clone(),
                    "created": session.created.clone(),
                    "token": session.token.clone(),
                })
            })
            .collect(),
    )
}

async fn hydrate_session_tokens(
    ctx: &CliContext,
    sandbox_id: &str,
    sessions: &mut [PtySessionSummary],
) {
    for session in sessions.iter_mut() {
        if let Some(token) = session.token.as_deref() {
            cache_pty_token(ctx, sandbox_id, &session.session_id, token).await;
            continue;
        }

        session.token = load_cached_pty_token(ctx, sandbox_id, &session.session_id).await;
    }
}

pub(crate) async fn cache_pty_token(
    ctx: &CliContext,
    sandbox_id: &str,
    session_id: &str,
    token: &str,
) {
    let cache = KvCache::new(PTY_TOKEN_CACHE_NAMESPACE);
    cache
        .set(&pty_token_cache_key(ctx, sandbox_id, session_id), token)
        .await;
}

async fn load_cached_pty_token(
    ctx: &CliContext,
    sandbox_id: &str,
    session_id: &str,
) -> Option<String> {
    let cache = KvCache::new(PTY_TOKEN_CACHE_NAMESPACE);
    cache
        .get(&pty_token_cache_key(ctx, sandbox_id, session_id))
        .await
}

async fn delete_cached_pty_token(ctx: &CliContext, sandbox_id: &str, session_id: &str) {
    let cache = KvCache::new(PTY_TOKEN_CACHE_NAMESPACE);
    cache
        .delete(&pty_token_cache_key(ctx, sandbox_id, session_id))
        .await;
}

fn pty_token_cache_key(ctx: &CliContext, sandbox_id: &str, session_id: &str) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        normalize_api_url(&ctx.api_url),
        ctx.effective_organization_id().unwrap_or_default(),
        ctx.effective_project_id().unwrap_or_default(),
        sandbox_id,
        session_id
    )
}

#[cfg(test)]
mod tests {
    use super::{
        PtySessionSummary, display_token, mask_token, parse_session_listing, sessions_to_json_value,
    };

    #[test]
    fn parse_session_listing_supports_wrapped_sessions_array() {
        let sessions = parse_session_listing(&serde_json::json!({
            "sessions": [
                {
                    "session_id": "sess-1",
                    "command": "/bin/bash",
                    "args": ["-l"],
                    "status": "running",
                    "client_count": 2,
                    "created_at": "2026-05-25T00:00:00Z",
                    "token": "tok-12345678"
                }
            ]
        }))
        .unwrap();

        assert_eq!(
            sessions,
            vec![PtySessionSummary {
                session_id: "sess-1".to_string(),
                command: Some("/bin/bash -l".to_string()),
                status: Some("running".to_string()),
                clients: Some("2".to_string()),
                created: Some("2026-05-25T00:00:00Z".to_string()),
                token: Some("tok-12345678".to_string()),
            }]
        );
    }

    #[test]
    fn parse_session_listing_supports_direct_array() {
        let sessions = parse_session_listing(&serde_json::json!([
            {
                "sessionId": "sess-2",
                "argv": ["/bin/sh", "-c", "echo hi"],
                "state": "exited"
            }
        ]))
        .unwrap();

        assert_eq!(
            sessions,
            vec![PtySessionSummary {
                session_id: "sess-2".to_string(),
                command: Some("/bin/sh -c echo hi".to_string()),
                status: Some("exited".to_string()),
                clients: None,
                created: None,
                token: None,
            }]
        );
    }

    #[test]
    fn parse_session_listing_rejects_unknown_shape() {
        let error = parse_session_listing(&serde_json::json!({"unexpected": []})).unwrap_err();
        assert!(error.to_string().contains("unexpected PTY session listing"));
    }

    #[test]
    fn mask_token_masks_middle_of_long_tokens() {
        assert_eq!(mask_token("tok-12345678"), "tok-...5678");
        assert_eq!(mask_token("token12"), "to...12");
        assert_eq!(mask_token("abcd"), "****");
    }

    #[test]
    fn display_token_hides_or_reveals_tokens() {
        assert_eq!(display_token(Some("tok-12345678"), false), "tok-...5678");
        assert_eq!(display_token(Some("tok-12345678"), true), "tok-12345678");
        assert_eq!(display_token(None, false), "-");
    }

    #[test]
    fn sessions_to_json_value_preserves_full_token() {
        let value = sessions_to_json_value(&[PtySessionSummary {
            session_id: "sess-1".to_string(),
            command: Some("/bin/bash".to_string()),
            status: Some("running".to_string()),
            clients: Some("1".to_string()),
            created: Some("2026-05-25T00:00:00Z".to_string()),
            token: Some("tok-12345678".to_string()),
        }]);

        assert_eq!(
            value,
            serde_json::json!([{
                "session_id": "sess-1",
                "command": "/bin/bash",
                "status": "running",
                "clients": "1",
                "created": "2026-05-25T00:00:00Z",
                "token": "tok-12345678"
            }])
        );
    }
}
