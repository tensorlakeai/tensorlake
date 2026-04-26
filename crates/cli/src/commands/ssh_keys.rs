use comfy_table::Cell;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use tensorlake::error::SdkError;
use tensorlake::{Client, ClientBuilder};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

const BASE_PATH: &str = "/platform/v1/users/me/ssh-keys";

#[derive(Debug, Clone, Deserialize)]
struct ApiSshKey {
    id: String,
    name: String,
    #[serde(rename = "keyType")]
    key_type: String,
    fingerprint: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "lastUsedAt")]
    last_used_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ListResponse {
    items: Vec<ApiSshKey>,
}

#[derive(Debug, Clone, Serialize)]
struct CreateRequest<'a> {
    name: &'a str,
    #[serde(rename = "publicKey")]
    public_key: &'a str,
}

pub async fn list(ctx: &CliContext) -> Result<()> {
    let client = http_client(ctx)?;
    let request = client
        .build_get_json_request(BASE_PATH, None)
        .map_err(map_sdk_error)?;
    let traced = client
        .execute_json::<ListResponse>(request)
        .await
        .map_err(map_sdk_error)?;
    let keys = traced.into_inner().items;

    if keys.is_empty() {
        println!("no ssh keys registered");
        return Ok(());
    }

    let mut table = new_table(&["ID", "Name", "Type", "Fingerprint", "Created", "Last Used"]);
    for key in &keys {
        table.add_row(vec![
            Cell::new(&key.id),
            Cell::new(&key.name),
            Cell::new(&key.key_type),
            Cell::new(&key.fingerprint),
            Cell::new(&key.created_at),
            Cell::new(key.last_used_at.as_deref().unwrap_or("-")),
        ]);
    }
    println!("{table}");

    let count = keys.len();
    if count == 1 {
        println!("1 ssh key");
    } else {
        println!("{count} ssh keys");
    }
    Ok(())
}

/// Add a key. `public_key_arg` is interpreted as a file path first; if the
/// path doesn't exist, the argument is treated as the literal key body. This
/// lets `tl ssh-keys add ~/.ssh/id_ed25519.pub` and
/// `tl ssh-keys add "ssh-ed25519 AAAA..."` both work.
pub async fn add(ctx: &CliContext, name: &str, public_key_arg: &str) -> Result<()> {
    let public_key = read_public_key(public_key_arg)?;
    let trimmed_name = name.trim();
    if trimmed_name.is_empty() {
        return Err(CliError::usage("name must not be empty"));
    }

    let client = http_client(ctx)?;
    let body = CreateRequest {
        name: trimmed_name,
        public_key: public_key.trim(),
    };
    let request = client
        .build_post_json_request(Method::POST, BASE_PATH, &body)
        .map_err(map_sdk_error)?;
    let traced = client
        .execute_json::<ApiSshKey>(request)
        .await
        .map_err(map_create_sdk_error)?;
    let created = traced.into_inner();
    println!("registered {}", created.id);
    println!("  name        {}", created.name);
    println!("  type        {}", created.key_type);
    println!("  fingerprint {}", created.fingerprint);
    Ok(())
}

pub async fn remove(ctx: &CliContext, key_ids: &[String]) -> Result<()> {
    if key_ids.is_empty() {
        return Err(CliError::usage("at least one key id is required"));
    }
    let client = http_client(ctx)?;

    // Resolve "name or id" inputs to ids. We list once then look each up.
    let request = client
        .build_get_json_request(BASE_PATH, None)
        .map_err(map_sdk_error)?;
    let listing = client
        .execute_json::<ListResponse>(request)
        .await
        .map_err(map_sdk_error)?
        .into_inner()
        .items;

    let mut removed = 0_usize;
    for raw in key_ids {
        let resolved = resolve_key_id(raw, &listing).ok_or_else(|| {
            CliError::usage(format!("no ssh key matches \"{raw}\""))
        })?;
        let path = format!("{BASE_PATH}/{resolved}");
        let request = client
            .request(Method::DELETE, &path)
            .build()
            .map_err(SdkError::from)
            .map_err(map_sdk_error)?;
        client.execute(request).await.map_err(map_sdk_error)?;
        removed += 1;
    }
    if removed == 1 {
        println!("removed 1 ssh key");
    } else {
        println!("removed {removed} ssh keys");
    }
    Ok(())
}

fn resolve_key_id(input: &str, listing: &[ApiSshKey]) -> Option<String> {
    // 1) Exact id match (the `ssh_key_<nanoid>` form returned by the API).
    if listing.iter().any(|k| k.id == input) {
        return Some(input.to_string());
    }
    // 2) Exact name match. Fingerprint match is intentionally not supported
    //    here — it would conflate "is this fingerprint registered?" with
    //    "delete the key with this fingerprint" and risks accidental deletes
    //    if the same key has been re-registered after a rotation.
    let matches: Vec<&ApiSshKey> = listing.iter().filter(|k| k.name == input).collect();
    match matches.as_slice() {
        [single] => Some(single.id.clone()),
        _ => None, // 0 or >1 — ambiguous
    }
}

fn read_public_key(arg: &str) -> Result<String> {
    let path = std::path::Path::new(arg);
    if path.is_file() {
        std::fs::read_to_string(path).map_err(|e| {
            CliError::usage(format!("failed to read {}: {e}", path.display()))
        })
    } else {
        Ok(arg.to_string())
    }
}

fn http_client(ctx: &CliContext) -> Result<Client> {
    let token = ctx.bearer_token()?;
    let mut builder = ClientBuilder::new(&ctx.api_url).bearer_token(&token);
    let use_scope_headers = ctx.personal_access_token.is_some() && ctx.api_key.is_none();
    if use_scope_headers
        && let (Some(org), Some(proj)) =
            (ctx.effective_organization_id(), ctx.effective_project_id())
    {
        builder = builder.scope(&org, &proj);
    }
    builder.build().map_err(Into::into)
}

fn map_sdk_error(error: SdkError) -> CliError {
    match error {
        SdkError::Authentication(_) => {
            CliError::auth("authentication failed; run 'tl login'")
        }
        SdkError::Authorization(_) => CliError::auth(
            "permission denied; run 'tl login' or check your account permissions",
        ),
        SdkError::ServerError { status, message } => {
            let code = status.as_u16();
            if (400..500).contains(&code) {
                CliError::usage(format!(
                    "ssh-keys request rejected ({code}): {}",
                    parse_remote_message(&message)
                ))
            } else {
                CliError::Other(anyhow::anyhow!(
                    "platform server error ({code}) on ssh-keys request"
                ))
            }
        }
        other => CliError::from(other),
    }
}

fn map_create_sdk_error(error: SdkError) -> CliError {
    if let SdkError::ServerError { status, message } = &error
        && status.as_u16() == 409
    {
        return CliError::usage(format!(
            "this ssh key is already registered: {}",
            parse_remote_message(message)
        ));
    }
    map_sdk_error(error)
}

fn parse_remote_message(raw: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|body| {
            body.get("message")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .unwrap_or_else(|| raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(id: &str, name: &str) -> ApiSshKey {
        ApiSshKey {
            id: id.into(),
            name: name.into(),
            key_type: "ssh-ed25519".into(),
            fingerprint: "SHA256:fake".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            last_used_at: None,
        }
    }

    #[test]
    fn resolve_id_matches_exact_id_first() {
        let listing = vec![mk("ssh_key_aaa", "laptop"), mk("ssh_key_bbb", "ssh_key_aaa")];
        assert_eq!(resolve_key_id("ssh_key_aaa", &listing).as_deref(), Some("ssh_key_aaa"));
    }

    #[test]
    fn resolve_id_matches_unique_name() {
        let listing = vec![mk("ssh_key_aaa", "laptop"), mk("ssh_key_bbb", "desktop")];
        assert_eq!(resolve_key_id("desktop", &listing).as_deref(), Some("ssh_key_bbb"));
    }

    #[test]
    fn resolve_id_rejects_ambiguous_name() {
        let listing = vec![mk("ssh_key_aaa", "laptop"), mk("ssh_key_bbb", "laptop")];
        assert_eq!(resolve_key_id("laptop", &listing), None);
    }

    #[test]
    fn resolve_id_rejects_unknown() {
        let listing = vec![mk("ssh_key_aaa", "laptop")];
        assert_eq!(resolve_key_id("nope", &listing), None);
    }

    #[test]
    fn read_public_key_returns_literal_when_not_a_path() {
        let literal = "ssh-ed25519 AAAA test@host";
        assert_eq!(read_public_key(literal).unwrap(), literal);
    }

    #[test]
    fn read_public_key_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("id_ed25519.pub");
        std::fs::write(&path, "ssh-ed25519 AAAA file@host\n").unwrap();
        let body = read_public_key(path.to_str().unwrap()).unwrap();
        assert!(body.starts_with("ssh-ed25519 AAAA"));
    }
}
