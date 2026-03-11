use comfy_table::Cell;
use tensorlake_cloud_sdk::{Client, ClientBuilder};
use tensorlake_cloud_sdk::error::SdkError;
use tensorlake_cloud_sdk::secrets::SecretsClient;
use tensorlake_cloud_sdk::secrets::models::{
    DeleteSecretRequest, ListSecretsRequest, NewSecret, UpsertSecret, UpsertSecretRequest,
};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub async fn list(ctx: &CliContext) -> Result<()> {
    let secrets = get_all_secrets(ctx).await?;
    if secrets.is_empty() {
        println!("no secrets found");
        return Ok(());
    }

    let mut table = new_table(&["Name", "Created At"]);
    for secret in &secrets {
        let name = secret.get("name").and_then(|v| v.as_str()).unwrap_or("-");
        let created_at = secret
            .get("createdAt")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        table.add_row(vec![Cell::new(name), Cell::new(created_at)]);
    }
    println!("{table}");

    let count = secrets.len();
    if count == 1 {
        println!("1 secret");
    } else {
        println!("{} secrets", count);
    }
    Ok(())
}

pub async fn set(ctx: &CliContext, pairs: &[String]) -> Result<()> {
    let mut upsert_secrets: Vec<NewSecret> = Vec::new();
    let mut seen_names = std::collections::HashSet::new();

    for pair in pairs {
        let eq_pos = pair.find('=').ok_or_else(|| {
            CliError::usage(format!("invalid secret format {}, missing '='", pair))
        })?;
        let name = &pair[..eq_pos];
        let value = &pair[eq_pos + 1..];

        if name.is_empty() {
            return Err(CliError::usage(format!(
                "invalid secret format {}, missing name",
                pair
            )));
        }
        if name.contains(' ') {
            return Err(CliError::usage(format!(
                "invalid secret name {}, spaces are not allowed",
                name
            )));
        }
        if !seen_names.insert(name.to_string()) {
            return Err(CliError::usage(format!("duplicate secret name: {}", name)));
        }

        upsert_secrets.push(NewSecret {
            name: name.to_string(),
            value: value.to_string(),
        });
    }

    let (organization_id, project_id) = org_and_project(ctx)?;
    let request = UpsertSecretRequest::builder()
        .organization_id(organization_id)
        .project_id(project_id)
        .secrets(UpsertSecret::Multiple(upsert_secrets.clone()))
        .build()
        .map_err(|e| CliError::usage(e.to_string()))?;

    secrets_client(ctx)?
        .upsert(request)
        .await
        .map_err(map_set_sdk_error)?;

    let count = upsert_secrets.len();
    if count == 1 {
        println!("1 secret set");
    } else {
        println!("{} secrets set", count);
    }
    Ok(())
}

pub async fn unset(ctx: &CliContext, names: &[String]) -> Result<()> {
    let secrets = get_all_secrets(ctx).await?;
    let secrets_map: std::collections::HashMap<&str, &serde_json::Value> = secrets
        .iter()
        .filter_map(|s| s.get("name").and_then(|n| n.as_str()).map(|name| (name, s)))
        .collect();

    let (organization_id, project_id) = org_and_project(ctx)?;
    let client = secrets_client(ctx)?;
    let mut num = 0;

    for name in names {
        if let Some(secret) = secrets_map.get(name.as_str())
            && let Some(id) = secret.get("id").and_then(|v| v.as_str())
        {
            let request = DeleteSecretRequest::builder()
                .organization_id(organization_id.clone())
                .project_id(project_id.clone())
                .secret_id(id)
                .build()
                .map_err(|e| CliError::usage(e.to_string()))?;
            client.delete(&request).await.map_err(map_unset_sdk_error)?;
            num += 1;
        }
    }

    if num == 1 {
        println!("1 secret unset");
    } else {
        println!("{} secrets unset", num);
    }
    Ok(())
}

async fn get_all_secrets(ctx: &CliContext) -> Result<Vec<serde_json::Value>> {
    let (organization_id, project_id) = org_and_project(ctx)?;
    let request = ListSecretsRequest::builder()
        .organization_id(organization_id)
        .project_id(project_id)
        .page_size(100)
        .build()
        .map_err(|e| CliError::usage(e.to_string()))?;
    let resp = secrets_client(ctx)?
        .list(&request)
        .await
        .map_err(map_list_sdk_error)?;

    Ok(resp
        .items
        .iter()
        .map(|item| {
            serde_json::json!({
                "id": item.id,
                "name": item.name,
                "createdAt": item.created_at,
            })
        })
        .collect())
}

fn org_and_project(ctx: &CliContext) -> Result<(String, String)> {
    let organization_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("missing organization ID; run `tl init`"))?;
    let project_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("missing project ID; run `tl init`"))?;
    Ok((organization_id, project_id))
}

fn secrets_client(ctx: &CliContext) -> Result<SecretsClient> {
    Ok(SecretsClient::new(secrets_http_client(ctx)?))
}

fn secrets_http_client(ctx: &CliContext) -> Result<Client> {
    let token = ctx.bearer_token()?;
    let mut builder = ClientBuilder::new(&ctx.api_url).bearer_token(&token);
    let use_scope_headers = ctx.personal_access_token.is_some() && ctx.api_key.is_none();

    if use_scope_headers
        && let (Some(organization_id), Some(project_id)) = (
        ctx.effective_organization_id(),
        ctx.effective_project_id(),
    ) {
        builder = builder.scope(&organization_id, &project_id);
    }

    builder.build().map_err(Into::into)
}

fn parse_remote_message(raw: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|body| {
            body.get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| raw.to_string())
}

fn map_list_sdk_error(error: SdkError) -> CliError {
    match error {
        SdkError::Authentication(_) => {
            CliError::auth("authentication failed. set TENSORLAKE_API_KEY or run 'tl login'.")
        }
        SdkError::Authorization(_) => CliError::auth(
            "permission denied. set TENSORLAKE_API_KEY with required permissions, or run 'tl init'.",
        ),
        SdkError::ServerError { status, .. } => {
            CliError::auth(format!("failed to fetch secrets (HTTP {})", status))
        }
        other => CliError::from(other),
    }
}

fn map_set_sdk_error(error: SdkError) -> CliError {
    match error {
        SdkError::Authentication(_) => {
            CliError::auth("authentication failed. set TENSORLAKE_API_KEY or run 'tl login'.")
        }
        SdkError::Authorization(_) => CliError::auth(
            "permission denied. set TENSORLAKE_API_KEY with required permissions, or run 'tl init'.",
        ),
        SdkError::ServerError { status, message } => {
            let code = status.as_u16();
            if (400..500).contains(&code) {
                CliError::usage(format!(
                    "could not set secrets: {}",
                    parse_remote_message(&message)
                ))
            } else {
                CliError::Other(anyhow::anyhow!(
                    "server error ({}) while setting secrets",
                    code
                ))
            }
        }
        other => CliError::from(other),
    }
}

fn map_unset_sdk_error(error: SdkError) -> CliError {
    match error {
        SdkError::Authentication(_) => {
            CliError::auth("authentication failed. set TENSORLAKE_API_KEY or run 'tl login'.")
        }
        SdkError::Authorization(_) => CliError::auth(
            "permission denied. set TENSORLAKE_API_KEY with required permissions, or run 'tl init'.",
        ),
        other => CliError::from(other),
    }
}

#[cfg(test)]
mod tests {
    use super::secrets_http_client;
    use crate::auth::context::CliContext;
    use crate::config::resolver::ResolvedConfig;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    fn test_ctx(
        api_url: &str,
        api_key: Option<&str>,
        personal_access_token: Option<&str>,
        organization_id: Option<&str>,
        project_id: Option<&str>,
    ) -> CliContext {
        CliContext::from_resolved(ResolvedConfig {
            api_url: api_url.to_string(),
            cloud_url: "https://cloud.tensorlake.ai".to_string(),
            namespace: "default".to_string(),
            api_key: api_key.map(str::to_string),
            personal_access_token: personal_access_token.map(str::to_string),
            organization_id: organization_id.map(str::to_string),
            project_id: project_id.map(str::to_string),
            debug: false,
        })
    }

    fn execute_and_capture_request(ctx: CliContext) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let bytes_read = stream.read(&mut buf).unwrap();
            tx.send(String::from_utf8_lossy(&buf[..bytes_read]).to_string())
                .unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
        });

        let request_ctx = test_ctx(
            &format!("http://{}", address),
            ctx.api_key.as_deref(),
            ctx.personal_access_token.as_deref(),
            ctx.organization_id.as_deref(),
            ctx.project_id.as_deref(),
        );

        let client = secrets_http_client(&request_ctx).unwrap();
        let request = client
            .build_get_json_request("/platform/v1/test", None)
            .unwrap();

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { client.execute_raw(request).await.unwrap() });

        let raw_request = rx.recv().unwrap().to_lowercase();
        server.join().unwrap();
        raw_request
    }

    #[test]
    fn secrets_client_includes_scope_headers_for_pat() {
        let raw_request = execute_and_capture_request(test_ctx(
            "http://unused",
            None,
            Some("fake_test_pat_for_header_assertions_only"),
            Some("org-123"),
            Some("proj-456"),
        ));

        assert!(
            raw_request.contains(
                "authorization: bearer fake_test_pat_for_header_assertions_only"
            )
        );
        assert!(raw_request.contains("x-forwarded-organization-id: org-123"));
        assert!(raw_request.contains("x-forwarded-project-id: proj-456"));
    }

    #[test]
    fn secrets_client_does_not_include_scope_headers_for_api_key() {
        let raw_request = execute_and_capture_request(test_ctx(
            "http://unused",
            Some("fake_test_api_key_for_header_assertions_only"),
            None,
            Some("org-123"),
            Some("proj-456"),
        ));

        assert!(
            raw_request.contains(
                "authorization: bearer fake_test_api_key_for_header_assertions_only"
            )
        );
        assert!(!raw_request.contains("x-forwarded-organization-id:"));
        assert!(!raw_request.contains("x-forwarded-project-id:"));
    }
}
