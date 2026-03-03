use comfy_table::Cell;
use tensorlake_cloud_sdk::Sdk;
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
    let token = ctx.bearer_token()?;
    let sdk = Sdk::new(&ctx.api_url, &token)?;
    Ok(sdk.secrets())
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
