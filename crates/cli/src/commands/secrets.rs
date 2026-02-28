use comfy_table::Cell;

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
    let mut upsert_secrets: Vec<serde_json::Value> = Vec::new();
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

        upsert_secrets.push(serde_json::json!({"name": name, "value": value}));
    }

    let client = ctx.client()?;
    let resp = client
        .put(format!(
            "{}/platform/v1/organizations/{}/projects/{}/secrets",
            ctx.api_url,
            ctx.effective_organization_id().unwrap_or_default(),
            ctx.effective_project_id().unwrap_or_default()
        ))
        .json(&upsert_secrets)
        .send()
        .await
        .map_err(CliError::Http)?;

    let status = resp.status().as_u16();
    if status == 401 {
        return Err(CliError::auth(
            "authentication failed. set TENSORLAKE_API_KEY or run 'tensorlake login'.",
        ));
    }
    if status == 403 {
        return Err(CliError::auth(
            "permission denied. set TENSORLAKE_API_KEY with required permissions, or run 'tensorlake init'.",
        ));
    }
    if (400..500).contains(&status) {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        return Err(CliError::usage(format!("could not set secrets: {}", msg)));
    }
    if !resp.status().is_success() {
        return Err(CliError::Other(anyhow::anyhow!(
            "server error ({}) while setting secrets",
            status
        )));
    }

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

    let client = ctx.client()?;
    let mut num = 0;

    for name in names {
        if let Some(secret) = secrets_map.get(name.as_str())
            && let Some(id) = secret.get("id").and_then(|v| v.as_str())
        {
            let resp = client
                .delete(format!(
                    "{}/platform/v1/organizations/{}/projects/{}/secrets/{}",
                    ctx.api_url,
                    ctx.effective_organization_id().unwrap_or_default(),
                    ctx.effective_project_id().unwrap_or_default(),
                    id
                ))
                .send()
                .await
                .map_err(CliError::Http)?;
            if resp.status().is_success() {
                num += 1;
            }
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
    let client = ctx.client()?;
    let resp = client
        .get(format!(
            "{}/platform/v1/organizations/{}/projects/{}/secrets?pageSize=100",
            ctx.api_url,
            ctx.effective_organization_id().unwrap_or_default(),
            ctx.effective_project_id().unwrap_or_default()
        ))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        return Err(CliError::auth(format!(
            "failed to fetch secrets (HTTP {})",
            status
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    Ok(body
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default())
}
