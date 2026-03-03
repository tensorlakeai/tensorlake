use crate::auth::context::CliContext;
use crate::error::Result;

pub async fn run(ctx: &mut CliContext, output_json: bool) -> Result<()> {
    if !ctx.has_authentication() {
        if output_json {
            println!(
                "{}",
                serde_json::json!({
                    "authenticated": false,
                    "message": "Not logged in and no API key provided"
                })
            );
        } else {
            eprintln!("You are not logged in and have not provided an API key.");
            eprintln!("Run 'tl login' to authenticate, or see 'tl --help' for API key options.");
        }
        return Ok(());
    }

    // If using API key, introspect to get key ID and org/project
    if ctx.api_key.is_some() {
        ctx.introspect().await?;
    }

    let mut data = serde_json::Map::new();
    data.insert(
        "endpoint".to_string(),
        serde_json::Value::String(ctx.api_url.clone()),
    );
    if let Some(org_id) = ctx.effective_organization_id() {
        data.insert(
            "organizationId".to_string(),
            serde_json::Value::String(org_id),
        );
    }
    if let Some(proj_id) = ctx.effective_project_id() {
        data.insert("projectId".to_string(), serde_json::Value::String(proj_id));
    }
    if let Some(key_id) = ctx.api_key_id() {
        data.insert("apiKeyId".to_string(), serde_json::Value::String(key_id));
    }
    if let Some(pat) = &ctx.personal_access_token {
        let masked = if pat.len() > 6 {
            format!("{}{}", "*".repeat(pat.len() - 6), &pat[pat.len() - 6..])
        } else {
            "*".repeat(pat.len())
        };
        data.insert(
            "personalAccessToken".to_string(),
            serde_json::Value::String(masked),
        );
    }

    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Object(data))?
        );
        return Ok(());
    }

    println!("Dashboard Endpoint    : {}", ctx.cloud_url);
    println!(
        "API Endpoint          : {}",
        data.get("endpoint").and_then(|v| v.as_str()).unwrap_or("-")
    );
    println!(
        "Organization ID       : {}",
        data.get("organizationId")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );
    println!(
        "Project ID            : {}",
        data.get("projectId")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );
    if let Some(key_id) = data.get("apiKeyId").and_then(|v| v.as_str()) {
        println!("API Key ID            : {}", key_id);
    }
    if let Some(pat) = data.get("personalAccessToken").and_then(|v| v.as_str()) {
        println!("Personal Access Token : {}", pat);
    }

    Ok(())
}
