use crate::auth::context::CliContext;
use crate::error::Result;

fn mask_credential(value: &str) -> String {
    let visible = 20.min(value.len());
    format!("{}****", &value[..visible])
}

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

    // Capture PAT org/project from config before introspection may populate shared fields.
    let pat_org = ctx.organization_id.clone();
    let pat_project = ctx.project_id.clone();

    if ctx.api_key.is_some() {
        ctx.introspect().await?;
    }

    let has_api_key = ctx.api_key.is_some();
    let has_pat = ctx.personal_access_token.is_some();

    if output_json {
        let mut root = serde_json::Map::new();

        root.insert(
            "endpoints".to_string(),
            serde_json::json!({
                "dashboard": ctx.cloud_url,
                "api": ctx.api_url,
            }),
        );

        if has_api_key {
            let mut obj = serde_json::Map::new();
            if let Some(key) = &ctx.api_key {
                obj.insert(
                    "key".to_string(),
                    serde_json::Value::String(mask_credential(key)),
                );
            }
            if let Some(id) = ctx.api_key_id() {
                obj.insert("keyId".to_string(), serde_json::Value::String(id));
            }
            if let Some(org) = ctx.introspect_org_id() {
                obj.insert(
                    "organizationId".to_string(),
                    serde_json::Value::String(org),
                );
            }
            if let Some(proj) = ctx.introspect_project_id() {
                obj.insert("projectId".to_string(), serde_json::Value::String(proj));
            }
            root.insert("apiKey".to_string(), serde_json::Value::Object(obj));
        }

        if has_pat {
            let mut obj = serde_json::Map::new();
            if let Some(pat) = &ctx.personal_access_token {
                obj.insert(
                    "token".to_string(),
                    serde_json::Value::String(mask_credential(pat)),
                );
            }
            if let Some(org) = pat_org {
                obj.insert(
                    "organizationId".to_string(),
                    serde_json::Value::String(org),
                );
            }
            if let Some(proj) = pat_project {
                obj.insert("projectId".to_string(), serde_json::Value::String(proj));
            }
            root.insert(
                "personalAccessToken".to_string(),
                serde_json::Value::Object(obj),
            );
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Object(root))?
        );
        return Ok(());
    }

    println!("Endpoints");
    println!("  Dashboard : {}", ctx.cloud_url);
    println!("  API       : {}", ctx.api_url);

    if has_api_key || has_pat {
        println!();
        println!("Credentials");

        if has_api_key && has_pat {
            println!("  Note: API keys take precedence when both are set.");
        }

        if has_api_key {
            println!();
            println!("  API Key");
            if let Some(key) = &ctx.api_key {
                println!("    Key          : {}", mask_credential(key));
            }
            if let Some(key_id) = ctx.api_key_id() {
                println!("    Key ID       : {}", key_id);
            }
            if let Some(org) = ctx.introspect_org_id() {
                println!("    Organization : {}", org);
            }
            if let Some(proj) = ctx.introspect_project_id() {
                println!("    Project      : {}", proj);
            }
        }

        if has_pat {
            println!();
            println!("  Personal Access Token");
            if let Some(pat) = &ctx.personal_access_token {
                println!("    Token        : {}", mask_credential(pat));
            }
            if let Some(org) = pat_org {
                println!("    Organization : {}", org);
            }
            if let Some(proj) = pat_project {
                println!("    Project      : {}", proj);
            }
        }
    }

    Ok(())
}
