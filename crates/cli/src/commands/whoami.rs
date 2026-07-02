use crate::auth::context::CliContext;
use crate::commands::sbx::resolve_sandbox_lifecycle_url;
use crate::error::Result;

#[derive(Debug, Clone)]
struct ProjectScopeInfo {
    organization_name: Option<String>,
    project_name: Option<String>,
}

fn mask_credential(value: &str) -> String {
    let visible = 20.min(value.len());
    format!("{}****", &value[..visible])
}

fn display_scope_value(id: &str, name: Option<&str>) -> String {
    match name.map(str::trim).filter(|name| !name.is_empty()) {
        Some(name) => format!("{name} ({id})"),
        None => id.to_string(),
    }
}

async fn fetch_project_scope_info(
    ctx: &CliContext,
    organization_id: Option<&str>,
    project_id: Option<&str>,
) -> Option<ProjectScopeInfo> {
    let organization_id = organization_id?;
    let project_id = project_id?;
    let client = ctx.client().ok()?;
    let url = format!(
        "{}/platform/v1/organizations/{}/projects/{}",
        ctx.api_url,
        urlencoding::encode(organization_id),
        urlencoding::encode(project_id),
    );

    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let body: serde_json::Value = resp.json().await.ok()?;
    Some(ProjectScopeInfo {
        organization_name: body
            .get("organizationName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        project_name: body
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
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

    let api_key_scope_info = if has_api_key {
        fetch_project_scope_info(
            ctx,
            ctx.introspect_org_id().as_deref(),
            ctx.introspect_project_id().as_deref(),
        )
        .await
    } else {
        None
    };
    let pat_scope_info = if has_pat && !has_api_key {
        fetch_project_scope_info(ctx, pat_org.as_deref(), pat_project.as_deref()).await
    } else {
        None
    };

    if output_json {
        let mut root = serde_json::Map::new();

        root.insert(
            "endpoints".to_string(),
            serde_json::json!({
                "dashboard": ctx.cloud_url,
                "cloudApi": ctx.api_url,
                "sandboxApi": resolve_sandbox_lifecycle_url(&ctx.api_url),
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
                obj.insert("organizationId".to_string(), serde_json::Value::String(org));
            }
            if let Some(name) = api_key_scope_info
                .as_ref()
                .and_then(|scope| scope.organization_name.as_ref())
            {
                obj.insert(
                    "organizationName".to_string(),
                    serde_json::Value::String(name.clone()),
                );
            }
            if let Some(proj) = ctx.introspect_project_id() {
                obj.insert("projectId".to_string(), serde_json::Value::String(proj));
            }
            if let Some(name) = api_key_scope_info
                .as_ref()
                .and_then(|scope| scope.project_name.as_ref())
            {
                obj.insert(
                    "projectName".to_string(),
                    serde_json::Value::String(name.clone()),
                );
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
            if let Some(org) = &pat_org {
                obj.insert(
                    "organizationId".to_string(),
                    serde_json::Value::String(org.clone()),
                );
            }
            if let Some(name) = pat_scope_info
                .as_ref()
                .and_then(|scope| scope.organization_name.as_ref())
            {
                obj.insert(
                    "organizationName".to_string(),
                    serde_json::Value::String(name.clone()),
                );
            }
            if let Some(proj) = &pat_project {
                obj.insert(
                    "projectId".to_string(),
                    serde_json::Value::String(proj.clone()),
                );
            }
            if let Some(name) = pat_scope_info
                .as_ref()
                .and_then(|scope| scope.project_name.as_ref())
            {
                obj.insert(
                    "projectName".to_string(),
                    serde_json::Value::String(name.clone()),
                );
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
    println!("  Dashboard   : {}", ctx.cloud_url);
    println!("  Cloud API   : {}", ctx.api_url);
    println!(
        "  Sandbox API : {}",
        resolve_sandbox_lifecycle_url(&ctx.api_url)
    );

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
                println!(
                    "    Organization : {}",
                    display_scope_value(
                        &org,
                        api_key_scope_info
                            .as_ref()
                            .and_then(|scope| scope.organization_name.as_deref())
                    )
                );
            }
            if let Some(proj) = ctx.introspect_project_id() {
                println!(
                    "    Project      : {}",
                    display_scope_value(
                        &proj,
                        api_key_scope_info
                            .as_ref()
                            .and_then(|scope| scope.project_name.as_deref())
                    )
                );
            }
        }

        if has_pat {
            println!();
            println!("  Personal Access Token");
            if let Some(pat) = &ctx.personal_access_token {
                println!("    Token        : {}", mask_credential(pat));
            }
            if let Some(org) = &pat_org {
                println!(
                    "    Organization : {}",
                    display_scope_value(
                        org,
                        pat_scope_info
                            .as_ref()
                            .and_then(|scope| scope.organization_name.as_deref())
                    )
                );
            }
            if let Some(proj) = &pat_project {
                println!(
                    "    Project      : {}",
                    display_scope_value(
                        proj,
                        pat_scope_info
                            .as_ref()
                            .and_then(|scope| scope.project_name.as_deref())
                    )
                );
            }
        }
    }

    Ok(())
}
