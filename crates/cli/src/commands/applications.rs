use chrono::{TimeZone, Utc};
use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

const APPLICATION_DETAILS_LABEL_WIDTH: usize = 20;

pub async fn ls(ctx: &CliContext) -> Result<()> {
    let client = ctx.client()?;
    let resp = client
        .get(format!(
            "{}/v1/namespaces/{}/applications",
            ctx.api_url, ctx.namespace
        ))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        return Err(CliError::Other(anyhow::anyhow!(
            "Failed to fetch applications: HTTP {}",
            resp.status()
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let applications = body
        .get("applications")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Filter out tombstoned applications
    let active: Vec<_> = applications
        .iter()
        .filter(|app| {
            !app.get("tombstoned")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        })
        .collect();

    if active.is_empty() {
        println!("No applications found");
        return Ok(());
    }

    let mut table = new_table(&["Name", "Description", "Public Endpoint ID", "Deployed At"]);
    for app in &active {
        let name = app.get("name").and_then(|v| v.as_str()).unwrap_or("-");
        let description = app
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let deployed_at = format_deployed_at(app.get("created_at"));

        table.add_row(vec![
            Cell::new(name),
            Cell::new(description),
            Cell::new(public_endpoint_id(app)),
            Cell::new(&deployed_at),
        ]);
    }

    println!("{table}");
    let count = active.len();
    if count == 1 {
        println!("1 application");
    } else {
        println!("{} applications", count);
    }

    // Show link to applications page
    if let (Some(org_id), Some(proj_id)) =
        (ctx.effective_organization_id(), ctx.effective_project_id())
    {
        println!(
            "\nView all applications: {}/organizations/{}/projects/{}/applications",
            ctx.cloud_url, org_id, proj_id
        );
    }

    Ok(())
}

pub async fn describe(ctx: &CliContext, application: &str) -> Result<()> {
    let client = ctx.client()?;
    let resp = client
        .get(format!(
            "{}/v1/namespaces/{}/applications/{}",
            ctx.api_url,
            ctx.namespace,
            urlencoding::encode(application)
        ))
        .send()
        .await
        .map_err(CliError::Http)?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(CliError::Other(anyhow::anyhow!(
            "application '{}' not found",
            application
        )));
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to fetch application '{}' (HTTP {}): {}",
            application,
            status,
            body
        )));
    }

    let item = resp
        .json::<serde_json::Value>()
        .await
        .map_err(CliError::Http)?;
    print!("{}", format_application_details(&item, &ctx.namespace));
    Ok(())
}

fn format_application_details(item: &serde_json::Value, namespace: &str) -> String {
    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("-");
    let description = item
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let version = item.get("version").and_then(|v| v.as_str()).unwrap_or("-");
    let state = format_application_state(item.get("state"));
    let entrypoint = item.get("entrypoint");
    let entrypoint_function = entrypoint
        .and_then(|v| v.get("function_name").or_else(|| v.get("functionName")))
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let input_serializer = entrypoint
        .and_then(|v| {
            v.get("input_serializer")
                .or_else(|| v.get("inputSerializer"))
        })
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let output_serializer = entrypoint
        .and_then(|v| {
            v.get("output_serializer")
                .or_else(|| v.get("outputSerializer"))
        })
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let functions = format_object_keys(item.get("functions"));
    let allow = format_string_array(item.get("allow"));
    let tags = format_tags(item.get("tags"));
    let deployed_at = format_deployed_at(item.get("created_at"));
    let tombstoned = item
        .get("tombstoned")
        .and_then(|v| v.as_bool())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());

    let mut output = String::new();
    push_detail(&mut output, "Name", name);
    push_detail(&mut output, "Namespace", namespace);
    push_detail(&mut output, "Description", description);
    push_detail(&mut output, "Version", version);
    push_detail(&mut output, "State", &state);
    push_detail(&mut output, "Public endpoint ID", public_endpoint_id(item));
    push_detail(&mut output, "Entrypoint", entrypoint_function);
    push_detail(&mut output, "Input serializer", input_serializer);
    push_detail(&mut output, "Output serializer", output_serializer);
    push_detail(&mut output, "Functions", &functions);
    push_detail(&mut output, "Allow", &allow);
    push_detail(&mut output, "Tags", &tags);
    push_detail(&mut output, "Deployed at", &deployed_at);
    push_detail(&mut output, "Tombstoned", &tombstoned);
    output
}

fn push_detail(output: &mut String, label: &str, value: &str) {
    output.push_str(&format!(
        "{:<APPLICATION_DETAILS_LABEL_WIDTH$}{}\n",
        format!("{label}:"),
        value
    ));
}

fn public_endpoint_id(item: &serde_json::Value) -> &str {
    item.get("public_endpoint_id")
        .or_else(|| item.get("publicEndpointId"))
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
}

fn format_deployed_at(created_at: Option<&serde_json::Value>) -> String {
    created_at
        .and_then(|v| v.as_i64())
        .and_then(|ts| Utc.timestamp_millis_opt(ts).single())
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn format_object_keys(value: Option<&serde_json::Value>) -> String {
    let mut keys = value
        .and_then(|v| v.as_object())
        .map(|object| object.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    keys.sort();
    if keys.is_empty() {
        "-".to_string()
    } else {
        keys.join(", ")
    }
}

fn format_string_array(value: Option<&serde_json::Value>) -> String {
    let values = value
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

fn format_tags(value: Option<&serde_json::Value>) -> String {
    let mut tags = value
        .and_then(|v| v.as_object())
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| {
                    let value = value
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| value.to_string());
                    format!("{key}={value}")
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    tags.sort();
    if tags.is_empty() {
        "-".to_string()
    } else {
        tags.join(", ")
    }
}

fn format_application_state(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(state)) => state.clone(),
        Some(serde_json::Value::Object(state)) => {
            if let Some(disabled) = state.get("disabled") {
                let reason = disabled
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if reason.is_empty() {
                    "disabled".to_string()
                } else {
                    format!("disabled ({reason})")
                }
            } else {
                serde_json::Value::Object(state.clone()).to_string()
            }
        }
        Some(value) if !value.is_null() => value.to_string(),
        _ => "-".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{format_application_details, public_endpoint_id};
    use serde_json::json;

    #[test]
    fn public_endpoint_id_supports_wire_name_and_missing_values() {
        assert_eq!(
            public_endpoint_id(&json!({"public_endpoint_id": "endpoint_snake"})),
            "endpoint_snake"
        );
        assert_eq!(
            public_endpoint_id(&json!({"publicEndpointId": "endpoint_camel"})),
            "endpoint_camel"
        );
        assert_eq!(public_endpoint_id(&json!({})), "-");
        assert_eq!(public_endpoint_id(&json!({"public_endpoint_id": ""})), "-");
    }

    #[test]
    fn application_details_include_deployment_and_public_endpoint_information() {
        let application = json!({
            "name": "weather",
            "description": "Current weather",
            "version": "v42",
            "state": "active",
            "public_endpoint_id": "endpoint_abc123",
            "entrypoint": {
                "function_name": "weather_app",
                "input_serializer": "json",
                "output_serializer": "pickle"
            },
            "functions": {
                "weather_app": {},
                "helper": {}
            },
            "allow": ["unauthenticated_requests"],
            "tags": {
                "team": "agents",
                "env": "prod"
            },
            "created_at": 1700000000000_i64,
            "tombstoned": false
        });

        let details = format_application_details(&application, "default");

        assert!(details.contains("Name:               weather\n"));
        assert!(details.contains("Namespace:          default\n"));
        assert!(details.contains("Version:            v42\n"));
        assert!(details.contains("State:              active\n"));
        assert!(details.contains("Public endpoint ID: endpoint_abc123\n"));
        assert!(details.contains("Entrypoint:         weather_app\n"));
        assert!(details.contains("Functions:          helper, weather_app\n"));
        assert!(details.contains("Allow:              unauthenticated_requests\n"));
        assert!(details.contains("Tags:               env=prod, team=agents\n"));
        assert!(details.contains("Deployed at:        2023-11-14 22:13:20\n"));
        assert!(details.contains("Tombstoned:         false\n"));
    }

    #[test]
    fn application_details_render_missing_optional_values_as_dashes() {
        let details = format_application_details(
            &json!({
                "name": "private-app",
                "entrypoint": {},
                "functions": {},
                "tags": {},
                "allow": []
            }),
            "project",
        );

        assert!(details.contains("Public endpoint ID: -\n"));
        assert!(details.contains("State:              -\n"));
        assert!(details.contains("Functions:          -\n"));
        assert!(details.contains("Deployed at:        -\n"));
    }
}
