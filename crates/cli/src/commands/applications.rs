use chrono::{TimeZone, Utc};
use comfy_table::Cell;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::Deserialize;
use std::io::Write;
use std::time::Duration;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

const APPLICATION_DETAILS_LABEL_WIDTH: usize = 20;
const APPLICATION_REQUEST_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Deserialize)]
struct InvokeApplicationResponse {
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct InvocationRequestStatus {
    outcome: Option<serde_json::Value>,
    request_error: Option<InvocationRequestError>,
}

#[derive(Debug, Deserialize)]
struct InvocationRequestError {
    function_name: String,
    message: String,
}

pub async fn invoke(
    ctx: &CliContext,
    application: &str,
    json: Option<&str>,
    wait: bool,
) -> Result<()> {
    let request_id = invoke_request(ctx, application, json).await?;
    if !wait {
        println!("{request_id}");
        return Ok(());
    }

    let output = wait_for_request_output(ctx, application, &request_id).await?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&output)?;
    if !output.is_empty() && !output.ends_with(b"\n") {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

async fn invoke_request(ctx: &CliContext, application: &str, json: Option<&str>) -> Result<String> {
    if application.is_empty() {
        return Err(CliError::usage("application name must not be empty"));
    }

    if let Some(json) = json {
        serde_json::from_str::<serde_json::Value>(json)
            .map_err(|error| CliError::usage(format!("invalid JSON for --json: {error}")))?;
    }

    let client = ctx.client()?;
    let url = format!(
        "{}/v1/namespaces/{}/applications/{}",
        ctx.api_url.trim_end_matches('/'),
        urlencoding::encode(&ctx.namespace),
        urlencoding::encode(application),
    );
    let mut request = client.post(url).header(ACCEPT, "application/json");
    if let Some(json) = json {
        request = request
            .header(CONTENT_TYPE, "application/json")
            .body(json.to_owned());
    }

    let response = request.send().await.map_err(CliError::Http)?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to invoke application '{}' (HTTP {}): {}",
            application,
            status,
            body,
        )));
    }

    let response = response
        .json::<InvokeApplicationResponse>()
        .await
        .map_err(CliError::Http)?;
    if response.request_id.is_empty() {
        return Err(CliError::Other(anyhow::anyhow!(
            "application invocation response did not include a request ID"
        )));
    }
    Ok(response.request_id)
}

async fn wait_for_request_output(
    ctx: &CliContext,
    application: &str,
    request_id: &str,
) -> Result<Vec<u8>> {
    wait_for_request_output_with_interval(
        ctx,
        application,
        request_id,
        APPLICATION_REQUEST_POLL_INTERVAL,
    )
    .await
}

async fn wait_for_request_output_with_interval(
    ctx: &CliContext,
    application: &str,
    request_id: &str,
    poll_interval: Duration,
) -> Result<Vec<u8>> {
    let client = ctx.client()?;
    let request_url = format!(
        "{}/v1/namespaces/{}/applications/{}/requests/{}",
        ctx.api_url.trim_end_matches('/'),
        urlencoding::encode(&ctx.namespace),
        urlencoding::encode(application),
        urlencoding::encode(request_id),
    );

    loop {
        let response = client
            .get(&request_url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(CliError::Http)?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to get application '{}' request '{}' (HTTP {}): {}",
                application,
                request_id,
                status,
                body,
            )));
        }

        let status = response
            .json::<InvocationRequestStatus>()
            .await
            .map_err(CliError::Http)?;
        match status.outcome.as_ref() {
            None | Some(serde_json::Value::Null) => {
                tokio::time::sleep(poll_interval).await;
            }
            Some(serde_json::Value::String(outcome)) if outcome == "success" => break,
            Some(serde_json::Value::Object(outcome)) => {
                let reason = outcome
                    .get("failure")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown failure");
                if let Some(request_error) = status.request_error {
                    return Err(CliError::Other(anyhow::anyhow!(
                        "application '{}' request '{}' failed in function '{}': {}",
                        application,
                        request_id,
                        request_error.function_name,
                        request_error.message,
                    )));
                }
                return Err(CliError::Other(anyhow::anyhow!(
                    "application '{}' request '{}' failed: {}",
                    application,
                    request_id,
                    reason,
                )));
            }
            Some(outcome) => {
                return Err(CliError::Other(anyhow::anyhow!(
                    "application '{}' request '{}' returned an unexpected outcome: {}",
                    application,
                    request_id,
                    outcome,
                )));
            }
        }
    }

    let response = client
        .get(format!("{request_url}/output"))
        .send()
        .await
        .map_err(CliError::Http)?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to download application '{}' request '{}' output (HTTP {}): {}",
            application,
            request_id,
            status,
            body,
        )));
    }

    Ok(response.bytes().await.map_err(CliError::Http)?.to_vec())
}

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
    use super::{
        format_application_details, invoke_request, public_endpoint_id,
        wait_for_request_output_with_interval,
    };
    use crate::auth::context::CliContext;
    use crate::config::resolver::ResolvedConfig;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn test_context(api_url: String) -> CliContext {
        CliContext::from_resolved(ResolvedConfig {
            api_url,
            cloud_url: "https://cloud.tensorlake.ai".to_string(),
            namespace: "customer namespace".to_string(),
            api_key: Some("test-api-key".to_string()),
            personal_access_token: None,
            organization_id: None,
            project_id: None,
            debug: false,
        })
    }

    async fn invocation_server(
        response_status: u16,
        response_body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            let header_end = loop {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0, "client closed before sending HTTP headers");
                request.extend_from_slice(&buffer[..count]);
                if let Some(offset) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    break offset + 4;
                }
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .map(str::parse::<usize>)
                })
                .transpose()
                .unwrap()
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0, "client closed before sending the HTTP body");
                request.extend_from_slice(&buffer[..count]);
            }

            let reason = if response_status == 200 {
                "OK"
            } else {
                "Test Error"
            };
            let response = format!(
                "HTTP/1.1 {response_status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                response_body.len(),
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (format!("http://{address}"), server)
    }

    async fn scripted_server(
        responses: Vec<(&'static str, &'static [u8])>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (content_type, response_body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "client closed before sending HTTP headers");
                    request.extend_from_slice(&buffer[..count]);
                    if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        break;
                    }
                }

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response_body.len(),
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(response_body).await.unwrap();
                stream.shutdown().await.unwrap();
                requests.push(String::from_utf8(request).unwrap());
            }
            requests
        });
        (format!("http://{address}"), server)
    }

    #[tokio::test]
    async fn invoke_sends_json_and_returns_the_request_id() {
        let (api_url, server) = invocation_server(200, r#"{"request_id":"request-123"}"#).await;
        let ctx = test_context(api_url);

        let request_id = invoke_request(&ctx, "support/ticket", Some(r#"{"priority":"high"}"#))
            .await
            .unwrap();
        let request = server.await.unwrap();

        assert_eq!(request_id, "request-123");
        assert!(request.starts_with(
            "POST /v1/namespaces/customer%20namespace/applications/support%2Fticket HTTP/1.1\r\n"
        ));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("content-type: application/json\r\n")
        );
        assert!(request.ends_with(r#"{"priority":"high"}"#));
    }

    #[tokio::test]
    async fn invoke_without_json_sends_an_empty_body() {
        let (api_url, server) = invocation_server(200, r#"{"request_id":"request-empty"}"#).await;
        let ctx = test_context(api_url);

        let request_id = invoke_request(&ctx, "zero_args", None).await.unwrap();
        let request = server.await.unwrap();
        let (_, body) = request.split_once("\r\n\r\n").unwrap();

        assert_eq!(request_id, "request-empty");
        assert!(body.is_empty());
        assert!(
            !request
                .to_ascii_lowercase()
                .contains("content-type: application/json\r\n")
        );
    }

    #[tokio::test]
    async fn invoke_rejects_invalid_json_before_sending_a_request() {
        let ctx = test_context("http://127.0.0.1:1".to_string());

        let error = invoke_request(&ctx, "application", Some("{"))
            .await
            .unwrap_err();

        assert!(error.to_string().starts_with("invalid JSON for --json:"));
    }

    #[tokio::test]
    async fn invoke_reports_server_response_errors() {
        let (api_url, server) = invocation_server(422, r#"{"error":"bad input"}"#).await;
        let ctx = test_context(api_url);

        let error = invoke_request(&ctx, "application", Some("null"))
            .await
            .unwrap_err();
        let _ = server.await.unwrap();

        assert_eq!(
            error.to_string(),
            r#"failed to invoke application 'application' (HTTP 422 Unprocessable Entity): {"error":"bad input"}"#,
        );
    }

    #[tokio::test]
    async fn wait_polls_until_success_and_downloads_the_output() {
        let (api_url, server) = scripted_server(vec![
            ("application/json", br#"{"outcome":null}"#),
            ("application/json", br#"{"outcome":"success"}"#),
            ("application/octet-stream", b"completed output"),
        ])
        .await;
        let ctx = test_context(api_url);

        let output = wait_for_request_output_with_interval(
            &ctx,
            "support/ticket",
            "request/123",
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        let requests = server.await.unwrap();

        assert_eq!(output, b"completed output");
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with(
            "GET /v1/namespaces/customer%20namespace/applications/support%2Fticket/requests/request%2F123 HTTP/1.1\r\n"
        ));
        assert!(requests[1].starts_with(
            "GET /v1/namespaces/customer%20namespace/applications/support%2Fticket/requests/request%2F123 HTTP/1.1\r\n"
        ));
        assert!(requests[2].starts_with(
            "GET /v1/namespaces/customer%20namespace/applications/support%2Fticket/requests/request%2F123/output HTTP/1.1\r\n"
        ));
    }

    #[tokio::test]
    async fn wait_reports_the_terminal_request_failure() {
        let (api_url, server) = scripted_server(vec![(
            "application/json",
            br#"{"outcome":{"failure":"function_error"},"request_error":null}"#,
        )])
        .await;
        let ctx = test_context(api_url);

        let error = wait_for_request_output_with_interval(
            &ctx,
            "application",
            "request-456",
            std::time::Duration::ZERO,
        )
        .await
        .unwrap_err();
        let _ = server.await.unwrap();

        assert_eq!(
            error.to_string(),
            "application 'application' request 'request-456' failed: function_error"
        );
    }

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
