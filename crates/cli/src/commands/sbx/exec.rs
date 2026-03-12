use std::future::pending;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::header::ACCEPT;

use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_env_vars, sandbox_proxy_base};
use crate::error::{CliError, Result};
use crate::http;

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    command: &str,
    args: &[String],
    timeout: Option<f64>,
    workdir: Option<&str>,
    env: &[String],
) -> Result<()> {
    let env_dict = parse_env_vars(env)?;
    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);

    // Build a client with optional Host header override (for localhost proxy)
    let mut client_builder = http::client_builder();
    if let Ok(token) = ctx.bearer_token() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
        if let Some(org_id) = ctx.effective_organization_id() {
            headers.insert("X-Forwarded-Organization-Id", org_id.parse().unwrap());
        }
        if let Some(proj_id) = ctx.effective_project_id() {
            headers.insert("X-Forwarded-Project-Id", proj_id.parse().unwrap());
        }
        if let Some(ref host) = host_override {
            headers.insert(reqwest::header::HOST, host.parse().unwrap());
        }
        client_builder = client_builder.default_headers(headers);
    }
    let client = client_builder
        .build()
        .map_err(|e| CliError::Other(anyhow::anyhow!("{}", e)))?;

    // Start process
    let mut body = serde_json::json!({
        "command": command,
    });
    if !args.is_empty() {
        body["args"] = serde_json::json!(args);
    }
    if let Some(env) = env_dict {
        body["env"] = env;
    }
    if let Some(wd) = workdir {
        body["working_dir"] = serde_json::Value::String(wd.to_string());
    }

    let resp = client
        .post(format!("{}/api/v1/processes", proxy_base))
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to start process (HTTP {}): {}",
            status,
            body
        )));
    }

    let proc_result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let pid = proc_result
        .get("pid")
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => v.to_string(),
        })
        .unwrap_or_default();

    let exit_code = stream_and_wait(&client, &proxy_base, &pid, timeout).await?;
    if exit_code != 0 {
        return Err(CliError::ExitCode(exit_code));
    }
    Ok(())
}

async fn stream_and_wait(
    client: &reqwest::Client,
    proxy_base: &str,
    pid: &str,
    timeout: Option<f64>,
) -> Result<i32> {
    let follow_resp = client
        .get(format!(
            "{}/api/v1/processes/{}/output/follow",
            proxy_base, pid
        ))
        .header(ACCEPT, "text/event-stream")
        .send()
        .await
        .map_err(CliError::Http)?;

    if !follow_resp.status().is_success() {
        let status = follow_resp.status();
        let body = follow_resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to stream process output (HTTP {}): {}",
            status,
            body
        )));
    }

    let deadline =
        timeout.map(|t| tokio::time::Instant::now() + std::time::Duration::from_secs_f64(t));
    let mut stream = Box::pin(follow_resp.bytes_stream().eventsource());

    loop {
        let timeout_future = async {
            if let Some(deadline) = deadline {
                tokio::time::sleep_until(deadline).await;
            } else {
                pending::<()>().await;
            }
        };
        tokio::pin!(timeout_future);

        tokio::select! {
            _ = &mut timeout_future => {
                let _ = client
                    .delete(format!("{}/api/v1/processes/{}", proxy_base, pid))
                    .send()
                    .await;
                return Err(CliError::Other(anyhow::anyhow!(
                    "Command timed out after {}s",
                    timeout.unwrap_or(0.0)
                )));
            }
            maybe_event = stream.next() => {
                match maybe_event {
                    Some(Ok(msg)) => {
                        if let Some(event) = parse_output_event(&msg.data)? {
                            print_output_event(&event);
                        }
                    }
                    Some(Err(error)) => {
                        return Err(CliError::Other(anyhow::anyhow!(
                            "failed to stream process output: {}",
                            error
                        )));
                    }
                    None => break,
                }
            }
        }
    }

    let info = process_info(client, proxy_base, pid).await?;
    let status = info.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status == "running" {
        return wait_for_exit_code(client, proxy_base, pid, deadline, timeout).await;
    }

    exit_code_from_info(&info)
}

async fn wait_for_exit_code(
    client: &reqwest::Client,
    proxy_base: &str,
    pid: &str,
    deadline: Option<tokio::time::Instant>,
    timeout: Option<f64>,
) -> Result<i32> {
    loop {
        if let Some(deadline) = deadline
            && tokio::time::Instant::now() > deadline
        {
            let _ = client
                .delete(format!("{}/api/v1/processes/{}", proxy_base, pid))
                .send()
                .await;
            return Err(CliError::Other(anyhow::anyhow!(
                "Command timed out after {}s",
                timeout.unwrap_or(0.0)
            )));
        }

        let info = process_info(client, proxy_base, pid).await?;
        let status = info.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "running" {
            return exit_code_from_info(&info);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn process_info(
    client: &reqwest::Client,
    proxy_base: &str,
    pid: &str,
) -> Result<serde_json::Value> {
    let info_resp = client
        .get(format!("{}/api/v1/processes/{}", proxy_base, pid))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !info_resp.status().is_success() {
        let status = info_resp.status();
        let body = info_resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to get process status (HTTP {}): {}",
            status,
            body
        )));
    }

    info_resp.json().await.map_err(CliError::Http)
}

fn exit_code_from_info(info: &serde_json::Value) -> Result<i32> {
    if let Some(code) = info.get("exit_code").and_then(|v| v.as_i64()) {
        return Ok(code as i32);
    }
    if let Some(signal) = info.get("signal").and_then(|v| v.as_i64()) {
        return Ok(128 + signal as i32);
    }
    Ok(1)
}

fn print_output_event(event: &StreamOutputEvent) {
    match event.stream.as_deref() {
        Some("stderr") => eprintln!("{}", event.line),
        _ => println!("{}", event.line),
    }
}

fn parse_output_event(data: &str) -> Result<Option<StreamOutputEvent>> {
    let trimmed = data.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let value: serde_json::Value = serde_json::from_str(trimmed)?;
    if should_skip_event(&value) {
        return Ok(None);
    }

    let Some(line) = value.get("line").and_then(|value| value.as_str()) else {
        return Ok(None);
    };

    let stream = value
        .get("stream")
        .and_then(|value| value.as_str())
        .map(str::to_string);

    Ok(Some(StreamOutputEvent {
        line: line.to_string(),
        stream,
    }))
}

fn should_skip_event(value: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };

    ["type", "event", "kind"]
        .into_iter()
        .filter_map(|key| obj.get(key).and_then(|value| value.as_str()))
        .any(|kind| matches!(kind, "heartbeat" | "keepalive"))
}

#[derive(Debug)]
struct StreamOutputEvent {
    line: String,
    stream: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::parse_output_event;

    #[test]
    fn parse_output_event_skips_empty_payloads() {
        assert!(parse_output_event("").unwrap().is_none());
        assert!(parse_output_event("   ").unwrap().is_none());
    }

    #[test]
    fn parse_output_event_skips_heartbeat_payloads() {
        assert!(
            parse_output_event(r#"{"type":"heartbeat"}"#)
                .unwrap()
                .is_none()
        );
        assert!(
            parse_output_event(r#"{"event":"keepalive"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parse_output_event_skips_unknown_json_frames() {
        assert!(
            parse_output_event(r#"{"status":"done"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parse_output_event_parses_output_lines() {
        let event = parse_output_event(r#"{"line":"hello","stream":"stdout"}"#)
            .unwrap()
            .unwrap();

        assert_eq!(event.line, "hello");
        assert_eq!(event.stream.as_deref(), Some("stdout"));
    }
}
