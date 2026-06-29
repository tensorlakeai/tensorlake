use serde_json::Value;
use tensorlake::sandboxes::models::{
    SandboxLogSignal, SandboxLogsResponse, SandboxProcessLogFilter,
    SandboxProcessLogFiltersResponse,
};

use crate::auth::context::CliContext;
use crate::commands::sbx::{
    ResolvedSandboxProxyTarget, resolve_sandbox_proxy_target, with_sandbox_headers,
};
use crate::error::{CliError, Result};

pub struct LogsArgs<'a> {
    pub levels: Vec<i8>,
    pub process_ids: Vec<String>,
    pub next_token: Option<&'a str>,
    pub head: Option<usize>,
    pub tail: Option<usize>,
    pub body: Option<&'a str>,
    pub json: bool,
}

pub async fn ps(
    ctx: &CliContext,
    sandbox_id: &str,
    process: Option<&str>,
    json: bool,
) -> Result<()> {
    let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
    let client = ctx.client()?;
    let path = match process {
        Some(process) => format!("/api/v1/processes/{process}"),
        None => "/api/v1/processes".to_string(),
    };
    let body = send_process_request(
        &target,
        client.get(format!("{}{}", target.proxy_base, path)),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }

    if process.is_some() {
        print_process_table(std::slice::from_ref(&body));
    } else {
        let processes = body
            .get("processes")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if processes.is_empty() {
            println!("No processes found.");
        } else {
            print_process_table(processes);
        }
    }
    Ok(())
}

pub async fn logs(ctx: &CliContext, sandbox_id: &str, args: LogsArgs<'_>) -> Result<()> {
    let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
    let client = ctx.client()?;
    let url = format!(
        "{}/v1/namespaces/{}/sandboxes/{}/logs",
        ctx.api_url.trim_end_matches('/'),
        ctx.namespace,
        target.sandbox_id
    );
    let mut request = client.get(url);
    for level in args.levels {
        request = request.query(&[("level", level)]);
    }
    for process_id in args.process_ids {
        request = request.query(&[("processId", process_id)]);
    }
    if let Some(next_token) = args.next_token {
        request = request.query(&[("nextToken", next_token)]);
    }
    if let Some(head) = args.head {
        request = request.query(&[("head", head)]);
    }
    if let Some(tail) = args.tail {
        request = request.query(&[("tail", tail)]);
    }
    if let Some(body) = args.body {
        request = request.query(&[("body", body)]);
    }

    let body: SandboxLogsResponse = send_api_request(request, "sandbox logs").await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }

    for log in &body.logs {
        println!("{}", default_log_line(log));
    }
    if let Some(next_token) = body.next_token {
        eprintln!("nextToken: {next_token}");
    }
    Ok(())
}

pub async fn log_processes(ctx: &CliContext, sandbox_id: &str, json: bool) -> Result<()> {
    let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
    let client = ctx.client()?;
    let url = format!(
        "{}/v1/namespaces/{}/sandboxes/{}/processes",
        ctx.api_url.trim_end_matches('/'),
        ctx.namespace,
        target.sandbox_id
    );
    let body: SandboxProcessLogFiltersResponse =
        send_api_request(client.get(url), "sandbox log processes").await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }

    if body.processes.is_empty() {
        println!("No process logs found.");
    } else {
        print_log_process_table(&body.processes);
    }
    Ok(())
}

pub async fn restart(ctx: &CliContext, sandbox_id: &str, process: &str) -> Result<()> {
    let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
    let client = ctx.client()?;
    let body = send_process_request(
        &target,
        client.post(format!(
            "{}/api/v1/processes/{process}/restart",
            target.proxy_base
        )),
    )
    .await?;
    match body.get("pid").and_then(Value::as_i64) {
        Some(next_pid) => println!("{next_pid}"),
        None => println!("{process}"),
    }
    Ok(())
}

pub async fn kill(ctx: &CliContext, sandbox_id: &str, process: &str) -> Result<()> {
    let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
    let client = ctx.client()?;
    send_process_request(
        &target,
        client.delete(format!("{}/api/v1/processes/{process}", target.proxy_base)),
    )
    .await?;
    println!("Killed process {process}.");
    Ok(())
}

async fn send_process_request(
    target: &ResolvedSandboxProxyTarget,
    request: reqwest::RequestBuilder,
) -> Result<Value> {
    let resp = with_sandbox_headers(request, target)
        .send()
        .await
        .map_err(CliError::Http)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "process request failed (HTTP {}): {}",
            status,
            body
        )));
    }
    let bytes = resp.bytes().await.map_err(CliError::Http)?;
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).map_err(Into::into)
}

async fn send_api_request<T>(request: reqwest::RequestBuilder, label: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let resp = request.send().await.map_err(CliError::Http)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "{label} request failed (HTTP {}): {}",
            status,
            body
        )));
    }
    let bytes = resp.bytes().await.map_err(CliError::Http)?;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

fn default_log_line(log: &SandboxLogSignal) -> &str {
    let log_attributes = log.log_attributes.trim();
    if has_log_attributes(log_attributes) {
        log_attributes
    } else {
        &log.body
    }
}

fn has_log_attributes(log_attributes: &str) -> bool {
    !matches!(log_attributes, "" | "{}" | "null")
}

fn print_process_table(processes: &[Value]) {
    println!("PID\tStatus\tManaged\tName\tRestarts\tHealth\tCommand");
    for process in processes {
        let pid = process
            .get("pid")
            .and_then(Value::as_i64)
            .map(|pid| pid.to_string())
            .unwrap_or_default();
        let status = process
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let command = format_command(process);
        let managed = process.get("managed").filter(|value| !value.is_null());
        let managed_flag = if managed.is_some() { "yes" } else { "no" };
        let name = managed
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let restarts = managed
            .and_then(|value| value.get("restart_count"))
            .and_then(Value::as_u64)
            .map(|value| value.to_string())
            .unwrap_or_default();
        let health = managed
            .and_then(|value| value.get("health_status"))
            .and_then(Value::as_str)
            .unwrap_or("");
        println!("{pid}\t{status}\t{managed_flag}\t{name}\t{restarts}\t{health}\t{command}");
    }
}

fn print_log_process_table(processes: &[SandboxProcessLogFilter]) {
    println!("Process ID\tPID\tManaged ID\tName\tLogs\tCommand");
    for process in processes {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            process.process_id,
            process.process_pid,
            process.process_managed_id,
            process.process_managed_name,
            process.log_count,
            process.process_command
        );
    }
}

fn format_command(process: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(command) = process.get("command").and_then(Value::as_str) {
        parts.push(command.to_string());
    }
    if let Some(args) = process.get("args").and_then(Value::as_array) {
        parts.extend(args.iter().filter_map(Value::as_str).map(str::to_string));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_signal(body: &str, log_attributes: &str) -> SandboxLogSignal {
        serde_json::from_str(&format!(
            r#"{{
                "timestamp": 1,
                "uuid": "018f06cc-0ba2-7def-86f4-3b5f64f847d0",
                "namespace": "default",
                "application": "",
                "sandboxId": "sbx",
                "resourceAttributes": [],
                "body": {body:?},
                "logAttributes": {log_attributes:?},
                "allocations": [],
                "functionRuns": []
            }}"#
        ))
        .unwrap()
    }

    #[test]
    fn default_log_line_prefers_log_attributes() {
        let log = log_signal("fallback body", r#"{"event":"started"}"#);

        assert_eq!(default_log_line(&log), r#"{"event":"started"}"#);
    }

    #[test]
    fn default_log_line_falls_back_to_body_without_log_attributes() {
        assert_eq!(default_log_line(&log_signal("from body", "")), "from body");
        assert_eq!(
            default_log_line(&log_signal("from body", "{}")),
            "from body"
        );
        assert_eq!(
            default_log_line(&log_signal("from body", "null")),
            "from body"
        );
    }
}
