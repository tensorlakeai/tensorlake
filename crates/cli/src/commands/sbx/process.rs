use serde_json::Value;

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

    let body = send_api_request(request, "sandbox logs").await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }

    let logs = body
        .get("logs")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    for log in logs {
        if let Some(line) = log.get("body").and_then(Value::as_str) {
            println!("{line}");
        }
    }
    if let Some(next_token) = body.get("nextToken").and_then(Value::as_str) {
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
    let body = send_api_request(client.get(url), "sandbox log processes").await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }

    let processes = body
        .get("processes")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if processes.is_empty() {
        println!("No process logs found.");
    } else {
        print_log_process_table(processes);
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

async fn send_api_request(request: reqwest::RequestBuilder, label: &str) -> Result<Value> {
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
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).map_err(Into::into)
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

fn print_log_process_table(processes: &[Value]) {
    println!("Process ID\tPID\tManaged ID\tName\tLogs\tCommand");
    for process in processes {
        let process_id = string_field(process, "processId");
        let pid = string_field(process, "processPid");
        let managed_id = string_field(process, "processManagedId");
        let name = string_field(process, "processManagedName");
        let command = string_field(process, "processCommand");
        let log_count = process
            .get("logCount")
            .and_then(Value::as_u64)
            .map(|value| value.to_string())
            .unwrap_or_default();
        println!("{process_id}\t{pid}\t{managed_id}\t{name}\t{log_count}\t{command}");
    }
}

fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
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
