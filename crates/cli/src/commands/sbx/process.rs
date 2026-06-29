use serde_json::Value;

use crate::auth::context::CliContext;
use crate::commands::sbx::{
    ResolvedSandboxProxyTarget, resolve_sandbox_proxy_target, with_sandbox_headers,
};
use crate::error::{CliError, Result};

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
