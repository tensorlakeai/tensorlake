use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::header::ACCEPT;
use serde_json::Value;

use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_env_vars, resolve_sandbox_proxy_target, with_sandbox_headers};
use crate::error::{CliError, Result};

#[derive(Debug, Clone, Copy)]
pub struct ExecOptions<'a> {
    pub timeout: Option<f64>,
    pub workdir: Option<&'a str>,
    pub env: &'a [String],
    pub user: Option<&'a str>,
    pub detach: bool,
    pub name: Option<&'a str>,
    pub restart_policy: Option<&'a str>,
    pub max_restarts: Option<u32>,
    pub initial_backoff_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
    pub health_http: Option<&'a str>,
    pub health_tcp: Option<u16>,
    pub health_initial_delay_ms: Option<u64>,
    pub health_interval_ms: Option<u64>,
    pub health_timeout_ms: Option<u64>,
    pub health_failure_threshold: Option<u32>,
}

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    command: &str,
    args: &[String],
    options: ExecOptions<'_>,
) -> Result<()> {
    let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
    let client = ctx.client()?;
    let body = build_process_payload(command, args, options)?;

    if options.detach {
        let resp = with_sandbox_headers(
            client
                .post(format!("{}/api/v1/processes", target.proxy_base))
                .json(&body),
            &target,
        )
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

        let process: Value = resp.json().await.map_err(CliError::Http)?;
        let pid = process
            .get("pid")
            .and_then(|value| value.as_i64())
            .ok_or_else(|| CliError::usage("start process response missing pid"))?;
        println!("{pid}");
        return Ok(());
    }

    // Single streaming POST: start process + stream output + get exit code
    let resp = with_sandbox_headers(
        client
            .post(format!("{}/api/v1/processes/run", target.proxy_base))
            .header(ACCEPT, "text/event-stream")
            .json(&body),
        &target,
    )
    .send()
    .await
    .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to run process (HTTP {}): {}",
            status,
            body
        )));
    }

    let exit_code = stream_run_events(resp).await?;
    if exit_code != 0 {
        return Err(CliError::ExitCode(exit_code));
    }
    Ok(())
}

fn build_process_payload(
    command: &str,
    args: &[String],
    options: ExecOptions<'_>,
) -> Result<Value> {
    if !options.detach && managed_or_detached_only_fields_present(options) {
        return Err(CliError::usage(
            "managed process flags require --detach; use plain `tl sbx exec` for blocking output",
        ));
    }
    if options.detach && options.timeout.is_some() {
        return Err(CliError::usage("--timeout cannot be used with --detach"));
    }

    let env_dict = parse_env_vars(options.env)?;
    let mut body = serde_json::json!({ "command": command });
    if !args.is_empty() {
        body["args"] = serde_json::json!(args);
    }
    if let Some(env) = env_dict {
        body["env"] = env;
    }
    if let Some(wd) = options.workdir {
        body["working_dir"] = Value::String(wd.to_string());
    }
    if let Some(t) = options.timeout {
        body["timeout"] = serde_json::json!(t);
    }
    if let Some(user) = options.user {
        if user.trim().is_empty() {
            return Err(CliError::usage("--user must not be empty"));
        }
        body["user"] = Value::String(user.to_string());
    }
    if let Some(name) = options.name {
        // Single source-of-truth rule shared with the SDK + daemon (URL-safe, not a number).
        tensorlake::sandboxes::validate_managed_name(name)
            .map_err(|e| CliError::usage(e.to_string()))?;
        body["name"] = Value::String(name.to_string());
    }
    if let Some(restart) = build_restart_config(options) {
        body["restart"] = restart;
    }
    if let Some(health_check) = build_health_check(options)? {
        body["health_check"] = health_check;
    }
    Ok(body)
}

fn managed_or_detached_only_fields_present(options: ExecOptions<'_>) -> bool {
    options.name.is_some()
        || options.restart_policy.is_some()
        || options.max_restarts.is_some()
        || options.initial_backoff_ms.is_some()
        || options.max_backoff_ms.is_some()
        || options.health_http.is_some()
        || options.health_tcp.is_some()
        || options.health_initial_delay_ms.is_some()
        || options.health_interval_ms.is_some()
        || options.health_timeout_ms.is_some()
        || options.health_failure_threshold.is_some()
}

fn build_restart_config(options: ExecOptions<'_>) -> Option<Value> {
    if options.restart_policy.is_none()
        && options.max_restarts.is_none()
        && options.initial_backoff_ms.is_none()
        && options.max_backoff_ms.is_none()
    {
        return None;
    }

    let mut restart = serde_json::Map::new();
    if let Some(policy) = options.restart_policy {
        restart.insert("policy".to_string(), Value::String(policy.to_string()));
    }
    if let Some(value) = options.max_restarts {
        restart.insert("max_restarts".to_string(), serde_json::json!(value));
    }
    if let Some(value) = options.initial_backoff_ms {
        restart.insert("initial_backoff_ms".to_string(), serde_json::json!(value));
    }
    if let Some(value) = options.max_backoff_ms {
        restart.insert("max_backoff_ms".to_string(), serde_json::json!(value));
    }
    Some(Value::Object(restart))
}

fn build_health_check(options: ExecOptions<'_>) -> Result<Option<Value>> {
    let timing_fields_present = options.health_initial_delay_ms.is_some()
        || options.health_interval_ms.is_some()
        || options.health_timeout_ms.is_some()
        || options.health_failure_threshold.is_some();

    let (kind, port, path) = match (options.health_http, options.health_tcp) {
        (Some(_), Some(_)) => {
            return Err(CliError::usage(
                "use only one of --health-http or --health-tcp",
            ));
        }
        (Some(spec), None) => {
            let (port, path) = parse_http_health_spec(spec)?;
            ("http", port, path)
        }
        (None, Some(port)) => ("tcp", port, None),
        (None, None) => {
            if timing_fields_present {
                return Err(CliError::usage(
                    "health timing flags require --health-http or --health-tcp",
                ));
            }
            return Ok(None);
        }
    };

    let mut health_check = serde_json::Map::new();
    health_check.insert("type".to_string(), Value::String(kind.to_string()));
    health_check.insert("port".to_string(), serde_json::json!(port));
    if let Some(path) = path {
        health_check.insert("path".to_string(), Value::String(path));
    }
    if let Some(value) = options.health_initial_delay_ms {
        health_check.insert("initial_delay_ms".to_string(), serde_json::json!(value));
    }
    if let Some(value) = options.health_interval_ms {
        health_check.insert("interval_ms".to_string(), serde_json::json!(value));
    }
    if let Some(value) = options.health_timeout_ms {
        health_check.insert("timeout_ms".to_string(), serde_json::json!(value));
    }
    if let Some(value) = options.health_failure_threshold {
        health_check.insert("failure_threshold".to_string(), serde_json::json!(value));
    }
    Ok(Some(Value::Object(health_check)))
}

fn parse_http_health_spec(spec: &str) -> Result<(u16, Option<String>)> {
    let (port_part, path_part) = spec.split_once(':').unwrap_or((spec, ""));
    let port = port_part
        .parse::<u16>()
        .map_err(|_| CliError::usage("--health-http must start with a TCP port"))?;
    if port == 0 {
        return Err(CliError::usage("--health-http port must be greater than 0"));
    }
    if path_part.is_empty() {
        return Ok((port, None));
    }
    if !path_part.starts_with('/') {
        return Err(CliError::usage("--health-http path must start with '/'"));
    }
    Ok((port, Some(path_part.to_string())))
}

/// Read a streaming `POST /api/v1/processes/run` SSE response, print output
/// lines to stdout/stderr, and return the exit code from the final event.
async fn stream_run_events(resp: reqwest::Response) -> Result<i32> {
    let mut stream = Box::pin(resp.bytes_stream().eventsource());
    let mut exit_code: Option<i32> = None;

    while let Some(event) = stream.next().await {
        match event {
            Ok(msg) => {
                if let Some(parsed) = parse_run_event(&msg.data)? {
                    match parsed {
                        RunEvent::Output { line, stream } => match stream.as_deref() {
                            Some("stderr") => eprintln!("{}", line),
                            _ => println!("{}", line),
                        },
                        RunEvent::Exited { code } => {
                            exit_code = Some(code);
                        }
                        RunEvent::Other => {}
                    }
                }
            }
            Err(error) => {
                return Err(CliError::Other(anyhow::anyhow!(
                    "failed to stream process output: {}",
                    error
                )));
            }
        }
    }

    Ok(exit_code.unwrap_or(1))
}

enum RunEvent {
    Output {
        line: String,
        stream: Option<String>,
    },
    Exited {
        code: i32,
    },
    Other,
}

fn parse_run_event(data: &str) -> Result<Option<RunEvent>> {
    let trimmed = data.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let value: serde_json::Value = serde_json::from_str(trimmed)?;
    if should_skip_event(&value) {
        return Ok(None);
    }

    // Output line event
    if let Some(line) = value.get("line").and_then(|v| v.as_str()) {
        let stream = value
            .get("stream")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        return Ok(Some(RunEvent::Output {
            line: line.to_string(),
            stream,
        }));
    }

    // Exit event
    if let Some(code) = value.get("exit_code").and_then(|v| v.as_i64()) {
        return Ok(Some(RunEvent::Exited { code: code as i32 }));
    }
    if let Some(signal) = value.get("signal").and_then(|v| v.as_i64()) {
        return Ok(Some(RunEvent::Exited {
            code: 128 + signal as i32,
        }));
    }

    Ok(Some(RunEvent::Other))
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

#[cfg(test)]
mod tests {
    use super::{ExecOptions, build_process_payload, parse_run_event};

    fn options<'a>() -> ExecOptions<'a> {
        ExecOptions {
            timeout: None,
            workdir: None,
            env: &[],
            user: None,
            detach: false,
            name: None,
            restart_policy: None,
            max_restarts: None,
            initial_backoff_ms: None,
            max_backoff_ms: None,
            health_http: None,
            health_tcp: None,
            health_initial_delay_ms: None,
            health_interval_ms: None,
            health_timeout_ms: None,
            health_failure_threshold: None,
        }
    }

    #[test]
    fn parse_run_event_skips_empty_payloads() {
        assert!(parse_run_event("").unwrap().is_none());
        assert!(parse_run_event("   ").unwrap().is_none());
    }

    #[test]
    fn parse_run_event_skips_heartbeat_payloads() {
        assert!(
            parse_run_event(r#"{"type":"heartbeat"}"#)
                .unwrap()
                .is_none()
        );
        assert!(
            parse_run_event(r#"{"event":"keepalive"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parse_run_event_parses_output_lines() {
        let event = parse_run_event(r#"{"line":"hello","stream":"stdout"}"#)
            .unwrap()
            .unwrap();

        match event {
            super::RunEvent::Output { line, stream } => {
                assert_eq!(line, "hello");
                assert_eq!(stream.as_deref(), Some("stdout"));
            }
            _ => panic!("expected Output"),
        }
    }

    #[test]
    fn parse_run_event_parses_exit_code() {
        let event = parse_run_event(r#"{"exit_code":0}"#).unwrap().unwrap();
        match event {
            super::RunEvent::Exited { code } => assert_eq!(code, 0),
            _ => panic!("expected Exited"),
        }
    }

    #[test]
    fn parse_run_event_parses_signal_as_exit_code() {
        let event = parse_run_event(r#"{"signal":9}"#).unwrap().unwrap();
        match event {
            super::RunEvent::Exited { code } => assert_eq!(code, 128 + 9),
            _ => panic!("expected Exited"),
        }
    }

    #[test]
    fn detached_payload_includes_managed_fields() {
        let mut opts = options();
        opts.detach = true;
        opts.name = Some("web");
        opts.restart_policy = Some("always");
        opts.max_restarts = Some(10);
        opts.health_http = Some("8000:/health");
        opts.health_interval_ms = Some(5_000);

        let payload = build_process_payload("python", &["app.py".to_string()], opts).unwrap();

        assert_eq!(payload["command"], "python");
        assert_eq!(payload["args"], serde_json::json!(["app.py"]));
        assert_eq!(payload["name"], "web");
        assert_eq!(payload["restart"]["policy"], "always");
        assert_eq!(payload["restart"]["max_restarts"], 10);
        assert_eq!(payload["health_check"]["type"], "http");
        assert_eq!(payload["health_check"]["port"], 8000);
        assert_eq!(payload["health_check"]["path"], "/health");
        assert_eq!(payload["health_check"]["interval_ms"], 5_000);
    }

    #[test]
    fn managed_flags_require_detach() {
        let mut opts = options();
        opts.name = Some("web");

        let result = build_process_payload("python", &[], opts);

        assert!(result.is_err());
    }

    #[test]
    fn health_timing_requires_health_check() {
        let mut opts = options();
        opts.detach = true;
        opts.health_interval_ms = Some(1_000);

        let result = build_process_payload("python", &[], opts);

        assert!(result.is_err());
    }
}
