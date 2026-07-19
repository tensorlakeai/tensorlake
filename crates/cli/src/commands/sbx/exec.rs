use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::header::ACCEPT;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

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
    /// Promote a directly-invoked `tl fs mount` or `tl git mount` to a detached sandbox process.
    /// The mount daemon must be the process-unit leader: a daemon forked by `/processes/run`
    /// remains in that one-shot unit's cgroup and is reaped when the unit finishes.
    pub protect_long_lived_mounts: bool,
}

/// Runtime marker inherited by descendants of the one-shot Sandbox Process API. This is not
/// user configuration: mount commands use it to fail closed instead of forking a daemon into an
/// exec cgroup that will be torn down as soon as the outer command exits.
pub const SANDBOX_EXEC_MODE_ENV: &str = "TENSORLAKE_SANDBOX_PROCESS_MODE";
pub const SANDBOX_EXEC_MODE_ONE_SHOT: &str = "one-shot";

const DEFAULT_MOUNT_READY_TIMEOUT: Duration = Duration::from_secs(25);

#[derive(Debug, Clone, PartialEq, Eq)]
struct DetachedMountCommand {
    args: Vec<String>,
    mountpoint: String,
    surface: &'static str,
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
    if !options.detach && managed_or_detached_only_fields_present(options) {
        return Err(CliError::usage(
            "managed process flags require --detach; use plain `tl sbx exec` for blocking output",
        ));
    }

    // `/api/v1/processes/run` owns one process group and one cgroup leaf. A normal
    // `tl fs mount` / `tl git mount` returns after forking `tl fs daemon`; that child remains
    // inside the one-shot unit and the sandbox runtime reaps it when the request ends. `setsid(2)`
    // cannot escape that cgroup. Make the foreground mount daemon itself a detached Process API unit.
    // Explicit `--foreground` retains its debugging/blocking contract, and shell-wrapped
    // commands are not guessed at because rewriting their quoting is unsafe.
    let detached_mount = (options.detach || options.protect_long_lived_mounts)
        .then(|| rewrite_direct_mount(command, args))
        .flatten();
    let effective_args = detached_mount
        .as_ref()
        .map_or(args, |mount| mount.args.as_slice());
    let body = if detached_mount.is_some() && !options.detach {
        let mut detached_options = options;
        detached_options.detach = true;
        detached_options.timeout = None;
        build_process_payload(command, effective_args, detached_options)?
    } else {
        build_process_payload(command, effective_args, options)?
    };

    if options.detach || detached_mount.is_some() {
        let process = start_detached_process(&client, &target, &body).await?;
        let pid = process_pid(&process)?;
        if options.detach {
            println!("{pid}");
            return Ok(());
        }

        let mount = detached_mount.expect("detached mount checked above");
        let ready_timeout = mount_ready_timeout(options.timeout)?;
        if let Err(error) = wait_for_mount_ready(
            &client,
            &target,
            &mount.mountpoint,
            pid,
            options.workdir,
            options.user,
            ready_timeout,
        )
        .await
        {
            let tail = process_output_tail(&client, &target, pid).await;
            stop_process(&client, &target, pid).await;
            let detail = tail
                .filter(|tail| !tail.is_empty())
                .map(|tail| format!("\nMount process output:\n  {}", tail.replace('\n', "\n  ")))
                .unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "sandbox mount process {pid} did not make {} ready: {error}.{detail}",
                mount.mountpoint
            )));
        }

        println!(
            "Mounted {} {} in sandbox process {pid}; the process remains attached to the sandbox lifecycle.",
            mount.surface, mount.mountpoint
        );
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

async fn start_detached_process(
    client: &reqwest::Client,
    target: &crate::commands::sbx::ResolvedSandboxProxyTarget,
    body: &Value,
) -> Result<Value> {
    let resp = with_sandbox_headers(
        client
            .post(format!("{}/api/v1/processes", target.proxy_base))
            .json(body),
        target,
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
    resp.json().await.map_err(CliError::Http)
}

fn process_pid(process: &Value) -> Result<i64> {
    process
        .get("pid")
        .and_then(Value::as_i64)
        .ok_or_else(|| CliError::usage("start process response missing pid"))
}

fn mount_ready_timeout(configured: Option<f64>) -> Result<Duration> {
    match configured {
        None => Ok(DEFAULT_MOUNT_READY_TIMEOUT),
        Some(seconds) if seconds.is_finite() && seconds > 0.0 => {
            Ok(Duration::from_secs_f64(seconds))
        }
        Some(_) => Err(CliError::usage("--timeout must be greater than zero")),
    }
}

fn rewrite_direct_mount(command: &str, args: &[String]) -> Option<DetachedMountCommand> {
    if Path::new(command).file_name()?.to_str()? != "tl" {
        return None;
    }
    let mount_index = args
        .windows(2)
        .position(|pair| matches!(pair[0].as_str(), "fs" | "git") && pair[1] == "mount")?;
    let surface = match args[mount_index].as_str() {
        "fs" => "filesystem",
        "git" => "repository",
        _ => unreachable!("the mount window accepted only fs or git"),
    };
    let mount_args = &args[mount_index + 2..];
    if mount_args.iter().any(|arg| arg == "--foreground") {
        return None;
    }

    let mountpoint = parse_mountpoint(&args[mount_index], mount_args)?;
    let mut detached_args = args.to_vec();
    detached_args.push("--foreground".to_string());
    Some(DetachedMountCommand {
        args: detached_args,
        mountpoint,
        surface,
    })
}

fn parse_mountpoint(surface: &str, args: &[String]) -> Option<String> {
    let mut positional = Vec::with_capacity(2);
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "--ro" | "--trace-ops" => index += 1,
            "--publish" if surface == "git" => index += 1,
            "--log-level" => {
                if index + 1 == args.len() {
                    return None;
                }
                index += 2;
            }
            "--workspace" if surface == "git" => {
                if index + 1 == args.len() {
                    return None;
                }
                index += 2;
            }
            "--" => {
                positional.extend(args[index + 1..].iter().cloned());
                break;
            }
            _ if arg.starts_with("--log-level=")
                || (surface == "git" && arg.starts_with("--workspace=")) =>
            {
                index += 1;
            }
            // Unknown options are left to the ordinary execution path. Guessing whether they
            // consume a value could accidentally treat that value as the mountpoint.
            _ if arg.starts_with('-') => return None,
            _ => {
                positional.push(arg.clone());
                index += 1;
            }
        }
    }
    (positional.len() == 2).then(|| positional[1].clone())
}

fn build_mount_readiness_payload(
    mountpoint: &str,
    pid: i64,
    workdir: Option<&str>,
    user: Option<&str>,
    timeout: Duration,
) -> Value {
    let attempts = timeout.as_millis().div_ceil(100).max(1);
    let script = "i=0; while [ \"$i\" -lt \"$2\" ]; do \
                  kill -0 \"$3\" 2>/dev/null || exit 125; \
                  mountpoint -q -- \"$1\" && exit 0; \
                  i=$((i + 1)); sleep 0.1; \
                  done; exit 124";
    let mut body = serde_json::json!({
        "command": "/bin/sh",
        "args": [
            "-c",
            script,
            "tlfs-mount-readiness",
            mountpoint,
            attempts.to_string(),
            pid.to_string(),
        ],
        "timeout": timeout.as_secs_f64() + 2.0,
    });
    if let Some(workdir) = workdir {
        body["working_dir"] = Value::String(workdir.to_string());
    }
    if let Some(user) = user {
        body["user"] = Value::String(user.to_string());
    }
    body
}

async fn wait_for_mount_ready(
    client: &reqwest::Client,
    target: &crate::commands::sbx::ResolvedSandboxProxyTarget,
    mountpoint: &str,
    pid: i64,
    workdir: Option<&str>,
    user: Option<&str>,
    timeout: Duration,
) -> Result<()> {
    let body = build_mount_readiness_payload(mountpoint, pid, workdir, user, timeout);
    let resp = with_sandbox_headers(
        client
            .post(format!("{}/api/v1/processes/run", target.proxy_base))
            .header(ACCEPT, "text/event-stream")
            .json(&body),
        target,
    )
    .send()
    .await
    .map_err(CliError::Http)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "readiness check failed (HTTP {status}): {body}"
        )));
    }
    match stream_run_events(resp).await? {
        0 => Ok(()),
        125 => Err(CliError::Other(anyhow::anyhow!(
            "the detached mount process exited during startup"
        ))),
        124 => Err(CliError::Other(anyhow::anyhow!(
            "the mount was not ready within {:.1}s",
            timeout.as_secs_f64()
        ))),
        code => Err(CliError::Other(anyhow::anyhow!(
            "mount readiness check exited with code {code}"
        ))),
    }
}

async fn process_output_tail(
    client: &reqwest::Client,
    target: &crate::commands::sbx::ResolvedSandboxProxyTarget,
    pid: i64,
) -> Option<String> {
    let response = with_sandbox_headers(
        client.get(format!(
            "{}/api/v1/processes/{pid}/output",
            target.proxy_base
        )),
        target,
    )
    .send()
    .await
    .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body: Value = response.json().await.ok()?;
    let lines = body.get("lines")?.as_array()?;
    Some(
        lines
            .iter()
            .filter_map(Value::as_str)
            .rev()
            .take(8)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

async fn stop_process(
    client: &reqwest::Client,
    target: &crate::commands::sbx::ResolvedSandboxProxyTarget,
    pid: i64,
) {
    let _ = with_sandbox_headers(
        client.delete(format!("{}/api/v1/processes/{pid}", target.proxy_base)),
        target,
    )
    .send()
    .await;
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

    let mut env_dict = parse_env_vars(options.env)?;
    if !options.detach {
        let env = env_dict.get_or_insert_with(|| Value::Object(serde_json::Map::new()));
        env.as_object_mut()
            .expect("parse_env_vars always returns an object")
            .insert(
                SANDBOX_EXEC_MODE_ENV.to_string(),
                Value::String(SANDBOX_EXEC_MODE_ONE_SHOT.to_string()),
            );
    }
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
    use std::time::Duration;

    use super::{
        ExecOptions, SANDBOX_EXEC_MODE_ENV, SANDBOX_EXEC_MODE_ONE_SHOT,
        build_mount_readiness_payload, build_process_payload, parse_run_event,
        rewrite_direct_mount,
    };

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
            protect_long_lived_mounts: true,
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
        assert!(payload["env"].get(SANDBOX_EXEC_MODE_ENV).is_none());
    }

    #[test]
    fn one_shot_payload_marks_descendants_and_caller_cannot_spoof_it() {
        let mut opts = options();
        let environment = ["TENSORLAKE_SANDBOX_PROCESS_MODE=detached".to_string()];
        opts.env = &environment;

        let payload = build_process_payload("python", &["remote.py".to_string()], opts).unwrap();

        assert_eq!(
            payload["env"][SANDBOX_EXEC_MODE_ENV],
            SANDBOX_EXEC_MODE_ONE_SHOT
        );
    }

    #[test]
    fn direct_filesystem_mount_becomes_foreground_process_unit() {
        let command = rewrite_direct_mount(
            "/home/tl-user/bin/tl",
            &[
                "fs".to_string(),
                "mount".to_string(),
                "drive".to_string(),
                "mnt/drive".to_string(),
                "--ro".to_string(),
                "--log-level=debug".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(command.surface, "filesystem");
        assert_eq!(command.mountpoint, "mnt/drive");
        assert_eq!(
            command.args.last().map(String::as_str),
            Some("--foreground")
        );
    }

    #[test]
    fn direct_git_mount_parses_workspace_option() {
        let command = rewrite_direct_mount(
            "tl",
            &[
                "git".to_string(),
                "mount".to_string(),
                "repo:main".to_string(),
                "--workspace".to_string(),
                "workspace-1".to_string(),
                "/code".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(command.surface, "repository");
        assert_eq!(command.mountpoint, "/code");
    }

    #[test]
    fn explicit_foreground_and_shell_wrapped_mounts_are_not_rewritten() {
        assert!(
            rewrite_direct_mount(
                "tl",
                &[
                    "fs".to_string(),
                    "mount".to_string(),
                    "drive".to_string(),
                    "/mnt".to_string(),
                    "--foreground".to_string(),
                ],
            )
            .is_none()
        );
        assert!(
            rewrite_direct_mount(
                "/bin/sh",
                &["-lc".to_string(), "tl fs mount drive /mnt".to_string()],
            )
            .is_none()
        );
    }

    #[test]
    fn readiness_probe_tracks_process_liveness_and_mountpoint() {
        let payload = build_mount_readiness_payload(
            "relative mount",
            4242,
            Some("/work"),
            Some("tl-user"),
            Duration::from_millis(250),
        );

        assert_eq!(payload["working_dir"], "/work");
        assert_eq!(payload["user"], "tl-user");
        assert_eq!(payload["args"][3], "relative mount");
        assert_eq!(payload["args"][4], "3");
        assert_eq!(payload["args"][5], "4242");
        assert!(
            payload["args"][1]
                .as_str()
                .unwrap()
                .contains("mountpoint -q")
        );
        assert!(payload["args"][1].as_str().unwrap().contains("kill -0"));
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
