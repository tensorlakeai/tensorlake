use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::header::ACCEPT;

use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_env_vars, sandbox_proxy_base, with_sandbox_headers};
use crate::error::{CliError, Result};

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
    let client = ctx.client()?;

    // Build request body
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
    if let Some(t) = timeout {
        body["timeout"] = serde_json::json!(t);
    }

    // Single streaming POST: start process + stream output + get exit code
    let resp = with_sandbox_headers(
        client
            .post(format!("{}/api/v1/processes/run", proxy_base))
            .header(ACCEPT, "text/event-stream")
            .json(&body),
        sandbox_id,
        host_override,
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
    use super::parse_run_event;

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
}
