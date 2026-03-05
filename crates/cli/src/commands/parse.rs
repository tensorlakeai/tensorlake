use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, remaining_args: &[String]) -> Result<()> {
    let mut cmd = tokio::process::Command::new("tensorlake-parse");
    cmd.args(remaining_args);

    // Pass auth context via environment
    cmd.env("TENSORLAKE_API_URL", &ctx.api_url);
    if let Some(key) = &ctx.api_key {
        cmd.env("TENSORLAKE_API_KEY", key);
    }
    if let Some(pat) = &ctx.personal_access_token {
        cmd.env("TENSORLAKE_PAT", pat);
    }

    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let mut child = cmd.spawn().map_err(|e: std::io::Error| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CliError::usage(
                "'tensorlake-parse' not found on PATH. \
                 Install the Python tensorlake package: pip install tensorlake",
            )
        } else {
            CliError::Io(e)
        }
    })?;

    let stdout = child.stdout.take().expect("stdout was piped");
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());

    loop {
        let maybe_line = tokio::select! {
            line_result = lines.next_line() => line_result,
            _ = &mut ctrl_c => {
                terminate_child(&mut child).await?;
                return Err(CliError::Cancelled);
            }
        }
        .map_err(CliError::Io)?;

        let Some(line) = maybe_line else {
            break;
        };

        let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
            eprintln!("{}", line);
            continue;
        };

        let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match event_type {
            "status" => {
                let message = event.get("message").and_then(|v| v.as_str()).unwrap_or("");
                eprintln!("{}", message);
            }
            "cached" => {
                // Cache hit — silent, output follows
            }
            "output" => {
                let content = event.get("content").and_then(|v| v.as_str()).unwrap_or("");
                println!("{}", content); // Markdown to stdout
            }
            "error" => {
                let message = event
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                eprintln!("Error: {}", message);
            }
            _ => {
                eprintln!("{}", line);
            }
        }
    }

    let status = child.wait().await.map_err(CliError::Io)?;

    if !status.success() {
        let code = status.code().unwrap_or(1);
        return Err(CliError::ExitCode(code));
    }

    Ok(())
}

async fn terminate_child(child: &mut tokio::process::Child) -> Result<()> {
    // Best-effort cancellation of the spawned Python process when the user presses Ctrl+C.
    match child.start_kill() {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::InvalidInput => {}
        Err(err) => return Err(CliError::Io(err)),
    }
    let _ = child.wait().await;
    Ok(())
}
