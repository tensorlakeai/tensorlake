use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

pub async fn run(
    ctx: &CliContext,
    dockerfile_path: &str,
    registered_name: Option<&str>,
    disk_gb: Option<u64>,
    is_public: bool,
) -> Result<()> {
    let mut cmd = tokio::process::Command::new("tensorlake-create-sandbox-image");
    cmd.arg(dockerfile_path);

    if let Some(name) = registered_name {
        cmd.arg("--name").arg(name);
    }
    if let Some(disk_gb) = disk_gb {
        cmd.arg("--disk").arg(disk_gb.to_string());
    }
    if is_public {
        cmd.arg("--public");
    }

    cmd.env("TENSORLAKE_API_URL", &ctx.api_url);
    if let Some(key) = &ctx.api_key {
        cmd.env("TENSORLAKE_API_KEY", key);
    }
    if let Some(pat) = &ctx.personal_access_token {
        cmd.env("TENSORLAKE_PAT", pat);
    }
    if let Some(org_id) = ctx.effective_organization_id() {
        cmd.env("TENSORLAKE_ORGANIZATION_ID", &org_id);
    }
    if let Some(proj_id) = ctx.effective_project_id() {
        cmd.env("TENSORLAKE_PROJECT_ID", &proj_id);
    }
    cmd.env("INDEXIFY_NAMESPACE", &ctx.namespace);
    if ctx.debug {
        cmd.env("TENSORLAKE_DEBUG", "1");
    }

    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let mut child = cmd.spawn().map_err(|e: std::io::Error| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CliError::usage(
                "'tensorlake-create-sandbox-image' not found on PATH. \
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
                eprintln!("⚙️  {}", message);
            }
            "build_log" => {
                let message = event.get("message").and_then(|v| v.as_str()).unwrap_or("");
                if !message.is_empty() {
                    eprintln!("{}", message);
                }
            }
            "warning" => {
                let message = event.get("message").and_then(|v| v.as_str()).unwrap_or("");
                eprintln!("⚠️  {}", message);
            }
            "snapshot_created" => {
                let snapshot_id = event
                    .get("snapshot_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                eprintln!("📸 Snapshot created: {}", snapshot_id);
            }
            "image_registered" => {
                let name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let image_id = event.get("image_id").and_then(|v| v.as_str()).unwrap_or("");
                eprintln!("✅ Image '{}' registered ({})", name, image_id);
            }
            "done" => {}
            "error" => {
                let message = event
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                eprintln!("Error: {}", message);
                if let Some(details) = event.get("details").and_then(|v| v.as_str())
                    && !details.is_empty()
                {
                    eprintln!("{}", details);
                }
                if let Some(traceback) = event.get("traceback").and_then(|v| v.as_str())
                    && !traceback.is_empty()
                {
                    eprintln!("\nPython traceback:\n{}", traceback);
                }
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
    match child.start_kill() {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::InvalidInput => {}
        Err(err) => return Err(CliError::Io(err)),
    }
    let _ = child.wait().await;
    Ok(())
}
