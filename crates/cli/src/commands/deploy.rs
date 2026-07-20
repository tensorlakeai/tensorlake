use std::{path::PathBuf, process::Stdio};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

mod typescript;

#[derive(Debug, PartialEq, Eq)]
struct TypeScriptDeployArgs {
    entrypoint: PathBuf,
    upgrade_running_requests: bool,
}

fn typescript_deploy_args(remaining_args: &[String]) -> Result<Option<TypeScriptDeployArgs>> {
    let positionals = remaining_args
        .iter()
        .filter(|argument| !argument.starts_with('-'))
        .collect::<Vec<_>>();
    for positional in &positionals {
        let extension = std::path::Path::new(positional.as_str())
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if matches!(extension.as_str(), "cts" | "cjs") {
            return Err(CliError::usage(
                "Tensorlake TypeScript applications support ESM only; use a .ts, .mts, .js, or .mjs entrypoint",
            ));
        }
    }

    let typescript_entries = positionals
        .iter()
        .filter(|positional| {
            matches!(
                std::path::Path::new(positional.as_str())
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .as_str(),
                "ts" | "mts" | "js" | "mjs"
            )
        })
        .collect::<Vec<_>>();
    if typescript_entries.is_empty() {
        return Ok(None);
    }
    if typescript_entries.len() != 1 || positionals.len() != 1 {
        return Err(CliError::usage(
            "Usage: tl deploy [--upgrade-running-requests] <application.ts>",
        ));
    }
    for argument in remaining_args {
        if argument.starts_with('-')
            && !matches!(
                argument.as_str(),
                "--" | "--upgrade-running-requests" | "-u"
            )
        {
            return Err(CliError::usage(format!(
                "Unknown TypeScript deployment option '{argument}'. Usage: tl deploy [--upgrade-running-requests] <application.ts>"
            )));
        }
    }
    Ok(Some(TypeScriptDeployArgs {
        entrypoint: PathBuf::from(typescript_entries[0].as_str()),
        upgrade_running_requests: remaining_args
            .iter()
            .any(|argument| matches!(argument.as_str(), "--upgrade-running-requests" | "-u")),
    }))
}

pub async fn run(ctx: &CliContext, remaining_args: &[String]) -> Result<()> {
    if let Some(args) = typescript_deploy_args(remaining_args)? {
        return typescript::run(ctx, &args.entrypoint, args.upgrade_running_requests).await;
    }
    run_python_deployer(ctx, remaining_args).await
}

async fn run_python_deployer(ctx: &CliContext, remaining_args: &[String]) -> Result<()> {
    let executable = PathBuf::from("tensorlake-deploy");
    let mut cmd = tokio::process::Command::new(&executable);
    cmd.args(remaining_args);

    // Pass auth context via environment
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
            CliError::usage(format!(
                "'{}' not found on PATH. Install the Python Tensorlake SDK with `pip install tensorlake`.",
                executable.display(),
            ))
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
            // Non-JSON line — pass through to stderr
            eprintln!("{}", line);
            continue;
        };

        let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match event_type {
            "status" => {
                let message = event.get("message").and_then(|v| v.as_str()).unwrap_or("");
                eprintln!("⚙️  {}", message);
            }
            "validation" => {
                let severity = event
                    .get("severity")
                    .and_then(|v| v.as_str())
                    .unwrap_or("info");
                let message = event.get("message").and_then(|v| v.as_str()).unwrap_or("");
                let location = event.get("location").and_then(|v| v.as_str()).unwrap_or("");
                match severity {
                    "error" => eprintln!("‼️  Error: {}\n{}", location, message),
                    "warning" => eprintln!("⚠️  Warning: {}\n{}", location, message),
                    _ => eprintln!("ℹ️  {}\n{}", location, message),
                }
            }
            "validation_failed" => {
                eprintln!("‼️  Deployment aborted due to validation errors");
            }
            "missing_secrets" => {
                if let Some(names) = event.get("names").and_then(|v| v.as_array()) {
                    let names: Vec<&str> = names.iter().filter_map(|v| v.as_str()).collect();
                    eprintln!(
                        "⚠️  Your Tensorlake project has missing secrets: {}. Application invocations may fail until these secrets are set.",
                        names.join(", ")
                    );
                } else if let Some(count) = event.get("count").and_then(|v| v.as_u64()) {
                    eprintln!(
                        "⚠️  Your Tensorlake project is missing {} secret(s). Application invocations may fail until these secrets are set.",
                        count
                    );
                }
            }
            "build_start" => {
                let image = event
                    .get("image")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                eprintln!("📦 Building `{}` image...", image);
            }
            "build_log" => {
                // Build logs normally go directly to stderr; this handles structured build logs too.
                let message = event.get("message").and_then(|v| v.as_str()).unwrap_or("");
                if !message.is_empty() {
                    eprintln!("{}", message);
                }
            }
            "build_done" => {
                eprintln!("\n✅ All images built successfully");
            }
            "build_failed" => {
                let image = event
                    .get("image")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let error = event
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                eprintln!("❌ Image '{}' build failed: {}", image, error);
                if let Some(traceback) = event.get("traceback").and_then(|v| v.as_str())
                    && !traceback.is_empty()
                {
                    eprintln!("\nTraceback:\n{}", traceback);
                }
            }
            "deployed" => {
                let application = event
                    .get("application")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                eprintln!("🚀 Application `{}` deployed successfully", application);
                if let Some(curl_command) = event.get("curl_command").and_then(|v| v.as_str()) {
                    eprintln!(
                        "\n💡 To invoke it, you can use the following cURL command:\n\n{}",
                        curl_command
                    );
                }
            }
            "done" => {
                let doc_url = event.get("doc_url").and_then(|v| v.as_str()).unwrap_or("");
                eprintln!(
                    "\n📚 Visit our documentation if you need more information about invoking applications: {}\n",
                    doc_url
                );
            }
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
                    eprintln!("\nTraceback:\n{}", traceback);
                }
            }
            _ => {
                // Unknown event type — pass through
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
    // Best-effort cancellation of the spawned deploy process when the user presses Ctrl+C.
    match child.start_kill() {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::InvalidInput => {}
        Err(err) => return Err(CliError::Io(err)),
    }
    let _ = child.wait().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TypeScriptDeployArgs, typescript_deploy_args};

    #[test]
    fn selects_rust_typescript_deployment_for_esm_entrypoints() {
        assert_eq!(typescript_deploy_args(&["app.py".into()]).unwrap(), None);
        assert_eq!(
            typescript_deploy_args(&["app.ts".into()]).unwrap(),
            Some(TypeScriptDeployArgs {
                entrypoint: "app.ts".into(),
                upgrade_running_requests: false,
            })
        );
        assert_eq!(
            typescript_deploy_args(&["--upgrade-running-requests".into(), "app.mjs".into()])
                .unwrap(),
            Some(TypeScriptDeployArgs {
                entrypoint: "app.mjs".into(),
                upgrade_running_requests: true,
            })
        );
    }

    #[test]
    fn rejects_commonjs_entrypoints() {
        assert!(typescript_deploy_args(&["app.cjs".into()]).is_err());
        assert!(typescript_deploy_args(&["app.cts".into()]).is_err());
    }

    #[test]
    fn rejects_extra_typescript_arguments() {
        assert!(
            typescript_deploy_args(&["--build-env".into(), "A=B".into(), "app.ts".into()]).is_err()
        );
        assert!(typescript_deploy_args(&["first.ts".into(), "second.ts".into()]).is_err());
    }
}
