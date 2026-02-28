use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_env_vars, sandbox_proxy_base};
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

    // Build a client with optional Host header override (for localhost proxy)
    let mut client_builder = reqwest::Client::builder();
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
    let client = client_builder.build().map_err(|e| CliError::Other(anyhow::anyhow!("{}", e)))?;

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

    // Poll process until done
    let exit_code = wait_and_print(&client, &proxy_base, &pid, timeout).await?;

    if exit_code != 0 {
        return Err(CliError::ExitCode(exit_code));
    }
    Ok(())
}

async fn wait_and_print(
    client: &reqwest::Client,
    proxy_base: &str,
    pid: &str,
    timeout: Option<f64>,
) -> Result<i32> {
    let deadline = timeout.map(|t| tokio::time::Instant::now() + std::time::Duration::from_secs_f64(t));

    loop {
        // Check timeout
        if let Some(dl) = deadline {
            if tokio::time::Instant::now() > dl {
                // Kill process
                let _ = client
                    .delete(format!("{}/api/v1/processes/{}", proxy_base, pid))
                    .send()
                    .await;
                return Err(CliError::Other(anyhow::anyhow!(
                    "Command timed out after {}s",
                    timeout.unwrap_or(0.0)
                )));
            }
        }

        let info_resp = client
            .get(format!("{}/api/v1/processes/{}", proxy_base, pid))
            .send()
            .await
            .map_err(CliError::Http)?;

        if !info_resp.status().is_success() {
            return Err(CliError::Other(anyhow::anyhow!("failed to get process status")));
        }

        let info: serde_json::Value = info_resp.json().await.map_err(CliError::Http)?;
        let status = info.get("status").and_then(|v| v.as_str()).unwrap_or("");

        if status != "running" {
            // Process done — fetch stdout/stderr
            let stdout_resp = client
                .get(format!("{}/api/v1/processes/{}/stdout", proxy_base, pid))
                .send()
                .await
                .map_err(CliError::Http)?;

            if stdout_resp.status().is_success() {
                let stdout_body: serde_json::Value = stdout_resp.json().await.unwrap_or_default();
                if let Some(lines) = stdout_body.get("lines").and_then(|v| v.as_array()) {
                    for line in lines {
                        if let Some(s) = line.as_str() {
                            println!("{}", s);
                        }
                    }
                }
            }

            let stderr_resp = client
                .get(format!("{}/api/v1/processes/{}/stderr", proxy_base, pid))
                .send()
                .await
                .map_err(CliError::Http)?;

            if stderr_resp.status().is_success() {
                let stderr_body: serde_json::Value = stderr_resp.json().await.unwrap_or_default();
                if let Some(lines) = stderr_body.get("lines").and_then(|v| v.as_array()) {
                    for line in lines {
                        if let Some(s) = line.as_str() {
                            eprintln!("{}", s);
                        }
                    }
                }
            }

            // Determine exit code
            if let Some(code) = info.get("exit_code").and_then(|v| v.as_i64()) {
                return Ok(code as i32);
            }
            if let Some(signal) = info.get("signal").and_then(|v| v.as_i64()) {
                return Ok(128 + signal as i32);
            }
            return Ok(1);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
