use serde::Deserialize;

use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_sandbox_path, sandbox_endpoint};
use crate::error::{CliError, Result};

const REQUEST_TIMEOUT_HEADER: &str = "X-Tensorlake-Request-Timeout-Ms";

#[derive(Debug, Deserialize)]
struct CopySandboxResponse {
    source_sandbox_id: String,
    sandboxes: Vec<CopiedSandbox>,
}

#[derive(Debug, Deserialize)]
struct CopiedSandbox {
    #[serde(alias = "id")]
    sandbox_id: String,
    status: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    termination_reason: Option<String>,
}

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    times: usize,
    timeout: Option<f64>,
) -> Result<()> {
    if parse_sandbox_path(sandbox_id).0.is_some() {
        return Err(CliError::usage(
            "Copying a running sandbox expects a sandbox ID or name, not a sandbox file path.",
        ));
    }

    if times == 1 {
        eprintln!("Copying sandbox {sandbox_id}...");
    } else {
        eprintln!("Copying sandbox {sandbox_id} into {times} copies...");
    }

    let response = request_sandbox_copy(ctx, sandbox_id, times, timeout).await?;
    if response.sandboxes.is_empty() {
        return Err(CliError::Other(anyhow::anyhow!(
            "copy endpoint returned no copied sandboxes for source {}",
            response.source_sandbox_id
        )));
    }

    let unexpected: Vec<&CopiedSandbox> = response
        .sandboxes
        .iter()
        .filter(|sandbox| sandbox.status != "running")
        .collect();
    if !unexpected.is_empty() {
        return Err(CliError::Other(anyhow::anyhow!(
            "copy endpoint returned non-running sandboxes for source {}: {}",
            response.source_sandbox_id,
            summarize_sandboxes(&unexpected)
        )));
    }

    println!("{}", created_sandboxes_message(&response.sandboxes));

    Ok(())
}

async fn request_sandbox_copy(
    ctx: &CliContext,
    sandbox_id: &str,
    times: usize,
    timeout: Option<f64>,
) -> Result<CopySandboxResponse> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/copy"));
    let mut request = client.post(&url).query(&[("times", times)]);
    if let Some(timeout) = timeout {
        let timeout_ms = timeout_to_millis(timeout)?;
        request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms.to_string());
    }

    let resp = request.send().await.map_err(CliError::Http)?;
    let status = resp.status();
    let body = resp.text().await.map_err(CliError::Http)?;
    let parsed = serde_json::from_str::<CopySandboxResponse>(&body);

    if !status.is_success() {
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to copy sandbox {} (HTTP {}): {}",
            sandbox_id,
            status,
            match parsed {
                Ok(response) => summarize_copy_response(&response),
                Err(_) => body,
            }
        )));
    }

    parsed.map_err(|err| {
        CliError::Other(anyhow::anyhow!(
            "failed to parse sandbox copy response: {err}; body: {body}"
        ))
    })
}

fn timeout_to_millis(timeout: f64) -> Result<u64> {
    if !timeout.is_finite() || timeout <= 0.0 {
        return Err(CliError::usage("`--timeout` must be greater than 0."));
    }
    let millis = timeout * 1000.0;
    if millis > u64::MAX as f64 {
        return Err(CliError::usage("`--timeout` is too large."));
    }
    Ok(millis.ceil() as u64)
}

fn summarize_copy_response(response: &CopySandboxResponse) -> String {
    if response.sandboxes.is_empty() {
        return format!(
            "source {} returned no copied sandboxes",
            response.source_sandbox_id
        );
    }
    let sandboxes: Vec<&CopiedSandbox> = response.sandboxes.iter().collect();
    format!(
        "source {}: {}",
        response.source_sandbox_id,
        summarize_sandboxes(&sandboxes)
    )
}

fn summarize_sandboxes(sandboxes: &[&CopiedSandbox]) -> String {
    sandboxes
        .iter()
        .map(|sandbox| {
            let reason = sandbox
                .reason
                .as_deref()
                .or(sandbox.termination_reason.as_deref())
                .map(|reason| format!(": {reason}"))
                .unwrap_or_default();
            format!("{} ({}){}", sandbox.sandbox_id, sandbox.status, reason)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn created_sandboxes_message(sandboxes: &[CopiedSandbox]) -> String {
    let ids = sandboxes
        .iter()
        .map(|sandbox| sandbox.sandbox_id.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    format!("Created sandboxes: {ids}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_sandboxes_message_lists_ids() {
        let sandboxes = vec![
            CopiedSandbox {
                sandbox_id: "sbx-1".to_string(),
                status: "running".to_string(),
                reason: None,
                termination_reason: None,
            },
            CopiedSandbox {
                sandbox_id: "sbx-2".to_string(),
                status: "running".to_string(),
                reason: None,
                termination_reason: None,
            },
        ];

        assert_eq!(
            created_sandboxes_message(&sandboxes),
            "Created sandboxes: sbx-1, sbx-2"
        );
    }
}
