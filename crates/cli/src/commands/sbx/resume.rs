use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, ResolvedSandboxProxyTarget, resolve_sandbox_proxy_target,
    sandbox_endpoint, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, sandbox_id: &str, wait: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/resume"));

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    if is_tty {
        eprintln!("Resuming sandbox {}...", sandbox_id);
    }

    let resp = client.post(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to resume sandbox (HTTP {}): {}",
            status,
            body
        )));
    }

    if wait {
        wait_for_sandbox_status(
            ctx,
            sandbox_id,
            "Waiting for sandbox to resume",
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
    }

    if is_tty {
        eprintln!("Sandbox {} is running.", sandbox_id);
        if wait {
            if let Ok(target) = resolve_sandbox_proxy_target(ctx, sandbox_id).await {
                eprintln!("{}", format_post_resume_url_line(&target));
            }
        }
    } else {
        println!("{sandbox_id}");
    }
    Ok(())
}

fn format_post_resume_url_line(target: &ResolvedSandboxProxyTarget) -> String {
    format!(
        "URL:             {}",
        target.sandbox_url.as_deref().unwrap_or(&target.proxy_base)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn post_resume_tip_prefers_sandbox_url() {
        let target = ResolvedSandboxProxyTarget {
            sandbox_id: "sbx-123".to_string(),
            proxy_base: "https://proxy.example.com".to_string(),
            host_override: None,
            routing_hint: Some("hint-1".to_string()),
            ingress_endpoint: Some("https://ingress.example.com".to_string()),
            sandbox_url: Some("https://returned.example.com".to_string()),
        };

        assert_eq!(
            format_post_resume_url_line(&target),
            "URL:             https://returned.example.com"
        );
    }
}
