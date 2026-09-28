use std::time::Duration;

use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_POLL_INTERVAL, ResolvedSandboxProxyTarget, resolve_sandbox_proxy_target,
    wait_for_sandbox_status_every,
};
use crate::error::Result;

/// `tl sbx wait <id>`: poll a sandbox until it is running (ADR 0086). Runs
/// from any process that knows the sandbox id, including after a
/// `tl sbx create --no-wait` or a create whose own wait ran out. Never
/// deletes the sandbox: when the budget runs out the sandbox keeps its place
/// in the queue and the error says how to keep waiting.
pub async fn run(ctx: &CliContext, sandbox_id: &str, timeout: Duration) -> Result<()> {
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    wait_for_sandbox_status_every(
        ctx,
        sandbox_id,
        &format!("Waiting for sandbox {sandbox_id} to be running"),
        "running",
        timeout,
        DEFAULT_SANDBOX_WAIT_POLL_INTERVAL,
    )
    .await?;

    if !is_tty {
        println!("{sandbox_id}");
        return Ok(());
    }
    eprintln!("Sandbox {sandbox_id} is running.");
    if let Ok(target) = resolve_sandbox_proxy_target(ctx, sandbox_id).await {
        eprintln!("{}", format_url_line(&target));
    }
    Ok(())
}

fn format_url_line(target: &ResolvedSandboxProxyTarget) -> String {
    format!(
        "URL:             {}",
        target.sandbox_url.as_deref().unwrap_or(&target.proxy_base)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_line_prefers_the_server_sandbox_url() {
        let target = ResolvedSandboxProxyTarget {
            sandbox_id: "sbx-1".to_string(),
            proxy_base: "https://proxy.example.com".to_string(),
            host_override: None,
            routing_hint: None,
            ingress_endpoint: None,
            sandbox_url: Some("https://sbx-1.sandbox.tensorlake.ai".to_string()),
        };
        assert_eq!(
            format_url_line(&target),
            "URL:             https://sbx-1.sandbox.tensorlake.ai"
        );
    }
}
