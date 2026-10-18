use std::time::Duration;

use anyhow::anyhow;
use reqwest::header::{AUTHORIZATION, HOST, HeaderMap, HeaderName, HeaderValue};
use tensorlake::sandboxes::tunnel::{TcpTunnel, TcpTunnelOptions};

use crate::auth::context::CliContext;
use crate::commands::sbx::{ResolvedSandboxProxyTarget, resolve_sandbox_proxy_target};
use crate::error::{CliError, Result};

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    remote_port: u16,
    listen_port: Option<u16>,
) -> Result<()> {
    let target = resolve_sandbox_proxy_target(ctx, sandbox_id).await?;
    let headers = build_proxy_headers(ctx, &target)?;
    let tunnel = TcpTunnel::listen(TcpTunnelOptions {
        base_url: target.proxy_base,
        headers,
        remote_port,
        local_host: "127.0.0.1".into(),
        local_port: listen_port.unwrap_or(remote_port),
        connect_timeout: Duration::from_secs(10),
    })
    .await
    .map_err(|error| CliError::Other(error.into()))?;
    let local_addr = tunnel.local_addr();
    eprintln!(
        "Listening on {} and forwarding to sandbox {} port {}. Press Ctrl-C to stop.",
        local_addr, sandbox_id, remote_port
    );
    let mut errors = tunnel.errors();
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { signal.map_err(CliError::Io)?; break; }
            _ = tunnel.closed() => break,
            Ok(error) = errors.recv() => eprintln!("Tunnel connection from {} failed: {}", error.peer_addr, error.message),
        }
    }
    tunnel.close().await;
    eprintln!("Stopping tunnel listener on {}.", local_addr);
    Ok(())
}

fn build_proxy_headers(ctx: &CliContext, target: &ResolvedSandboxProxyTarget) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", ctx.bearer_token()?))
            .map_err(|error| CliError::Other(anyhow!("invalid bearer token header: {}", error)))?,
    );

    if let Some(org_id) = ctx.effective_organization_id() {
        headers.insert(
            HeaderName::from_static("x-forwarded-organization-id"),
            HeaderValue::from_str(&org_id).map_err(|error| {
                CliError::Other(anyhow!("invalid organization id header: {}", error))
            })?,
        );
    }

    if let Some(project_id) = ctx.effective_project_id() {
        headers.insert(
            HeaderName::from_static("x-forwarded-project-id"),
            HeaderValue::from_str(&project_id).map_err(|error| {
                CliError::Other(anyhow!("invalid project id header: {}", error))
            })?,
        );
    }

    if let Some(host) = target.host_override.as_deref() {
        headers.insert(
            HOST,
            HeaderValue::from_str(host)
                .map_err(|error| CliError::Other(anyhow!("invalid host header: {}", error)))?,
        );
    } else {
        headers.insert(
            HeaderName::from_static("x-tensorlake-sandbox-id"),
            HeaderValue::from_str(&target.sandbox_id).map_err(|error| {
                CliError::Other(anyhow!("invalid sandbox id header: {}", error))
            })?,
        );
    }
    if let Some(hint) = target.routing_hint.as_deref() {
        headers.insert(
            HeaderName::from_static("x-tensorlake-route-hint"),
            HeaderValue::from_str(hint).map_err(|error| {
                CliError::Other(anyhow!("invalid route hint header: {}", error))
            })?,
        );
    }

    Ok(headers)
}
