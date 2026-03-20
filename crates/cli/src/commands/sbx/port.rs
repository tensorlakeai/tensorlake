use std::collections::BTreeSet;

use comfy_table::Cell;
use serde::Serialize;

use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SandboxProxySettings {
    allow_unauthenticated_access: bool,
    exposed_ports: Vec<u16>,
    sandbox_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct PatchSandboxRequest {
    allow_unauthenticated_access: bool,
    exposed_ports: Vec<u16>,
}

pub async fn list(ctx: &CliContext, sandbox_id: &str) -> Result<()> {
    let sandbox = get_sandbox(ctx, sandbox_id).await?;
    let settings = parse_proxy_settings(&sandbox);

    println!("Sandbox: {}", sandbox_id);
    println!(
        "Unauthenticated access: {}",
        if settings.allow_unauthenticated_access {
            "enabled"
        } else {
            "disabled"
        }
    );
    if let Some(sandbox_url) = settings.sandbox_url {
        println!("Management URL: {}", sandbox_url);
    }

    if settings.exposed_ports.is_empty() {
        println!("No user-exposed ports.");
        return Ok(());
    }

    let mut table = new_table(&["Port"]);
    for port in settings.exposed_ports {
        table.add_row(vec![Cell::new(port)]);
    }
    println!("{table}");

    Ok(())
}

pub async fn expose(ctx: &CliContext, sandbox_id: &str, ports: &[u16]) -> Result<()> {
    let sandbox = get_sandbox(ctx, sandbox_id).await?;
    let settings = parse_proxy_settings(&sandbox);
    let desired_ports = merge_ports(&settings.exposed_ports, ports);

    if desired_ports == settings.exposed_ports && settings.allow_unauthenticated_access {
        println!(
            "Sandbox {} already exposes {}",
            sandbox_id,
            format_ports(&desired_ports)
        );
        return Ok(());
    }

    let updated = patch_proxy_settings(ctx, sandbox_id, true, &desired_ports).await?;
    let updated_settings = parse_proxy_settings(&updated);

    println!(
        "Exposed {} on sandbox {}",
        format_ports(&updated_settings.exposed_ports),
        sandbox_id
    );
    println!("Unauthenticated access enabled");
    Ok(())
}

pub async fn remove(ctx: &CliContext, sandbox_id: &str, ports: &[u16]) -> Result<()> {
    let sandbox = get_sandbox(ctx, sandbox_id).await?;
    let settings = parse_proxy_settings(&sandbox);
    let desired_ports = remove_ports(&settings.exposed_ports, ports);
    let allow_unauthenticated_access = !desired_ports.is_empty();

    if desired_ports == settings.exposed_ports
        && allow_unauthenticated_access == settings.allow_unauthenticated_access
    {
        if desired_ports.is_empty() {
            println!("Sandbox {} has no user-exposed ports.", sandbox_id);
        } else {
            println!(
                "Sandbox {} still exposes {}",
                sandbox_id,
                format_ports(&desired_ports)
            );
        }
        return Ok(());
    }

    let updated = patch_proxy_settings(
        ctx,
        sandbox_id,
        allow_unauthenticated_access,
        &desired_ports,
    )
    .await?;
    let updated_settings = parse_proxy_settings(&updated);

    if updated_settings.exposed_ports.is_empty() {
        println!("Removed all user-exposed ports from sandbox {}", sandbox_id);
        println!("Unauthenticated access disabled");
    } else {
        println!(
            "Sandbox {} now exposes {}",
            sandbox_id,
            format_ports(&updated_settings.exposed_ports)
        );
        println!("Unauthenticated access enabled");
    }

    Ok(())
}

async fn get_sandbox(ctx: &CliContext, sandbox_id: &str) -> Result<serde_json::Value> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to get sandbox (HTTP {}): {}",
            status,
            body
        )));
    }

    resp.json().await.map_err(CliError::Http)
}

async fn patch_proxy_settings(
    ctx: &CliContext,
    sandbox_id: &str,
    allow_unauthenticated_access: bool,
    exposed_ports: &[u16],
) -> Result<serde_json::Value> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));
    let body = PatchSandboxRequest {
        allow_unauthenticated_access,
        exposed_ports: exposed_ports.to_vec(),
    };

    let resp = client
        .patch(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to update sandbox ports (HTTP {}): {}",
            status,
            body
        )));
    }

    resp.json().await.map_err(CliError::Http)
}

fn parse_proxy_settings(sandbox: &serde_json::Value) -> SandboxProxySettings {
    let allow_unauthenticated_access = sandbox
        .get("allow_unauthenticated_access")
        .or_else(|| sandbox.get("allow_unauthenticated_proxy_access"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    let exposed_ports = sorted_unique_ports(
        sandbox
            .get("exposed_ports")
            .or_else(|| sandbox.get("exposedPorts"))
            .and_then(|value| value.as_array())
            .map(|ports| {
                ports
                    .iter()
                    .filter_map(|value| value.as_u64())
                    .filter_map(|port| u16::try_from(port).ok())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );

    let sandbox_url = sandbox
        .get("sandbox_url")
        .or_else(|| sandbox.get("sandboxUrl"))
        .and_then(|value| value.as_str())
        .map(str::to_string);

    SandboxProxySettings {
        allow_unauthenticated_access,
        exposed_ports,
        sandbox_url,
    }
}

fn merge_ports(current: &[u16], requested: &[u16]) -> Vec<u16> {
    let mut ports = current.to_vec();
    ports.extend_from_slice(requested);
    sorted_unique_ports(ports)
}

fn remove_ports(current: &[u16], requested: &[u16]) -> Vec<u16> {
    let requested: BTreeSet<u16> = requested.iter().copied().collect();
    sorted_unique_ports(
        current
            .iter()
            .copied()
            .filter(|port| !requested.contains(port))
            .collect(),
    )
}

fn sorted_unique_ports(ports: Vec<u16>) -> Vec<u16> {
    ports
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn format_ports(ports: &[u16]) -> String {
    ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{merge_ports, parse_proxy_settings, remove_ports};

    #[test]
    fn parse_proxy_settings_defaults_to_empty_ports() {
        let settings = parse_proxy_settings(&serde_json::json!({}));

        assert!(!settings.allow_unauthenticated_access);
        assert!(settings.exposed_ports.is_empty());
        assert!(settings.sandbox_url.is_none());
    }

    #[test]
    fn merge_ports_sorts_and_deduplicates() {
        let merged = merge_ports(&[8080, 3000], &[8080, 9090, 80]);

        assert_eq!(merged, vec![80, 3000, 8080, 9090]);
    }

    #[test]
    fn remove_ports_drops_requested_ports() {
        let remaining = remove_ports(&[80, 3000, 8080, 9090], &[8080, 1234, 80]);

        assert_eq!(remaining, vec![3000, 9090]);
    }
}
