use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_IMAGE_DISPLAY_NAME, SandboxFailureDetails, SandboxTimestamp,
    error_details_message, native_ssh, sandbox_endpoint,
};
use crate::error::{CliError, Result};
use serde::Deserialize;

#[derive(Deserialize)]
struct SandboxDescription {
    #[serde(alias = "id")]
    sandbox_id: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    status: Option<String>,
    #[serde(flatten)]
    failure: SandboxFailureDetails,
    resources: Option<SandboxResources>,
    #[serde(alias = "allow_unauthenticated_proxy_access")]
    allow_unauthenticated_access: Option<bool>,
    network: Option<SandboxNetwork>,
    created_at: Option<SandboxTimestamp>,
    terminated_at: Option<SandboxTimestamp>,
    archived_at: Option<SandboxTimestamp>,
    timeout_secs: Option<i64>,
    #[serde(alias = "sandboxUrl")]
    sandbox_url: Option<String>,
    entrypoint: Option<Vec<String>>,
    #[serde(alias = "exposedPorts")]
    exposed_ports: Option<Vec<u64>>,
    outcome: Option<String>,
}

#[derive(Deserialize)]
struct SandboxResources {
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    disk_mb: Option<i64>,
    ephemeral_disk_mb: Option<i64>,
}

#[derive(Deserialize)]
struct SandboxNetwork {
    allow_internet_access: Option<bool>,
    allow_out: Option<Vec<String>>,
    deny_out: Option<Vec<String>>,
}

pub async fn run(ctx: &CliContext, sandbox_id: &str) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return run_archived(ctx, sandbox_id).await;
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to fetch sandbox '{}' (HTTP {}): {}",
            sandbox_id,
            status,
            body
        )));
    }

    let item = resp
        .json::<SandboxDescription>()
        .await
        .map_err(CliError::Http)?;
    print_sandbox_details(&item);
    print_ssh_config_details(&item)?;
    Ok(())
}

async fn run_archived(ctx: &CliContext, sandbox_id: &str) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("archived-sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(CliError::Other(anyhow::anyhow!(
            "sandbox '{}' not found",
            sandbox_id
        )));
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to fetch archived sandbox '{}' (HTTP {}): {}",
            sandbox_id,
            status,
            body
        )));
    }

    let item = resp
        .json::<SandboxDescription>()
        .await
        .map_err(CliError::Http)?;
    print_archived_sandbox_details(&item);
    print_ssh_config_details(&item)?;
    Ok(())
}

fn print_archived_sandbox_details(item: &SandboxDescription) {
    print_sandbox_details(item);
    let archived_at = item
        .archived_at
        .as_ref()
        .map(SandboxTimestamp::format)
        .unwrap_or_default();
    println!("Archived:        {}", archived_at);
}

fn print_sandbox_details(item: &SandboxDescription) {
    let id = item.sandbox_id.as_deref().unwrap_or("-");
    let name = item.name.as_deref().unwrap_or("");
    let status = item.status.as_deref().unwrap_or("-");
    let image = item
        .failure
        .image
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SANDBOX_IMAGE_DISPLAY_NAME);
    let namespace = item.namespace.as_deref().unwrap_or("-");

    println!("ID:              {}", id);
    println!("Name:            {}", name);
    println!("Namespace:       {}", namespace);
    println!("Status:          {}", status);
    println!("Image:           {}", image);

    let resources = item.resources.as_ref();
    let cpus = resources
        .and_then(|r| r.cpus)
        .map(|v| format!("{}", v))
        .unwrap_or_else(|| "-".to_string());
    let memory = resources
        .and_then(|r| r.memory_mb)
        .map(|v| format!("{} MB", v))
        .unwrap_or_else(|| "-".to_string());
    let disk = resources
        .and_then(|r| r.disk_mb.or(r.ephemeral_disk_mb))
        .map(|v| format!("{} MB", v))
        .unwrap_or_else(|| "-".to_string());
    println!("CPUs:            {}", cpus);
    println!("Memory:          {}", memory);
    println!("Disk:            {}", disk);

    let allow_unauthenticated = item.allow_unauthenticated_access.unwrap_or(false);
    println!(
        "Proxy auth:      {}",
        if allow_unauthenticated {
            "unauthenticated"
        } else {
            "required"
        }
    );

    let network = item.network.as_ref();
    let internet = network
        .and_then(|n| n.allow_internet_access)
        .unwrap_or(true);
    println!(
        "Internet:        {}",
        if internet { "allowed" } else { "blocked" }
    );

    println!(
        "Created:         {}",
        item.created_at
            .as_ref()
            .map(SandboxTimestamp::format)
            .unwrap_or_else(|| "-".to_string())
    );

    // Optional fields
    let timeout = item
        .timeout_secs
        .map(|v| format!("{}s", v))
        .unwrap_or_default();
    println!("Timeout:         {}", timeout);

    let sandbox_url = item.sandbox_url.as_deref().unwrap_or("");
    println!("URL:             {}", sandbox_url);

    let entrypoint = item
        .entrypoint
        .as_ref()
        .map(|args| args.join(" "))
        .unwrap_or_default();
    println!("Entrypoint:      {}", entrypoint);

    let ports = item
        .exposed_ports
        .as_ref()
        .map(|ports| {
            ports
                .iter()
                .map(|port| port.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    println!("Ports:           {}", ports);

    let allow_out = network
        .and_then(|n| n.allow_out.as_ref())
        .map(|rules| rules.join(", "))
        .unwrap_or_default();
    println!("Allow out:       {}", allow_out);

    let deny_out = network
        .and_then(|n| n.deny_out.as_ref())
        .map(|rules| rules.join(", "))
        .unwrap_or_default();
    println!("Deny out:        {}", deny_out);

    // Termination group — only shown for terminated sandboxes
    if status.eq_ignore_ascii_case("terminated") {
        let terminated_at = item
            .terminated_at
            .as_ref()
            .map(SandboxTimestamp::format)
            .unwrap_or_default();
        println!("Terminated:      {}", terminated_at);

        let reason = item.failure.termination_reason.as_deref().unwrap_or("");
        println!("Reason:          {}", reason);
        if let Some(details) = item
            .failure
            .error_details
            .as_ref()
            .and_then(error_details_message)
        {
            println!("Error details:   {}", details);
        }

        let outcome = item.outcome.as_deref().unwrap_or("");
        println!("Outcome:         {}", outcome);
    }
}

fn print_ssh_config_details(item: &SandboxDescription) -> Result<()> {
    // Terminated sandboxes have no routable guest. Their diagnosis must remain
    // inspectable even when the server has already removed sandbox_url.
    if item
        .status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case("terminated"))
    {
        return Ok(());
    }
    let id = item
        .sandbox_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("-");
    let name = item.name.as_deref();
    let sandbox_url = item.sandbox_url.as_deref();
    let sandbox = native_ssh::ResolvedSandbox::with_sandbox_url(id, name, sandbox_url)?;
    let config = native_ssh::format_ssh_config(&sandbox, None, None)?;

    println!("SSH Config:");
    print!("{config}");
    Ok(())
}
