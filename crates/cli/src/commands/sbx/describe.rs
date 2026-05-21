use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_IMAGE_DISPLAY_NAME, format_created_at, native_ssh, sandbox_endpoint,
};
use crate::error::{CliError, Result};

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
        .json::<serde_json::Value>()
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
        .json::<serde_json::Value>()
        .await
        .map_err(CliError::Http)?;
    print_archived_sandbox_details(&item);
    print_ssh_config_details(&item)?;
    Ok(())
}

fn print_archived_sandbox_details(item: &serde_json::Value) {
    print_sandbox_details(item);
    let archived_at = item
        .get("archived_at")
        .filter(|v| !v.is_null())
        .map(|v| format_created_at(Some(v)))
        .unwrap_or_default();
    println!("Archived:        {}", archived_at);
}

fn print_sandbox_details(item: &serde_json::Value) {
    let id = item
        .get("sandbox_id")
        .or_else(|| item.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let status = item.get("status").and_then(|v| v.as_str()).unwrap_or("-");
    let image = item
        .get("image")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SANDBOX_IMAGE_DISPLAY_NAME);
    let namespace = item
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("-");

    println!("ID:              {}", id);
    println!("Name:            {}", name);
    println!("Namespace:       {}", namespace);
    println!("Status:          {}", status);
    println!("Image:           {}", image);

    let resources = item.get("resources");
    let cpus = resources
        .and_then(|r| r.get("cpus"))
        .and_then(|v| v.as_f64())
        .map(|v| format!("{}", v))
        .unwrap_or_else(|| "-".to_string());
    let memory = resources
        .and_then(|r| r.get("memory_mb"))
        .and_then(|v| v.as_i64())
        .map(|v| format!("{} MB", v))
        .unwrap_or_else(|| "-".to_string());
    let disk = resources
        .and_then(|r| r.get("disk_mb").or_else(|| r.get("ephemeral_disk_mb")))
        .and_then(|v| v.as_i64())
        .map(|v| format!("{} MB", v))
        .unwrap_or_else(|| "-".to_string());
    println!("CPUs:            {}", cpus);
    println!("Memory:          {}", memory);
    println!("Disk:            {}", disk);

    let allow_unauthenticated = item
        .get("allow_unauthenticated_access")
        .or_else(|| item.get("allow_unauthenticated_proxy_access"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    println!(
        "Proxy auth:      {}",
        if allow_unauthenticated {
            "unauthenticated"
        } else {
            "required"
        }
    );

    let network = item.get("network");
    let internet = network
        .and_then(|n| n.get("allow_internet_access"))
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    println!(
        "Internet:        {}",
        if internet { "allowed" } else { "blocked" }
    );

    println!(
        "Created:         {}",
        format_created_at(item.get("created_at"))
    );

    // Optional fields
    let timeout = item
        .get("timeout_secs")
        .and_then(|v| v.as_i64())
        .map(|v| format!("{}s", v))
        .unwrap_or_default();
    println!("Timeout:         {}", timeout);

    let sandbox_url = item
        .get("sandbox_url")
        .or_else(|| item.get("sandboxUrl"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    println!("URL:             {}", sandbox_url);

    let entrypoint = item
        .get("entrypoint")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();
    println!("Entrypoint:      {}", entrypoint);

    let secrets = item
        .get("secret_names")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    println!("Secrets:         {}", secrets);

    let ports = item
        .get("exposed_ports")
        .or_else(|| item.get("exposedPorts"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64())
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    println!("Ports:           {}", ports);

    let allow_out = network
        .and_then(|n| n.get("allow_out"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    println!("Allow out:       {}", allow_out);

    let deny_out = network
        .and_then(|n| n.get("deny_out"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    println!("Deny out:        {}", deny_out);

    // Termination group — only shown for terminated sandboxes
    if status.eq_ignore_ascii_case("terminated") {
        let terminated_at = item
            .get("terminated_at")
            .filter(|v| !v.is_null())
            .map(|v| format_created_at(Some(v)))
            .unwrap_or_default();
        println!("Terminated:      {}", terminated_at);

        let reason = item
            .get("termination_reason")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        println!("Reason:          {}", reason);

        let outcome = item.get("outcome").and_then(|v| v.as_str()).unwrap_or("");
        println!("Outcome:         {}", outcome);
    }
}

fn print_ssh_config_details(item: &serde_json::Value) -> Result<()> {
    let id = item
        .get("sandbox_id")
        .or_else(|| item.get("id"))
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-");
    let name = item.get("name").and_then(|v| v.as_str());
    let sandbox = native_ssh::ResolvedSandbox::new(id, name);
    let config = native_ssh::format_ssh_config(&sandbox, None, None)?;

    println!("SSH Config:");
    print!("{config}");
    Ok(())
}
