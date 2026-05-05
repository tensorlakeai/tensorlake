use crate::auth::context::CliContext;
use crate::commands::sbx::{format_created_at, sandbox_endpoint};
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, sandbox_id: &str) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

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

    let item = resp.json::<serde_json::Value>().await.map_err(CliError::Http)?;
    print_sandbox_details(&item);
    Ok(())
}

fn print_sandbox_details(item: &serde_json::Value) {
    let id = item
        .get("sandbox_id")
        .or_else(|| item.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let name = item
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let status = item.get("status").and_then(|v| v.as_str()).unwrap_or("-");
    let image = item
        .get("image")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let namespace = item
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("-");

    println!("ID:          {}", id);
    if !name.is_empty() {
        println!("Name:        {}", name);
    }
    println!("Namespace:   {}", namespace);
    println!("Status:      {}", status);
    println!("Image:       {}", image);

    if let Some(resources) = item.get("resources") {
        let cpus = resources
            .get("cpus")
            .and_then(|v| v.as_f64())
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "-".to_string());
        let memory = resources
            .get("memory_mb")
            .and_then(|v| v.as_i64())
            .map(|v| format!("{} MB", v))
            .unwrap_or_else(|| "-".to_string());
        let disk = resources
            .get("disk_mb")
            .or_else(|| resources.get("ephemeral_disk_mb"))
            .and_then(|v| v.as_i64())
            .map(|v| format!("{} MB", v))
            .unwrap_or_else(|| "-".to_string());
        println!("CPUs:        {}", cpus);
        println!("Memory:      {}", memory);
        println!("Disk:        {}", disk);
    }

    if let Some(timeout) = item.get("timeout_secs").and_then(|v| v.as_i64()) {
        println!("Timeout:     {}s", timeout);
    }

    if let Some(entrypoint) = item.get("entrypoint").and_then(|v| v.as_array()) {
        let parts: Vec<&str> = entrypoint
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        if !parts.is_empty() {
            println!("Entrypoint:  {}", parts.join(" "));
        }
    }

    if let Some(secrets) = item.get("secret_names").and_then(|v| v.as_array()) {
        let names: Vec<&str> = secrets.iter().filter_map(|v| v.as_str()).collect();
        if !names.is_empty() {
            println!("Secrets:     {}", names.join(", "));
        }
    }

    if let Some(ports) = item.get("exposed_ports").and_then(|v| v.as_array()) {
        let port_list: Vec<String> = ports
            .iter()
            .filter_map(|v| v.as_u64())
            .map(|p| p.to_string())
            .collect();
        if !port_list.is_empty() {
            println!("Ports:       {}", port_list.join(", "));
        }
    }

    if let Some(url) = item.get("sandbox_url").and_then(|v| v.as_str()) {
        println!("URL:         {}", url);
    }

    if let Some(network) = item.get("network") {
        let internet = network
            .get("allow_internet_access")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        println!("Internet:    {}", if internet { "allowed" } else { "blocked" });

        if let Some(allow) = network.get("allow_out").and_then(|v| v.as_array()) {
            let cidrs: Vec<&str> = allow.iter().filter_map(|v| v.as_str()).collect();
            if !cidrs.is_empty() {
                println!("Allow out:   {}", cidrs.join(", "));
            }
        }

        if let Some(deny) = network.get("deny_out").and_then(|v| v.as_array()) {
            let cidrs: Vec<&str> = deny.iter().filter_map(|v| v.as_str()).collect();
            if !cidrs.is_empty() {
                println!("Deny out:    {}", cidrs.join(", "));
            }
        }
    }

    let created_at = format_created_at(item.get("created_at"));
    println!("Created:     {}", created_at);

    if let Some(terminated_at) = item.get("terminated_at") {
        if !terminated_at.is_null() {
            println!("Terminated:  {}", format_created_at(Some(terminated_at)));
        }
    }

    if let Some(reason) = item
        .get("termination_reason")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        println!("Reason:      {}", reason);
    }

    if let Some(outcome) = item
        .get("outcome")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        println!("Outcome:     {}", outcome);
    }
}
