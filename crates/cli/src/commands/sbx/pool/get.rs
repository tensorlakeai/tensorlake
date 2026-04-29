use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::commands::sbx::{format_created_at, sandbox_endpoint};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub async fn run(ctx: &CliContext, pool_id: &str) -> Result<()> {
    let pool = fetch_pool(ctx, pool_id).await?;

    let id = pool
        .get("pool_id")
        .or_else(|| pool.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or(pool_id);
    let namespace = pool.get("namespace").and_then(|v| v.as_str()).unwrap_or("-");
    let image = pool.get("image").and_then(|v| v.as_str()).unwrap_or("-");
    let resources = pool.get("resources");
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
        .and_then(|r| r.get("ephemeral_disk_mb"))
        .and_then(|v| v.as_i64())
        .map(|v| format!("{} MB", v))
        .unwrap_or_else(|| "-".to_string());
    let warm = pool
        .get("warm_containers")
        .and_then(|v| v.as_i64())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    let max = pool
        .get("max_containers")
        .and_then(|v| v.as_i64())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    let min = pool
        .get("min_containers")
        .and_then(|v| v.as_i64())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    let timeout = pool
        .get("timeout_secs")
        .and_then(|v| v.as_i64())
        .map(|v| if v == 0 { "—".to_string() } else { format!("{}s", v) })
        .unwrap_or_else(|| "-".to_string());
    let created_at = format_created_at(pool.get("created_at"));
    let updated_at = format_created_at(pool.get("updated_at"));
    let allow_unauth = pool
        .get("allow_unauthenticated_access")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let exposed_ports = pool
        .get("exposed_ports")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|p| p.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();

    println!("Pool: {}", id);
    println!("  Namespace:                       {}", namespace);
    println!("  Image:                           {}", image);
    println!("  CPUs:                            {}", cpus);
    println!("  Memory:                          {}", memory);
    println!("  Ephemeral disk:                  {}", disk);
    println!("  Warm containers:                 {}", warm);
    println!("  Min containers:                  {}", min);
    println!("  Max containers:                  {}", max);
    println!("  Timeout:                         {}", timeout);
    println!("  Allow unauthenticated access:    {}", allow_unauth);
    if !exposed_ports.is_empty() {
        println!("  Exposed ports:                   {}", exposed_ports);
    }
    if let Some(net) = pool.get("network_policy") {
        let internet = net
            .get("allow_internet_access")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        println!("  Internet access:                 {}", internet);
        if let Some(allow_out) = net.get("allow_out").and_then(|v| v.as_array())
            && !allow_out.is_empty()
        {
            let list: Vec<&str> = allow_out.iter().filter_map(|v| v.as_str()).collect();
            println!("  Network allow:                   {}", list.join(", "));
        }
        if let Some(deny_out) = net.get("deny_out").and_then(|v| v.as_array())
            && !deny_out.is_empty()
        {
            let list: Vec<&str> = deny_out.iter().filter_map(|v| v.as_str()).collect();
            println!("  Network deny:                    {}", list.join(", "));
        }
    }
    println!("  Created:                         {}", created_at);
    if updated_at != "-" {
        println!("  Updated:                         {}", updated_at);
    }

    let containers = pool
        .get("containers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if containers.is_empty() {
        println!();
        println!("No containers in this pool yet.");
    } else {
        println!();
        let mut table = new_table(&["Container ID", "State", "Sandbox ID", "Executor"]);
        for c in &containers {
            let cid = c.get("id").and_then(|v| v.as_str()).unwrap_or("-");
            let state = c.get("state").and_then(|v| v.as_str()).unwrap_or("-");
            let sid = c.get("sandbox_id").and_then(|v| v.as_str()).unwrap_or("");
            let exec = c.get("executor_id").and_then(|v| v.as_str()).unwrap_or("-");
            table.add_row(vec![Cell::new(cid), Cell::new(state), Cell::new(sid), Cell::new(exec)]);
        }
        println!("{table}");
    }

    Ok(())
}

/// Fetch full pool detail (including containers). Shared with `update` and
/// `rm` so they can branch on current state without an extra round-trip.
pub async fn fetch_pool(ctx: &CliContext, pool_id: &str) -> Result<serde_json::Value> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandbox-pools/{pool_id}"));
    let resp = client.get(&url).send().await.map_err(CliError::Http)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to fetch sandbox pool {} (HTTP {}): {}",
            pool_id,
            status,
            body
        )));
    }
    resp.json::<serde_json::Value>().await.map_err(CliError::Http)
}
