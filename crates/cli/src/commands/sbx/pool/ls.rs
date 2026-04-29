use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::commands::sbx::{created_at_sort_key, format_created_at, sandbox_endpoint};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub async fn run(ctx: &CliContext, quiet: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandbox-pools");

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to list sandbox pools (HTTP {}): {}",
            status,
            body
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let mut pools = body
        .get("pools")
        .or_else(|| body.get("items"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    pools.sort_by(|a, b| {
        let a_at = created_at_sort_key(a.get("created_at"));
        let b_at = created_at_sort_key(b.get("created_at"));
        b_at.cmp(&a_at)
    });

    if quiet {
        for p in &pools {
            let id = p
                .get("pool_id")
                .or_else(|| p.get("id"))
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            println!("{}", id);
        }
        return Ok(());
    }

    if pools.is_empty() {
        println!("No sandbox pools found.");
        return Ok(());
    }

    let mut table = new_table(&[
        "Pool ID",
        "Image",
        "CPUs",
        "Memory",
        "Warm",
        "Max",
        "Timeout",
        "Created At",
    ]);

    for p in &pools {
        let id = p
            .get("pool_id")
            .or_else(|| p.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let image = p.get("image").and_then(|v| v.as_str()).unwrap_or("-");
        let resources = p.get("resources");
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
        let warm = p
            .get("warm_containers")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let max = p
            .get("max_containers")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let timeout = p
            .get("timeout_secs")
            .and_then(|v| v.as_i64())
            .map(|v| if v == 0 { "—".to_string() } else { format!("{}s", v) })
            .unwrap_or_else(|| "-".to_string());
        let created_at = format_created_at(p.get("created_at"));

        table.add_row(vec![
            Cell::new(id),
            Cell::new(image),
            Cell::new(&cpus),
            Cell::new(&memory),
            Cell::new(&warm),
            Cell::new(&max),
            Cell::new(&timeout),
            Cell::new(created_at),
        ]);
    }

    println!("{table}");
    let count = pools.len();
    println!("{} pool{}", count, if count != 1 { "s" } else { "" });

    Ok(())
}
