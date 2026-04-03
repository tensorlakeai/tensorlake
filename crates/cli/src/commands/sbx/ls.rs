use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::commands::sbx::{created_at_sort_key, format_created_at, sandbox_endpoint};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub async fn run(ctx: &CliContext, running_only: bool, include_terminated: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandboxes");

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to list sandboxes (HTTP {}): {}",
            status,
            body
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let mut sandboxes = body
        .get("sandboxes")
        .or_else(|| body.get("items"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let terminated_hidden = if include_terminated {
        0
    } else {
        sandboxes
            .iter()
            .filter(|sandbox| is_terminated_sandbox(sandbox))
            .count()
    };

    if !include_terminated {
        sandboxes.retain(is_non_terminated_sandbox);
    }

    if running_only {
        sandboxes.retain(is_running_sandbox);
    }

    sandboxes.sort_by(|a, b| {
        let a_created_at = created_at_sort_key(a.get("created_at"));
        let b_created_at = created_at_sort_key(b.get("created_at"));
        b_created_at.cmp(&a_created_at)
    });

    if sandboxes.is_empty() {
        println!("No sandboxes found.");
        return Ok(());
    }

    let mut table = new_table(&[
        "ID",
        "Name",
        "Status",
        "Image",
        "CPUs",
        "Memory",
        "Disk",
        "Created At",
    ]);

    for s in &sandboxes {
        let id = s
            .get("sandbox_id")
            .or_else(|| s.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let name = s.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let status = s.get("status").and_then(|v| v.as_str()).unwrap_or("-");
        let image = s.get("image").and_then(|v| v.as_str()).unwrap_or("-");

        let resources = s.get("resources");
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

        let created_at = format_created_at(s.get("created_at"));

        table.add_row(vec![
            Cell::new(id),
            Cell::new(name),
            Cell::new(status),
            Cell::new(image),
            Cell::new(&cpus),
            Cell::new(&memory),
            Cell::new(&disk),
            Cell::new(created_at),
        ]);
    }

    println!("{table}");
    let count = sandboxes.len();
    println!(
        "{} sandbox{}, {} terminated hidden (use --all to show)",
        count,
        if count != 1 { "es" } else { "" },
        terminated_hidden
    );

    Ok(())
}

fn is_running_sandbox(sandbox: &serde_json::Value) -> bool {
    sandbox
        .get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|status| status.eq_ignore_ascii_case("running"))
}

fn is_non_terminated_sandbox(sandbox: &serde_json::Value) -> bool {
    !is_terminated_sandbox(sandbox)
}

fn is_terminated_sandbox(sandbox: &serde_json::Value) -> bool {
    sandbox
        .get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|status| status.eq_ignore_ascii_case("terminated"))
}

#[cfg(test)]
mod tests {
    use super::{is_non_terminated_sandbox, is_running_sandbox, is_terminated_sandbox};

    #[test]
    fn running_filter_matches_only_running_status() {
        let running = serde_json::json!({ "status": "running" });
        let terminated = serde_json::json!({ "status": "terminated" });
        let pending = serde_json::json!({ "status": "pending" });

        assert!(is_running_sandbox(&running));
        assert!(!is_running_sandbox(&terminated));
        assert!(!is_running_sandbox(&pending));
    }

    #[test]
    fn default_filter_hides_terminated_sandboxes() {
        let running = serde_json::json!({ "status": "running" });
        let terminated = serde_json::json!({ "status": "terminated" });
        let pending = serde_json::json!({ "status": "pending" });

        assert!(is_non_terminated_sandbox(&running));
        assert!(!is_non_terminated_sandbox(&terminated));
        assert!(is_non_terminated_sandbox(&pending));
    }

    #[test]
    fn terminated_filter_matches_only_terminated_status() {
        let running = serde_json::json!({ "status": "running" });
        let terminated = serde_json::json!({ "status": "terminated" });
        let pending = serde_json::json!({ "status": "pending" });

        assert!(!is_terminated_sandbox(&running));
        assert!(is_terminated_sandbox(&terminated));
        assert!(!is_terminated_sandbox(&pending));
    }
}
