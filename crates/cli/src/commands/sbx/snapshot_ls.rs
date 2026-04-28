use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::commands::sbx::{created_at_sort_key, format_created_at, sandbox_endpoint};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub async fn run(ctx: &CliContext) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "snapshots");

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to list snapshots (HTTP {}): {}",
            status,
            body
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let mut snapshots = body
        .get("snapshots")
        .or_else(|| body.get("items"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    snapshots.sort_by(|a, b| {
        let a_created_at = created_at_sort_key(a.get("created_at"));
        let b_created_at = created_at_sort_key(b.get("created_at"));
        b_created_at.cmp(&a_created_at)
    });

    if snapshots.is_empty() {
        println!("No snapshots found.");
        return Ok(());
    }

    let mut table = new_table(&[
        "ID",
        "Status",
        "Content Mode",
        "Sandbox ID",
        "Base Image",
        "Size",
        "Created At",
    ]);

    for snapshot in &snapshots {
        let snapshot_id = snapshot
            .get("snapshot_id")
            .or_else(|| snapshot.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let content_mode = format_content_mode(snapshot);
        let sandbox_id = snapshot
            .get("sandbox_id")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let base_image = snapshot
            .get("base_image")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let size = format_size(snapshot.get("size_bytes"));
        let created_at = format_created_at(snapshot.get("created_at"));

        table.add_row(vec![
            Cell::new(snapshot_id),
            Cell::new(status),
            Cell::new(content_mode),
            Cell::new(sandbox_id),
            Cell::new(base_image),
            Cell::new(size),
            Cell::new(created_at),
        ]);
    }

    println!("{table}");
    let count = snapshots.len();
    println!("{} snapshot{}", count, if count != 1 { "s" } else { "" });

    Ok(())
}

fn format_size(size_bytes: Option<&serde_json::Value>) -> String {
    let Some(size_bytes) = size_bytes.and_then(|v| v.as_i64()) else {
        return "-".to_string();
    };

    if size_bytes >= 1024 * 1024 {
        format!("{:.1} MB", size_bytes as f64 / (1024.0 * 1024.0))
    } else if size_bytes >= 1024 {
        format!("{:.1} KB", size_bytes as f64 / 1024.0)
    } else {
        format!("{} B", size_bytes)
    }
}

fn format_content_mode(snapshot: &serde_json::Value) -> String {
    let raw_mode = snapshot
        .get("content_mode")
        .or_else(|| snapshot.get("snapshot_content_mode"))
        .and_then(|v| v.as_str());

    match raw_mode {
        Some("filesystem") => "filesystem_only".to_string(),
        Some("memory") => "full".to_string(),
        Some(mode) => mode.to_string(),
        None => "-".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::format_content_mode;

    #[test]
    fn format_content_mode_uses_content_mode_field() {
        let snapshot = serde_json::json!({"content_mode": "filesystem_only"});
        assert_eq!(format_content_mode(&snapshot), "filesystem_only");
    }

    #[test]
    fn format_content_mode_uses_legacy_snapshot_content_mode_field() {
        let snapshot = serde_json::json!({"snapshot_content_mode": "full"});
        assert_eq!(format_content_mode(&snapshot), "full");
    }

    #[test]
    fn format_content_mode_normalizes_alias_values() {
        let filesystem_snapshot = serde_json::json!({"content_mode": "filesystem"});
        let memory_snapshot = serde_json::json!({"content_mode": "memory"});
        assert_eq!(format_content_mode(&filesystem_snapshot), "filesystem_only");
        assert_eq!(format_content_mode(&memory_snapshot), "full");
    }

    #[test]
    fn format_content_mode_handles_missing_mode() {
        let snapshot = serde_json::json!({});
        assert_eq!(format_content_mode(&snapshot), "-");
    }
}
