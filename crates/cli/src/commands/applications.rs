use chrono::{TimeZone, Utc};
use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub async fn ls(ctx: &CliContext) -> Result<()> {
    let client = ctx.client()?;
    let resp = client
        .get(format!(
            "{}/v1/namespaces/{}/applications",
            ctx.api_url, ctx.namespace
        ))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        return Err(CliError::Other(anyhow::anyhow!(
            "Failed to fetch applications: HTTP {}",
            resp.status()
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let applications = body
        .get("applications")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Filter out tombstoned applications
    let active: Vec<_> = applications
        .iter()
        .filter(|app| {
            !app.get("tombstoned")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        })
        .collect();

    if active.is_empty() {
        println!("No applications found");
        return Ok(());
    }

    let mut table = new_table(&["Name", "Description", "Deployed At"]);
    for app in &active {
        let name = app.get("name").and_then(|v| v.as_str()).unwrap_or("-");
        let description = app
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let deployed_at = app
            .get("created_at")
            .and_then(|v| v.as_i64())
            .map(|ts| {
                // created_at is Unix timestamp in milliseconds
                Utc.timestamp_millis_opt(ts)
                    .single()
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "-".to_string())
            })
            .unwrap_or_else(|| "-".to_string());

        table.add_row(vec![
            Cell::new(name),
            Cell::new(description),
            Cell::new(&deployed_at),
        ]);
    }

    println!("{table}");
    let count = active.len();
    if count == 1 {
        println!("1 application");
    } else {
        println!("{} applications", count);
    }

    // Show link to applications page
    if let (Some(org_id), Some(proj_id)) =
        (ctx.effective_organization_id(), ctx.effective_project_id())
    {
        println!(
            "\nView all applications: {}/organizations/{}/projects/{}/applications",
            ctx.cloud_url, org_id, proj_id
        );
    }

    Ok(())
}
