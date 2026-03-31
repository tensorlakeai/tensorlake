use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, name_or_id: &str) -> Result<()> {
    let (base_url, _, _) = super::templates_base_url(ctx)?;
    let client = ctx.client()?;
    let direct_url = format!("{}/{}", base_url, name_or_id);

    if ctx.debug {
        eprintln!("DEBUG image describe: trying direct GET {}", direct_url);
    }

    let resp = client
        .get(&direct_url)
        .send()
        .await
        .map_err(CliError::Http)?;

    let item = if resp.status().is_success() {
        if ctx.debug {
            eprintln!("DEBUG image describe: direct lookup succeeded");
        }
        resp.json::<serde_json::Value>()
            .await
            .map_err(CliError::Http)?
    } else if resp.status().as_u16() == 404 {
        if ctx.debug {
            eprintln!(
                "DEBUG image describe: direct lookup returned 404, falling back to paginated list"
            );
        }
        super::find_image_item_in_paginated_list(ctx, &client, &base_url, name_or_id)
            .await?
            .ok_or_else(|| {
                CliError::Other(anyhow::anyhow!("image '{}' not found", name_or_id))
            })?
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to fetch image '{}' (HTTP {}): {}",
            name_or_id,
            status,
            body
        )));
    };

    print_image_details(&item);
    Ok(())
}

fn print_image_details(item: &serde_json::Value) {
    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("-");
    let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("-");
    let snapshot_id = item.get("snapshotId").and_then(|v| v.as_str()).unwrap_or("-");

    println!("Name:        {}", name);
    println!("ID:          {}", id);
    println!("Snapshot ID: {}", snapshot_id);

    // Print any additional fields we haven't explicitly handled
    if let Some(obj) = item.as_object() {
        for (key, value) in obj {
            match key.as_str() {
                "name" | "id" | "snapshotId" => {}
                _ => {
                    let display = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    // Capitalize first letter of key for display
                    let label: String = key
                        .chars()
                        .next()
                        .map(|c| c.to_uppercase().to_string())
                        .unwrap_or_default()
                        + &key[1..];
                    println!("{:<12} {}", format!("{}:", label), display);
                }
            }
        }
    }
}
