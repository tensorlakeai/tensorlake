use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

/// `tl sbx image describe <name|id>`: one lookup through the merged catalog.
/// A `sandbox_template_` id is a legacy image, an `image_` id a CAS image,
/// anything else a name resolved in the Image Service first and then the
/// platform registry.
pub async fn run(ctx: &CliContext, name_or_id: &str) -> Result<()> {
    let catalog = super::sandbox_image_catalog(ctx)?;
    let image = catalog
        .find(name_or_id)
        .await?
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("image '{}' not found", name_or_id)))?;
    print_image_details(&serde_json::to_value(&image)?);
    Ok(())
}

fn print_image_details(item: &serde_json::Value) {
    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("-");
    let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("-");
    let snapshot_id = item
        .get("snapshotId")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let format = super::format_image_family(
        item.get("snapshotFormatVersion")
            .and_then(serde_json::Value::as_str),
    );

    println!("Name:        {}", name);
    println!("ID:          {}", id);
    println!("Snapshot ID: {}", snapshot_id);
    println!("Format:      {}", format);

    // Print additional fields, skipping the ones already shown above.
    // Dockerfile gets special multi-line formatting.
    if let Some(obj) = item.as_object() {
        for (key, value) in obj {
            match key.as_str() {
                "name" | "id" | "snapshotId" => {}
                "dockerfile" => {
                    if let Some(df) = value.as_str()
                        && !df.is_empty()
                    {
                        print_dockerfile(df);
                    }
                }
                _ => {
                    let display = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let label = capitalize_key(key);
                    println!("{:<13}{}", format!("{}:", label), display);
                }
            }
        }
    }
}

fn print_dockerfile(dockerfile: &str) {
    println!("Dockerfile:");
    for line in dockerfile.lines() {
        println!("  {}", line);
    }
}

fn capitalize_key(key: &str) -> String {
    let mut chars = key.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}
