use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::error::Result;
use crate::output::table::new_table;

pub async fn run(ctx: &CliContext, output_json: bool) -> Result<()> {
    let base_url = super::templates_base_url(ctx)?;
    let client = ctx.client()?;

    let mut items = super::list_all_images(ctx, &client, &base_url).await?;

    if output_json {
        for item in &mut items {
            let inferred_type = image_type(item);
            if let Some(object) = item.as_object_mut() {
                object
                    .entry("type")
                    .or_insert_with(|| serde_json::Value::String(inferred_type.to_string()));
            }
        }
        println!("{}", serde_json::to_string_pretty(&items)?);
        return Ok(());
    }

    if items.is_empty() {
        println!("No images found.");
        return Ok(());
    }

    let mut table = new_table(&["Name", "Type", "ID", "Snapshot ID"]);

    for item in &items {
        let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("-");
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("-");
        let snapshot_id = item
            .get("snapshotId")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let image_type = image_type(item);

        table.add_row(vec![
            Cell::new(name),
            Cell::new(image_type),
            Cell::new(id),
            Cell::new(snapshot_id),
        ]);
    }

    println!("{table}");
    let count = items.len();
    println!("{} image{}", count, if count != 1 { "s" } else { "" });

    Ok(())
}

fn image_type(item: &serde_json::Value) -> &'static str {
    let format = item
        .get("snapshotFormatVersion")
        .or_else(|| item.get("snapshot_format_version"))
        .or_else(|| item.get("rootfsFormat"))
        .or_else(|| item.get("type"))
        .and_then(|value| value.as_str());

    match format {
        Some("content_addressed_streaming_v1" | "cas-v1" | "cas" | "CAS") => "CAS",
        // The sandbox-template endpoint predates an explicit format field.
        // Those records are tlsnap images; only the newer CAS records carry
        // the content-addressed format marker.
        _ => "TLSnap",
    }
}

#[cfg(test)]
mod tests {
    use super::image_type;
    use serde_json::json;

    #[test]
    fn labels_content_addressed_images_as_cas() {
        for item in [
            json!({"snapshotFormatVersion": "content_addressed_streaming_v1"}),
            json!({"snapshot_format_version": "content_addressed_streaming_v1"}),
            json!({"rootfsFormat": "cas-v1"}),
        ] {
            assert_eq!(image_type(&item), "CAS");
        }
    }

    #[test]
    fn labels_archive_and_legacy_images_as_tlsnap() {
        assert_eq!(
            image_type(&json!({"snapshotFormatVersion": "durable_archive_v1"})),
            "TLSnap"
        );
        assert_eq!(image_type(&json!({"name": "legacy-image"})), "TLSnap");
    }
}
