use comfy_table::Cell;
use serde::Serialize;

use crate::auth::context::CliContext;
use crate::error::Result;
use crate::output::table::new_table;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ListedImage {
    name: String,
    #[serde(rename = "type")]
    image_type: String,
    id: Option<String>,
    reference: String,
    snapshot_id: Option<String>,
    public: bool,
}

pub async fn run(ctx: &CliContext, output_json: bool, cas_only: bool) -> Result<()> {
    let client = ctx.client()?;
    let mut images = if cas_only {
        super::list_all_cas_images(ctx, &client)
            .await?
            .iter()
            .map(cas_image)
            .collect::<Vec<_>>()
    } else {
        let templates_url = super::templates_base_url(ctx)?;
        let (tlsnap, cas) = tokio::try_join!(
            super::list_all_images(ctx, &client, &templates_url),
            super::list_all_cas_images(ctx, &client),
        )?;
        tlsnap
            .iter()
            .map(tlsnap_image)
            .chain(cas.iter().map(cas_image))
            .collect::<Vec<_>>()
    };
    images.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.image_type.cmp(&right.image_type))
            .then_with(|| left.reference.cmp(&right.reference))
    });

    if output_json {
        println!("{}", serde_json::to_string_pretty(&images)?);
        return Ok(());
    }

    if images.is_empty() {
        println!("No {}images found.", if cas_only { "CAS " } else { "" });
        return Ok(());
    }

    let mut table = new_table(&["Name", "Type", "ID / Reference", "Snapshot ID"]);
    for image in &images {
        let identifier = if image.image_type == "CAS" {
            image.reference.as_str()
        } else {
            image.id.as_deref().unwrap_or(&image.reference)
        };
        table.add_row(vec![
            Cell::new(&image.name),
            Cell::new(&image.image_type),
            Cell::new(identifier),
            Cell::new(image.snapshot_id.as_deref().unwrap_or("-")),
        ]);
    }

    println!("{table}");
    let count = images.len();
    println!("{} image{}", count, if count != 1 { "s" } else { "" });
    Ok(())
}

fn tlsnap_image(item: &serde_json::Value) -> ListedImage {
    let name = string_field(item, &["name"]);
    ListedImage {
        reference: name.clone().unwrap_or_else(|| "-".to_string()),
        name: name.unwrap_or_else(|| "-".to_string()),
        image_type: "TLSnap".to_string(),
        id: string_field(item, &["id"]),
        snapshot_id: string_field(item, &["snapshotId", "snapshot_id"]),
        public: bool_field(item, &["public"]),
    }
}

fn cas_image(item: &serde_json::Value) -> ListedImage {
    let id = string_field(item, &["image_id", "imageId"]);
    ListedImage {
        name: string_field(item, &["name"]).unwrap_or_else(|| "-".to_string()),
        image_type: "CAS".to_string(),
        reference: string_field(item, &["image"])
            .or_else(|| id.as_ref().map(|id| format!("cas-v1:{id}")))
            .unwrap_or_else(|| "-".to_string()),
        id,
        snapshot_id: None,
        public: bool_field(item, &["public"]),
    }
}

fn string_field(item: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| item.get(key).and_then(serde_json::Value::as_str))
        .map(str::to_string)
}

fn bool_field(item: &serde_json::Value, keys: &[&str]) -> bool {
    keys.iter()
        .find_map(|key| item.get(key).and_then(serde_json::Value::as_bool))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{cas_image, tlsnap_image};

    #[test]
    fn normalizes_tlsnap_catalog_items() {
        let image = tlsnap_image(&json!({
            "id": "template-1",
            "name": "python",
            "snapshotId": "snapshot-1",
            "public": true,
        }));
        assert_eq!(image.image_type, "TLSnap");
        assert_eq!(image.reference, "python");
        assert_eq!(image.id.as_deref(), Some("template-1"));
        assert_eq!(image.snapshot_id.as_deref(), Some("snapshot-1"));
        assert!(image.public);
    }

    #[test]
    fn normalizes_cas_catalog_items() {
        let image = cas_image(&json!({
            "name": "gpu-python",
            "image_id": "abc123",
            "image": "cas-v1:abc123",
            "public": false,
        }));
        assert_eq!(image.image_type, "CAS");
        assert_eq!(image.reference, "cas-v1:abc123");
        assert_eq!(image.id.as_deref(), Some("abc123"));
        assert_eq!(image.snapshot_id, None);
        assert!(!image.public);
    }
}
