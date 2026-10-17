use comfy_table::Cell;
use tensorlake::sandbox_templates::models::SandboxTemplate;

use crate::auth::context::CliContext;
use crate::commands::sbx::format_created_at;
use crate::error::Result;
use crate::output::table::new_table;

/// `tl sbx image ls`: every image of both families, newest first. `--cas` is
/// accepted for compatibility and ignored; the merged catalog always lists
/// CAS and legacy images together, with the Format column telling them apart.
pub async fn run(ctx: &CliContext, output_json: bool, _cas_only: bool) -> Result<()> {
    let catalog = super::sandbox_image_catalog(ctx)?;
    let images = catalog.list().await?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&images)?);
        return Ok(());
    }

    if images.is_empty() {
        println!("No images found.");
        return Ok(());
    }

    let mut table = new_table(&["Name", "Format", "ID", "Snapshot ID", "Created"]);
    for image in &images {
        table.add_row(table_row(image));
    }

    println!("{table}");
    let count = images.len();
    println!("{} image{}", count, if count != 1 { "s" } else { "" });
    Ok(())
}

fn table_row(image: &SandboxTemplate) -> Vec<Cell> {
    let created_at = image
        .created_at
        .as_deref()
        .map(|value| serde_json::Value::String(value.to_string()));
    vec![
        Cell::new(image.name.as_deref().unwrap_or("-")),
        Cell::new(super::format_image_family(
            image.snapshot_format_version.as_deref(),
        )),
        Cell::new(image.id.as_deref().unwrap_or("-")),
        Cell::new(image.snapshot_id.as_deref().unwrap_or("-")),
        Cell::new(format_created_at(created_at.as_ref())),
    ]
}

#[cfg(test)]
mod tests {
    use super::table_row;
    use tensorlake::sandbox_templates::models::SandboxTemplate;

    #[test]
    fn rows_show_name_family_ids_and_creation_time() {
        let image: SandboxTemplate = serde_json::from_value(serde_json::json!({
            "id": "image_abc",
            "name": "team/app",
            "snapshotId": "snap-1",
            "snapshotFormatVersion": "content_addressed_streaming_v1",
            "createdAt": "2020-01-01T00:00:00Z",
        }))
        .unwrap();
        let cells: Vec<String> = table_row(&image)
            .into_iter()
            .map(|cell| cell.content().to_string())
            .collect();
        assert_eq!(cells[..4], ["team/app", "CAS", "image_abc", "snap-1"]);
        assert_eq!(cells[4], "2020-01-01 00:00");

        let bare: SandboxTemplate = serde_json::from_value(serde_json::json!({
            "id": "sandbox_template_1",
            "name": "python",
        }))
        .unwrap();
        let cells: Vec<String> = table_row(&bare)
            .into_iter()
            .map(|cell| cell.content().to_string())
            .collect();
        assert_eq!(cells, ["python", "-", "sandbox_template_1", "-", "-"]);
    }
}
