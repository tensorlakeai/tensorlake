use comfy_table::Cell;

use crate::auth::context::CliContext;
use crate::error::Result;
use crate::output::table::new_table;

pub async fn run(ctx: &CliContext) -> Result<()> {
    let (base_url, _, _) = super::templates_base_url(ctx)?;
    let client = ctx.client()?;

    let items = super::list_all_images(ctx, &client, &base_url).await?;

    if items.is_empty() {
        println!("No images found.");
        return Ok(());
    }

    let mut table = new_table(&["Name", "ID", "Snapshot ID"]);

    for item in &items {
        let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("-");
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("-");
        let snapshot_id = item.get("snapshotId").and_then(|v| v.as_str()).unwrap_or("-");

        table.add_row(vec![Cell::new(name), Cell::new(id), Cell::new(snapshot_id)]);
    }

    println!("{table}");
    let count = items.len();
    println!("{} image{}", count, if count != 1 { "s" } else { "" });

    Ok(())
}
