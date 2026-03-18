use crate::auth::context::CliContext;
use crate::commands::sbx::{create, snapshot};
use crate::error::Result;

pub async fn run(ctx: &CliContext, sandbox_id: &str, timeout: f64) -> Result<()> {
    eprintln!("Cloning sandbox {}...", sandbox_id);

    let snapshot_id = snapshot::create_snapshot(ctx, sandbox_id, timeout).await?;

    eprintln!("Creating new sandbox from snapshot {}...", snapshot_id);

    let sandbox_id = create::create_with_request(
        ctx,
        serde_json::json!({
            "snapshot_id": snapshot_id,
        }),
        true,
    )
    .await?;

    println!("{}", sandbox_id);
    Ok(())
}
