use crate::auth::context::CliContext;
use crate::commands::sbx::{create, snapshot};
use crate::error::Result;

pub async fn run(ctx: &CliContext, sandbox_id: &str, timeout: f64, times: usize) -> Result<()> {
    if times == 1 {
        eprintln!("Cloning sandbox {}...", sandbox_id);
    } else {
        eprintln!("Cloning sandbox {} into {} copies...", sandbox_id, times);
    }

    let snapshot_id = snapshot::create_snapshot(ctx, sandbox_id, timeout).await?;

    for copy_index in 0..times {
        if times == 1 {
            eprintln!("Creating new sandbox from snapshot {}...", snapshot_id);
        } else {
            eprintln!(
                "Creating sandbox {}/{} from snapshot {}...",
                copy_index + 1,
                times,
                snapshot_id
            );
        }

        let cloned_sandbox_id = create::create_with_request(
            ctx,
            serde_json::json!({
                "snapshot_id": snapshot_id.as_str(),
            }),
            true,
        )
        .await?;

        println!("{}", cloned_sandbox_id);
    }

    Ok(())
}
