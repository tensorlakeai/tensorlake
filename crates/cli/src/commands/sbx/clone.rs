use crate::auth::context::CliContext;
use crate::commands::sbx::copy;
use crate::error::Result;

pub async fn run(ctx: &CliContext, sandbox_id: &str, timeout: f64, times: usize) -> Result<()> {
    copy::run_sandbox_copy(ctx, sandbox_id, times, Some(timeout), "Cloning").await
}
