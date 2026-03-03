use crate::auth::context::CliContext;
use crate::auth::login::run_login_flow;
use crate::error::Result;

pub async fn run(ctx: &CliContext) -> Result<()> {
    run_login_flow(ctx, true).await?;
    Ok(())
}
