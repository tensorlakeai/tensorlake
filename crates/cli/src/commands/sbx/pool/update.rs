use crate::auth::context::CliContext;
use crate::commands::sbx::pool::get::fetch_pool;
use crate::commands::sbx::pool::{PoolBodyArgs, merge_pool_update_body};
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub struct UpdateArgs<'a> {
    pub pool_id: &'a str,
    pub image: Option<&'a str>,
    pub cpus: Option<f64>,
    pub memory: Option<i64>,
    pub disk_mb: Option<u64>,
    pub timeout: Option<i64>,
    pub entrypoint: &'a [String],
    pub max_containers: Option<i64>,
    pub warm_containers: Option<i64>,
}

pub async fn run(ctx: &CliContext, args: UpdateArgs<'_>) -> Result<()> {
    let UpdateArgs {
        pool_id,
        image,
        cpus,
        memory,
        disk_mb,
        timeout,
        entrypoint,
        max_containers,
        warm_containers,
    } = args;

    let current = fetch_pool(ctx, pool_id).await?;

    let body = merge_pool_update_body(
        &current,
        &PoolBodyArgs {
            image,
            cpus,
            memory_mb: memory,
            disk_mb,
            timeout,
            entrypoint,
            max_containers,
            warm_containers,
            ports: &[],
            allow_unauthenticated_access: false,
            no_internet: false,
            network_allow: &[],
            network_deny: &[],
        },
    );

    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandbox-pools/{pool_id}"));
    let resp = client
        .put(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to update sandbox pool {} (HTTP {}): {}",
            pool_id,
            status,
            text
        )));
    }

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    if is_tty {
        eprintln!("Pool {} updated.", pool_id);
    } else {
        println!("{}", pool_id);
    }
    Ok(())
}
