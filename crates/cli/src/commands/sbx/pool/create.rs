use crate::auth::context::CliContext;
use crate::commands::sbx::pool::{PoolBodyArgs, build_pool_create_body};
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub struct CreateArgs<'a> {
    pub image: Option<&'a str>,
    pub cpus: Option<f64>,
    pub memory: Option<i64>,
    pub disk_mb: Option<u64>,
    pub timeout: Option<i64>,
    pub entrypoint: &'a [String],
    pub max_containers: Option<i64>,
    pub warm_containers: Option<i64>,
    pub ports: &'a [u16],
    pub allow_unauthenticated_access: bool,
    pub no_internet: bool,
    pub network_allow: &'a [String],
    pub network_deny: &'a [String],
}

pub async fn run(ctx: &CliContext, args: CreateArgs<'_>) -> Result<()> {
    let CreateArgs {
        image,
        cpus,
        memory,
        disk_mb,
        timeout,
        entrypoint,
        max_containers,
        warm_containers,
        ports,
        allow_unauthenticated_access,
        no_internet,
        network_allow,
        network_deny,
    } = args;

    let body = build_pool_create_body(&PoolBodyArgs {
        image,
        cpus,
        memory_mb: memory,
        disk_mb,
        timeout,
        entrypoint,
        max_containers,
        warm_containers,
        ports,
        allow_unauthenticated_access,
        no_internet,
        network_allow,
        network_deny,
    });

    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandbox-pools");
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to create sandbox pool (HTTP {}): {}",
            status,
            text
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let pool_id = result
        .get("pool_id")
        .or_else(|| result.get("id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!("create response missing pool_id"))
        })?;

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    if is_tty {
        eprintln!("Pool {} created.", pool_id);
        eprintln!();
        eprintln!("Claim a sandbox from this pool:");
        eprintln!("  tl sbx pool claim {pool_id}");
    } else {
        println!("{}", pool_id);
    }

    Ok(())
}
