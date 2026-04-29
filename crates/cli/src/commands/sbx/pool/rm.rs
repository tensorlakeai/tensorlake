use crate::auth::context::CliContext;
use crate::commands::sbx::pool::get::fetch_pool;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, pool_id: &str, force: bool) -> Result<()> {
    let active = active_sandbox_count(ctx, pool_id).await?;

    if active > 0 {
        let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
        if !force {
            if !is_tty {
                return Err(CliError::Other(anyhow::anyhow!(
                    "pool {} has {} active sandbox{}; pass --force to terminate them",
                    pool_id,
                    active,
                    if active == 1 { "" } else { "es" }
                )));
            }

            let prompt = format!(
                "Pool {} has {} active sandbox{}. Terminate them and delete the pool?",
                pool_id,
                active,
                if active == 1 { "" } else { "es" }
            );
            let confirmed = dialoguer::Confirm::new()
                .with_prompt(prompt)
                .default(false)
                .interact()
                .map_err(|_| CliError::Cancelled)?;
            if !confirmed {
                return Err(CliError::Cancelled);
            }
        }
    }

    let force_flag = force || active > 0;
    let client = ctx.client()?;
    let suffix = if force_flag {
        format!("sandbox-pools/{pool_id}?force=true")
    } else {
        format!("sandbox-pools/{pool_id}")
    };
    let url = sandbox_endpoint(ctx, &suffix);
    let resp = client.delete(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to delete sandbox pool {} (HTTP {}): {}",
            pool_id,
            status,
            body
        )));
    }

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    if is_tty {
        eprintln!("Deleted sandbox pool {}", pool_id);
    } else {
        println!("{}", pool_id);
    }
    Ok(())
}

/// Count containers in the pool that have been claimed (have a sandbox_id).
/// Used to decide whether to prompt the user before deletion.
async fn active_sandbox_count(ctx: &CliContext, pool_id: &str) -> Result<usize> {
    let pool = fetch_pool(ctx, pool_id).await?;
    Ok(pool
        .get("containers")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter(|c| {
                    c.get("sandbox_id")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .is_some()
                })
                .count()
        })
        .unwrap_or(0))
}
