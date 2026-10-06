//! Shared CLI presentation and resumable waits for live resource updates.
use std::{io::IsTerminal, time::Duration};

use tensorlake::{
    Traced,
    error::SdkError,
    sandboxes::{SandboxesClient, models::SandboxInfo},
};

use crate::{
    auth::context::CliContext,
    error::{CliError, Result},
};

pub(super) fn client(ctx: &CliContext) -> Result<SandboxesClient> {
    Ok(SandboxesClient::new(
        ctx.scoped_cloud_client()?
            .with_base_url(&super::resolve_sandbox_lifecycle_url(&ctx.api_url)),
        &ctx.namespace,
        super::is_localhost(&ctx.api_url),
    ))
}

pub(super) fn print_id(info: &SandboxInfo) {
    if !std::io::stdout().is_terminal() {
        println!("{}", info.sandbox_id);
    }
}

pub(super) fn print_requested(info: &SandboxInfo) {
    if let Some(resize) = &info.resource_resize {
        eprintln!(
            "Sandbox {} resize generation {}: {}",
            info.sandbox_id, resize.generation, resize.status
        );
        eprintln!(
            "Requested: {} CPUs, {} MiB memory, {} MiB disk",
            resize.requested.cpus, resize.requested.memory_mb, resize.requested.disk_mb
        );
    }
}

pub(super) fn print_confirmed(info: &SandboxInfo) {
    eprintln!(
        "Confirmed allocation: {} CPUs, {} MiB memory, {} MiB disk",
        info.resources.cpus, info.resources.memory_mb, info.resources.disk_mb
    );
}

pub(super) fn print_completed(info: &SandboxInfo) {
    if let Some(resize) = &info.resource_resize {
        eprintln!(
            "Sandbox {} resize generation {}: {}",
            info.sandbox_id, resize.generation, resize.status
        );
    }
    print_confirmed(info);
}

fn follow_up(sandbox_id: &str, generation: u64) -> String {
    format!(
        "Wait with: tl sbx wait {sandbox_id} --resize {generation} --timeout 300\nInspect with: tl sbx describe {sandbox_id}"
    )
}

pub(super) fn print_follow_up(info: &SandboxInfo, generation: u64) {
    eprintln!("{}", follow_up(&info.sandbox_id, generation));
}

pub(super) async fn wait(
    client: &SandboxesClient,
    sandbox_id: &str,
    generation: u64,
    timeout: Duration,
    initial: Option<&SandboxInfo>,
) -> Result<Traced<SandboxInfo>> {
    let spinner = std::io::stdout()
        .is_terminal()
        .then(|| super::new_spinner("Waiting for resource resize..."));
    let result = client
        .wait_for_resource_resize(sandbox_id, generation, timeout, Duration::from_secs(1))
        .await;
    if let Some(spinner) = spinner {
        spinner.finish_and_clear();
    }
    result.map_err(|error| {
        let error = match error {
            SdkError::SandboxResize(mut error) => {
                if error.info.is_none() {
                    error.info = initial.cloned().map(Box::new);
                }
                if error.reason == "timeout" {
                    error.message = "wait timed out; resize is not cancelled".into();
                    let id = error
                        .info
                        .as_ref()
                        .map_or(error.sandbox_id.as_str(), |i| &i.sandbox_id);
                    let hint = follow_up(id, error.generation);
                    return CliError::Other(anyhow::anyhow!("{error}\n{hint}"));
                }
                SdkError::SandboxResize(error)
            }
            error => error,
        };
        error.into()
    })
}
