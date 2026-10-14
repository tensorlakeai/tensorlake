//! `tl sbx fs` — attach/detach/list file systems on a specific
//! sandbox.
//!
//! These are sandbox-lifecycle operations: they hit the sandbox lifecycle API
//! (`{sandbox_endpoint}/sandboxes/{id}/file_systems`), not the platform
//! file-system registry. Registering and deleting file systems
//! themselves lives under the top-level `tl fs` command.
//!
//! The server accepts an attach or detach before the dataplane has changed
//! anything inside the guest: the 200 carries the generation the update was
//! accepted as (`file_system_generation`), and the sandbox later reports the
//! generation the dataplane applied (`applied_file_system_generation`) or the
//! rejection it recorded (`file_system_update_error`). `attach` and `detach`
//! therefore poll the sandbox until the accepted generation is applied,
//! rejected, or the sandbox stops running, so a successful exit means the
//! mount is live in the guest, not merely accepted.

use std::future::Future;
use std::time::Duration;

use comfy_table::Cell;
use reqwest::Response;
use tensorlake::sandboxes::models::{FileSystemMount, FileSystemUpdateError, SandboxInfo};

use crate::auth::context::CliContext;
use crate::commands::sbx::{DEFAULT_SANDBOX_WAIT_POLL_INTERVAL, new_spinner, sandbox_endpoint};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

/// How `attach` and `detach` wait for the dataplane to apply the change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileSystemUpdateWaitOptions {
    /// Return as soon as the server accepts the change, without polling.
    pub no_wait: bool,
    /// Maximum time to wait for the dataplane to apply the change.
    pub timeout: Duration,
}

/// Parse a sandbox lifecycle response into typed [`SandboxInfo`], surfacing a
/// useful error on non-2xx.
async fn parse_sandbox_response(resp: Response, action: &str) -> Result<SandboxInfo> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to {action} (HTTP {}): {}",
            status,
            body
        )));
    }
    resp.json().await.map_err(CliError::Http)
}

async fn fetch_sandbox(ctx: &CliContext, sandbox_id: &str) -> Result<SandboxInfo> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));
    let resp = client.get(&url).send().await.map_err(CliError::Http)?;
    parse_sandbox_response(resp, "fetch sandbox").await
}

/// Render a mount the way the `--filesystem <name>[@<snapshot_id>]:<path>`
/// input syntax spells it.
fn format_mount_source(mount: &FileSystemMount) -> String {
    match &mount.snapshot_id {
        Some(snapshot_id) => format!("{}@{snapshot_id}", mount.file_system_id),
        None => mount.file_system_id.clone(),
    }
}

/// Render a sandbox's currently-mounted file systems as a table. A
/// snapshot-pinned mount shows its pin as `<name>@<snapshot_id>`, mirroring
/// the `--filesystem <name>[@<snapshot_id>]:<path>[:<opts>]` input syntax.
fn print_mounts_table(mounts: &[FileSystemMount]) {
    if mounts.is_empty() {
        println!("No file systems mounted.");
        return;
    }

    let mut table = new_table(&["File System ID", "Mount Path", "Options"]);
    for mount in mounts {
        let mut options = Vec::new();
        if mount.read_only {
            options.push("ro".to_string());
        }
        if mount.prefetch {
            options.push("prefetch".to_string());
        }
        if let Some(owner) = &mount.owner {
            options.push(format!("owner={owner}"));
        }
        table.add_row(vec![
            Cell::new(format_mount_source(mount)),
            Cell::new(mount.mount_path.as_str()),
            Cell::new(options.join(",")),
        ]);
    }
    println!("{table}");
}

/// One line describing a recorded rejection, for `fs ls` and for the
/// `attach`/`detach` failure path.
fn format_update_error(error: &FileSystemUpdateError) -> String {
    let mut message = format!("{}: {}", error.reason, error.error_message);
    if !error.rejected_file_systems.is_empty() {
        let rejected: Vec<String> = error
            .rejected_file_systems
            .iter()
            .map(|mount| format!("{}:{}", format_mount_source(mount), mount.mount_path))
            .collect();
        message.push_str(&format!(" (rejected mounts: {})", rejected.join(", ")));
    }
    message
}

/// Where a file-system update stands relative to the generation the server
/// accepted it as.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FileSystemUpdateProgress {
    /// The dataplane applied this generation (or a later one, which
    /// supersedes it).
    Applied,
    /// The server does not report applied generations, so there is nothing
    /// to wait for.
    Unconfirmed,
    /// The dataplane refused this exact generation; the previous mounts stay.
    Rejected(FileSystemUpdateError),
    /// The sandbox is no longer running, so the update will never apply.
    SandboxNotRunning(String),
    /// The dataplane has not reported this generation yet.
    Pending {
        /// The newest generation the dataplane has applied.
        applied: u64,
    },
}

/// Classify one sandbox observation against the generation an attach or
/// detach was accepted as. A rejection recorded for exactly that generation
/// wins over everything else: it is the most specific answer, and it stays
/// on the sandbox even if the sandbox later stops. A rejection for another
/// generation is stale and ignored.
pub(crate) fn file_system_update_progress(
    info: &SandboxInfo,
    target: u64,
) -> FileSystemUpdateProgress {
    if let Some(error) = &info.file_system_update_error
        && error.generation == Some(target)
    {
        return FileSystemUpdateProgress::Rejected(error.clone());
    }
    if info.status != "running" {
        return FileSystemUpdateProgress::SandboxNotRunning(info.status.clone());
    }
    match info.applied_file_system_generation {
        None => FileSystemUpdateProgress::Unconfirmed,
        Some(applied) if applied >= target => FileSystemUpdateProgress::Applied,
        Some(applied) => FileSystemUpdateProgress::Pending { applied },
    }
}

/// Terminal result of waiting for a file-system update.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FileSystemUpdateWait {
    Applied(SandboxInfo),
    /// The server reports no applied generation; the update is accepted but
    /// the CLI cannot confirm it reached the guest.
    Unconfirmed(SandboxInfo),
    Rejected {
        info: SandboxInfo,
        error: FileSystemUpdateError,
    },
    SandboxNotRunning(SandboxInfo),
    /// The deadline passed with the update still pending. `last` is the most
    /// recent observation, so the caller can report both generations.
    TimedOut {
        target: u64,
        last: SandboxInfo,
    },
}

/// Poll `fetch` every `poll_interval` until the update accepted as `target`
/// is applied, rejected, or moot, or until `timeout` elapses. `accepted` is
/// the response that accepted the update; it is classified first so an
/// already-applied update costs no extra request. `on_pending` runs after
/// each pending observation (the spinner update).
pub(crate) async fn wait_for_file_system_update<F, Fut>(
    accepted: SandboxInfo,
    target: u64,
    mut fetch: F,
    timeout: Duration,
    poll_interval: Duration,
    mut on_pending: impl FnMut(u64),
) -> Result<FileSystemUpdateWait>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<SandboxInfo>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    let mut info = accepted;
    loop {
        match file_system_update_progress(&info, target) {
            FileSystemUpdateProgress::Applied => return Ok(FileSystemUpdateWait::Applied(info)),
            FileSystemUpdateProgress::Unconfirmed => {
                return Ok(FileSystemUpdateWait::Unconfirmed(info));
            }
            FileSystemUpdateProgress::Rejected(error) => {
                return Ok(FileSystemUpdateWait::Rejected { info, error });
            }
            FileSystemUpdateProgress::SandboxNotRunning(_) => {
                return Ok(FileSystemUpdateWait::SandboxNotRunning(info));
            }
            FileSystemUpdateProgress::Pending { applied } => on_pending(applied),
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(FileSystemUpdateWait::TimedOut { target, last: info });
        }
        tokio::time::sleep(poll_interval.min(remaining)).await;
        info = fetch().await?;
    }
}

/// Wait for the update `accepted` describes, driving a spinner on a TTY.
/// `change` reads like "mount file system 'skills' at '/mnt/skills'".
async fn await_file_system_update(
    ctx: &CliContext,
    sandbox_id: &str,
    accepted: SandboxInfo,
    change: &str,
    timeout: Duration,
) -> Result<FileSystemUpdateWait> {
    let Some(target) = accepted.file_system_generation else {
        // The server predates file-system generations; nothing to poll for.
        return Ok(FileSystemUpdateWait::Unconfirmed(accepted));
    };

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let waiting_message = format!("Waiting for sandbox {sandbox_id} to {change}");
    let spinner = is_tty.then(|| new_spinner(&format!("{waiting_message}...")));

    let outcome = wait_for_file_system_update(
        accepted,
        target,
        || fetch_sandbox(ctx, sandbox_id),
        timeout,
        DEFAULT_SANDBOX_WAIT_POLL_INTERVAL,
        |applied| {
            if let Some(spinner) = &spinner {
                spinner.set_message(format!(
                    "{waiting_message} (applied generation {applied}, waiting for {target})..."
                ));
            }
        },
    )
    .await;
    if let Some(spinner) = &spinner {
        spinner.finish_and_clear();
    }
    outcome
}

/// Print the result of an attach or detach. `success_line` is the sentence
/// printed once the change is applied (or accepted, when the server cannot
/// confirm application); `change` is the same phrase the spinner used.
fn report_file_system_update(
    outcome: FileSystemUpdateWait,
    sandbox_id: &str,
    change: &str,
    success_line: &str,
    timeout: Duration,
    output_json: bool,
) -> Result<()> {
    match outcome {
        FileSystemUpdateWait::Applied(info) => {
            if output_json {
                println!("{}", serde_json::to_string_pretty(&info.file_systems)?);
                return Ok(());
            }
            println!("{success_line}");
            print_mounts_table(&info.file_systems);
            Ok(())
        }
        FileSystemUpdateWait::Unconfirmed(info) => {
            let note = format!(
                "Note: the server accepted the change but does not report when the dataplane \
                 applies it; run `tl sbx fs ls {sandbox_id}` to check."
            );
            if output_json {
                println!("{}", serde_json::to_string_pretty(&info.file_systems)?);
                eprintln!("{note}");
                return Ok(());
            }
            println!("{success_line}");
            println!("{note}");
            print_mounts_table(&info.file_systems);
            Ok(())
        }
        FileSystemUpdateWait::Rejected { error, .. } => {
            if output_json {
                println!("{}", serde_json::to_string_pretty(&error)?);
                eprintln!(
                    "Error: sandbox {sandbox_id} could not {change}: {}",
                    format_update_error(&error)
                );
                return Err(CliError::ExitCode(1));
            }
            Err(CliError::Other(anyhow::anyhow!(
                "sandbox {sandbox_id} could not {change}: {}; the previously mounted file \
                 systems are unchanged",
                format_update_error(&error)
            )))
        }
        FileSystemUpdateWait::SandboxNotRunning(info) => {
            let mut message = format!(
                "sandbox {sandbox_id} is {} and will not {change}",
                info.status
            );
            if let Some(reason) = &info.termination_reason {
                message.push_str(&format!(" (termination reason: {reason})"));
            }
            Err(CliError::Other(anyhow::anyhow!(message)))
        }
        FileSystemUpdateWait::TimedOut { target, last } => {
            let applied = last
                .applied_file_system_generation
                .map_or_else(|| "none".to_string(), |applied| applied.to_string());
            Err(CliError::Other(anyhow::anyhow!(
                "sandbox {sandbox_id} has not finished the request to {change} after {}s: the \
                 change is file-system generation {target}, the dataplane has applied {applied}. \
                 The change is still in progress; run `tl sbx fs ls {sandbox_id}` to check again \
                 or rerun with a longer --timeout.",
                timeout.as_secs()
            )))
        }
    }
}

/// Build the attach request body. The option keys are present only when set:
/// older servers deserialize attach bodies with `deny_unknown_fields` and
/// reject an explicit `false` (or an unknown pin field). A snapshot pin
/// without `--read-only` is rejected here, mirroring the server's 400 so
/// users fail fast offline.
fn build_attach_body(
    file_system_id: &str,
    mount_path: &str,
    read_only: bool,
    prefetch: bool,
    snapshot_id: Option<&str>,
    owner: Option<&str>,
) -> Result<serde_json::Value> {
    if snapshot_id.is_some() && !read_only {
        return Err(CliError::usage(
            "--snapshot requires --read-only: snapshot-pinned mounts are read-only".to_string(),
        ));
    }
    let mut body = serde_json::json!({
        "file_system_id": file_system_id,
        "mount_path": mount_path,
    });
    if read_only {
        body["read_only"] = serde_json::Value::Bool(true);
    }
    if prefetch {
        body["prefetch"] = serde_json::Value::Bool(true);
    }
    if let Some(snapshot_id) = snapshot_id {
        body["snapshot_id"] = serde_json::Value::String(snapshot_id.to_string());
    }
    if let Some(owner) = owner {
        body["owner"] = serde_json::Value::String(owner.to_string());
    }
    Ok(body)
}

/// Resolve the accepted response into the outcome to report: either the
/// accepted state itself (`--no-wait`) or the result of waiting for the
/// dataplane to apply it.
async fn settle_file_system_update(
    ctx: &CliContext,
    sandbox_id: &str,
    accepted: SandboxInfo,
    change: &str,
    wait: FileSystemUpdateWaitOptions,
) -> Result<FileSystemUpdateWait> {
    if wait.no_wait {
        return Ok(FileSystemUpdateWait::Applied(accepted));
    }
    await_file_system_update(ctx, sandbox_id, accepted, change, wait.timeout).await
}

#[allow(clippy::too_many_arguments)]
pub async fn attach(
    ctx: &CliContext,
    sandbox_id: &str,
    file_system_id: &str,
    mount_path: &str,
    read_only: bool,
    prefetch: bool,
    snapshot_id: Option<&str>,
    owner: Option<&str>,
    wait: FileSystemUpdateWaitOptions,
    output_json: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/file_systems"));
    let body = build_attach_body(
        file_system_id,
        mount_path,
        read_only,
        prefetch,
        snapshot_id,
        owner,
    )?;

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;
    let accepted = parse_sandbox_response(resp, "attach file system").await?;

    let change = format!("mount file system '{file_system_id}' at '{mount_path}'");
    let outcome = settle_file_system_update(ctx, sandbox_id, accepted, &change, wait).await?;
    let success_line = format!(
        "Attached file system '{}' at '{}' on sandbox {}.",
        file_system_id, mount_path, sandbox_id
    );
    report_file_system_update(
        outcome,
        sandbox_id,
        &change,
        &success_line,
        wait.timeout,
        output_json,
    )
}

pub async fn detach(
    ctx: &CliContext,
    sandbox_id: &str,
    mount_path: &str,
    wait: FileSystemUpdateWaitOptions,
    output_json: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}/file_systems"));
    let body = serde_json::json!({ "mount_path": mount_path });

    let resp = client
        .delete(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;
    let accepted = parse_sandbox_response(resp, "detach file system").await?;

    let change = format!("unmount '{mount_path}'");
    let outcome = settle_file_system_update(ctx, sandbox_id, accepted, &change, wait).await?;
    let success_line = format!(
        "Detached file system at '{}' from sandbox {}.",
        mount_path, sandbox_id
    );
    report_file_system_update(
        outcome,
        sandbox_id,
        &change,
        &success_line,
        wait.timeout,
        output_json,
    )
}

/// Lines `fs ls` prints under the mount table: an in-flight update and the
/// last recorded rejection, when the server reports them.
fn format_update_status_lines(info: &SandboxInfo) -> Vec<String> {
    let mut lines = Vec::new();
    if let (Some(desired), Some(applied)) = (
        info.file_system_generation,
        info.applied_file_system_generation,
    ) && applied < desired
        && info
            .file_system_update_error
            .as_ref()
            .is_none_or(|error| error.generation != Some(desired))
    {
        lines.push(format!(
            "A file-system change is still being applied (generation {desired}; the dataplane \
             has applied {applied})."
        ));
    }
    if let Some(error) = &info.file_system_update_error {
        lines.push(format!(
            "The last file-system change was rejected; the mounts above are unchanged. {}",
            format_update_error(error)
        ));
    }
    lines
}

pub async fn list(ctx: &CliContext, sandbox_id: &str, output_json: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;
    let info = parse_sandbox_response(resp, "list sandbox file systems").await?;

    let status_lines = format_update_status_lines(&info);
    if output_json {
        println!("{}", serde_json::to_string_pretty(&info.file_systems)?);
        for line in &status_lines {
            eprintln!("{line}");
        }
        return Ok(());
    }

    print_mounts_table(&info.file_systems);
    for line in &status_lines {
        println!("{line}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    fn sandbox(
        status: &str,
        desired: Option<u64>,
        applied: Option<u64>,
        error: Option<serde_json::Value>,
    ) -> SandboxInfo {
        let mut value = serde_json::json!({
            "sandbox_id": "sbx-1",
            "namespace": "ns",
            "status": status,
            "resources": {"cpus": 1.0, "memory_mb": 1024, "disk_mb": 10240},
            "file_systems": [
                {"file_system_id": "skills", "mount_path": "/mnt/skills", "read_only": true}
            ],
        });
        if let Some(desired) = desired {
            value["file_system_generation"] = desired.into();
        }
        if let Some(applied) = applied {
            value["applied_file_system_generation"] = applied.into();
        }
        if let Some(error) = error {
            value["file_system_update_error"] = error;
        }
        serde_json::from_value(value).unwrap()
    }

    fn rejection(generation: Option<u64>) -> serde_json::Value {
        let mut error = serde_json::json!({
            "reason": "ConfigurationError",
            "error_message": "mount at /mnt/data overlaps the registered mount",
            "rejected_file_systems": [
                {"file_system_id": "data", "mount_path": "/mnt/data"}
            ],
            "rejected_at": 1_760_400_000_000_u64,
        });
        if let Some(generation) = generation {
            error["generation"] = generation.into();
        }
        error
    }

    /// A fetch that hands out scripted observations in order and records
    /// how many were taken.
    fn scripted_fetch(
        observations: Vec<SandboxInfo>,
    ) -> (
        impl FnMut() -> std::future::Ready<Result<SandboxInfo>>,
        Arc<Mutex<usize>>,
    ) {
        let queue = Arc::new(Mutex::new(VecDeque::from(observations)));
        let polls = Arc::new(Mutex::new(0_usize));
        let counter = Arc::clone(&polls);
        let fetch = move || {
            *counter.lock().unwrap() += 1;
            let next = queue
                .lock()
                .unwrap()
                .pop_front()
                .expect("the wait polled more often than the script allows");
            std::future::ready(Ok(next))
        };
        (fetch, polls)
    }

    const FAST: Duration = Duration::from_millis(1);
    const PATIENT: Duration = Duration::from_secs(30);

    #[test]
    fn progress_is_applied_once_the_dataplane_reaches_the_target() {
        let info = sandbox("running", Some(4), Some(4), None);
        assert_eq!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::Applied
        );
        // A later generation supersedes the target.
        let info = sandbox("running", Some(5), Some(5), None);
        assert_eq!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::Applied
        );
    }

    #[test]
    fn progress_is_pending_while_the_dataplane_lags() {
        let info = sandbox("running", Some(4), Some(3), None);
        assert_eq!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::Pending { applied: 3 }
        );
    }

    #[test]
    fn progress_is_unconfirmed_when_the_server_reports_no_applied_generation() {
        let info = sandbox("running", Some(4), None, None);
        assert_eq!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::Unconfirmed
        );
    }

    #[test]
    fn progress_reports_a_rejection_of_the_target_generation() {
        let info = sandbox("running", Some(4), Some(3), Some(rejection(Some(4))));
        match file_system_update_progress(&info, 4) {
            FileSystemUpdateProgress::Rejected(error) => {
                assert_eq!(error.reason, "ConfigurationError");
                assert_eq!(error.generation, Some(4));
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn progress_ignores_a_stale_rejection_from_an_earlier_generation() {
        let info = sandbox("running", Some(4), Some(4), Some(rejection(Some(3))));
        assert_eq!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::Applied
        );
        let info = sandbox("running", Some(4), Some(3), Some(rejection(Some(2))));
        assert_eq!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::Pending { applied: 3 }
        );
    }

    #[test]
    fn progress_reports_a_sandbox_that_left_running() {
        let info = sandbox("terminated", Some(4), Some(3), None);
        assert_eq!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::SandboxNotRunning("terminated".to_string())
        );
        // A rejection recorded for the target is the more specific answer.
        let info = sandbox("terminated", Some(4), Some(3), Some(rejection(Some(4))));
        assert!(matches!(
            file_system_update_progress(&info, 4),
            FileSystemUpdateProgress::Rejected(_)
        ));
    }

    #[tokio::test]
    async fn wait_returns_without_polling_when_the_response_is_already_applied() {
        let accepted = sandbox("running", Some(4), Some(4), None);
        let (fetch, polls) = scripted_fetch(Vec::new());
        let outcome =
            wait_for_file_system_update(accepted.clone(), 4, fetch, PATIENT, FAST, |_| {})
                .await
                .unwrap();
        assert_eq!(outcome, FileSystemUpdateWait::Applied(accepted));
        assert_eq!(*polls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn wait_polls_until_the_target_generation_is_applied() {
        let accepted = sandbox("running", Some(4), Some(3), None);
        let applied = sandbox("running", Some(4), Some(4), None);
        let (fetch, polls) = scripted_fetch(vec![
            sandbox("running", Some(4), Some(3), None),
            applied.clone(),
        ]);
        let pending_seen = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&pending_seen);
        let outcome = wait_for_file_system_update(accepted, 4, fetch, PATIENT, FAST, |applied| {
            seen.lock().unwrap().push(applied)
        })
        .await
        .unwrap();
        assert_eq!(outcome, FileSystemUpdateWait::Applied(applied));
        assert_eq!(*polls.lock().unwrap(), 2);
        assert_eq!(*pending_seen.lock().unwrap(), vec![3, 3]);
    }

    #[tokio::test]
    async fn wait_reports_a_rejection_recorded_while_polling() {
        let accepted = sandbox("running", Some(4), Some(3), None);
        let rejected = sandbox("running", Some(4), Some(3), Some(rejection(Some(4))));
        let (fetch, polls) = scripted_fetch(vec![rejected.clone()]);
        let outcome = wait_for_file_system_update(accepted, 4, fetch, PATIENT, FAST, |_| {})
            .await
            .unwrap();
        match outcome {
            FileSystemUpdateWait::Rejected { info, error } => {
                assert_eq!(info, rejected);
                assert_eq!(
                    error.error_message,
                    "mount at /mnt/data overlaps the registered mount"
                );
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
        assert_eq!(*polls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn wait_stops_when_the_sandbox_leaves_running() {
        let accepted = sandbox("running", Some(4), Some(3), None);
        let terminated = sandbox("terminated", Some(4), Some(3), None);
        let (fetch, _) = scripted_fetch(vec![terminated.clone()]);
        let outcome = wait_for_file_system_update(accepted, 4, fetch, PATIENT, FAST, |_| {})
            .await
            .unwrap();
        assert_eq!(outcome, FileSystemUpdateWait::SandboxNotRunning(terminated));
    }

    #[tokio::test]
    async fn wait_times_out_with_the_last_observation() {
        let accepted = sandbox("running", Some(4), Some(2), None);
        let last = sandbox("running", Some(4), Some(3), None);
        let (fetch, polls) = scripted_fetch(vec![last.clone(); 64]);
        let outcome = wait_for_file_system_update(
            accepted,
            4,
            fetch,
            Duration::from_millis(20),
            FAST,
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(outcome, FileSystemUpdateWait::TimedOut { target: 4, last });
        let polls = *polls.lock().unwrap();
        assert!(
            polls >= 1,
            "the wait must observe the sandbox before giving up"
        );
    }

    #[tokio::test]
    async fn wait_is_unconfirmed_without_polling_when_applied_generation_is_absent() {
        let accepted = sandbox("running", Some(4), None, None);
        let (fetch, polls) = scripted_fetch(Vec::new());
        let outcome =
            wait_for_file_system_update(accepted.clone(), 4, fetch, PATIENT, FAST, |_| {})
                .await
                .unwrap();
        assert_eq!(outcome, FileSystemUpdateWait::Unconfirmed(accepted));
        assert_eq!(*polls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn wait_propagates_fetch_errors() {
        let accepted = sandbox("running", Some(4), Some(3), None);
        let fetch = || std::future::ready(Err(CliError::Other(anyhow::anyhow!("boom"))));
        let error = wait_for_file_system_update(accepted, 4, fetch, PATIENT, FAST, |_| {})
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("boom"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn report_rejection_names_the_change_and_the_reason() {
        let info = sandbox("running", Some(4), Some(3), Some(rejection(Some(4))));
        let error = info.file_system_update_error.clone().unwrap();
        let result = report_file_system_update(
            FileSystemUpdateWait::Rejected { info, error },
            "sbx-1",
            "mount file system 'data' at '/mnt/data'",
            "unused",
            PATIENT,
            false,
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("sbx-1 could not mount file system 'data' at '/mnt/data'"));
        assert!(message.contains("ConfigurationError: mount at /mnt/data overlaps"));
        assert!(message.contains("rejected mounts: data:/mnt/data"));
        assert!(message.contains("previously mounted file systems are unchanged"));
    }

    #[test]
    fn report_timeout_shows_both_generations() {
        let last = sandbox("running", Some(4), Some(3), None);
        let result = report_file_system_update(
            FileSystemUpdateWait::TimedOut { target: 4, last },
            "sbx-1",
            "unmount '/mnt/data'",
            "unused",
            Duration::from_secs(120),
            false,
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("after 120s"),
            "unexpected message: {message}"
        );
        assert!(
            message.contains("generation 4"),
            "unexpected message: {message}"
        );
        assert!(
            message.contains("has applied 3"),
            "unexpected message: {message}"
        );
        assert!(
            message.contains("tl sbx fs ls sbx-1"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn report_sandbox_not_running_includes_the_termination_reason() {
        let mut info = sandbox("terminated", Some(4), Some(3), None);
        info.termination_reason = Some("Timeout".to_string());
        let result = report_file_system_update(
            FileSystemUpdateWait::SandboxNotRunning(info),
            "sbx-1",
            "unmount '/mnt/data'",
            "unused",
            PATIENT,
            false,
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("sbx-1 is terminated and will not unmount '/mnt/data'"));
        assert!(message.contains("termination reason: Timeout"));
    }

    #[test]
    fn ls_status_lines_describe_pending_and_rejected_updates() {
        let info = sandbox("running", Some(4), Some(3), None);
        let lines = format_update_status_lines(&info);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("generation 4"));
        assert!(lines[0].contains("has applied 3"));

        let info = sandbox("running", Some(4), Some(3), Some(rejection(Some(4))));
        let lines = format_update_status_lines(&info);
        assert_eq!(lines.len(), 1, "a rejected generation is not also pending");
        assert!(lines[0].contains("rejected"));
        assert!(lines[0].contains("ConfigurationError: mount at /mnt/data overlaps"));

        let info = sandbox("running", Some(4), Some(4), None);
        assert!(format_update_status_lines(&info).is_empty());

        let info = sandbox("running", None, None, None);
        assert!(format_update_status_lines(&info).is_empty());
    }

    #[test]
    fn attach_body_omits_unset_mount_modes() {
        let body = build_attach_body("skills", "/mnt/skills", false, false, None, None).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
            })
        );
    }

    #[test]
    fn attach_body_includes_snapshot_pin_with_read_only() {
        let body = build_attach_body(
            "skills",
            "/mnt/skills",
            true,
            false,
            Some("0abc123def"),
            None,
        )
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
                "snapshot_id": "0abc123def",
            })
        );
    }

    #[test]
    fn attach_body_rejects_snapshot_pin_without_read_only() {
        let error = build_attach_body(
            "skills",
            "/mnt/skills",
            false,
            true,
            Some("0abc123def"),
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("snapshot-pinned mounts are read-only"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn attach_body_includes_owner_only_when_set() {
        let body =
            build_attach_body("skills", "/mnt/skills", false, false, None, Some("agent")).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "owner": "agent",
            })
        );
    }
}
