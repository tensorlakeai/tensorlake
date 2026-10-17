//! `tl sbx image build get|wait|cancel`: the Image Service build operation
//! behind CAS images, plus the CAS-only interruption plumbing that `create`,
//! `import` and `register` share when they run a CAS operation.
//!
//! None of this touches legacy images: a `sandbox_template_` reference is
//! refused before any request, and the legacy create/import/register
//! branches never install the Ctrl+C handler below.

use std::sync::{Arc, Mutex};

use comfy_table::Cell;
use tensorlake::{
    image_builds::{
        CancelBuildOutcome, CancelBuildResponse, CancelBuildSelector, ImageBuildClient,
        ImageBuildClientOptions, ImageBuildError, ImageBuildInfo, ImageBuildWaitOptions,
        InterruptSignal, InterruptionOutcome, InterruptionReport,
    },
    sandbox_images::{SandboxImageBuildError, SandboxImageBuildEvent},
};

use crate::{
    auth::context::CliContext,
    error::{CliError, Result},
    output::table::new_table,
};

use super::ImageBuildEventRenderer;

/// The exit status of a CAS operation ended by Ctrl+C: the conventional
/// SIGINT status, so scripts see an interrupt rather than a failure.
pub const INTERRUPT_EXIT_CODE: i32 = 130;

/// The exact command that cancels a build by id.
pub fn cancel_command(build_id: &str) -> String {
    format!("tl sbx image build cancel {build_id}")
}

/// The exact command that cancels a build whose admission response was lost.
pub fn cancel_by_request_command(request_id: &str) -> String {
    format!("tl sbx image build cancel --request-id {request_id}")
}

pub fn image_build_client(ctx: &CliContext) -> Result<ImageBuildClient> {
    ImageBuildClient::new(ImageBuildClientOptions {
        api_url: ctx.api_url.clone(),
        bearer_token: ctx.bearer_token()?,
        use_scope_headers: ctx.personal_access_token.is_some() && ctx.api_key.is_none(),
        organization_id: ctx.effective_organization_id(),
        project_id: ctx.effective_project_id(),
        namespace: ctx.namespace.clone(),
        user_agent: Some(format!(
            "Tensorlake CLI (rust/{})",
            env!("CARGO_PKG_VERSION")
        )),
    })
    .map_err(build_error)
}

/// Ctrl+C handling for one CAS operation. The first Ctrl+C announces the
/// cancellation and triggers the core's bounded handshake; the second forces
/// an immediate exit after saying the remote cancellation might not be
/// confirmed. Installing it takes over SIGINT for the rest of the process,
/// which is why legacy commands never create one.
pub struct CasInterruptGuard {
    interrupt: InterruptSignal,
    build_id: Arc<Mutex<Option<String>>>,
    request_id: Arc<Mutex<Option<String>>>,
    handler: tokio::task::JoinHandle<()>,
}

impl CasInterruptGuard {
    pub fn install() -> Self {
        let interrupt = InterruptSignal::new();
        let build_id: Arc<Mutex<Option<String>>> = Arc::default();
        let request_id: Arc<Mutex<Option<String>>> = Arc::default();
        let handler = {
            let interrupt = interrupt.clone();
            let build_id = build_id.clone();
            let request_id = request_id.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
                let known_build = build_id.lock().unwrap_or_else(|e| e.into_inner()).clone();
                match &known_build {
                    Some(build_id) => eprintln!("\nCancelling build {build_id}..."),
                    None => eprintln!("\nCancelling build (admission pending)..."),
                }
                interrupt.trigger();
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
                let known_build = build_id.lock().unwrap_or_else(|e| e.into_inner()).clone();
                let known_request = request_id.lock().unwrap_or_else(|e| e.into_inner()).clone();
                match (&known_build, &known_request) {
                    (Some(build_id), _) => eprintln!(
                        "\nInterrupted again; remote cancellation of build {build_id} might not be \
                         confirmed. Retry with: {}",
                        cancel_command(build_id)
                    ),
                    (None, Some(request_id)) => eprintln!(
                        "\nInterrupted again before the build was admitted; remote cancellation \
                         might not be confirmed. If it was admitted, cancel it with: {}",
                        cancel_by_request_command(request_id)
                    ),
                    (None, None) => eprintln!(
                        "\nInterrupted again; remote cancellation might not be confirmed."
                    ),
                }
                std::process::exit(INTERRUPT_EXIT_CODE);
            })
        };
        Self {
            interrupt,
            build_id,
            request_id,
            handler,
        }
    }

    pub fn interrupt(&self) -> &InterruptSignal {
        &self.interrupt
    }

    /// Remember the admitted build id for the interruption messages.
    pub fn observe(&self, event: &SandboxImageBuildEvent) {
        if let SandboxImageBuildEvent::Accepted {
            build_id,
            client_request_id,
        } = event
        {
            *self.build_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(build_id.clone());
            *self.request_id.lock().unwrap_or_else(|e| e.into_inner()) = client_request_id.clone();
        }
    }
}

impl Drop for CasInterruptGuard {
    fn drop(&mut self) {
        self.handler.abort();
    }
}

/// Lines printed after an interruption report, telling the user exactly how
/// to finish what the handshake could not confirm.
pub fn interruption_hints(report: &InterruptionReport) -> Vec<String> {
    match (&report.outcome, &report.build_id, &report.client_request_id) {
        (InterruptionOutcome::Unconfirmed(_), Some(build_id), _) => {
            vec![format!(
                "Retry the cancellation with: {}",
                cancel_command(build_id)
            )]
        }
        (InterruptionOutcome::Unconfirmed(_), None, Some(request_id)) => vec![format!(
            "The admission was unresolved; cancel it with: {}",
            cancel_by_request_command(request_id)
        )],
        (InterruptionOutcome::NotAdmitted, _, Some(request_id)) => vec![format!(
            "This is not a confirmed cancellation. If the build is admitted later, cancel it \
             with: {}",
            cancel_by_request_command(request_id)
        )],
        (InterruptionOutcome::AlreadyCompleted(_), _, _) => {
            vec!["Build already completed; the published image stands.".to_string()]
        }
        _ => Vec::new(),
    }
}

/// Map a CAS operation failure to the CLI's error. An interruption has
/// already been reported through the event renderer and ends with the
/// interrupt exit status; conflicts and timeouts print the exact next
/// command.
pub fn cas_operation_error(error: SandboxImageBuildError) -> CliError {
    match error {
        SandboxImageBuildError::Build(error) => build_error(error),
        other => CliError::Other(other.into()),
    }
}

pub fn build_error(error: ImageBuildError) -> CliError {
    match error {
        ImageBuildError::Interrupted(_) => CliError::ExitCode(INTERRUPT_EXIT_CODE),
        ImageBuildError::InProgress(conflict) => {
            let hint = conflict
                .active_build_id
                .as_deref()
                .map(|build_id| format!("\nCancel it with: {}", cancel_command(build_id)))
                .unwrap_or_default();
            CliError::Other(anyhow::anyhow!("{conflict}{hint}"))
        }
        ImageBuildError::Timeout { build_id, waited } => CliError::Other(anyhow::anyhow!(
            "build {build_id} did not finish within {}s of waiting; it may still be running.\n\
             Check it with: tl sbx image build get {build_id}\n\
             Wait again with: tl sbx image build wait {build_id}\n\
             Cancel it with: {}",
            waited.as_secs(),
            cancel_command(&build_id)
        )),
        ImageBuildError::Usage(message) => CliError::usage(message),
        ImageBuildError::Auth(message) => CliError::auth(message),
        other => CliError::Other(anyhow::anyhow!("{other}")),
    }
}

/// `tl sbx image build get BUILD_ID [--json]`.
pub async fn get(ctx: &CliContext, build_id: &str, output_json: bool) -> Result<()> {
    let client = image_build_client(ctx)?;
    let handle = client.get(build_id).await.map_err(build_error)?;
    if output_json {
        println!("{}", serde_json::to_string_pretty(handle.info())?);
        return Ok(());
    }
    print_build_info(handle.info());
    Ok(())
}

/// `tl sbx image build wait BUILD_ID [--json] [--timeout SECS]`: wait for
/// the operation's own result. The record stands until a later build of the
/// image publishes or the image is deleted, which retires it (then the build
/// is not found). Ctrl+C cancels the build, from the first request on: the
/// handler is installed before the lookup, and the lookup runs inside the
/// interruptible wait, so an interrupt during a slow or stalled first
/// request still sends the cancellation (the id is sufficient for it).
pub async fn wait(
    ctx: &CliContext,
    build_id: &str,
    timeout_secs: Option<f64>,
    output_json: bool,
) -> Result<()> {
    let client = image_build_client(ctx)?;
    let timeout = match timeout_secs {
        Some(secs) if secs.is_finite() && secs >= 0.0 => {
            Some(std::time::Duration::from_secs_f64(secs))
        }
        Some(_) => return Err(CliError::usage("--timeout must be a non-negative number")),
        None => None,
    };
    let guard = CasInterruptGuard::install();
    guard.observe(&SandboxImageBuildEvent::Accepted {
        build_id: build_id.to_string(),
        client_request_id: None,
    });
    let mut renderer = ImageBuildEventRenderer::new();
    let result = client
        .wait_build(
            build_id,
            ImageBuildWaitOptions {
                timeout,
                follow_logs: true,
            },
            guard.interrupt(),
            |event| renderer.render(event),
        )
        .await
        .map_err(build_error)?;
    drop(renderer);
    eprintln!(
        "⚙️  Image '{}' registered ({})",
        result.name.as_deref().unwrap_or("-"),
        result.id.as_deref().unwrap_or("-")
    );
    if output_json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}

/// `tl sbx image build cancel BUILD_ID [--json]` or `cancel --request-id
/// REQUEST_ID [--json]`. The build id is the sufficient, documented way;
/// the request id only recovers an admission whose response was lost, and
/// its `not_admitted` answer is an observation, not a cancellation.
pub async fn cancel(
    ctx: &CliContext,
    build_id: Option<&str>,
    request_id: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let selector = match (build_id, request_id) {
        (Some(build_id), None) => CancelBuildSelector::BuildId(build_id.to_string()),
        (None, Some(request_id)) => CancelBuildSelector::ClientRequestId(request_id.to_string()),
        _ => {
            return Err(CliError::usage(
                "pass exactly one of BUILD_ID or --request-id REQUEST_ID",
            ));
        }
    };
    let client = image_build_client(ctx)?;
    let response = client.cancel(selector.clone()).await.map_err(build_error)?;
    if output_json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    for line in cancel_outcome_lines(&selector, &response) {
        println!("{line}");
    }
    Ok(())
}

/// Human-readable cancel outcome, distinguishing a confirmed cancellation
/// from publication winning and from `not_admitted`.
pub fn cancel_outcome_lines(
    selector: &CancelBuildSelector,
    response: &CancelBuildResponse,
) -> Vec<String> {
    let build_id = response
        .build
        .as_ref()
        .map(|build| build.build_id.clone())
        .or_else(|| match selector {
            CancelBuildSelector::BuildId(build_id) => Some(build_id.clone()),
            CancelBuildSelector::ClientRequestId(_) => None,
        })
        .unwrap_or_else(|| "-".to_string());
    match &response.outcome {
        CancelBuildOutcome::Cancelled => vec![format!("Build {build_id} cancelled.")],
        CancelBuildOutcome::AlreadyCancelled => {
            vec![format!("Build {build_id} was already cancelled.")]
        }
        CancelBuildOutcome::AlreadyCompleted => {
            let mut lines = vec![format!("Build already completed ({build_id}).")];
            if let Some(result) = response
                .build
                .as_ref()
                .and_then(|build| build.result.as_ref())
            {
                lines.push(format!(
                    "Image '{}' registered ({})",
                    result.name.as_deref().unwrap_or("-"),
                    result.id.as_deref().unwrap_or("-")
                ));
            }
            lines
        }
        CancelBuildOutcome::AlreadyFailed => {
            let error = response
                .build
                .as_ref()
                .and_then(|build| build.error.clone())
                .unwrap_or_else(|| "no error recorded".to_string());
            vec![format!("Build {build_id} had already failed: {error}")]
        }
        CancelBuildOutcome::NotAdmitted => {
            let request_id = match selector {
                CancelBuildSelector::ClientRequestId(request_id) => request_id.as_str(),
                CancelBuildSelector::BuildId(_) => "-",
            };
            vec![
                format!("No build was admitted for client request id {request_id}."),
                "This is not a cancellation: a delayed admission may still create the build. \
                 Re-run this command later to cancel it."
                    .to_string(),
            ]
        }
        CancelBuildOutcome::Unknown(other) => {
            vec![format!(
                "Build {build_id}: unknown cancel outcome {other:?}"
            )]
        }
    }
}

fn print_build_info(info: &ImageBuildInfo) {
    let mut table = new_table(&["Field", "Value"]);
    let phase = info
        .phase
        .as_ref()
        .map(|phase| phase.as_str().to_string())
        .unwrap_or_else(|| "-".to_string());
    let rows: Vec<(&str, String)> = vec![
        ("Build ID", info.build_id.clone()),
        (
            "Image name",
            info.name.clone().unwrap_or_else(|| "-".to_string()),
        ),
        ("Kind", info.kind.as_str().to_string()),
        ("Status", info.status.as_str().to_string()),
        ("Phase", phase),
        ("Attempt", info.attempt_no.to_string()),
        (
            "Builder sandbox",
            info.builder_sandbox_id
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        ),
        ("Created", format_epoch_ms(info.created_at_ms)),
        ("Updated", format_epoch_ms(info.updated_at_ms)),
        (
            "Operation deadline",
            format_epoch_ms(info.operation_deadline_ms),
        ),
        (
            "Result",
            info.result
                .as_ref()
                .map(|result| {
                    format!(
                        "{} ({})",
                        result.name.as_deref().unwrap_or("-"),
                        result.id.as_deref().unwrap_or("-")
                    )
                })
                .unwrap_or_else(|| "-".to_string()),
        ),
        (
            "Error",
            info.error.clone().unwrap_or_else(|| "-".to_string()),
        ),
    ];
    for (field, value) in rows {
        table.add_row(vec![Cell::new(field), Cell::new(value)]);
    }
    println!("{table}");
}

fn format_epoch_ms(value: Option<u64>) -> String {
    value
        .and_then(|ms| chrono::DateTime::from_timestamp_millis(ms as i64))
        .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tensorlake::image_builds::ImageBuildStatus;

    #[test]
    fn cancel_outcomes_distinguish_completion_and_absence() {
        let by_id = CancelBuildSelector::BuildId("build-1".to_string());
        let cancelled = CancelBuildResponse {
            outcome: CancelBuildOutcome::Cancelled,
            build: None,
        };
        assert_eq!(
            cancel_outcome_lines(&by_id, &cancelled),
            vec!["Build build-1 cancelled.".to_string()]
        );
        let completed = CancelBuildResponse {
            outcome: CancelBuildOutcome::AlreadyCompleted,
            build: None,
        };
        assert_eq!(
            cancel_outcome_lines(&by_id, &completed),
            vec!["Build already completed (build-1).".to_string()]
        );
        let by_request = CancelBuildSelector::ClientRequestId("rid".to_string());
        let not_admitted = CancelBuildResponse {
            outcome: CancelBuildOutcome::NotAdmitted,
            build: None,
        };
        let lines = cancel_outcome_lines(&by_request, &not_admitted);
        assert!(lines[0].contains("No build was admitted for client request id rid"));
        assert!(lines[1].contains("not a cancellation"));
    }

    #[test]
    fn interruption_hints_print_the_exact_next_command() {
        let unconfirmed = InterruptionReport {
            build_id: Some("build-1".to_string()),
            client_request_id: Some("rid".to_string()),
            outcome: InterruptionOutcome::Unconfirmed("timed out".to_string()),
        };
        assert_eq!(
            interruption_hints(&unconfirmed),
            vec!["Retry the cancellation with: tl sbx image build cancel build-1".to_string()]
        );
        let unresolved = InterruptionReport {
            build_id: None,
            client_request_id: Some("rid".to_string()),
            outcome: InterruptionOutcome::NotAdmitted,
        };
        let hints = interruption_hints(&unresolved);
        assert!(hints[0].contains("not a confirmed cancellation"));
        assert!(hints[0].ends_with("tl sbx image build cancel --request-id rid"));
        assert!(
            interruption_hints(&InterruptionReport {
                build_id: Some("build-1".to_string()),
                client_request_id: None,
                outcome: InterruptionOutcome::Cancelled,
            })
            .is_empty()
        );
    }

    #[test]
    fn conflicts_and_timeouts_name_the_cancel_command() {
        let conflict = build_error(ImageBuildError::InProgress(
            tensorlake::image_builds::ImageBuildConflict {
                image_name: Some("img".to_string()),
                active_build_id: Some("build-9".to_string()),
                active_kind: None,
                active_status: Some(ImageBuildStatus::Running),
                message: "Image 'img' already has an unfinished build 'build-9'.".to_string(),
            },
        ));
        assert!(
            conflict
                .to_string()
                .ends_with("Cancel it with: tl sbx image build cancel build-9")
        );
        let timeout = build_error(ImageBuildError::Timeout {
            build_id: "build-1".to_string(),
            waited: std::time::Duration::from_secs(7),
        });
        assert!(
            timeout
                .to_string()
                .contains("tl sbx image build wait build-1")
        );
        assert!(matches!(
            build_error(ImageBuildError::Interrupted(InterruptionReport {
                build_id: None,
                client_request_id: None,
                outcome: InterruptionOutcome::NotAdmitted,
            })),
            CliError::ExitCode(INTERRUPT_EXIT_CODE)
        ));
    }
}
