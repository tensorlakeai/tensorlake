pub mod create;
pub mod describe;
pub mod import;
pub mod ls;
pub mod register;
pub mod rm;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use indicatif::{ProgressBar, ProgressStyle};
use tensorlake::{
    sandbox_image_catalog::SandboxImageCatalog, sandbox_images::SandboxImageBuildEvent,
    sandbox_templates::SandboxTemplatesClient,
};

const BUILD_CONTEXT_PROGRESS_PREFIX: &str = "Creating build context archive:";
const UPLOAD_CONTEXT_PROGRESS_PREFIX: &str = "Uploading build context archive:";

pub struct ImageBuildEventRenderer {
    progress: Option<ProgressBar>,
}

impl ImageBuildEventRenderer {
    pub fn new() -> Self {
        Self { progress: None }
    }

    pub fn render(&mut self, event: SandboxImageBuildEvent) {
        match event {
            SandboxImageBuildEvent::Status(message) if is_progress_status(&message) => {
                self.render_progress(&message);
            }
            SandboxImageBuildEvent::Status(message) => {
                self.finish_progress_line();
                eprintln!("⚙️  {message}");
            }
            SandboxImageBuildEvent::BuildLog { message, .. } => {
                self.finish_progress_line();
                eprintln!("{message}");
            }
            SandboxImageBuildEvent::Warning(message) => {
                self.finish_progress_line();
                eprintln!("⚠️  {message}");
            }
        }
    }

    fn render_progress(&mut self, message: &str) {
        let progress = self.progress.get_or_insert_with(|| {
            let progress = ProgressBar::new_spinner();
            progress.set_style(ProgressStyle::with_template("{msg}").unwrap());
            progress
        });
        progress.set_message(format!("⚙️  {message}"));
        progress.tick();
    }

    fn finish_progress_line(&mut self) {
        if let Some(progress) = self.progress.take() {
            progress.finish_and_clear();
        }
    }
}

impl Drop for ImageBuildEventRenderer {
    fn drop(&mut self) {
        self.finish_progress_line();
    }
}

fn is_progress_status(message: &str) -> bool {
    message.starts_with(BUILD_CONTEXT_PROGRESS_PREFIX)
        || message.starts_with(UPLOAD_CONTEXT_PROGRESS_PREFIX)
}

pub fn org_and_project(ctx: &CliContext) -> Result<(String, String)> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required for --image"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required for --image"))?;
    Ok((org_id, proj_id))
}

/// The explicit organization/project scope PAT callers address; API keys are
/// self-scoping and use the token-scoped routes.
fn template_scope(ctx: &CliContext) -> Result<Option<(String, String)>> {
    if ctx.api_key.is_some() {
        return Ok(None);
    }
    org_and_project(ctx).map(Some)
}

pub fn sandbox_templates_client(ctx: &CliContext) -> Result<SandboxTemplatesClient> {
    let client = ctx.scoped_cloud_client()?;
    Ok(match template_scope(ctx)? {
        Some((org_id, proj_id)) => SandboxTemplatesClient::new(client, org_id, proj_id),
        None => SandboxTemplatesClient::new_api_key_scoped(client),
    })
}

/// The merged image catalog: platform templates (legacy images) and the
/// Image Service (CAS images) behind one lookup, delete and list.
pub fn sandbox_image_catalog(ctx: &CliContext) -> Result<SandboxImageCatalog> {
    let client = ctx.scoped_cloud_client()?;
    Ok(SandboxImageCatalog::new(
        client,
        &ctx.api_url,
        template_scope(ctx)?,
    ))
}

/// The family label `ls` and `describe` print for a snapshot format version.
pub fn format_image_family(snapshot_format_version: Option<&str>) -> &'static str {
    match snapshot_format_version {
        Some("content_addressed_streaming_v1") => "CAS",
        Some("durable_archive_v1") => "TLSnap",
        Some("local_directory_v1") => "Local",
        _ => "-",
    }
}

#[cfg(test)]
mod tests {
    use super::format_image_family;

    #[test]
    fn image_family_labels_follow_the_snapshot_format() {
        assert_eq!(
            format_image_family(Some("content_addressed_streaming_v1")),
            "CAS"
        );
        assert_eq!(format_image_family(Some("durable_archive_v1")), "TLSnap");
        assert_eq!(format_image_family(Some("local_directory_v1")), "Local");
        assert_eq!(format_image_family(Some("other")), "-");
        assert_eq!(format_image_family(None), "-");
    }
}
