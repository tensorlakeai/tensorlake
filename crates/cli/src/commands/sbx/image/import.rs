use tensorlake::sandbox_images::{
    SandboxImageBuildEvent, SandboxImageBuildOptions, build_sandbox_image,
};

use crate::{
    auth::context::CliContext,
    error::{CliError, Result},
};

/// Import a registry image directly into a Tensorlake sandbox image, with no
/// Dockerfile and no Docker daemon: the builder pulls the image's layers and
/// writes them straight into the rootfs.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    ctx: &CliContext,
    image_reference: &str,
    registered_name: Option<&str>,
    disk_mb: Option<u64>,
    builder_disk_mb: Option<u64>,
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    is_public: bool,
    output_json: bool,
) -> Result<()> {
    let options = SandboxImageBuildOptions {
        api_url: ctx.api_url.clone(),
        bearer_token: ctx.bearer_token()?,
        use_scope_headers: ctx.personal_access_token.is_some() && ctx.api_key.is_none(),
        organization_id: ctx.effective_organization_id(),
        project_id: ctx.effective_project_id(),
        namespace: ctx.namespace.clone(),
        // Unused on the import path, but the option struct is shared with the
        // Dockerfile build flow.
        dockerfile_path: std::path::PathBuf::new(),
        dockerfile_text: None,
        context_dir: None,
        import_image_reference: Some(image_reference.to_string()),
        registered_name: registered_name.map(str::to_string),
        disk_mb,
        builder_disk_mb,
        cpus,
        memory_mb,
        is_public,
        user_agent: Some(format!(
            "Tensorlake CLI (rust/{})",
            env!("CARGO_PKG_VERSION")
        )),
    };

    let registered = build_sandbox_image(options, |event| match event {
        SandboxImageBuildEvent::Status(message) => eprintln!("⚙️  {message}"),
        SandboxImageBuildEvent::BuildLog { message, .. } => eprintln!("{message}"),
        SandboxImageBuildEvent::Warning(message) => eprintln!("⚠️  {message}"),
    })
    .await
    .map_err(|error| CliError::Other(error.into()))?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&registered)?);
    }

    Ok(())
}
