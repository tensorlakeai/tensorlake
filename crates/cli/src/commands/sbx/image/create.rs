use std::path::PathBuf;

use tensorlake::sandbox_images::{
    CommonBuildOptions, SandboxImageBuildOptions, build_sandbox_image,
};

use crate::{
    auth::context::CliContext,
    error::{CliError, Result},
};

use super::ImageBuildEventRenderer;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    ctx: &CliContext,
    dockerfile_path: &str,
    registered_name: Option<&str>,
    disk_mb: Option<u64>,
    builder_disk_mb: Option<u64>,
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    is_public: bool,
    docker_compat: bool,
    cas: bool,
    output_json: bool,
) -> Result<()> {
    let options = SandboxImageBuildOptions {
        common: CommonBuildOptions {
            api_url: ctx.api_url.clone(),
            bearer_token: ctx.bearer_token()?,
            use_scope_headers: ctx.personal_access_token.is_some() && ctx.api_key.is_none(),
            organization_id: ctx.effective_organization_id(),
            project_id: ctx.effective_project_id(),
            namespace: ctx.namespace.clone(),
            registered_name: registered_name.map(str::to_string),
            disk_mb,
            builder_disk_mb,
            cpus,
            memory_mb,
            is_public,
            cas,
            user_agent: Some(format!(
                "Tensorlake CLI (rust/{})",
                env!("CARGO_PKG_VERSION")
            )),
            docker_compat,
        },
        dockerfile_path: PathBuf::from(dockerfile_path),
        dockerfile_text: None,
        context_dir: None,
    };

    let mut renderer = ImageBuildEventRenderer::new();
    let registered = build_sandbox_image(options, |event| renderer.render(event))
        .await
        .map_err(|error| CliError::Other(error.into()))?;

    if output_json || cas {
        // Simulation: --cas must expose the unregistered, builder-local CAS
        // receipt even when --json was not requested.
        // Final implementation: this prints the normal registered Platform
        // image response, while --json remains the durable-path opt-in.
        println!("{}", serde_json::to_string_pretty(&registered)?);
    }

    Ok(())
}
