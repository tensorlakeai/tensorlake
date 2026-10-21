use std::path::PathBuf;

use tensorlake::sandbox_images::{
    CommonBuildOptions, SandboxImageBuildOptions, build_sandbox_image,
    build_sandbox_image_with_interrupt,
};

use crate::{
    auth::context::CliContext,
    error::{CliError, Result},
};

use super::{
    ImageBuildEventRenderer,
    build::{CasInterruptGuard, cas_operation_error},
};

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
    build_args: &[String],
    no_wait: bool,
    output_json: bool,
) -> Result<()> {
    if no_wait && !cas {
        return Err(CliError::usage(
            "--no-wait is not supported with --no-cas: a legacy image build has no build \
             operation to leave running",
        ));
    }
    let build_args = build_args
        .iter()
        .map(|raw| {
            raw.split_once('=')
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .ok_or_else(|| CliError::usage(format!("--build-arg {raw:?} must be KEY=VALUE")))
        })
        .collect::<Result<Vec<_>>>()?;
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
        context_files: Vec::new(),
        build_args,
    };

    let mut renderer = ImageBuildEventRenderer::new();
    if cas {
        // CAS builds are Image Service operations: the build id is printed
        // at admission and Ctrl+C runs the remote cancellation handshake.
        let guard = CasInterruptGuard::install();
        if no_wait {
            let handle = tensorlake::image_builds::start_build(
                options,
                ctx.docker_config_json(),
                guard.interrupt(),
                |event| {
                    guard.observe(&event);
                    renderer.render(event)
                },
            )
            .await
            .map_err(super::build::build_error)?;
            drop(renderer);
            eprintln!(
                "⚙️  Build {} admitted; wait for it with: tl sbx image build wait {}",
                handle.build_id(),
                handle.build_id()
            );
            if output_json {
                println!("{}", serde_json::to_string_pretty(handle.info())?);
            } else {
                println!("{}", handle.build_id());
            }
            return Ok(());
        }
        let registered = build_sandbox_image_with_interrupt(
            options,
            ctx.docker_config_json(),
            guard.interrupt(),
            |event| {
                guard.observe(&event);
                renderer.render(event)
            },
        )
        .await
        .map_err(cas_operation_error)?;
        if output_json {
            println!("{}", serde_json::to_string_pretty(&registered)?);
        }
        return Ok(());
    }

    let registered = build_sandbox_image(options, ctx.docker_config_json(), |event| {
        renderer.render(event)
    })
    .await
    .map_err(|error| CliError::Other(error.into()))?;

    if output_json {
        // Both families answer the platform template shape (ADR 0107 D28);
        // --json is the only opt-in for printing it.
        println!("{}", serde_json::to_string_pretty(&registered)?);
    }

    Ok(())
}
