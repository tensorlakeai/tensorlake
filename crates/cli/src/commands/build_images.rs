use bollard::Docker;
use bollard::query_parameters::{BuildImageOptions, PushImageOptions, TagImageOptions};
use bytes::Bytes;
use docker_credentials_config::{DockerConfig, image_registry};
use futures::StreamExt;
use http_body_util::{Either, Full};
use minijinja::Environment;
use tensorlake::images::models::{Image, ImageBuildOperation, ImageBuildOperationType};

use crate::error::{CliError, Result};

struct ImageBuildContext {
    name: String,
    tag: String,
    image: Image,
    sdk_version: String,
}

pub struct BuildImageArgs<'a> {
    pub application_file_path: &'a str,
    pub repository: Option<&'a str>,
    pub tag: Option<&'a str>,
    pub image_name: Option<&'a str>,
    pub stage: &'a str,
    pub template: Option<&'a str>,
    pub push: bool,
    pub build_envs: &'a [String],
}

pub async fn run(args: BuildImageArgs<'_>) -> Result<()> {
    let BuildImageArgs {
        application_file_path,
        repository,
        tag,
        image_name,
        stage,
        template,
        push,
        build_envs,
    } = args;
    let images = collect_image_contexts(application_file_path, tag, image_name).await?;

    if images.is_empty() {
        eprintln!("No images found in the application file.");
        return Ok(());
    }

    let docker = Docker::connect_with_defaults()
        .map_err(|e| CliError::Other(anyhow::anyhow!("Failed to connect to Docker daemon: {e}")))?;

    docker.ping().await.map_err(|e| {
        CliError::Other(anyhow::anyhow!(
            "Docker daemon not reachable: {e}. Is Docker running?"
        ))
    })?;

    let docker_config = DockerConfig::load()
        .await
        .map_err(|e| CliError::Other(anyhow::anyhow!("Failed to load Docker config: {e}")))?;

    for ctx in &images {
        let local_name = format!("{}:{}", ctx.name, ctx.tag);

        let mut context_data: Vec<u8> = Vec::new();
        let dockerfile = ctx.image.dockerfile_content(&ctx.sdk_version, Some(stage));
        let dockerfile = inject_build_envs(dockerfile, build_envs);
        let dockerfile = match template {
            Some(template_path) => render_template(template_path, &dockerfile)?,
            None => dockerfile,
        };
        ctx.image
            .create_context_archive_with_dockerfile(&mut context_data, &dockerfile)
            .map_err(CliError::Io)?;

        eprintln!("\n📦 Building `{}`...", local_name);
        build_image(
            &docker,
            &docker_config,
            &local_name,
            Bytes::from(context_data),
        )
        .await?;
        eprintln!("✅ Built `{}`", local_name);

        if push {
            let (target_repo, target_tag) = match repository {
                Some(repo) => (
                    format!("{}/{}", repo.trim_end_matches('/'), ctx.name),
                    ctx.tag.clone(),
                ),
                None => (ctx.name.clone(), ctx.tag.clone()),
            };
            let target_full = format!("{}:{}", target_repo, target_tag);

            if target_full != local_name {
                eprintln!("🏷️  Tagging `{}` → `{}`", local_name, target_full);
                docker
                    .tag_image(
                        &local_name,
                        Some(TagImageOptions {
                            repo: Some(target_repo.clone()),
                            tag: Some(target_tag.clone()),
                        }),
                    )
                    .await
                    .map_err(|e| {
                        CliError::Other(anyhow::anyhow!("Failed to tag `{}`: {e}", local_name))
                    })?;
            }

            eprintln!("⬆️  Pushing `{}`...", target_full);
            push_image(&docker, &docker_config, &target_repo, &target_tag).await?;
            eprintln!("✅ Pushed `{}`", target_full);
        }
    }

    Ok(())
}

/// Inject extra ENV directives into a Dockerfile after the PIP_BREAK_SYSTEM_PACKAGES line.
fn inject_build_envs(dockerfile: String, build_envs: &[String]) -> String {
    if build_envs.is_empty() {
        return dockerfile;
    }
    let env_lines: String = build_envs
        .iter()
        .filter_map(|e| {
            let (key, val) = e.split_once('=')?;
            Some(format!("ENV {}={}", key.trim(), val.trim()))
        })
        .collect::<Vec<_>>()
        .join("\n");
    dockerfile.replace(
        "ENV PIP_BREAK_SYSTEM_PACKAGES=1",
        &format!("ENV PIP_BREAK_SYSTEM_PACKAGES=1\n{}", env_lines),
    )
}

/// Render a MiniJinja template, exposing `tensorlake_image` as the Dockerfile content.
fn render_template(template_path: &str, dockerfile: &str) -> Result<String> {
    let source = std::fs::read_to_string(template_path).map_err(|e| {
        CliError::Other(anyhow::anyhow!(
            "Failed to read template '{}': {e}",
            template_path
        ))
    })?;

    let mut env = Environment::new();
    env.add_template("dockerfile", &source).map_err(|e| {
        CliError::Other(anyhow::anyhow!("Invalid template '{}': {e}", template_path))
    })?;

    let tmpl = env.get_template("dockerfile").unwrap();
    tmpl.render(minijinja::context! { tensorlake_image => dockerfile })
        .map_err(|e| {
            CliError::Other(anyhow::anyhow!(
                "Failed to render template '{}': {e}",
                template_path
            ))
        })
}

/// Parse the application file using the Rust Python AST parser and return
/// the image definitions needed to drive the Docker build.
async fn collect_image_contexts(
    application_file_path: &str,
    tag: Option<&str>,
    image_name: Option<&str>,
) -> Result<Vec<ImageBuildContext>> {
    let path = std::path::Path::new(application_file_path);
    if !path.is_file() {
        return Err(CliError::usage(format!(
            "Application file not found: {}",
            application_file_path
        )));
    }

    let app_dir = path.parent().unwrap_or(std::path::Path::new("."));
    let image_defs = crate::python_ast::collect_images(path, app_dir);

    if image_defs.is_empty() {
        return Err(CliError::usage(format!(
            "No Image definitions found in '{}'",
            application_file_path
        )));
    }

    let sdk_version = env!("CARGO_PKG_VERSION");
    let mut result = Vec::new();

    for def in image_defs {
        if let Some(filter) = image_name
            && def.name != filter
        {
            continue;
        }

        let effective_tag = tag.unwrap_or(&def.tag).to_string();

        let operations: Vec<ImageBuildOperation> = def
            .operations
            .iter()
            .filter_map(|op| {
                let op_type = match op.op_type.as_str() {
                    "RUN" => ImageBuildOperationType::RUN,
                    "COPY" => ImageBuildOperationType::COPY,
                    "ADD" => ImageBuildOperationType::ADD,
                    "ENV" => ImageBuildOperationType::ENV,
                    _ => return None,
                };
                ImageBuildOperation::builder()
                    .operation_type(op_type)
                    .args(op.args.clone())
                    .options(op.options.clone())
                    .build()
                    .ok()
            })
            .collect();

        let image = Image::builder()
            .name(def.name.clone())
            .base_image(def.base_image.clone())
            .build_operations(operations)
            .build()
            .map_err(|e| {
                CliError::Other(anyhow::anyhow!("Failed to build image definition: {e}"))
            })?;

        result.push(ImageBuildContext {
            name: def.name,
            tag: effective_tag,
            image,
            sdk_version: sdk_version.to_string(),
        });
    }

    if result.is_empty() {
        return Err(CliError::usage(match image_name {
            Some(n) => format!(
                "No image named '{}' found in '{}'",
                n, application_file_path
            ),
            None => format!("No images found in '{}'", application_file_path),
        }));
    }

    Ok(result)
}

async fn build_image(
    docker: &Docker,
    docker_config: &DockerConfig,
    image_name: &str,
    context: Bytes,
) -> Result<()> {
    let options = BuildImageOptions {
        t: Some(image_name.to_string()),
        rm: true,
        ..Default::default()
    };

    let all = docker_config.all_credentials();
    let credentials = if all.is_empty() { None } else { Some(all) };

    let body = Either::Left(Full::new(context));
    let mut stream = docker.build_image(options, credentials, Some(body));

    while let Some(result) = stream.next().await {
        match result {
            Ok(info) => {
                if let Some(s) = info.stream {
                    let trimmed = s.trim_end_matches('\n');
                    if !trimmed.is_empty() {
                        eprintln!("{}", trimmed);
                    }
                }
                if let Some(detail) = info.error_detail {
                    let msg = detail
                        .message
                        .unwrap_or_else(|| "unknown build error".into());
                    return Err(CliError::Other(anyhow::anyhow!("Build failed: {msg}")));
                }
            }
            Err(e) => {
                return Err(CliError::Other(anyhow::anyhow!("Build error: {e}")));
            }
        }
    }

    Ok(())
}

async fn push_image(
    docker: &Docker,
    docker_config: &DockerConfig,
    repository: &str,
    tag: &str,
) -> Result<()> {
    let registry = image_registry(repository);
    let credentials = docker_config.credentials_for_registry(&registry);

    let options = PushImageOptions {
        tag: Some(tag.to_string()),
        platform: None,
    };

    let mut stream = docker.push_image(repository, Some(options), credentials);

    while let Some(result) = stream.next().await {
        match result {
            Ok(info) => {
                if let Some(detail) = info.error_detail {
                    let msg = detail
                        .message
                        .unwrap_or_else(|| "unknown push error".into());
                    return Err(CliError::Other(anyhow::anyhow!("Push failed: {msg}")));
                }
                if let Some(status) = info.status {
                    eprintln!("{}", status);
                }
            }
            Err(e) => {
                return Err(CliError::Other(anyhow::anyhow!("Push error: {e}")));
            }
        }
    }

    Ok(())
}
