use std::process::Stdio;

use bollard::Docker;
use bollard::query_parameters::{BuildImageOptions, PushImageOptions, TagImageOptions};
use bytes::Bytes;
use docker_credentials_config::{DockerConfig, image_registry};
use futures::StreamExt;
use http_body_util::{Either, Full};
use minijinja::Environment;
use tensorlake_cloud_sdk::images::models::{Image, ImageBuildOperation};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::error::{CliError, Result};

struct ImageBuildContext {
    name: String,
    tag: String,
    image: Image,
    sdk_version: String,
}

pub async fn run(
    application_file_path: &str,
    repository: Option<&str>,
    tag: Option<&str>,
    image_name: Option<&str>,
    stage: &str,
    template: Option<&str>,
    push: bool,
) -> Result<()> {
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
        if let Some(template_path) = template {
            let dockerfile = ctx.image.dockerfile_content(&ctx.sdk_version, Some(stage));
            let rendered = render_template(template_path, &dockerfile)?;
            ctx.image
                .create_context_archive_with_dockerfile(&mut context_data, &rendered)
                .map_err(CliError::Io)?;
        } else {
            ctx.image
                .create_context_archive(&mut context_data, &ctx.sdk_version, Some(stage))
                .map_err(CliError::Io)?;
        }

        eprintln!("\n📦 Building `{}`...", local_name);
        build_image(&docker, &docker_config, &local_name, Bytes::from(context_data)).await?;
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

/// Spawn the Python wrapper and collect image definitions as NDJSON.
async fn collect_image_contexts(
    application_file_path: &str,
    tag: Option<&str>,
    image_name: Option<&str>,
) -> Result<Vec<ImageBuildContext>> {
    let mut cmd = tokio::process::Command::new("tensorlake-build-images");
    cmd.arg(application_file_path);
    if let Some(t) = tag {
        cmd.args(["--tag", t]);
    }
    if let Some(n) = image_name {
        cmd.args(["--image-name", n]);
    }
    if std::env::var("TENSORLAKE_DEBUG").is_ok() {
        cmd.env("TENSORLAKE_DEBUG", "1");
    }
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let mut child = cmd.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CliError::usage(
                "'tensorlake-build-images' not found on PATH. \
                 Install the Python tensorlake package: pip install tensorlake",
            )
        } else {
            CliError::Io(e)
        }
    })?;

    let stdout = child.stdout.take().expect("stdout was piped");
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut images = Vec::new();

    while let Some(line) = lines.next_line().await.map_err(CliError::Io)? {
        let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
            eprintln!("{}", line);
            continue;
        };

        match event.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "image" => {
                let name = event
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let tag = event
                    .get("tag")
                    .and_then(|v| v.as_str())
                    .unwrap_or("latest")
                    .to_string();
                let base_image = event
                    .get("base_image")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let sdk_version = event
                    .get("sdk_version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let operations: Vec<ImageBuildOperation> = event
                    .get("operations")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();

                let image = Image::builder()
                    .name(name.clone())
                    .base_image(base_image)
                    .build_operations(operations)
                    .build()
                    .map_err(|e| {
                        CliError::Other(anyhow::anyhow!("Failed to build image definition: {e}"))
                    })?;

                images.push(ImageBuildContext {
                    name,
                    tag,
                    image,
                    sdk_version,
                });
            }
            "done" => {}
            "error" => {
                let message = event
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                eprintln!("Error: {}", message);
                if let Some(details) = event.get("details").and_then(|v| v.as_str())
                    && !details.is_empty()
                {
                    eprintln!("{}", details);
                }
                if let Some(tb) = event.get("traceback").and_then(|v| v.as_str())
                    && !tb.is_empty()
                {
                    eprintln!("\nPython traceback:\n{}", tb);
                }
                let _ = child.wait().await;
                return Err(CliError::ExitCode(1));
            }
            _ => {
                eprintln!("{}", line);
            }
        }
    }

    let status = child.wait().await.map_err(CliError::Io)?;
    if !status.success() {
        return Err(CliError::ExitCode(status.code().unwrap_or(1)));
    }

    Ok(images)
}

async fn build_image(docker: &Docker, docker_config: &DockerConfig, image_name: &str, context: Bytes) -> Result<()> {
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

async fn push_image(docker: &Docker, docker_config: &DockerConfig, repository: &str, tag: &str) -> Result<()> {
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
