use std::collections::HashMap;
use std::path::Path;

use tensorlake_cloud_sdk::images::models::{Image, ImageBuildOperation, ImageBuildOperationType};

use crate::auth::context::CliContext;
use crate::commands::sbx::{self, sandbox_proxy_base};
use crate::error::{CliError, Result};
use crate::http;
use crate::python_ast::{self, ImageDef, OpDef};

pub async fn run(
    ctx: &CliContext,
    image_file_path: &str,
    image_name: Option<&str>,
    registered_name: Option<&str>,
) -> Result<()> {
    // 1. Parse the Python file for Image definitions.
    let abs_path = tokio::fs::canonicalize(image_file_path)
        .await
        .map_err(|e| CliError::usage(format!("Cannot read '{}': {}", image_file_path, e)))?;
    let app_dir = abs_path.parent().unwrap_or(&abs_path).to_path_buf();

    eprintln!("\u{2699}\u{fe0f}  Loading {}...", image_file_path);
    let image_defs = python_ast::collect_images(&abs_path, &app_dir);

    // 2. Select the image.
    let image_def = select_image(&image_defs, image_name)?;
    let effective_name = registered_name.unwrap_or(&image_def.name).to_string();
    eprintln!("\u{2699}\u{fe0f}  Selected image: {}", image_def.name);

    // 3. Create sandbox.
    eprintln!("\u{2699}\u{fe0f}  Creating sandbox (2 CPUs, 4096 MB)...");
    let sandbox_body = serde_json::json!({
        "image": image_def.base_image,
        "resources": { "cpus": 2, "memory_mb": 4096 }
    });
    let sandbox_id = sbx::create::create_with_request(ctx, sandbox_body, true).await?;
    eprintln!("\u{2699}\u{fe0f}  Sandbox {} is running", sandbox_id);

    // 4. Execute build operations (always terminate sandbox on exit).
    let inner_result =
        run_build_and_register(ctx, &sandbox_id, image_def, &effective_name).await;

    // Always attempt sandbox termination, even on error.
    let _ = terminate_sandbox(ctx, &sandbox_id).await;

    inner_result
}

async fn run_build_and_register(
    ctx: &CliContext,
    sandbox_id: &str,
    image_def: &ImageDef,
    image_name: &str,
) -> Result<()> {
    execute_operations(ctx, sandbox_id, image_def).await?;

    // 5. Snapshot (filesystem only — skip memory for faster snapshots).
    let snapshot_id =
        sbx::snapshot::create_snapshot(ctx, sandbox_id, 300.0, Some("filesystem_only")).await?;
    eprintln!("\u{1f4f8} Snapshot created: {}", snapshot_id);

    // 6. Build Dockerfile text.
    let sdk_version = env!("CARGO_PKG_VERSION");
    let image = build_cloud_sdk_image(image_def)?;
    let dockerfile = image.dockerfile_content(sdk_version, None);

    // 7. Register image via Platform API.
    eprintln!("\u{2699}\u{fe0f}  Registering image...");
    let image_id = register_image(ctx, image_name, &dockerfile, &snapshot_id).await?;
    eprintln!(
        "\u{2705} Image '{}' registered ({})",
        image_name, image_id
    );

    Ok(())
}

/// Execute all Image build operations inside the running sandbox.
async fn execute_operations(
    ctx: &CliContext,
    sandbox_id: &str,
    image_def: &ImageDef,
) -> Result<()> {
    // Accumulated env vars — mirrors the Python `process_env` dict.
    let mut env: HashMap<String, String> = HashMap::new();
    env.insert("PIP_BREAK_SYSTEM_PACKAGES".to_string(), "1".to_string());

    // Set up /app working directory.
    let env_vec = env_to_vec(&env);
    sbx::exec::run(
        ctx,
        sandbox_id,
        "mkdir",
        &["-p".to_string(), "/app".to_string()],
        None,
        None,
        &env_vec,
    )
    .await?;

    for op in &image_def.operations {
        execute_single_op(ctx, sandbox_id, op, &mut env).await?;
    }

    Ok(())
}

async fn execute_single_op(
    ctx: &CliContext,
    sandbox_id: &str,
    op: &OpDef,
    env: &mut HashMap<String, String>,
) -> Result<()> {
    match op.op_type.as_str() {
        "RUN" => {
            for cmd in &op.args {
                eprintln!("\u{2699}\u{fe0f}  RUN {}", cmd);
                let env_vec = env_to_vec(env);
                sbx::exec::run(
                    ctx,
                    sandbox_id,
                    "sh",
                    &["-c".to_string(), cmd.clone()],
                    None,
                    Some("/app"),
                    &env_vec,
                )
                .await?;
            }
        }
        "COPY" | "ADD" => {
            let src = op.args.first().map(String::as_str).unwrap_or("");
            let dest = op.args.get(1).map(String::as_str).unwrap_or(src);
            eprintln!("\u{2699}\u{fe0f}  {} {} -> {}", op.op_type, src, dest);
            upload_to_sandbox(ctx, sandbox_id, src, dest).await?;
        }
        "ENV" => {
            let key = op.args.first().cloned().unwrap_or_default();
            let val = op.args.get(1).cloned().unwrap_or_default();
            eprintln!("\u{2699}\u{fe0f}  ENV {}={}", key, val);
            env.insert(key.clone(), val.clone());
            // Persist for future shell sessions inside the sandbox.
            let persist_cmd = format!("echo 'export {}=\"{}\"' >> /etc/environment", key, val);
            let env_vec = env_to_vec(env);
            sbx::exec::run(
                ctx,
                sandbox_id,
                "sh",
                &["-c".to_string(), persist_cmd],
                None,
                None,
                &env_vec,
            )
            .await?;
        }
        _ => {}
    }

    Ok(())
}

/// Upload local file(s) to the sandbox at `dest_path`.
async fn upload_to_sandbox(
    ctx: &CliContext,
    sandbox_id: &str,
    local_path: &str,
    dest_path: &str,
) -> Result<()> {
    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
    let client = build_proxy_client(ctx, host_override.as_deref())?;

    let path = Path::new(local_path);
    if path.is_file() {
        let data = tokio::fs::read(path).await.map_err(CliError::Io)?;
        upload_bytes(&client, &proxy_base, dest_path, data).await?;
    } else if path.is_dir() {
        upload_dir(&client, &proxy_base, path, dest_path).await?;
    } else {
        return Err(CliError::usage(format!(
            "Local path not found: {}",
            local_path
        )));
    }

    Ok(())
}

async fn upload_dir(
    client: &reqwest::Client,
    proxy_base: &str,
    local_dir: &Path,
    dest_dir: &str,
) -> Result<()> {
    let mut stack = vec![local_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await.map_err(CliError::Io)?;
        while let Some(entry) = entries.next_entry().await.map_err(CliError::Io)? {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
            } else {
                let rel = entry_path.strip_prefix(local_dir).unwrap_or(&entry_path);
                let remote_dest = format!("{}/{}", dest_dir.trim_end_matches('/'), rel.display());
                let data = tokio::fs::read(&entry_path).await.map_err(CliError::Io)?;
                upload_bytes(client, proxy_base, &remote_dest, data).await?;
            }
        }
    }
    Ok(())
}

async fn upload_bytes(
    client: &reqwest::Client,
    proxy_base: &str,
    dest_path: &str,
    data: Vec<u8>,
) -> Result<()> {
    let resp = client
        .put(format!(
            "{}/api/v1/files?path={}",
            proxy_base,
            urlencoding::encode(dest_path)
        ))
        .body(data)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "File upload to sandbox failed (HTTP {}): {}",
            status,
            body
        )));
    }

    Ok(())
}

/// Register the image via the Platform API.
async fn register_image(
    ctx: &CliContext,
    name: &str,
    dockerfile: &str,
    snapshot_id: &str,
) -> Result<String> {
    let (base_url, _, _) = super::templates_base_url(ctx)?;

    let client = ctx.client()?;
    let body = serde_json::json!({
        "name": name,
        "dockerfile": dockerfile,
        "snapshotId": snapshot_id,
    });

    let resp = client
        .post(&base_url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let msg = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "Image registration failed (HTTP {}): {}",
            status,
            msg
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let image_id = result
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Ok(image_id)
}

/// Terminate the sandbox, ignoring errors (best-effort cleanup).
async fn terminate_sandbox(ctx: &CliContext, sandbox_id: &str) -> Result<()> {
    let client = ctx.client()?;
    let url = sbx::sandbox_endpoint(ctx, &format!("sandboxes/{}", sandbox_id));
    let _ = client.delete(&url).send().await;
    Ok(())
}

/// Select an image from the discovered definitions.
fn select_image<'a>(image_defs: &'a [ImageDef], image_name: Option<&str>) -> Result<&'a ImageDef> {
    if image_defs.is_empty() {
        return Err(CliError::usage(
            "No Image definitions found in the application file.\n\
             Make sure your .py file defines an Image() object at module level.",
        ));
    }

    if let Some(name) = image_name {
        image_defs.iter().find(|d| d.name == name).ok_or_else(|| {
            let available: Vec<&str> = image_defs.iter().map(|d| d.name.as_str()).collect();
            CliError::usage(format!(
                "Image '{}' not found. Available: {}",
                name,
                available.join(", ")
            ))
        })
    } else if image_defs.len() == 1 {
        Ok(&image_defs[0])
    } else {
        let names: Vec<&str> = image_defs.iter().map(|d| d.name.as_str()).collect();
        Err(CliError::usage(format!(
            "Multiple images found: {}. Use --image-name / -i to select one.",
            names.join(", ")
        )))
    }
}

/// Convert an [`ImageDef`] to the cloud-SDK [`Image`] type for Dockerfile generation.
fn build_cloud_sdk_image(def: &ImageDef) -> Result<Image> {
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

    Image::builder()
        .name(def.name.clone())
        .base_image(def.base_image.clone())
        .build_operations(operations)
        .build()
        .map_err(|e| CliError::Other(anyhow::anyhow!("Failed to build image model: {}", e)))
}

/// Build a reqwest client with auth headers and optional Host override for the sandbox proxy.
fn build_proxy_client(ctx: &CliContext, host_override: Option<&str>) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Ok(token) = ctx.bearer_token() {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
    }
    if let Some(org_id) = ctx.effective_organization_id() {
        headers.insert("X-Forwarded-Organization-Id", org_id.parse().unwrap());
    }
    if let Some(proj_id) = ctx.effective_project_id() {
        headers.insert("X-Forwarded-Project-Id", proj_id.parse().unwrap());
    }
    if let Some(host) = host_override {
        headers.insert(reqwest::header::HOST, host.parse().unwrap());
    }
    http::client_builder()
        .default_headers(headers)
        .build()
        .map_err(|e| CliError::Other(anyhow::anyhow!("{}", e)))
}

/// Convert an env HashMap to `["KEY=VALUE", ...]` strings for `exec::run`.
fn env_to_vec(env: &HashMap<String, String>) -> Vec<String> {
    env.iter().map(|(k, v)| format!("{}={}", k, v)).collect()
}
