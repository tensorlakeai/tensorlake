use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use docker_credentials_config::DockerConfig;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use shlex::split as shlex_split;
use tensorlake::{
    Client,
    error::SdkError,
    sandboxes::{
        SandboxProxyClient, SandboxesClient,
        models::{CreateSandboxRequest, CreateSandboxResources, ProcessInfo},
    },
};

use crate::{
    auth::context::CliContext,
    commands::sbx::{DEFAULT_SANDBOX_WAIT_TIMEOUT, sandbox_proxy_base, wait_for_sandbox_status},
    error::{CliError, Result},
};

const DEFAULT_ROOTFS_DISK_MB: u64 = 10 * 1024;
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(300);
const PROCESS_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(200);
const PROCESS_EXIT_POLL_ATTEMPTS: usize = 10;
const PROXY_READY_TIMEOUT: Duration = Duration::from_secs(120);
const PROXY_READY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const REMOTE_BUILD_DIR: &str = "/var/lib/tensorlake/rootfs-builder/build";
const REMOTE_CONTEXT_DIR: &str = "/var/lib/tensorlake/rootfs-builder/build/context";
const REMOTE_SPEC_PATH: &str = "/var/lib/tensorlake/rootfs-builder/build/spec.json";
const REMOTE_METADATA_PATH: &str = "/var/lib/tensorlake/rootfs-builder/build/metadata.json";
const ROOTFS_BUILDER_BIN_DIR: &str = "/usr/local/bin";
const ROOTFS_BUILDER_PATH: &str = "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const ROOTFS_BUILDER_COMMAND: &str = "tl-rootfs-build";

#[derive(Debug, Clone, PartialEq, Eq)]
struct DockerfileBuildPlan {
    context_dir: PathBuf,
    registered_name: String,
    dockerfile_text: String,
    base_image: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreparedSandboxTemplateBuild {
    build_id: String,
    snapshot_id: String,
    snapshot_uri: String,
    rootfs_node_kind: String,
    builder: PreparedRootfsBuilder,
    parent: Option<PreparedRootfsParent>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreparedRootfsBuilder {
    image: String,
    command: String,
    cpus: f64,
    memory_mb: i64,
    disk_mb: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreparedRootfsParent {
    parent_manifest_uri: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CompleteSandboxTemplateBuildRequest {
    snapshot_id: String,
    snapshot_uri: String,
    snapshot_format_version: String,
    snapshot_size_bytes: u64,
    rootfs_disk_bytes: u64,
    rootfs_node_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_manifest_uri: Option<String>,
}

pub async fn run(
    ctx: &CliContext,
    dockerfile_path: &str,
    registered_name: Option<&str>,
    disk_mb: Option<u64>,
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    is_public: bool,
) -> Result<()> {
    eprintln!("⚙️  Loading Dockerfile...");
    let plan = load_dockerfile_plan(dockerfile_path, registered_name)?;
    eprintln!("⚙️  Selected image name: {}", plan.registered_name);

    eprintln!("⚙️  Preparing rootfs build...");
    let (prepared, prepared_spec) = prepare_rootfs_build(ctx, &plan, is_public).await?;
    eprintln!(
        "⚙️  Build mode: Rootfs{}",
        match prepared.rootfs_node_kind.as_str() {
            "diff" => "Diff",
            _ => "Base",
        }
    );

    let client = super::sandbox_lifecycle_client(ctx)?;
    let sandboxes = SandboxesClient::new(client.clone(), ctx.namespace.clone(), is_localhost(ctx));
    let builder_disk_mb = disk_mb
        .map(|requested| requested.max(prepared.builder.disk_mb))
        .unwrap_or(prepared.builder.disk_mb);
    let resources = CreateSandboxResources {
        cpus: cpus.unwrap_or(prepared.builder.cpus),
        memory_mb: memory_mb.unwrap_or(prepared.builder.memory_mb),
        disk_mb: Some(builder_disk_mb),
    };

    eprintln!(
        "⚙️  Creating rootfs builder sandbox from {}...",
        prepared.builder.image
    );
    let created = sandboxes
        .create(&CreateSandboxRequest {
            image: Some(prepared.builder.image.clone()),
            resources,
            secret_names: None,
            timeout_secs: None,
            entrypoint: None,
            network: None,
            snapshot_id: None,
            name: None,
        })
        .await?;
    let sandbox_id = created.sandbox_id.clone();
    let routing_hint = created.routing_hint.clone();

    let result = async {
        wait_for_sandbox_status(
            ctx,
            &sandbox_id,
            &format!("Waiting for rootfs builder sandbox {sandbox_id}"),
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
        eprintln!("⚙️  Rootfs builder sandbox {sandbox_id} is running");

        let proxy = sandbox_proxy_client(ctx, &client, &sandbox_id, routing_hint)?;
        wait_for_proxy_ready(&proxy).await?;
        upload_build_inputs(&proxy, &plan, &prepared_spec, disk_mb).await?;

        eprintln!("⚙️  Running offline rootfs builder...");
        run_rootfs_builder(&proxy, &prepared.builder.command).await?;

        let metadata = read_build_metadata(&proxy).await?;
        let complete_request = complete_request_from_metadata(&prepared, &metadata)?;

        eprintln!("⚙️  Completing image registration...");
        let registered = complete_rootfs_build(ctx, &prepared.build_id, &complete_request).await?;
        let template_id = registered.get("id").and_then(Value::as_str).unwrap_or("-");
        eprintln!(
            "✅ Image '{}' registered ({})",
            plan.registered_name, template_id
        );
        Ok(())
    }
    .await;

    if let Err(error) = sandboxes.delete(&sandbox_id).await {
        eprintln!(
            "⚠️  Failed to terminate rootfs builder sandbox {} during cleanup: {}",
            sandbox_id, error
        );
    }

    result
}

async fn prepare_rootfs_build(
    ctx: &CliContext,
    plan: &DockerfileBuildPlan,
    is_public: bool,
) -> Result<(PreparedSandboxTemplateBuild, Value)> {
    let client = ctx.client()?;
    let url = sandbox_template_builds_url(ctx)?;
    let response = client
        .post(url)
        .json(&json!({
            "name": plan.registered_name,
            "dockerfile": plan.dockerfile_text,
            "baseImage": plan.base_image,
            "public": is_public,
        }))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to prepare sandbox image build (HTTP {}): {}",
            status,
            body
        )));
    }

    let raw: Value = response.json().await.map_err(CliError::Http)?;
    let prepared = serde_json::from_value(raw.clone()).map_err(CliError::Json)?;
    Ok((prepared, raw))
}

async fn complete_rootfs_build(
    ctx: &CliContext,
    build_id: &str,
    request: &CompleteSandboxTemplateBuildRequest,
) -> Result<Value> {
    let client = ctx.client()?;
    let url = format!(
        "{}/{}/complete",
        sandbox_template_builds_url(ctx)?,
        build_id
    );
    let response = client
        .post(url)
        .json(request)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to complete sandbox image build (HTTP {}): {}",
            status,
            body
        )));
    }

    response.json().await.map_err(CliError::Http)
}

fn sandbox_template_builds_url(ctx: &CliContext) -> Result<String> {
    let (org_id, project_id) = super::org_and_project(ctx)?;
    Ok(format!(
        "{}/platform/v1/organizations/{}/projects/{}/sandbox-template-builds",
        ctx.api_url.trim_end_matches('/'),
        org_id,
        project_id
    ))
}

fn sandbox_proxy_client(
    ctx: &CliContext,
    client: &Client,
    sandbox_id: &str,
    routing_hint: Option<String>,
) -> Result<SandboxProxyClient> {
    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
    let sandbox_id_header = host_override.is_none().then(|| sandbox_id.to_string());
    Ok(
        SandboxProxyClient::new(client.with_base_url(&proxy_base), host_override)
            .with_sandbox_id(sandbox_id_header)
            .with_routing_hint(routing_hint),
    )
}

async fn wait_for_proxy_ready(proxy: &SandboxProxyClient) -> Result<()> {
    let deadline = tokio::time::Instant::now() + PROXY_READY_TIMEOUT;
    loop {
        match run_streaming_process(proxy, "/bin/true", Vec::new(), None, None).await {
            Ok(()) => return Ok(()),
            Err(error) if is_transient_proxy_error(&error) => {
                if tokio::time::Instant::now() > deadline {
                    return Err(error);
                }
                tokio::time::sleep(PROXY_READY_POLL_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_transient_proxy_error(error: &CliError) -> bool {
    match error {
        CliError::Sdk(SdkError::ServerError { status, message }) => {
            matches!(
                *status,
                StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ) || (*status == StatusCode::BAD_REQUEST && message.contains("not running"))
                || message.contains("PROXY_ERROR")
                || message.contains("Failed to proxy request")
        }
        CliError::Http(error) => error.is_timeout() || error.is_connect(),
        _ => false,
    }
}

async fn upload_build_inputs(
    proxy: &SandboxProxyClient,
    plan: &DockerfileBuildPlan,
    prepared_spec: &Value,
    disk_mb: Option<u64>,
) -> Result<()> {
    eprintln!("⚙️  Uploading build context...");
    copy_local_path(proxy, &plan.context_dir, REMOTE_CONTEXT_DIR).await?;

    let docker_config_json = resolved_docker_config_json().await?;
    let spec = build_rootfs_spec(prepared_spec, plan, disk_mb, docker_config_json)?;
    ensure_remote_parent_dir(proxy, REMOTE_SPEC_PATH).await?;
    proxy
        .write_file(
            REMOTE_SPEC_PATH,
            serde_json::to_vec_pretty(&spec).map_err(CliError::Json)?,
        )
        .await?;
    Ok(())
}

fn build_rootfs_spec(
    prepared_spec: &Value,
    plan: &DockerfileBuildPlan,
    disk_mb: Option<u64>,
    docker_config_json: Option<String>,
) -> Result<Value> {
    let mut spec = prepared_spec.clone();
    let object = spec.as_object_mut().ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "platform API returned a non-object rootfs build spec"
        ))
    })?;

    object.insert(
        "dockerfile".to_string(),
        Value::String(plan.dockerfile_text.clone()),
    );
    object.insert(
        "contextDir".to_string(),
        Value::String(REMOTE_CONTEXT_DIR.to_string()),
    );
    object.insert(
        "baseImage".to_string(),
        Value::String(plan.base_image.clone()),
    );
    object.insert(
        "rootfsDiskBytes".to_string(),
        Value::Number(rootfs_disk_bytes(disk_mb)?.into()),
    );
    if let Some(docker_config_json) = docker_config_json {
        object.insert(
            "dockerConfigJson".to_string(),
            Value::String(docker_config_json),
        );
    }

    Ok(spec)
}

fn rootfs_disk_bytes(disk_mb: Option<u64>) -> Result<u64> {
    disk_mb
        .unwrap_or(DEFAULT_ROOTFS_DISK_MB)
        .checked_mul(1024 * 1024)
        .ok_or_else(|| CliError::usage("--disk_mb is too large to convert to bytes"))
}

async fn resolved_docker_config_json() -> Result<Option<String>> {
    let docker_config = DockerConfig::load().await.map_err(|error| {
        CliError::Other(anyhow::anyhow!("Failed to load Docker config: {error}"))
    })?;
    let credentials = docker_config.all_credentials();
    if credentials.is_empty() {
        return Ok(None);
    }

    docker_config_json_from_credentials(credentials)
        .map(Some)
        .map_err(CliError::Json)
}

fn docker_config_json_from_credentials(
    credentials: HashMap<String, bollard::auth::DockerCredentials>,
) -> serde_json::Result<String> {
    let mut auths = Map::new();
    for (registry, creds) in credentials {
        let mut entry = Map::new();
        if let Some(identity_token) = creds.identitytoken {
            entry.insert("identitytoken".to_string(), Value::String(identity_token));
        }
        if let (Some(username), Some(password)) = (creds.username, creds.password) {
            let encoded = STANDARD.encode(format!("{username}:{password}"));
            entry.insert("auth".to_string(), Value::String(encoded));
        }
        if !entry.is_empty() {
            auths.insert(registry, Value::Object(entry));
        }
    }

    serde_json::to_string(&json!({ "auths": auths }))
}

async fn run_rootfs_builder(proxy: &SandboxProxyClient, command: &str) -> Result<()> {
    let parts = shlex_split(command).ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "invalid rootfs builder command returned by platform API: {}",
            command
        ))
    })?;
    let Some((executable, command_args)) = parts.split_first() else {
        return Err(CliError::Other(anyhow::anyhow!(
            "empty rootfs builder command returned by platform API"
        )));
    };
    let mut args = command_args.to_vec();
    args.extend([
        "--spec".to_string(),
        REMOTE_SPEC_PATH.to_string(),
        "--metadata-out".to_string(),
        REMOTE_METADATA_PATH.to_string(),
    ]);

    let executable = rootfs_builder_executable(executable);
    run_streaming_process(
        proxy,
        &executable,
        args,
        Some(rootfs_builder_env()),
        Some(REMOTE_BUILD_DIR.to_string()),
    )
    .await
}

fn rootfs_builder_executable(executable: &str) -> String {
    if executable == ROOTFS_BUILDER_COMMAND {
        format!("{ROOTFS_BUILDER_BIN_DIR}/{ROOTFS_BUILDER_COMMAND}")
    } else {
        executable.to_string()
    }
}

fn rootfs_builder_env() -> Map<String, Value> {
    let mut env = Map::new();
    env.insert(
        "PATH".to_string(),
        Value::String(ROOTFS_BUILDER_PATH.to_string()),
    );
    env
}

async fn read_build_metadata(proxy: &SandboxProxyClient) -> Result<Value> {
    let content = proxy.read_file(REMOTE_METADATA_PATH).await?.into_inner();
    serde_json::from_slice(&content).map_err(CliError::Json)
}

fn complete_request_from_metadata(
    prepared: &PreparedSandboxTemplateBuild,
    metadata: &Value,
) -> Result<CompleteSandboxTemplateBuildRequest> {
    let rootfs_node_kind = metadata_string(metadata, "rootfs_node_kind", "rootfsNodeKind")
        .unwrap_or_else(|| prepared.rootfs_node_kind.clone());
    let parent_manifest_uri = metadata_string(metadata, "parent_manifest_uri", "parentManifestUri")
        .or_else(|| {
            (rootfs_node_kind == "diff")
                .then(|| {
                    prepared
                        .parent
                        .as_ref()
                        .map(|parent| parent.parent_manifest_uri.clone())
                })
                .flatten()
        });

    if rootfs_node_kind == "diff" && parent_manifest_uri.is_none() {
        return Err(CliError::Other(anyhow::anyhow!(
            "rootfs diff build completed without parent_manifest_uri"
        )));
    }

    Ok(CompleteSandboxTemplateBuildRequest {
        snapshot_id: metadata_string(metadata, "snapshot_id", "snapshotId")
            .unwrap_or_else(|| prepared.snapshot_id.clone()),
        snapshot_uri: metadata_string(metadata, "snapshot_uri", "snapshotUri")
            .unwrap_or_else(|| prepared.snapshot_uri.clone()),
        snapshot_format_version: required_metadata_string(
            metadata,
            "snapshot_format_version",
            "snapshotFormatVersion",
        )?,
        snapshot_size_bytes: required_metadata_u64(
            metadata,
            "snapshot_size_bytes",
            "snapshotSizeBytes",
        )?,
        rootfs_disk_bytes: required_metadata_u64(metadata, "rootfs_disk_bytes", "rootfsDiskBytes")?,
        rootfs_node_kind,
        parent_manifest_uri,
    })
}

fn required_metadata_string(metadata: &Value, snake_key: &str, camel_key: &str) -> Result<String> {
    metadata_string(metadata, snake_key, camel_key).ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "rootfs builder metadata is missing {}",
            snake_key
        ))
    })
}

fn metadata_string(metadata: &Value, snake_key: &str, camel_key: &str) -> Option<String> {
    metadata
        .get(snake_key)
        .or_else(|| metadata.get(camel_key))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn required_metadata_u64(metadata: &Value, snake_key: &str, camel_key: &str) -> Result<u64> {
    metadata
        .get(snake_key)
        .or_else(|| metadata.get(camel_key))
        .and_then(|value| match value {
            Value::Number(number) => number.as_u64(),
            Value::String(value) => value.parse::<u64>().ok(),
            _ => None,
        })
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "rootfs builder metadata is missing numeric {}",
                snake_key
            ))
        })
}

async fn run_streaming_process(
    proxy: &SandboxProxyClient,
    command: &str,
    args: Vec<String>,
    env: Option<Map<String, Value>>,
    working_dir: Option<String>,
) -> Result<()> {
    let mut payload = Map::new();
    payload.insert("command".to_string(), Value::String(command.to_string()));
    payload.insert(
        "args".to_string(),
        Value::Array(args.into_iter().map(Value::String).collect()),
    );
    if let Some(env) = env {
        payload.insert("env".to_string(), Value::Object(env));
    }
    if let Some(working_dir) = working_dir {
        payload.insert("working_dir".to_string(), Value::String(working_dir));
    }

    let started = proxy.start_process(&Value::Object(payload)).await?;
    let pid = started.pid;
    let mut stdout_seen = 0usize;
    let mut stderr_seen = 0usize;
    let mut info: ProcessInfo;

    loop {
        let stdout = proxy.get_stdout(pid).await?;
        for line in stdout.lines.iter().skip(stdout_seen) {
            eprintln!("{}", line);
        }
        stdout_seen = stdout.lines.len();

        let stderr = proxy.get_stderr(pid).await?;
        for line in stderr.lines.iter().skip(stderr_seen) {
            eprintln!("{}", line);
        }
        stderr_seen = stderr.lines.len();

        info = proxy.get_process(pid).await?.into_inner();
        if info.status != "running" {
            break;
        }

        tokio::time::sleep(PROCESS_POLL_INTERVAL).await;
    }

    for _ in 0..PROCESS_EXIT_POLL_ATTEMPTS {
        if info.exit_code.is_some() || info.signal.is_some() {
            break;
        }
        tokio::time::sleep(PROCESS_EXIT_POLL_INTERVAL).await;
        info = proxy.get_process(pid).await?.into_inner();
    }

    let exit_code = if let Some(code) = info.exit_code {
        code
    } else if let Some(signal) = info.signal {
        -signal
    } else {
        0
    };

    if exit_code != 0 {
        return Err(CliError::Other(anyhow::anyhow!(
            "Command '{}' failed with exit code {}",
            command,
            exit_code
        )));
    }

    Ok(())
}

async fn copy_local_path(
    proxy: &SandboxProxyClient,
    local_path: &Path,
    remote_path: &str,
) -> Result<()> {
    if local_path.is_file() {
        ensure_remote_parent_dir(proxy, remote_path).await?;
        let content = tokio::fs::read(local_path).await.map_err(CliError::Io)?;
        proxy.write_file(remote_path, content).await?;
        return Ok(());
    }

    if local_path.is_dir() {
        for (full_path, relative_path) in collect_dir_files(local_path, local_path)? {
            let remote_destination = join_posix(remote_path, &relative_path);
            ensure_remote_parent_dir(proxy, &remote_destination).await?;
            let content = tokio::fs::read(&full_path).await.map_err(CliError::Io)?;
            proxy.write_file(&remote_destination, content).await?;
        }
        return Ok(());
    }

    Err(CliError::Other(anyhow::anyhow!(
        "Local path not found: {}",
        local_path.display()
    )))
}

async fn ensure_remote_parent_dir(proxy: &SandboxProxyClient, remote_path: &str) -> Result<()> {
    let parent_dir = parent_posix(remote_path);
    run_streaming_process(
        proxy,
        "mkdir",
        vec!["-p".to_string(), parent_dir],
        None,
        None,
    )
    .await
}

fn is_localhost(ctx: &CliContext) -> bool {
    if let Ok(parsed) = url::Url::parse(&ctx.api_url) {
        return matches!(parsed.host_str(), Some("localhost" | "127.0.0.1"));
    }
    false
}

fn load_dockerfile_plan(
    dockerfile_path: &str,
    registered_name: Option<&str>,
) -> Result<DockerfileBuildPlan> {
    let path = Path::new(dockerfile_path);
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(CliError::Io)?.join(path)
    };
    if !absolute_path.is_file() {
        return Err(CliError::Other(anyhow::anyhow!(
            "Dockerfile not found: {}",
            dockerfile_path
        )));
    }

    let dockerfile_text = std::fs::read_to_string(&absolute_path).map_err(CliError::Io)?;
    let mut base_image: Option<String> = None;

    for (line_number, line) in logical_dockerfile_lines(&dockerfile_text) {
        let (keyword, value) = split_instruction(&line, line_number)?;
        if keyword == "FROM" {
            if base_image.is_some() {
                return Err(CliError::Other(anyhow::anyhow!(
                    "line {}: multi-stage Dockerfiles are not supported for sandbox image creation",
                    line_number
                )));
            }
            base_image = Some(parse_from_value(&value, line_number)?);
        }
    }

    let base_image = base_image.ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "Dockerfile must contain a FROM instruction"
        ))
    })?;

    Ok(DockerfileBuildPlan {
        context_dir: absolute_path
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf(),
        registered_name: registered_name
            .map(str::to_string)
            .unwrap_or_else(|| default_registered_name(&absolute_path)),
        dockerfile_text,
        base_image,
    })
}

fn default_registered_name(dockerfile_path: &Path) -> String {
    let stem = dockerfile_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if stem.eq_ignore_ascii_case("dockerfile") {
        return dockerfile_path
            .parent()
            .and_then(|value| value.file_name())
            .and_then(|value| value.to_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("sandbox-image")
            .to_string();
    }
    if stem.is_empty() {
        "sandbox-image".to_string()
    } else {
        stem.to_string()
    }
}

fn logical_dockerfile_lines(dockerfile_text: &str) -> Vec<(usize, String)> {
    let mut logical_lines = Vec::new();
    let mut parts = Vec::new();
    let mut start_line = None;

    for (index, raw_line) in dockerfile_text.lines().enumerate() {
        let line_number = index + 1;
        let stripped = raw_line.trim();
        if parts.is_empty() && (stripped.is_empty() || stripped.starts_with('#')) {
            continue;
        }

        if start_line.is_none() {
            start_line = Some(line_number);
        }

        let mut line = raw_line.trim_end().to_string();
        let continued = line.ends_with('\\');
        if continued {
            line.pop();
        }

        let normalized = line.trim();
        if !normalized.is_empty() && !normalized.starts_with('#') {
            parts.push(normalized.to_string());
        }

        if continued {
            continue;
        }

        if !parts.is_empty() {
            logical_lines.push((start_line.unwrap_or(1), parts.join(" ")));
        }
        parts.clear();
        start_line = None;
    }

    if !parts.is_empty() {
        logical_lines.push((start_line.unwrap_or(1), parts.join(" ")));
    }

    logical_lines
}

fn split_instruction(line: &str, line_number: usize) -> Result<(String, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(CliError::Other(anyhow::anyhow!(
            "line {}: empty Dockerfile instruction",
            line_number
        )));
    }
    if let Some(index) = trimmed.find(char::is_whitespace) {
        let keyword = trimmed[..index].to_ascii_uppercase();
        let value = trimmed[index..].trim().to_string();
        Ok((keyword, value))
    } else {
        Ok((trimmed.to_ascii_uppercase(), String::new()))
    }
}

fn parse_from_value(value: &str, line_number: usize) -> Result<String> {
    let (_, remainder) = strip_leading_flags(value)?;
    let tokens = shlex_split(&remainder).ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "line {}: invalid FROM syntax '{}'",
            line_number,
            value
        ))
    })?;
    if tokens.is_empty() {
        return Err(CliError::Other(anyhow::anyhow!(
            "line {}: FROM must include a base image",
            line_number
        )));
    }
    if tokens.len() > 1 && !tokens[1].eq_ignore_ascii_case("as") {
        return Err(CliError::Other(anyhow::anyhow!(
            "line {}: unsupported FROM syntax '{}'",
            line_number,
            value
        )));
    }
    Ok(tokens[0].clone())
}

fn strip_leading_flags(value: &str) -> Result<(Vec<(String, String)>, String)> {
    let mut flags = Vec::new();
    let mut remaining = value.trim_start().to_string();

    while remaining.starts_with("--") {
        let (token, rest) = match remaining.split_once(' ') {
            Some((token, rest)) => (token.to_string(), rest.trim_start().to_string()),
            None => {
                return Err(CliError::Other(anyhow::anyhow!(
                    "invalid Dockerfile flag syntax: {}",
                    value
                )));
            }
        };

        let flag_body = &token[2..];
        if let Some((key, flag_value)) = flag_body.split_once('=') {
            flags.push((key.to_string(), flag_value.to_string()));
            remaining = rest;
        } else if let Some((flag_value, tail)) = rest.split_once(' ') {
            flags.push((flag_body.to_string(), flag_value.to_string()));
            remaining = tail.trim_start().to_string();
        } else {
            return Err(CliError::Other(anyhow::anyhow!(
                "missing value for Dockerfile flag '{}'",
                token
            )));
        }
    }

    Ok((flags, remaining))
}

fn normalize_posix(path: &str) -> String {
    let mut parts = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }

    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn parent_posix(path: &str) -> String {
    let normalized = normalize_posix(path);
    if normalized == "/" {
        return "/".to_string();
    }
    match normalized.rsplit_once('/') {
        Some(("", _)) | None => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
    }
}

fn join_posix(base: &str, child: &str) -> String {
    normalize_posix(&format!("{}/{}", base.trim_end_matches('/'), child))
}

fn collect_dir_files(root: &Path, current: &Path) -> Result<Vec<(PathBuf, String)>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(current).map_err(CliError::Io)? {
        let entry = entry.map_err(CliError::Io)?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(collect_dir_files(root, &path)?);
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| CliError::Other(anyhow::anyhow!("{}", error)))?;
            let relative = relative
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            files.push((path, relative));
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::{
        CompleteSandboxTemplateBuildRequest, PreparedRootfsBuilder, PreparedRootfsParent,
        PreparedSandboxTemplateBuild, build_rootfs_spec, complete_request_from_metadata,
        default_registered_name, load_dockerfile_plan, logical_dockerfile_lines, normalize_posix,
        rootfs_builder_env, rootfs_builder_executable, rootfs_disk_bytes,
    };
    use serde_json::{Value, json};
    use std::io::Write;

    #[test]
    fn default_registered_name_uses_parent_for_dockerfile() {
        let path = std::path::Path::new("/tmp/example/Dockerfile");
        assert_eq!(default_registered_name(path), "example");
    }

    #[test]
    fn logical_dockerfile_lines_collapses_continuations() {
        let lines = logical_dockerfile_lines("FROM ubuntu\nRUN echo one \\\n  && echo two\n");
        assert_eq!(
            lines,
            vec![
                (1, "FROM ubuntu".to_string()),
                (2, "RUN echo one && echo two".to_string())
            ]
        );
    }

    #[test]
    fn load_dockerfile_plan_reads_base_image_and_name() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        let mut file = std::fs::File::create(&dockerfile_path).unwrap();
        writeln!(file, "FROM python:3.12-slim\nUSER app\nRUN echo hi").unwrap();

        let plan = load_dockerfile_plan(dockerfile_path.to_str().unwrap(), None).unwrap();
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert_eq!(
            plan.registered_name,
            temp_dir.path().file_name().unwrap().to_string_lossy()
        );
    }

    #[test]
    fn load_dockerfile_plan_rejects_multistage_builds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(&dockerfile_path, "FROM alpine AS build\nFROM scratch\n").unwrap();

        let error = load_dockerfile_plan(dockerfile_path.to_str().unwrap(), None).unwrap_err();
        assert!(
            error.to_string().contains("multi-stage Dockerfiles"),
            "{error}"
        );
    }

    #[test]
    fn rootfs_disk_bytes_uses_default_and_validates_overflow() {
        assert_eq!(rootfs_disk_bytes(None).unwrap(), 10 * 1024 * 1024 * 1024);
        assert!(rootfs_disk_bytes(Some(u64::MAX)).is_err());
    }

    #[test]
    fn build_rootfs_spec_adds_cli_builder_inputs() {
        let prepared = json!({
            "buildId": "build-1",
            "snapshotId": "snapshot-1",
            "snapshotUri": "s3://bucket/snapshot.tlsnap",
            "rootfsNodeKind": "base",
            "builder": {
                "image": "tensorlake/rootfs-builder",
                "command": "tl-rootfs-build",
                "cpus": 2,
                "memoryMb": 4096,
                "diskMb": 30720
            },
            "upload": {
                "kind": "single_put",
                "method": "PUT",
                "url": "https://example/upload",
                "headers": {},
                "expiresAt": "2026-05-12T00:00:00Z"
            },
            "runtimeContract": {
                "guestRuntimeLayout": "embedded",
                "guestRuntimeDriveFormat": "none",
                "guestBootContract": "supervisor-init-wrapper-v1"
            }
        });
        let plan = super::DockerfileBuildPlan {
            context_dir: "/tmp/context".into(),
            registered_name: "child".to_string(),
            dockerfile_text: "FROM alpine\nRUN echo hi\n".to_string(),
            base_image: "alpine".to_string(),
        };

        let spec = build_rootfs_spec(&prepared, &plan, Some(2048), Some("{}".to_string())).unwrap();
        assert_eq!(spec["dockerfile"], "FROM alpine\nRUN echo hi\n");
        assert_eq!(
            spec["contextDir"],
            "/var/lib/tensorlake/rootfs-builder/build/context"
        );
        assert_eq!(spec["rootfsDiskBytes"], 2048_u64 * 1024 * 1024);
        assert_eq!(spec["dockerConfigJson"], "{}");
    }

    #[test]
    fn rootfs_builder_command_uses_installed_path_and_tool_path() {
        assert_eq!(
            rootfs_builder_executable("tl-rootfs-build"),
            "/usr/local/bin/tl-rootfs-build"
        );
        assert_eq!(
            rootfs_builder_executable("/custom/tl-rootfs-build"),
            "/custom/tl-rootfs-build"
        );

        let env = rootfs_builder_env();
        assert_eq!(
            env.get("PATH").and_then(Value::as_str),
            Some("/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
        );
    }

    #[test]
    fn complete_request_maps_snapshotter_metadata_to_platform_api_shape() {
        let prepared = prepared_build("diff");
        let metadata = json!({
            "snapshot_id": "snapshot-1",
            "snapshot_uri": "s3://bucket/child.tlsnap",
            "snapshot_format_version": "durable_archive_v1",
            "snapshot_size_bytes": 1234,
            "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64,
            "rootfs_node_kind": "diff",
            "parent_manifest_uri": "s3://bucket/parent.tlsnap"
        });

        let request = complete_request_from_metadata(&prepared, &metadata).unwrap();
        let body = serde_json::to_value(&request).unwrap();
        assert_eq!(body["snapshotId"], "snapshot-1");
        assert_eq!(body["snapshotUri"], "s3://bucket/child.tlsnap");
        assert_eq!(body["snapshotFormatVersion"], "durable_archive_v1");
        assert_eq!(body["snapshotSizeBytes"], 1234);
        assert_eq!(body["rootfsNodeKind"], "diff");
        assert_eq!(body["parentManifestUri"], "s3://bucket/parent.tlsnap");
    }

    #[test]
    fn complete_request_uses_prepared_parent_for_diff_when_metadata_omits_it() {
        let prepared = prepared_build("diff");
        let metadata = json!({
            "snapshot_format_version": "durable_archive_v1",
            "snapshot_size_bytes": "1234",
            "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64
        });

        let request = complete_request_from_metadata(&prepared, &metadata).unwrap();
        assert_eq!(request.snapshot_id, "snapshot-prepared");
        assert_eq!(request.snapshot_uri, "s3://bucket/prepared.tlsnap");
        assert_eq!(
            request.parent_manifest_uri.as_deref(),
            Some("s3://bucket/parent.tlsnap")
        );
    }

    #[test]
    fn normalize_posix_collapses_dot_segments() {
        assert_eq!(normalize_posix("/a//b/../c"), "/a/c");
    }

    fn prepared_build(rootfs_node_kind: &str) -> PreparedSandboxTemplateBuild {
        PreparedSandboxTemplateBuild {
            build_id: "build-1".to_string(),
            snapshot_id: "snapshot-prepared".to_string(),
            snapshot_uri: "s3://bucket/prepared.tlsnap".to_string(),
            rootfs_node_kind: rootfs_node_kind.to_string(),
            builder: PreparedRootfsBuilder {
                image: "tensorlake/rootfs-builder".to_string(),
                command: "tl-rootfs-build".to_string(),
                cpus: 2.0,
                memory_mb: 4096,
                disk_mb: 30720,
            },
            parent: Some(PreparedRootfsParent {
                parent_manifest_uri: "s3://bucket/parent.tlsnap".to_string(),
            }),
        }
    }

    #[allow(dead_code)]
    fn assert_serialize(_: &CompleteSandboxTemplateBuildRequest, _: &Value) {}
}
