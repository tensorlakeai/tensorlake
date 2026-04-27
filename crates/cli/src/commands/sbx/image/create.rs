use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

use serde_json::{Map, Value};
use shlex::split as shlex_split;
use tensorlake::{
    Client,
    sandbox_templates::models::CreateSandboxTemplateRequest,
    sandboxes::{
        SandboxProxyClient, SandboxesClient,
        models::{
            CreateSandboxRequest, CreateSandboxResources, ProcessInfo, SnapshotContentMode,
            SnapshotInfo,
        },
    },
};

use crate::{
    auth::context::CliContext,
    commands::sbx::{DEFAULT_SANDBOX_WAIT_TIMEOUT, sandbox_proxy_base, wait_for_sandbox_status},
    error::{CliError, Result},
};

const BUILD_SANDBOX_PIP_ENV: &[(&str, &str)] = &[("PIP_BREAK_SYSTEM_PACKAGES", "1")];
const DEFAULT_CPUS: f64 = 2.0;
const DEFAULT_MEMORY_MB: i64 = 4096;
const SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const SNAPSHOT_WAIT_TIMEOUT: Duration = Duration::from_secs(300);
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(300);
const PROCESS_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(200);
const PROCESS_EXIT_POLL_ATTEMPTS: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DockerfileInstruction {
    keyword: String,
    value: String,
    line_number: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DockerfileBuildPlan {
    context_dir: PathBuf,
    registered_name: String,
    dockerfile_text: String,
    base_image: String,
    instructions: Vec<DockerfileInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotRegistrationMetadata {
    snapshot_id: String,
    sandbox_id: String,
    snapshot_uri: String,
    snapshot_size_bytes: u64,
    rootfs_disk_bytes: u64,
}

pub async fn run(
    ctx: &CliContext,
    dockerfile_path: &str,
    registered_name: Option<&str>,
    disk_gb: Option<u64>,
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    is_public: bool,
) -> Result<()> {
    eprintln!("⚙️  Loading Dockerfile...");
    let plan = load_dockerfile_plan(dockerfile_path, registered_name)?;
    eprintln!("⚙️  Selected image name: {}", plan.registered_name);

    let client = super::scoped_cloud_client(ctx)?;
    let sandboxes = SandboxesClient::new(client.clone(), ctx.namespace.clone(), is_localhost(ctx));
    let templates = super::sandbox_templates_client(ctx)?;

    let resources = CreateSandboxResources {
        cpus: cpus.unwrap_or(DEFAULT_CPUS),
        memory_mb: memory_mb.unwrap_or(DEFAULT_MEMORY_MB),
        disk_mb: disk_gb
            .map(|gib| {
                gib.checked_mul(1024).ok_or_else(|| {
                    CliError::usage("disk size is too large to express in MiB".to_string())
                })
            })
            .transpose()?,
    };

    eprintln!("⚙️  Creating build sandbox...");
    let created = sandboxes
        .create(&CreateSandboxRequest {
            image: Some(plan.base_image.clone()),
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
            &format!("Waiting for build sandbox {sandbox_id}"),
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
        eprintln!("⚙️  Build sandbox {sandbox_id} is running");

        let proxy = sandbox_proxy_client(ctx, &client, &sandbox_id, routing_hint)?;
        execute_dockerfile_plan(&proxy, &plan).await?;

        eprintln!("⚙️  Creating snapshot...");
        let snapshot = create_snapshot_and_wait(&sandboxes, &sandbox_id).await?;
        eprintln!("📸 Snapshot created: {}", snapshot.snapshot_id);

        eprintln!("⚙️  Registering image '{}'...", plan.registered_name);
        let registered = templates
            .create(&CreateSandboxTemplateRequest {
                name: plan.registered_name.clone(),
                dockerfile: plan.dockerfile_text.clone(),
                snapshot_id: snapshot.snapshot_id.clone(),
                snapshot_sandbox_id: snapshot.sandbox_id.clone(),
                snapshot_uri: snapshot.snapshot_uri.clone(),
                snapshot_size_bytes: snapshot.snapshot_size_bytes,
                rootfs_disk_bytes: snapshot.rootfs_disk_bytes,
                public: is_public,
            })
            .await?;

        let template_id = registered.id.as_deref().unwrap_or("-");
        eprintln!(
            "✅ Image '{}' registered ({})",
            plan.registered_name, template_id
        );
        Ok(())
    }
    .await;

    if let Err(error) = sandboxes.delete(&sandbox_id).await {
        eprintln!(
            "⚠️  Failed to terminate build sandbox {} during cleanup: {}",
            sandbox_id, error
        );
    }

    result
}

fn sandbox_proxy_client(
    ctx: &CliContext,
    client: &Client,
    sandbox_id: &str,
    routing_hint: Option<String>,
) -> Result<SandboxProxyClient> {
    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
    Ok(
        SandboxProxyClient::new(client.with_base_url(&proxy_base), host_override)
            .with_routing_hint(routing_hint),
    )
}

async fn create_snapshot_and_wait(
    sandboxes: &SandboxesClient,
    sandbox_id: &str,
) -> Result<SnapshotRegistrationMetadata> {
    let snapshot = sandboxes
        .snapshot(sandbox_id, Some(SnapshotContentMode::FilesystemOnly))
        .await?;
    let snapshot_id = snapshot.snapshot_id.clone();

    wait_for_snapshot(&snapshot_id, SNAPSHOT_WAIT_TIMEOUT, || {
        let snapshot_id = snapshot_id.clone();
        async move {
            sandboxes
                .get_snapshot(&snapshot_id)
                .await
                .map(|snapshot| snapshot.into_inner())
                .map_err(Into::into)
        }
    })
    .await
}

async fn wait_for_snapshot<F, Fut>(
    snapshot_id: &str,
    timeout: Duration,
    mut fetch_snapshot: F,
) -> Result<SnapshotRegistrationMetadata>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<SnapshotInfo>>,
{
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(CliError::Other(anyhow::anyhow!(
                "snapshot {} did not complete within {}s",
                snapshot_id,
                timeout.as_secs()
            )));
        }

        let info = fetch_snapshot().await?;
        match info.status.as_str() {
            "completed" => return parse_completed_snapshot(&info),
            "failed" => {
                let error = info.error.as_deref().unwrap_or("unknown error");
                return Err(CliError::Other(anyhow::anyhow!(
                    "snapshot {} failed: {}",
                    snapshot_id,
                    error
                )));
            }
            _ => {
                tokio::time::sleep(SNAPSHOT_POLL_INTERVAL).await;
            }
        }
    }
}

fn parse_completed_snapshot(info: &SnapshotInfo) -> Result<SnapshotRegistrationMetadata> {
    let snapshot_uri = info.snapshot_uri.clone().ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "snapshot {} completed without snapshot_uri",
            info.snapshot_id
        ))
    })?;
    let snapshot_size_bytes = info
        .size_bytes
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "snapshot {} completed without size_bytes",
                info.snapshot_id
            ))
        })?
        .try_into()
        .map_err(|_| {
            CliError::Other(anyhow::anyhow!(
                "snapshot {} reported a negative size_bytes",
                info.snapshot_id
            ))
        })?;
    let rootfs_disk_bytes = info.rootfs_disk_bytes.ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "snapshot {} completed without rootfs_disk_bytes",
            info.snapshot_id
        ))
    })?;

    Ok(SnapshotRegistrationMetadata {
        snapshot_id: info.snapshot_id.clone(),
        sandbox_id: info.sandbox_id.clone(),
        snapshot_uri,
        snapshot_size_bytes,
        rootfs_disk_bytes,
    })
}

async fn execute_dockerfile_plan(
    proxy: &SandboxProxyClient,
    plan: &DockerfileBuildPlan,
) -> Result<()> {
    let mut process_env = BUILD_SANDBOX_PIP_ENV
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect::<Vec<(String, String)>>();
    let mut working_dir = "/".to_string();

    for instruction in &plan.instructions {
        match instruction.keyword.as_str() {
            "RUN" => {
                eprintln!("⚙️  RUN {}", instruction.value);
                run_streaming_process(
                    proxy,
                    "sh",
                    vec!["-c".to_string(), instruction.value.clone()],
                    Some(build_env_map(&process_env)),
                    Some(working_dir.clone()),
                )
                .await?;
            }
            "WORKDIR" => {
                let tokens = shlex_split(&instruction.value).ok_or_else(|| {
                    CliError::Other(anyhow::anyhow!(
                        "line {}: invalid WORKDIR syntax",
                        instruction.line_number
                    ))
                })?;
                if tokens.len() != 1 {
                    return Err(CliError::Other(anyhow::anyhow!(
                        "line {}: WORKDIR must include exactly one path",
                        instruction.line_number
                    )));
                }
                working_dir = resolve_container_path(&tokens[0], &working_dir);
                eprintln!("⚙️  WORKDIR {}", working_dir);
                run_streaming_process(
                    proxy,
                    "mkdir",
                    vec!["-p".to_string(), working_dir.clone()],
                    Some(build_env_map(&process_env)),
                    None,
                )
                .await?;
            }
            "ENV" => {
                for (key, value) in parse_env_pairs(&instruction.value, instruction.line_number)? {
                    eprintln!("⚙️  ENV {}={}", key, value);
                    process_env.push((key.clone(), value.clone()));
                    persist_env_var(proxy, &process_env, &key, &value).await?;
                }
            }
            "COPY" => {
                let (_, sources, destination) = parse_copy_like_values(
                    &instruction.value,
                    instruction.line_number,
                    &instruction.keyword,
                )?;
                copy_from_context(
                    proxy,
                    &plan.context_dir,
                    &sources,
                    &destination,
                    &working_dir,
                    &instruction.keyword,
                    &process_env,
                )
                .await?;
            }
            "ADD" => {
                let (_, sources, destination) = parse_copy_like_values(
                    &instruction.value,
                    instruction.line_number,
                    &instruction.keyword,
                )?;
                if sources.len() == 1 && is_http_source(&sources[0]) {
                    add_url_to_sandbox(
                        proxy,
                        &sources[0],
                        &destination,
                        &working_dir,
                        &process_env,
                    )
                    .await?;
                } else {
                    copy_from_context(
                        proxy,
                        &plan.context_dir,
                        &sources,
                        &destination,
                        &working_dir,
                        &instruction.keyword,
                        &process_env,
                    )
                    .await?;
                }
            }
            "CMD" | "ENTRYPOINT" | "EXPOSE" | "HEALTHCHECK" | "LABEL" | "STOPSIGNAL" | "VOLUME" => {
                eprintln!(
                    "⚠️  Skipping Dockerfile instruction '{}' during snapshot materialization. It is still preserved in the registered Dockerfile.",
                    instruction.keyword
                );
            }
            other => {
                return Err(CliError::Other(anyhow::anyhow!(
                    "line {}: Dockerfile instruction '{}' is not supported for sandbox image creation",
                    instruction.line_number,
                    other
                )));
            }
        }
    }

    Ok(())
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

async fn persist_env_var(
    proxy: &SandboxProxyClient,
    process_env: &[(String, String)],
    key: &str,
    value: &str,
) -> Result<()> {
    let escaped_value = value.replace('\\', "\\\\").replace('"', "\\\"");
    let export_line = format!("export {key}=\"{escaped_value}\"");
    run_streaming_process(
        proxy,
        "sh",
        vec![
            "-c".to_string(),
            format!(
                "printf '%s\\n' {} >> /etc/environment",
                shell_quote(&export_line)
            ),
        ],
        Some(build_env_map(process_env)),
        None,
    )
    .await
}

async fn copy_from_context(
    proxy: &SandboxProxyClient,
    context_dir: &Path,
    sources: &[String],
    destination: &str,
    working_dir: &str,
    keyword: &str,
    process_env: &[(String, String)],
) -> Result<()> {
    let destination_path = resolve_container_path(destination, working_dir);
    if sources.len() > 1 && !destination_path.ends_with('/') {
        return Err(CliError::Other(anyhow::anyhow!(
            "{} with multiple sources requires a directory destination ending in '/'",
            keyword
        )));
    }

    for source in sources {
        let local_source = resolve_context_source_path(context_dir, source)?;
        let remote_destination = if sources.len() > 1 {
            join_posix(
                destination_path.trim_end_matches('/'),
                file_name_string(Path::new(source))?.as_str(),
            )
        } else if local_source.is_file() && destination_path.ends_with('/') {
            join_posix(
                destination_path.trim_end_matches('/'),
                file_name_string(Path::new(source))?.as_str(),
            )
        } else {
            destination_path.clone()
        };

        eprintln!("⚙️  {} {} -> {}", keyword, source, remote_destination);
        copy_local_path(proxy, &local_source, &remote_destination, process_env).await?;
    }

    Ok(())
}

async fn add_url_to_sandbox(
    proxy: &SandboxProxyClient,
    url: &str,
    destination: &str,
    working_dir: &str,
    process_env: &[(String, String)],
) -> Result<()> {
    let mut destination_path = resolve_container_path(destination, working_dir);
    let parsed = url::Url::parse(url).map_err(|error| {
        CliError::Other(anyhow::anyhow!("invalid ADD URL '{}': {}", url, error))
    })?;
    let file_name = parsed
        .path_segments()
        .and_then(|segments| segments.filter(|segment| !segment.is_empty()).next_back())
        .unwrap_or("downloaded");
    if destination_path.ends_with('/') {
        destination_path = join_posix(destination_path.trim_end_matches('/'), file_name);
    }
    let parent_dir = parent_posix(&destination_path);

    eprintln!("⚙️  ADD {} -> {}", url, destination_path);
    run_streaming_process(
        proxy,
        "mkdir",
        vec!["-p".to_string(), parent_dir.clone()],
        Some(build_env_map(process_env)),
        None,
    )
    .await?;
    run_streaming_process(
        proxy,
        "sh",
        vec![
            "-c".to_string(),
            format!(
                "curl -fsSL --location {} -o {}",
                shell_quote(url),
                shell_quote(&destination_path)
            ),
        ],
        Some(build_env_map(process_env)),
        Some(working_dir.to_string()),
    )
    .await
}

async fn copy_local_path(
    proxy: &SandboxProxyClient,
    local_path: &Path,
    remote_path: &str,
    process_env: &[(String, String)],
) -> Result<()> {
    if local_path.is_file() {
        ensure_remote_parent_dir(proxy, remote_path, process_env).await?;
        let content = tokio::fs::read(local_path).await.map_err(CliError::Io)?;
        proxy.write_file(remote_path, content).await?;
        return Ok(());
    }

    if local_path.is_dir() {
        for (full_path, relative_path) in collect_dir_files(local_path, local_path)? {
            let remote_destination = join_posix(remote_path, &relative_path);
            ensure_remote_parent_dir(proxy, &remote_destination, process_env).await?;
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

async fn ensure_remote_parent_dir(
    proxy: &SandboxProxyClient,
    remote_path: &str,
    process_env: &[(String, String)],
) -> Result<()> {
    let parent_dir = parent_posix(remote_path);
    run_streaming_process(
        proxy,
        "mkdir",
        vec!["-p".to_string(), parent_dir],
        Some(build_env_map(process_env)),
        None,
    )
    .await
}

fn resolve_context_source_path(context_dir: &Path, source: &str) -> Result<PathBuf> {
    let resolved_context_dir = std::fs::canonicalize(context_dir).map_err(CliError::Io)?;
    let candidate = if Path::new(source).is_absolute() {
        PathBuf::from(source)
    } else {
        resolved_context_dir.join(source)
    };
    let resolved_source = std::fs::canonicalize(&candidate).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            CliError::Other(anyhow::anyhow!(
                "Local path not found: {}",
                candidate.display()
            ))
        } else {
            CliError::Io(error)
        }
    })?;

    if !resolved_source.starts_with(&resolved_context_dir) {
        return Err(CliError::Other(anyhow::anyhow!(
            "Local path escapes the build context: {}",
            source
        )));
    }

    Ok(resolved_source)
}

fn build_env_map(env: &[(String, String)]) -> Map<String, Value> {
    env.iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect()
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
    let mut instructions = Vec::new();

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
            continue;
        }

        if matches!(keyword.as_str(), "ARG" | "ONBUILD" | "SHELL" | "USER") {
            return Err(CliError::Other(anyhow::anyhow!(
                "line {}: Dockerfile instruction '{}' is not supported for sandbox image creation",
                line_number,
                keyword
            )));
        }

        instructions.push(DockerfileInstruction {
            keyword,
            value,
            line_number,
        });
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
        instructions,
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

fn parse_copy_like_values(
    value: &str,
    line_number: usize,
    keyword: &str,
) -> Result<(Vec<(String, String)>, Vec<String>, String)> {
    let (flags, payload) = strip_leading_flags(value)?;
    if flags.iter().any(|(key, _)| key == "from") {
        return Err(CliError::Other(anyhow::anyhow!(
            "line {}: {} --from is not supported for sandbox image creation",
            line_number,
            keyword
        )));
    }
    let payload = payload.trim();
    if payload.is_empty() {
        return Err(CliError::Other(anyhow::anyhow!(
            "line {}: {} must include source and destination",
            line_number,
            keyword
        )));
    }

    let parts = if payload.starts_with('[') {
        let items: Vec<String> = serde_json::from_str(payload).map_err(|error| {
            CliError::Other(anyhow::anyhow!(
                "line {}: invalid JSON array syntax for {}: {}",
                line_number,
                keyword,
                error
            ))
        })?;
        if items.len() < 2 {
            return Err(CliError::Other(anyhow::anyhow!(
                "line {}: {} JSON array form requires at least two string values",
                line_number,
                keyword
            )));
        }
        items
    } else {
        let parts = shlex_split(payload).ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "line {}: invalid {} syntax",
                line_number,
                keyword
            ))
        })?;
        if parts.len() < 2 {
            return Err(CliError::Other(anyhow::anyhow!(
                "line {}: {} must include at least one source and one destination",
                line_number,
                keyword
            )));
        }
        parts
    };

    let (destination, sources) = parts.split_last().unwrap();
    Ok((flags, sources.to_vec(), destination.clone()))
}

fn parse_env_pairs(value: &str, line_number: usize) -> Result<Vec<(String, String)>> {
    let tokens = shlex_split(value).ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "line {}: invalid ENV syntax '{}'",
            line_number,
            value
        ))
    })?;
    if tokens.is_empty() {
        return Err(CliError::Other(anyhow::anyhow!(
            "line {}: ENV must include a key and value",
            line_number
        )));
    }

    if tokens.iter().all(|token| token.contains('=')) {
        return tokens
            .into_iter()
            .map(|token| {
                let (key, env_value) = token.split_once('=').ok_or_else(|| {
                    CliError::Other(anyhow::anyhow!(
                        "line {}: invalid ENV token '{}'",
                        line_number,
                        token
                    ))
                })?;
                if key.is_empty() {
                    return Err(CliError::Other(anyhow::anyhow!(
                        "line {}: invalid ENV token '{}'",
                        line_number,
                        token
                    )));
                }
                Ok((key.to_string(), env_value.to_string()))
            })
            .collect();
    }

    if tokens.len() < 2 {
        return Err(CliError::Other(anyhow::anyhow!(
            "line {}: ENV must include a key and value",
            line_number
        )));
    }

    Ok(vec![(tokens[0].clone(), tokens[1..].join(" "))])
}

fn resolve_container_path(path: &str, working_dir: &str) -> String {
    if path.is_empty() {
        return working_dir.to_string();
    }
    let preserve_trailing_slash = path.ends_with('/');
    let raw = if path.starts_with('/') {
        path.to_string()
    } else if working_dir == "/" {
        format!("/{}", path)
    } else {
        format!("{}/{}", working_dir.trim_end_matches('/'), path)
    };

    let normalized = normalize_posix(&raw);
    if preserve_trailing_slash && normalized != "/" {
        format!("{}/", normalized.trim_end_matches('/'))
    } else {
        normalized
    }
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

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn is_http_source(source: &str) -> bool {
    url::Url::parse(source)
        .map(|url| matches!(url.scheme(), "http" | "https"))
        .unwrap_or(false)
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

fn file_name_string(path: &Path) -> Result<String> {
    path.file_name()
        .map(OsString::from)
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "path '{}' has no file name",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::{
        default_registered_name, load_dockerfile_plan, logical_dockerfile_lines, normalize_posix,
        parse_copy_like_values, parse_env_pairs, resolve_container_path,
        resolve_context_source_path, wait_for_snapshot,
    };
    use std::{cell::Cell, io::Write, time::Duration};
    use tensorlake::sandboxes::models::SnapshotInfo;

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
    fn parse_copy_like_values_supports_json_array_form() {
        let (_, sources, destination) =
            parse_copy_like_values(r#"["a.txt", "b.txt", "/dst/"]"#, 1, "COPY").unwrap();
        assert_eq!(sources, vec!["a.txt".to_string(), "b.txt".to_string()]);
        assert_eq!(destination, "/dst/");
    }

    #[test]
    fn parse_env_pairs_supports_equals_form() {
        let pairs = parse_env_pairs("A=1 B=two", 1).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "two".to_string())
            ]
        );
    }

    #[test]
    fn resolve_container_path_normalizes_relative_paths() {
        assert_eq!(
            resolve_container_path("app", "/workspace"),
            "/workspace/app"
        );
        assert_eq!(
            resolve_container_path("../bin", "/workspace/app"),
            "/workspace/bin"
        );
        assert_eq!(normalize_posix("/a//b/../c"), "/a/c");
    }

    #[test]
    fn resolve_container_path_preserves_trailing_slash_for_directories() {
        assert_eq!(resolve_container_path("/dst/", "/"), "/dst/");
        assert_eq!(
            resolve_container_path("dst/", "/workspace"),
            "/workspace/dst/"
        );
    }

    #[test]
    fn load_dockerfile_plan_reads_base_image_and_name() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        let mut file = std::fs::File::create(&dockerfile_path).unwrap();
        writeln!(file, "FROM python:3.12-slim\nRUN echo hi").unwrap();

        let plan = load_dockerfile_plan(dockerfile_path.to_str().unwrap(), None).unwrap();
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert_eq!(
            plan.registered_name,
            temp_dir.path().file_name().unwrap().to_string_lossy()
        );
        assert_eq!(plan.instructions.len(), 1);
    }

    #[test]
    fn resolve_context_source_path_rejects_escaping_the_build_context() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context_dir = temp_dir.path().join("context");
        std::fs::create_dir_all(&context_dir).unwrap();
        let outside_file = temp_dir.path().join("secret.txt");
        std::fs::write(&outside_file, b"secret").unwrap();

        let error = resolve_context_source_path(&context_dir, "../secret.txt").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Local path escapes the build context"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn wait_for_snapshot_times_out() {
        let polls = Cell::new(0usize);
        let error = wait_for_snapshot("snap-1", Duration::from_secs(0), || {
            polls.set(polls.get() + 1);
            async {
                Ok(SnapshotInfo {
                    snapshot_id: "snap-1".to_string(),
                    namespace: "ns".to_string(),
                    sandbox_id: "sbx-1".to_string(),
                    base_image: None,
                    status: "pending".to_string(),
                    error: None,
                    snapshot_uri: None,
                    size_bytes: None,
                    rootfs_disk_bytes: None,
                    created_at: None,
                })
            }
        })
        .await
        .unwrap_err();

        assert!(error.to_string().contains("did not complete within 0s"));
        assert_eq!(polls.get(), 0);
    }
}
