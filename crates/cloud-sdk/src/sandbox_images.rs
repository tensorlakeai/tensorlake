use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use docker_credentials_config::DockerConfig;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use shlex::split as shlex_split;
use thiserror::Error;

use crate::{
    Client, ClientBuilder,
    error::SdkError,
    resolve_sandbox_lifecycle_url,
    sandboxes::{
        SandboxProxyClient, SandboxesClient,
        models::{CreateSandboxRequest, CreateSandboxResources, ProcessInfo, SandboxInfo},
    },
};

type Result<T> = std::result::Result<T, SandboxImageBuildError>;

const DEFAULT_ROOTFS_DISK_MB: u64 = 10 * 1024;
const DEFAULT_SANDBOX_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const SANDBOX_WAIT_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Lifetime budget requested for the temporary rootfs-builder sandbox. The
/// builder is short-lived (Dockerfile-defined RUN steps), but it has to outlive
/// long single steps like `dd if=/dev/urandom ...` or large `apt install` runs
/// that produce no client traffic. Setting an explicit value here makes the
/// CLI's behavior independent of whatever default the Platform API hands back
/// today. Paired with the keepalive loop below, which renews this budget while
/// the build is in flight.
const BUILDER_SANDBOX_TIMEOUT_SECS: i64 = 300;
/// Cadence at which the SDK pings the builder sandbox while the offline
/// rootfs builder is running, to keep it from being suspended due to
/// inactivity / lifetime expiry mid-build. Must be shorter than
/// `BUILDER_SANDBOX_TIMEOUT_SECS`.
const BUILDER_SANDBOX_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
const PROCESS_START_ATTEMPTS: usize = 3;
const PROCESS_REATTACH_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const PROCESS_REATTACH_ATTEMPTS: usize = 10;
const PROXY_READY_TIMEOUT: Duration = Duration::from_secs(120);
const PROXY_READY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const REMOTE_BUILD_DIR: &str = "/var/lib/tensorlake/rootfs-builder/build";
const REMOTE_CONTEXT_DIR: &str = "/var/lib/tensorlake/rootfs-builder/build/context";
const REMOTE_SPEC_PATH: &str = "/var/lib/tensorlake/rootfs-builder/build/spec.json";
const REMOTE_METADATA_PATH: &str = "/var/lib/tensorlake/rootfs-builder/build/metadata.json";
const ROOTFS_BUILDER_BIN_DIR: &str = "/usr/local/bin";
const ROOTFS_BUILDER_PATH: &str = "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const ROOTFS_BUILDER_COMMAND: &str = "tl-rootfs-build";
const ROOTFS_BUILDER_PROCESS_USER: &str = "root";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessTerminalStatus {
    code: i64,
    oom_killed: bool,
}

/// Dockerfile instructions rejected during sandbox-image build. `ONBUILD`
/// registers deferred triggers that only fire when the image is used as a base
/// in a downstream build, and `SHELL` changes the shell used for shell-form
/// `RUN`/`CMD`/`ENTRYPOINT`; neither has a meaningful effect on the rootfs path
/// (`docker build --output type=tar` discards image config and there is no
/// child build), so we reject them rather than accept a silent no-op.
// ARG used to be in this list; it is accepted at top-level scope so a
// Dockerfile can declare global defaults that Docker resolves at build time,
// but the SDK does not substitute ARG values at parse time.
const UNSUPPORTED_DOCKERFILE_INSTRUCTIONS: &[&str] = &["ONBUILD", "SHELL"];

/// Dockerfile instructions that affect image config (exposed ports, labels,
/// etc.) rather than the filesystem. `docker build --output type=tar` discards
/// the image config, so these are effectively no-ops on the rootfs path — we
/// warn to surface that to the user and pass them through to the builder.
const IGNORED_DOCKERFILE_INSTRUCTIONS: &[&str] =
    &["EXPOSE", "HEALTHCHECK", "LABEL", "STOPSIGNAL", "VOLUME"];

#[derive(Debug, Error)]
pub enum SandboxImageBuildError {
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    Auth(String),
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Sdk(#[from] SdkError),
    #[error("{0}")]
    Other(String),
}

impl SandboxImageBuildError {
    fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    fn auth(message: impl Into<String>) -> Self {
        Self::Auth(message.into())
    }

    fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }
}

#[derive(Debug, Clone)]
pub struct SandboxImageBuildOptions {
    pub api_url: String,
    pub bearer_token: String,
    pub use_scope_headers: bool,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub namespace: String,
    pub dockerfile_path: PathBuf,
    pub dockerfile_text: Option<String>,
    pub context_dir: Option<PathBuf>,
    pub registered_name: Option<String>,
    pub disk_mb: Option<u64>,
    pub builder_disk_mb: Option<u64>,
    pub cpus: Option<f64>,
    pub memory_mb: Option<i64>,
    pub is_public: bool,
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxImageBuildEvent {
    Status(String),
    BuildLog { stream: String, message: String },
    Warning(String),
}

#[derive(Debug, Clone)]
struct ResolvedBuildContext {
    api_url: String,
    bearer_token: String,
    use_scope_headers: bool,
    organization_id: String,
    project_id: String,
    namespace: String,
    user_agent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DockerfileBuildPlan {
    context_dir: PathBuf,
    registered_name: String,
    dockerfile_text: String,
    /// The final-stage FROM image reference — the exact string the user wrote.
    /// Determines the snapshot's lineage parent when it resolves to a
    /// registered Tensorlake template.
    base_image: String,
    /// True when `base_image` matches a stage alias defined earlier in the
    /// Dockerfile (`FROM ubuntu AS base; FROM base`). In that case the
    /// final FROM is an internal reference to an earlier stage and we do
    /// not look it up as an external template.
    base_image_is_internal_stage: bool,
    /// Every external image reference encountered in the Dockerfile other
    /// than `base_image`: earlier-stage FROMs, `COPY --from=<image>`, and
    /// `RUN --mount=type=cache,from=<image>`. Excludes `scratch`, internal
    /// stage names defined by `FROM ... AS <name>` clauses, references
    /// containing `$` variable expansions or `@` digest pins, and the
    /// final-stage FROM itself (which lives in `base_image`). Deduped, in
    /// first-seen order.
    additional_image_references: Vec<String>,
    /// References the SDK could not resolve at planning time and forwarded
    /// to Docker for registry pull at build time. Tagged with a reason so
    /// the caller can emit an appropriate warning.
    unresolvable_image_references: Vec<UnresolvableImageReference>,
    /// Instructions that hit `IGNORED_DOCKERFILE_INSTRUCTIONS` during parse.
    /// Surfaced as warnings by the caller; preserved in `dockerfile_text` and
    /// forwarded to `docker build`.
    ignored_instructions: Vec<(usize, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvableImageReference {
    line_number: usize,
    reference: String,
    reason: UnresolvableImageReferenceReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnresolvableImageReferenceReason {
    /// Reference contained `$VAR` / `${VAR}` expansion. The SDK does not
    /// resolve build-args at planning time.
    BuildArgExpansion,
    /// Reference carried an `@<digest>` suffix (e.g. `@sha256:...`).
    /// Locally-loaded images cannot match a user-supplied digest, so even if
    /// the name matches a registered template the reference falls through
    /// to a registry pull.
    DigestPin,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreparedSandboxTemplateBuild {
    build_id: String,
    snapshot_id: String,
    snapshot_uri: String,
    rootfs_node_kind: String,
    builder: PreparedRootfsBuilder,
    parent: Option<PreparedRootfsParent>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreparedRootfsBuilder {
    image: String,
    command: String,
    cpus: f64,
    memory_mb: i64,
    disk_mb: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreparedRootfsParent {
    parent_manifest_uri: String,
    #[serde(default)]
    rootfs_disk_bytes: Option<u64>,
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

pub async fn build_sandbox_image<F>(options: SandboxImageBuildOptions, mut emit: F) -> Result<Value>
where
    F: FnMut(SandboxImageBuildEvent),
{
    emit(SandboxImageBuildEvent::Status(
        "Loading Dockerfile...".to_string(),
    ));
    let plan = if let Some(dockerfile_text) = &options.dockerfile_text {
        load_dockerfile_text_plan(
            &options.dockerfile_path,
            options.context_dir.as_deref(),
            dockerfile_text.clone(),
            options.registered_name.as_deref(),
        )?
    } else {
        load_dockerfile_plan(&options.dockerfile_path, options.registered_name.as_deref())?
    };
    emit(SandboxImageBuildEvent::Status(format!(
        "Selected image name: {}",
        plan.registered_name
    )));

    for (_line_number, keyword) in &plan.ignored_instructions {
        emit(SandboxImageBuildEvent::Warning(format!(
            "Skipping Dockerfile instruction '{}' during snapshot materialization. \
             It is still preserved in the registered Dockerfile.",
            keyword
        )));
    }
    for unresolvable in &plan.unresolvable_image_references {
        let detail = match unresolvable.reason {
            UnresolvableImageReferenceReason::BuildArgExpansion => {
                "contains a build-arg expansion that the SDK does not resolve at planning time"
            }
            UnresolvableImageReferenceReason::DigestPin => {
                "is pinned to a content digest, which cannot be matched by a locally-loaded image"
            }
        };
        emit(SandboxImageBuildEvent::Warning(format!(
            "line {}: image reference '{}' {}. The build will pull this image from the configured \
             registry instead of using a Tensorlake template. If '{}' was meant to resolve to a \
             Tensorlake template, the resulting build image will be larger than a diff against \
             that template.",
            unresolvable.line_number, unresolvable.reference, detail, unresolvable.reference,
        )));
    }

    let ctx = resolve_build_context(options.clone()).await?;

    emit(SandboxImageBuildEvent::Status(
        "Preparing rootfs build...".to_string(),
    ));
    let platform_client = platform_client(&ctx)?;
    let (prepared, prepared_spec) =
        prepare_rootfs_build(&ctx, &platform_client, &plan, options.is_public).await?;
    emit(SandboxImageBuildEvent::Status(format!(
        "Build mode: Rootfs{}",
        match prepared.rootfs_node_kind.as_str() {
            "diff" => "Diff",
            _ => "Base",
        }
    )));

    let client = sandbox_lifecycle_client(&ctx)?;
    let sandboxes = SandboxesClient::new(
        client.clone(),
        ctx.namespace.clone(),
        is_localhost(&ctx.api_url),
    );
    let rootfs_disk_bytes = rootfs_disk_bytes(options.disk_mb, &prepared)?;
    let builder_disk_mb = rootfs_disk_bytes_to_mb(rootfs_disk_bytes)?
        .max(options.builder_disk_mb.unwrap_or(prepared.builder.disk_mb));
    let resources = CreateSandboxResources {
        cpus: options.cpus.unwrap_or(prepared.builder.cpus),
        memory_mb: options.memory_mb.unwrap_or(prepared.builder.memory_mb),
        disk_mb: Some(builder_disk_mb),
    };

    emit(SandboxImageBuildEvent::Status(format!(
        "Creating rootfs builder sandbox from {}...",
        prepared.builder.image
    )));
    let created = sandboxes
        .create(&CreateSandboxRequest {
            image: Some(prepared.builder.image.clone()),
            resources,
            timeout_secs: Some(BUILDER_SANDBOX_TIMEOUT_SECS),
            entrypoint: None,
            network: None,
            snapshot_id: None,
            name: None,
            cloud_init_base64: None,
        })
        .await?;
    let sandbox_id = created.sandbox_id.clone();
    let routing_hint = created.routing_hint.clone();

    let result = async {
        let running_info = wait_for_sandbox_status(
            &sandboxes,
            &sandbox_id,
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
        let ingress_endpoint = created
            .ingress_endpoint
            .clone()
            .or_else(|| running_info.ingress_endpoint.clone());
        emit(SandboxImageBuildEvent::Status(format!(
            "Rootfs builder sandbox {sandbox_id} is running"
        )));

        let proxy = sandbox_proxy_client(
            &ctx,
            &client,
            &sandbox_id,
            ingress_endpoint.as_deref(),
            routing_hint,
        )?;
        wait_for_proxy_ready(&proxy).await?;
        upload_build_inputs(
            &proxy,
            &plan,
            &prepared,
            &prepared_spec,
            options.disk_mb,
            &mut emit,
        )
        .await?;

        emit(SandboxImageBuildEvent::Status(
            "Running offline rootfs builder...".to_string(),
        ));
        // The rootfs builder runs entirely inside the sandbox and can stay
        // silent for minutes at a time (e.g. a single large `RUN dd ...` step
        // in the user's Dockerfile produces no client traffic). Keep the
        // sandbox visibly alive while that step is in flight so the Platform
        // doesn't suspend it out from under us. Aborted as soon as the
        // builder returns, regardless of outcome.
        let keepalive_task = spawn_builder_keepalive(proxy.clone());
        let builder_result = run_rootfs_builder(&proxy, &prepared.builder.command, &mut emit).await;
        keepalive_task.abort();
        builder_result?;

        let metadata = read_build_metadata(&proxy).await?;
        let complete_request = complete_request_from_metadata(&prepared, &metadata)?;

        emit(SandboxImageBuildEvent::Status(
            "Completing image registration...".to_string(),
        ));
        let registered = complete_rootfs_build(
            &ctx,
            &platform_client,
            &prepared.build_id,
            &complete_request,
        )
        .await?;
        let template_id = registered.get("id").and_then(Value::as_str).unwrap_or("-");
        emit(SandboxImageBuildEvent::Status(format!(
            "Image '{}' registered ({})",
            plan.registered_name, template_id
        )));
        Ok(registered)
    }
    .await;

    if let Err(error) = sandboxes.delete(&sandbox_id).await {
        emit(SandboxImageBuildEvent::Warning(format!(
            "Failed to terminate rootfs builder sandbox {} during cleanup: {}",
            sandbox_id, error
        )));
    }

    result
}

async fn resolve_build_context(options: SandboxImageBuildOptions) -> Result<ResolvedBuildContext> {
    let client = unscoped_client(&options)?;
    let (organization_id, project_id) = if options.use_scope_headers {
        match (options.organization_id.clone(), options.project_id.clone()) {
            (Some(organization_id), Some(project_id)) => (organization_id, project_id),
            _ => {
                return Err(SandboxImageBuildError::auth(
                    "Organization ID and project ID are required for sandbox image builds with PAT authentication",
                ));
            }
        }
    } else {
        let scope = introspect_scope(&client).await?;
        (scope.organization_id, scope.project_id)
    };

    Ok(ResolvedBuildContext {
        api_url: options.api_url,
        bearer_token: options.bearer_token,
        use_scope_headers: options.use_scope_headers,
        organization_id,
        project_id,
        namespace: options.namespace,
        user_agent: options.user_agent,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IntrospectScope {
    organization_id: String,
    project_id: String,
}

async fn introspect_scope(client: &Client) -> Result<IntrospectScope> {
    let request = client
        .request(Method::POST, "/platform/v1/keys/introspect")
        .build()?;
    let response = client.execute_raw(request).await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::auth(format!(
            "API key introspection failed (HTTP {}): {}",
            status, body
        )));
    }
    response.json().await.map_err(Into::into)
}

fn unscoped_client(options: &SandboxImageBuildOptions) -> Result<Client> {
    client_builder(
        &options.api_url,
        &options.bearer_token,
        false,
        options.organization_id.as_deref(),
        options.project_id.as_deref(),
        options.user_agent.as_deref(),
    )
    .build()
    .map_err(Into::into)
}

fn platform_client(ctx: &ResolvedBuildContext) -> Result<Client> {
    client_builder(
        &ctx.api_url,
        &ctx.bearer_token,
        ctx.use_scope_headers,
        Some(&ctx.organization_id),
        Some(&ctx.project_id),
        ctx.user_agent.as_deref(),
    )
    .build()
    .map_err(Into::into)
}

fn sandbox_lifecycle_client(ctx: &ResolvedBuildContext) -> Result<Client> {
    let lifecycle_url = resolve_sandbox_lifecycle_url(&ctx.api_url);
    client_builder(
        &lifecycle_url,
        &ctx.bearer_token,
        ctx.use_scope_headers,
        Some(&ctx.organization_id),
        Some(&ctx.project_id),
        ctx.user_agent.as_deref(),
    )
    .build()
    .map_err(Into::into)
}

fn client_builder(
    base_url: &str,
    bearer_token: &str,
    use_scope_headers: bool,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    user_agent: Option<&str>,
) -> ClientBuilder {
    let mut builder = ClientBuilder::new(base_url).bearer_token(bearer_token);
    if let Some(user_agent) = user_agent {
        builder = builder.user_agent(user_agent);
    }
    if use_scope_headers
        && let (Some(organization_id), Some(project_id)) = (organization_id, project_id)
    {
        builder = builder.scope(organization_id, project_id);
    }
    builder
}

async fn wait_for_sandbox_status(
    sandboxes: &SandboxesClient,
    sandbox_id: &str,
    target_status: &str,
    timeout: Duration,
) -> Result<SandboxInfo> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return Err(SandboxImageBuildError::other(format!(
                "Sandbox {} did not reach '{}' within {}s",
                sandbox_id,
                target_status,
                timeout.as_secs()
            )));
        }

        let info = sandboxes.get(sandbox_id).await?;
        let current_status = info.status.clone();
        if current_status == target_status {
            return Ok(info.into_inner());
        }
        if current_status == "terminated" && target_status != "terminated" {
            return Err(SandboxImageBuildError::other(format!(
                "Sandbox {} terminated before reaching '{}'",
                sandbox_id, target_status
            )));
        }

        tokio::time::sleep(SANDBOX_WAIT_POLL_INTERVAL).await;
    }
}

/// Look up `reference` as a registered sandbox template and return the
/// JSON entry the prepare endpoint expects for that local-image slot, or
/// `None` if the lookup did not resolve.
///
/// References that resolve but are not usable as a build image (only
/// `durable_archive_v1` base templates are supported by the rootfs builder
/// today) fail with a clear message tied to the offending reference so the
/// user gets feedback on the exact image string that's incompatible.
async fn resolve_template_payload(
    templates: &crate::sandbox_templates::SandboxTemplatesClient,
    reference: &str,
) -> Result<Option<Value>> {
    let Some(found) = templates.find_by_name(reference).await? else {
        return Ok(None);
    };
    let template = found.into_inner();
    let template_id = template.id.clone().ok_or_else(|| {
        SandboxImageBuildError::other(format!(
            "platform returned a template lookup for '{}' without an id",
            reference
        ))
    })?;
    let name = template.name.clone().ok_or_else(|| {
        SandboxImageBuildError::other(format!(
            "platform returned a template lookup for '{}' without a name",
            reference
        ))
    })?;
    let snapshot_id = template.snapshot_id.clone().ok_or_else(|| {
        SandboxImageBuildError::other(format!(
            "platform returned a template lookup for '{}' without a snapshot id",
            reference
        ))
    })?;
    let is_public = template.public.unwrap_or(false);
    if let Some(kind) = template.rootfs_node_kind.as_deref()
        && kind != "base"
    {
        return Err(SandboxImageBuildError::other(format!(
            "template '{}' cannot be used as a build image (only base templates are supported, got rootfsNodeKind='{}'). \
             Build a base image from this template first.",
            reference, kind
        )));
    }
    if let Some(fmt) = template.snapshot_format_version.as_deref()
        && fmt != "durable_archive_v1"
    {
        return Err(SandboxImageBuildError::other(format!(
            "template '{}' uses snapshot format '{}', which the rootfs builder cannot materialize. \
             Re-register the template with durable_archive_v1.",
            reference, fmt
        )));
    }
    Ok(Some(json!({
        "templateId": template_id,
        "name": name,
        "reference": reference,
        "snapshotId": snapshot_id,
        "public": is_public,
    })))
}

async fn prepare_rootfs_build(
    ctx: &ResolvedBuildContext,
    client: &Client,
    plan: &DockerfileBuildPlan,
    is_public: bool,
) -> Result<(PreparedSandboxTemplateBuild, Value)> {
    // Resolve every external image reference against the platform's template
    // registry. The final-stage FROM is treated separately so its resolution
    // becomes the lineage parent; the additional references (earlier stages,
    // COPY --from, RUN --mount=,from) become preload-only local images.
    let templates = crate::sandbox_templates::SandboxTemplatesClient::new(
        client.clone(),
        ctx.organization_id.clone(),
        ctx.project_id.clone(),
    );

    // Skip the lookup when the final-stage FROM is `FROM <stage-alias>` —
    // the value is an internal reference to an earlier-defined stage, not
    // an external image we should resolve as a template. Also skip when
    // the base image contains `$` (build-arg) or `@` (digest pin); those
    // were already recorded as unresolvable and warned about.
    let parent_template_payload = if plan.base_image_is_internal_stage
        || plan.base_image.contains('$')
        || plan.base_image.contains('@')
    {
        None
    } else {
        resolve_template_payload(&templates, &plan.base_image).await?
    };
    let mut additional_payload: Vec<Value> =
        Vec::with_capacity(plan.additional_image_references.len());
    for reference in &plan.additional_image_references {
        if let Some(payload) = resolve_template_payload(&templates, reference).await? {
            additional_payload.push(payload);
        }
    }
    let rootfs_node_kind = if parent_template_payload.is_some() {
        "diff"
    } else {
        "base"
    };
    let parent_template_json = parent_template_payload.unwrap_or(Value::Null);

    let request = client
        .request(Method::POST, &sandbox_template_builds_path(ctx))
        .json(&json!({
            "name": plan.registered_name,
            "dockerfile": plan.dockerfile_text,
            "baseImage": plan.base_image,
            "public": is_public,
            "rootfsNodeKind": rootfs_node_kind,
            "parentTemplate": parent_template_json,
            "additionalLocalImages": additional_payload,
        }))
        .build()?;
    let response = client.execute_raw(request).await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "failed to prepare sandbox image build (HTTP {}): {}",
            status, body
        )));
    }

    let raw: Value = response.json().await?;
    let prepared = serde_json::from_value(raw.clone())?;
    Ok((prepared, raw))
}

async fn complete_rootfs_build(
    ctx: &ResolvedBuildContext,
    client: &Client,
    build_id: &str,
    request: &CompleteSandboxTemplateBuildRequest,
) -> Result<Value> {
    let path = format!(
        "{}/{}/complete",
        sandbox_template_builds_path(ctx),
        build_id
    );
    let request = client.request(Method::POST, &path).json(request).build()?;
    let response = client.execute_raw(request).await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "failed to complete sandbox image build (HTTP {}): {}",
            status, body
        )));
    }

    response.json().await.map_err(Into::into)
}

fn sandbox_template_builds_path(ctx: &ResolvedBuildContext) -> String {
    format!(
        "/platform/v1/organizations/{}/projects/{}/sandbox-template-builds",
        ctx.organization_id, ctx.project_id
    )
}

fn sandbox_proxy_base(
    api_url: &str,
    sandbox_id: &str,
    ingress_endpoint: Option<&str>,
) -> (String, Option<String>) {
    let proxy_url = ingress_endpoint
        .map(str::to_string)
        .unwrap_or_else(|| resolve_proxy_url(api_url));

    if let Ok(parsed) = url::Url::parse(&proxy_url) {
        let host = parsed.host_str().unwrap_or("");
        if host == "localhost" || host == "127.0.0.1" {
            return (proxy_url, Some(format!("{sandbox_id}.local")));
        }
        let port_part = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
        let base_url = format!("{}://{host}{port_part}", parsed.scheme());
        return (base_url, None);
    }

    (proxy_url, None)
}

fn resolve_proxy_url(api_url: &str) -> String {
    if let Ok(url) = std::env::var("TENSORLAKE_SANDBOX_PROXY_URL") {
        return url;
    }
    if is_localhost(api_url) {
        return "http://localhost:9443".to_string();
    }
    if let Ok(parsed) = url::Url::parse(api_url) {
        let host = parsed.host_str().unwrap_or("");
        if let Some(rest) = host.strip_prefix("api.") {
            return format!("{}://sandbox.{}", parsed.scheme(), rest);
        }
    }
    "https://sandbox.tensorlake.ai".to_string()
}

fn sandbox_proxy_client(
    ctx: &ResolvedBuildContext,
    client: &Client,
    sandbox_id: &str,
    ingress_endpoint: Option<&str>,
    routing_hint: Option<String>,
) -> Result<SandboxProxyClient> {
    let (proxy_base, host_override) =
        sandbox_proxy_base(&ctx.api_url, sandbox_id, ingress_endpoint);
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
        let mut emit = |_| {};
        match run_streaming_process(proxy, "/bin/true", Vec::new(), None, None, false, &mut emit)
            .await
        {
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

fn is_transient_proxy_error(error: &SandboxImageBuildError) -> bool {
    match error {
        SandboxImageBuildError::Sdk(SdkError::ServerError { status, message }) => {
            matches!(
                *status,
                StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ) || (*status == StatusCode::BAD_REQUEST && message.contains("not running"))
                || message.contains("PROXY_ERROR")
                || message.contains("Failed to proxy request")
        }
        SandboxImageBuildError::Http(error) => error.is_timeout() || error.is_connect(),
        _ => false,
    }
}

async fn upload_build_inputs(
    proxy: &SandboxProxyClient,
    plan: &DockerfileBuildPlan,
    prepared: &PreparedSandboxTemplateBuild,
    prepared_spec: &Value,
    disk_mb: Option<u64>,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    emit(SandboxImageBuildEvent::Status(
        "Uploading build context...".to_string(),
    ));
    // Pre-create REMOTE_BUILD_DIR with permissive mode as root so the
    // sandbox-user file API can write into it. The path lives under
    // /var/lib/tensorlake/ which is root-owned in the rootfs-builder image,
    // so a plain `mkdir -p` issued as the sandbox user can't traverse and
    // create the leaf. Once the build root is world-writable, the per-file
    // `mkdir -p`s inside `copy_local_path` and the subsequent
    // `PUT /api/v1/files` calls (both running as the sandbox user) succeed
    // without further root involvement.
    ensure_remote_build_root(proxy).await?;
    copy_local_path(proxy, &plan.context_dir, REMOTE_CONTEXT_DIR).await?;

    let docker_config_json = resolved_docker_config_json().await?;
    let spec = build_rootfs_spec(prepared_spec, prepared, plan, disk_mb, docker_config_json)?;
    ensure_remote_parent_dir(proxy, REMOTE_SPEC_PATH).await?;
    proxy
        .write_file(REMOTE_SPEC_PATH, serde_json::to_vec_pretty(&spec)?)
        .await?;
    Ok(())
}

fn build_rootfs_spec(
    prepared_spec: &Value,
    prepared: &PreparedSandboxTemplateBuild,
    plan: &DockerfileBuildPlan,
    disk_mb: Option<u64>,
    docker_config_json: Option<String>,
) -> Result<Value> {
    let mut spec = prepared_spec.clone();
    let object = spec.as_object_mut().ok_or_else(|| {
        SandboxImageBuildError::other("platform API returned a non-object rootfs build spec")
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
        Value::Number(rootfs_disk_bytes(disk_mb, prepared)?.into()),
    );
    if let Some(docker_config_json) = docker_config_json {
        object.insert(
            "dockerConfigJson".to_string(),
            Value::String(docker_config_json),
        );
    }

    Ok(spec)
}

fn rootfs_disk_bytes(disk_mb: Option<u64>, prepared: &PreparedSandboxTemplateBuild) -> Result<u64> {
    if let Some(disk_mb) = disk_mb {
        return disk_mb.checked_mul(1024 * 1024).ok_or_else(|| {
            SandboxImageBuildError::usage("--disk_mb is too large to convert to bytes")
        });
    }

    if let Some(parent) = &prepared.parent {
        return parent.rootfs_disk_bytes.ok_or_else(|| {
            SandboxImageBuildError::other(
                "platform API did not return parent rootfsDiskBytes for diff build; pass --disk_mb explicitly or update Platform API"
            )
        });
    }

    Ok(DEFAULT_ROOTFS_DISK_MB * 1024 * 1024)
}

fn rootfs_disk_bytes_to_mb(rootfs_disk_bytes: u64) -> Result<u64> {
    rootfs_disk_bytes
        .checked_add((1024 * 1024) - 1)
        .ok_or_else(|| {
            SandboxImageBuildError::usage("rootfsDiskBytes is too large to convert to megabytes")
        })
        .map(|bytes| bytes / (1024 * 1024))
}

async fn resolved_docker_config_json() -> Result<Option<String>> {
    let docker_config = DockerConfig::load().await.map_err(|error| {
        SandboxImageBuildError::other(format!("Failed to load Docker config: {error}"))
    })?;
    let credentials = docker_config.all_credentials();
    if credentials.is_empty() {
        return Ok(None);
    }

    docker_config_json_from_credentials(credentials)
        .map(Some)
        .map_err(Into::into)
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

/// Spawn a background task that pings the builder sandbox at a fixed cadence
/// to keep it from being suspended due to inactivity / lifetime expiry. The
/// caller MUST `.abort()` the returned handle when the build phase finishes
/// (success or failure) — otherwise the task would outlive the build and keep
/// poking a sandbox we're about to delete.
fn spawn_builder_keepalive(proxy: SandboxProxyClient) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(BUILDER_SANDBOX_KEEPALIVE_INTERVAL);
        // The first tick fires immediately; skip it — we don't need a ping
        // right after the build kicks off, since the upload that just ran
        // already counts as activity.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            // Best-effort: a transient error here is uninteresting. If the
            // sandbox is genuinely gone, run_rootfs_builder's streaming
            // process call will surface the real error.
            let _ = proxy.health().await;
        }
    })
}

async fn run_rootfs_builder(
    proxy: &SandboxProxyClient,
    command: &str,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    let parts = shlex_split(command).ok_or_else(|| {
        SandboxImageBuildError::other(format!(
            "invalid rootfs builder command returned by platform API: {}",
            command
        ))
    })?;
    let Some((executable, command_args)) = parts.split_first() else {
        return Err(SandboxImageBuildError::other(
            "empty rootfs builder command returned by platform API",
        ));
    };
    let mut args = command_args.to_vec();
    args.extend([
        "--spec".to_string(),
        REMOTE_SPEC_PATH.to_string(),
        "--metadata-out".to_string(),
        REMOTE_METADATA_PATH.to_string(),
    ]);

    let executable = rootfs_builder_executable(executable);
    // The rootfs builder needs root inside the VM to run `docker build`,
    // mount loop devices, write to /var/lib/docker, etc. Everything else
    // (proxy probes, upload-prep `mkdir`) stays on the sandbox user so the
    // file API can write into the directories it creates.
    run_streaming_process(
        proxy,
        &executable,
        args,
        Some(rootfs_builder_env()),
        Some(REMOTE_BUILD_DIR.to_string()),
        true,
        emit,
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
    serde_json::from_slice(&content).map_err(Into::into)
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
        return Err(SandboxImageBuildError::other(
            "rootfs diff build completed without parent_manifest_uri",
        ));
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
        SandboxImageBuildError::other(format!("rootfs builder metadata is missing {}", snake_key))
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
            SandboxImageBuildError::other(format!(
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
    run_as_root: bool,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    let expected_args = args.clone();
    let payload = streaming_process_payload(command, args, env, working_dir, run_as_root);

    let started = start_or_recover_process(proxy, &payload, command, &expected_args, emit).await?;

    let terminal_status = stream_started_process(proxy, started.pid, emit).await?;

    if terminal_status.code != 0 {
        let reason = if terminal_status.oom_killed {
            " (process was killed by the kernel OOM killer)"
        } else {
            ""
        };
        return Err(SandboxImageBuildError::other(format!(
            "Command '{}' failed with exit code {}{}",
            command, terminal_status.code, reason
        )));
    }

    Ok(())
}

async fn start_or_recover_process(
    proxy: &SandboxProxyClient,
    payload: &Value,
    command: &str,
    expected_args: &[String],
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<ProcessInfo> {
    let mut last_error = None;

    for attempt in 1..=PROCESS_START_ATTEMPTS {
        match proxy.start_process(payload).await {
            Ok(started) => return Ok(started.into_inner()),
            Err(start_error) => {
                emit(SandboxImageBuildEvent::Status(format!(
                    "Process start attempt {} failed; looking for an already-started process...",
                    attempt
                )));
                match recover_started_process(proxy, command, expected_args).await {
                    Ok(process) => return Ok(process),
                    Err(recover_error) => {
                        last_error = Some(format!(
                            "start failed: {}; recovery failed: {}",
                            start_error, recover_error
                        ));
                    }
                }
            }
        }

        if attempt < PROCESS_START_ATTEMPTS {
            tokio::time::sleep(PROCESS_REATTACH_RETRY_INTERVAL).await;
        }
    }

    Err(SandboxImageBuildError::other(last_error.unwrap_or_else(
        || format!("Failed to start process '{}'", command),
    )))
}

async fn recover_started_process(
    proxy: &SandboxProxyClient,
    command: &str,
    expected_args: &[String],
) -> Result<ProcessInfo> {
    let mut last_error = None;

    for _ in 0..PROCESS_REATTACH_ATTEMPTS {
        match proxy.list_processes().await {
            Ok(processes) => {
                if let Some(process) = processes
                    .into_inner()
                    .into_iter()
                    .filter(|process| process.command == command && process.args == expected_args)
                    .max_by_key(|process| process.pid)
                {
                    return Ok(process);
                }
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
        tokio::time::sleep(PROCESS_REATTACH_RETRY_INTERVAL).await;
    }

    let message = if let Some(error) = last_error {
        format!(
            "No process found for command '{}' after process-list errors: {}",
            command, error
        )
    } else {
        format!("No process found for command '{}'", command)
    };
    Err(SandboxImageBuildError::other(message))
}

async fn stream_started_process(
    proxy: &SandboxProxyClient,
    pid: i64,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<ProcessTerminalStatus> {
    let mut output_events_seen = 0usize;
    let mut attempts = 0usize;

    loop {
        let follow_result = follow_process_output(proxy, pid, &mut output_events_seen, emit).await;

        if let Err(error) = follow_result {
            attempts += 1;
            if let Some(status) =
                get_process_terminal_status_with_retries(proxy, pid, &mut attempts).await?
            {
                return Ok(status);
            }
            if attempts >= PROCESS_REATTACH_ATTEMPTS {
                return Err(error);
            }
            emit(SandboxImageBuildEvent::Status(format!(
                "Process stream interrupted; reattaching to process {}...",
                pid
            )));
            tokio::time::sleep(PROCESS_REATTACH_RETRY_INTERVAL).await;
            continue;
        }

        attempts = 0;
        if let Some(status) =
            get_process_terminal_status_with_retries(proxy, pid, &mut attempts).await?
        {
            return Ok(status);
        }

        emit(SandboxImageBuildEvent::Status(format!(
            "Process output stream ended before process {} exited; reattaching...",
            pid
        )));
        tokio::time::sleep(PROCESS_REATTACH_RETRY_INTERVAL).await;
    }
}

async fn follow_process_output(
    proxy: &SandboxProxyClient,
    pid: i64,
    output_events_seen: &mut usize,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    let mut replayed_events_seen = 0usize;
    proxy
        .follow_output_streaming(pid, |output| {
            if replayed_events_seen < *output_events_seen {
                replayed_events_seen += 1;
                return;
            }
            replayed_events_seen += 1;
            *output_events_seen += 1;
            emit(SandboxImageBuildEvent::BuildLog {
                stream: output.stream.unwrap_or_else(|| "stdout".to_string()),
                message: output.line,
            });
        })
        .await?;

    Ok(())
}

async fn get_process_terminal_status_with_retries(
    proxy: &SandboxProxyClient,
    pid: i64,
    attempts: &mut usize,
) -> Result<Option<ProcessTerminalStatus>> {
    loop {
        match proxy.get_process(pid).await {
            Ok(info) => return Ok(process_terminal_status(&info.into_inner())),
            Err(error) => {
                *attempts += 1;
                if *attempts >= PROCESS_REATTACH_ATTEMPTS {
                    return Err(error.into());
                }
                tokio::time::sleep(PROCESS_REATTACH_RETRY_INTERVAL).await;
            }
        }
    }
}

fn process_terminal_status(info: &ProcessInfo) -> Option<ProcessTerminalStatus> {
    let oom_killed = info.oom_killed
        || info.status == "oom_killed"
        || info
            .managed
            .as_ref()
            .and_then(|managed| managed.last_exit.as_ref())
            .is_some_and(|last_exit| last_exit.oom_killed);

    if oom_killed {
        Some(ProcessTerminalStatus {
            code: info
                .signal
                .map(|signal| -signal)
                .or(info.exit_code)
                .unwrap_or(-9),
            oom_killed,
        })
    } else if let Some(code) = info.exit_code {
        Some(ProcessTerminalStatus { code, oom_killed })
    } else if let Some(signal) = info.signal {
        Some(ProcessTerminalStatus {
            code: -signal,
            oom_killed,
        })
    } else if info.status != "running" {
        Some(ProcessTerminalStatus {
            code: 0,
            oom_killed,
        })
    } else {
        None
    }
}

fn streaming_process_payload(
    command: &str,
    args: Vec<String>,
    env: Option<Map<String, Value>>,
    working_dir: Option<String>,
    run_as_root: bool,
) -> Value {
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
    // Only opt into root for the actual rootfs build command. Daemon-side, the
    // process API runs as the sandbox user by default while the file API
    // performs writes with the sandbox user's fsuid/fsgid (setfsuid/setfsgid
    // in container-daemon's file_manager). If we ran the upload-prep `mkdir`
    // as root we'd end up with directories the file API can't write into
    // and the very first `PUT /api/v1/files` would fail with EACCES (surfaced
    // as 500 "Failed to create file: …"). See the SDK PR description for the
    // dataplane log trace.
    if run_as_root {
        payload.insert(
            "user".to_string(),
            Value::String(ROOTFS_BUILDER_PROCESS_USER.to_string()),
        );
    }

    Value::Object(payload)
}

async fn copy_local_path(
    proxy: &SandboxProxyClient,
    local_path: &Path,
    remote_path: &str,
) -> Result<()> {
    if local_path.is_file() {
        ensure_remote_parent_dir(proxy, remote_path).await?;
        proxy.upload_file(remote_path, local_path).await?;
        return Ok(());
    }

    if local_path.is_dir() {
        for (full_path, relative_path) in collect_dir_files(local_path, local_path)? {
            let remote_destination = join_posix(remote_path, &relative_path);
            ensure_remote_parent_dir(proxy, &remote_destination).await?;
            proxy.upload_file(&remote_destination, &full_path).await?;
        }
        return Ok(());
    }

    Err(SandboxImageBuildError::other(format!(
        "Local path not found: {}",
        local_path.display()
    )))
}

/// Create `REMOTE_BUILD_DIR` as root and chmod it 0777 so subsequent
/// uploads — which run via the file API with the sandbox user's fsuid —
/// can write inside. Must be called once before any per-file
/// `ensure_remote_parent_dir` for paths under `REMOTE_BUILD_DIR`.
async fn ensure_remote_build_root(proxy: &SandboxProxyClient) -> Result<()> {
    let mut emit = |_| {};
    // mkdir as root: needed to traverse the root-owned ancestor
    // /var/lib/tensorlake/rootfs-builder/.
    run_streaming_process(
        proxy,
        "mkdir",
        vec!["-p".to_string(), REMOTE_BUILD_DIR.to_string()],
        None,
        None,
        true,
        &mut emit,
    )
    .await?;
    // chmod as root: open it up so the sandbox user can create the
    // upload-temp files the file API needs. Mode 0777 is fine here because
    // the builder sandbox is single-tenant and ephemeral.
    run_streaming_process(
        proxy,
        "chmod",
        vec!["0777".to_string(), REMOTE_BUILD_DIR.to_string()],
        None,
        None,
        true,
        &mut emit,
    )
    .await
}

async fn ensure_remote_parent_dir(proxy: &SandboxProxyClient, remote_path: &str) -> Result<()> {
    let parent_dir = parent_posix(remote_path);
    let mut emit = |_| {};
    // Stay on the sandbox user. The follow-up `PUT /api/v1/files` writes via
    // the file API, which the container-daemon executes with sandbox fsuid;
    // running `mkdir` as root here would create root-owned directories the
    // file API can't write into.
    run_streaming_process(
        proxy,
        "mkdir",
        vec!["-p".to_string(), parent_dir],
        None,
        None,
        false,
        &mut emit,
    )
    .await
}

fn is_localhost(url: &str) -> bool {
    if let Ok(parsed) = url::Url::parse(url) {
        return matches!(parsed.host_str(), Some("localhost" | "127.0.0.1"));
    }
    false
}

fn load_dockerfile_plan(
    dockerfile_path: &Path,
    registered_name: Option<&str>,
) -> Result<DockerfileBuildPlan> {
    let absolute_path = if dockerfile_path.is_absolute() {
        dockerfile_path.to_path_buf()
    } else {
        std::env::current_dir()?.join(dockerfile_path)
    };
    if !absolute_path.is_file() {
        return Err(SandboxImageBuildError::other(format!(
            "Dockerfile not found: {}",
            dockerfile_path.display()
        )));
    }

    let dockerfile_text = std::fs::read_to_string(&absolute_path)?;
    load_dockerfile_text_plan(&absolute_path, None, dockerfile_text, registered_name)
}

fn load_dockerfile_text_plan(
    dockerfile_path: &Path,
    context_dir: Option<&Path>,
    dockerfile_text: String,
    registered_name: Option<&str>,
) -> Result<DockerfileBuildPlan> {
    let absolute_path = if dockerfile_path.is_absolute() {
        dockerfile_path.to_path_buf()
    } else {
        std::env::current_dir()?.join(dockerfile_path)
    };
    let context_dir = if let Some(context_dir) = context_dir {
        if context_dir.is_absolute() {
            context_dir.to_path_buf()
        } else {
            std::env::current_dir()?.join(context_dir)
        }
    } else {
        absolute_path
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf()
    };
    // Track stage aliases (`FROM ... AS <name>`) so we can distinguish
    // `COPY --from=<stage>` (internal reference, skip lookup) from
    // `COPY --from=<image>` (external reference, look up as template).
    let mut stage_aliases: Vec<String> = Vec::new();
    // Final-stage FROM image. Each new FROM overwrites this so the last one
    // wins — matches Docker's "the final stage is the resulting image"
    // semantics.
    let mut final_from_image: Option<String> = None;
    // Order-preserving deduped set of additional external image references.
    // We keep insertion order via a separate Vec while tracking membership
    // in a HashSet for O(1) dedup.
    let mut additional_refs: Vec<String> = Vec::new();
    let mut additional_refs_seen: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut unresolvable_image_references: Vec<UnresolvableImageReference> = Vec::new();
    let mut ignored_instructions: Vec<(usize, String)> = Vec::new();

    for (line_number, line) in logical_dockerfile_lines(&dockerfile_text) {
        let (keyword, value) = split_instruction(&line, line_number)?;
        if keyword == "FROM" {
            let (image, alias) = parse_from_value_with_alias(&value, line_number)?;
            // If a prior FROM had set the "final image" candidate, demote it
            // now — it's actually an earlier-stage FROM. Push it to the
            // additional-refs set so the rootfs builder still loads it
            // locally (e.g., for COPY --from referring to it by image name).
            // Skip the demotion when the prior image equals the new final
            // FROM (would duplicate the entry) or is itself unresolvable.
            if let Some(prior) = final_from_image.take()
                && prior != image
                && !prior.eq_ignore_ascii_case("scratch")
                && !prior.contains('$')
                && !prior.contains('@')
                && !stage_aliases.iter().any(|alias| alias == prior.as_str())
                && additional_refs_seen.insert(prior.clone())
            {
                additional_refs.push(prior);
            }
            if image.contains('$') {
                unresolvable_image_references.push(UnresolvableImageReference {
                    line_number,
                    reference: image.clone(),
                    reason: UnresolvableImageReferenceReason::BuildArgExpansion,
                });
            } else if image.contains('@') {
                unresolvable_image_references.push(UnresolvableImageReference {
                    line_number,
                    reference: image.clone(),
                    reason: UnresolvableImageReferenceReason::DigestPin,
                });
            }
            // `scratch`, variable references, and digest-pinned references are
            // tracked but never put in additional_refs — they're either
            // built-ins or unresolvable.
            final_from_image = Some(image);
            if let Some(alias) = alias {
                stage_aliases.push(alias);
            }
            continue;
        }
        if keyword == "COPY" {
            for from_value in copy_from_values(&value) {
                accumulate_side_reference(
                    line_number,
                    from_value,
                    &stage_aliases,
                    &mut additional_refs,
                    &mut additional_refs_seen,
                    &mut unresolvable_image_references,
                );
            }
            continue;
        }
        if keyword == "RUN" {
            for from_value in run_mount_from_values(&value) {
                accumulate_side_reference(
                    line_number,
                    from_value,
                    &stage_aliases,
                    &mut additional_refs,
                    &mut additional_refs_seen,
                    &mut unresolvable_image_references,
                );
            }
            continue;
        }
        if UNSUPPORTED_DOCKERFILE_INSTRUCTIONS.contains(&keyword.as_str()) {
            return Err(SandboxImageBuildError::other(format!(
                "line {}: Dockerfile instruction '{}' is not supported for sandbox image creation",
                line_number, keyword
            )));
        }
        if IGNORED_DOCKERFILE_INSTRUCTIONS.contains(&keyword.as_str()) {
            ignored_instructions.push((line_number, keyword));
        }
        let _ = value;
    }

    let base_image = final_from_image.ok_or_else(|| {
        SandboxImageBuildError::other("Dockerfile must contain a FROM instruction")
    })?;
    // Final pass: a side-channel reference (COPY --from / RUN --mount=,from)
    // that happens to name the final-stage image must not appear in both
    // `base_image` and `additional_image_references`. Filter here rather
    // than in the inner loop because the final base isn't known until every
    // FROM has been seen.
    additional_refs.retain(|reference| reference != &base_image);
    // Detect `FROM <stage-alias>` in the final stage. When the final FROM
    // matches an earlier-defined stage alias the value is an internal
    // reference rather than an external image, so we must not look it up
    // as a template.
    let base_image_is_internal_stage = stage_aliases
        .iter()
        .any(|alias| alias == base_image.as_str());

    Ok(DockerfileBuildPlan {
        context_dir,
        registered_name: registered_name
            .map(str::to_string)
            .unwrap_or_else(|| default_registered_name(&absolute_path)),
        dockerfile_text,
        base_image,
        base_image_is_internal_stage,
        additional_image_references: additional_refs,
        unresolvable_image_references,
        ignored_instructions,
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
        return Err(SandboxImageBuildError::other(format!(
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

/// Parse a FROM instruction value into `(image, alias)`.
///
/// `value` is the text after the `FROM` keyword (e.g.
/// `--platform=linux/amd64 python:3.12-slim AS builder`). Returns the image
/// reference exactly as the user wrote it, plus any `AS <alias>` stage name.
fn parse_from_value_with_alias(
    value: &str,
    line_number: usize,
) -> Result<(String, Option<String>)> {
    let (_, remainder) = strip_leading_flags(value)?;
    let tokens = shlex_split(&remainder).ok_or_else(|| {
        SandboxImageBuildError::other(format!(
            "line {}: invalid FROM syntax '{}'",
            line_number, value
        ))
    })?;
    if tokens.is_empty() {
        return Err(SandboxImageBuildError::other(format!(
            "line {}: FROM must include a base image",
            line_number
        )));
    }
    let image = tokens[0].clone();
    if tokens.len() == 1 {
        return Ok((image, None));
    }
    if !tokens[1].eq_ignore_ascii_case("as") {
        return Err(SandboxImageBuildError::other(format!(
            "line {}: unsupported FROM syntax '{}'",
            line_number, value
        )));
    }
    if tokens.len() < 3 {
        return Err(SandboxImageBuildError::other(format!(
            "line {}: FROM ... AS must include a stage name",
            line_number
        )));
    }
    Ok((image, Some(tokens[2].clone())))
}

/// Classify a side-channel image reference (COPY --from / RUN --mount=,from)
/// and either accumulate it into the additional-references list or record
/// it as unresolvable (variable / digest-pin / built-in / stage alias).
fn accumulate_side_reference(
    line_number: usize,
    value: String,
    stage_aliases: &[String],
    additional_refs: &mut Vec<String>,
    additional_refs_seen: &mut std::collections::HashSet<String>,
    unresolvable_image_references: &mut Vec<UnresolvableImageReference>,
) {
    if value.eq_ignore_ascii_case("scratch") {
        return;
    }
    if value.contains('$') {
        unresolvable_image_references.push(UnresolvableImageReference {
            line_number,
            reference: value,
            reason: UnresolvableImageReferenceReason::BuildArgExpansion,
        });
        return;
    }
    if value.contains('@') {
        unresolvable_image_references.push(UnresolvableImageReference {
            line_number,
            reference: value,
            reason: UnresolvableImageReferenceReason::DigestPin,
        });
        return;
    }
    if stage_aliases.iter().any(|alias| alias == value.as_str()) {
        return;
    }
    if additional_refs_seen.insert(value.clone()) {
        additional_refs.push(value);
    }
}

/// Extract `--from=<value>` arguments from a COPY instruction's tail.
///
/// COPY supports at most one `--from` flag per instruction. The flag can be
/// written as `--from=<value>` or `--from <value>`. Anything else (the source
/// paths, the destination, other flags) is ignored.
fn copy_from_values(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut tokens = value.split_whitespace();
    while let Some(token) = tokens.next() {
        if let Some(v) = token.strip_prefix("--from=") {
            if !v.is_empty() {
                out.push(v.to_string());
            }
        } else if token == "--from"
            && let Some(next) = tokens.next()
        {
            out.push(next.to_string());
        }
    }
    out
}

/// Extract image references from `RUN --mount=type=cache,from=<value>` and
/// `RUN --mount=type=bind,from=<value>` flags.
///
/// BuildKit's mount syntax uses comma-separated key/value pairs after
/// `--mount=`. We pick out the `from=` element and, for the cases where it
/// names an image (rather than a stage), the rootfs builder will need to
/// have that image loaded locally.
fn run_mount_from_values(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in value.split_whitespace() {
        let body = match token.strip_prefix("--mount=") {
            Some(body) => body,
            None => continue,
        };
        for entry in body.split(',') {
            if let Some(v) = entry.strip_prefix("from=")
                && !v.is_empty()
            {
                out.push(v.to_string());
            }
        }
    }
    out
}

fn strip_leading_flags(value: &str) -> Result<(Vec<(String, String)>, String)> {
    let mut flags = Vec::new();
    let mut remaining = value.trim_start().to_string();

    while remaining.starts_with("--") {
        let (token, rest) = match remaining.split_once(' ') {
            Some((token, rest)) => (token.to_string(), rest.trim_start().to_string()),
            None => {
                return Err(SandboxImageBuildError::other(format!(
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
            return Err(SandboxImageBuildError::other(format!(
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
    let dockerignore = dockerignore_matcher(root)?;
    collect_dir_files_filtered(root, current, dockerignore.as_ref(), &mut files)?;
    Ok(files)
}

fn dockerignore_matcher(root: &Path) -> Result<Option<Gitignore>> {
    let dockerignore_path = root.join(".dockerignore");
    if !dockerignore_path.is_file() {
        return Ok(None);
    }

    let mut builder = GitignoreBuilder::new(root);
    if let Some(error) = builder.add(&dockerignore_path) {
        return Err(SandboxImageBuildError::other(format!(
            "failed to parse {}: {}",
            dockerignore_path.display(),
            error
        )));
    }
    builder
        .build()
        .map(Some)
        .map_err(|error| SandboxImageBuildError::other(error.to_string()))
}

fn collect_dir_files_filtered(
    root: &Path,
    current: &Path,
    dockerignore: Option<&Gitignore>,
    files: &mut Vec<(PathBuf, String)>,
) -> Result<()> {
    for entry in std::fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if is_dockerignored(root, &path, true, dockerignore) {
                continue;
            }
            collect_dir_files_filtered(root, &path, dockerignore, files)?;
        } else if path.is_file() {
            if is_dockerignored(root, &path, false, dockerignore) {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|error| SandboxImageBuildError::other(error.to_string()))?;
            let relative = relative
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            files.push((path, relative));
        }
    }
    Ok(())
}

fn is_dockerignored(
    root: &Path,
    path: &Path,
    is_dir: bool,
    dockerignore: Option<&Gitignore>,
) -> bool {
    let Some(dockerignore) = dockerignore else {
        return false;
    };
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    if relative.as_os_str().is_empty() {
        return false;
    }
    dockerignore.matched(relative, is_dir).is_ignore()
}

#[cfg(test)]
mod tests {
    use super::{
        CompleteSandboxTemplateBuildRequest, PreparedRootfsBuilder, PreparedRootfsParent,
        PreparedSandboxTemplateBuild, build_rootfs_spec, collect_dir_files,
        complete_request_from_metadata, default_registered_name, load_dockerfile_plan,
        load_dockerfile_text_plan, logical_dockerfile_lines, normalize_posix,
        process_terminal_status, rootfs_builder_env, rootfs_builder_executable, rootfs_disk_bytes,
        rootfs_disk_bytes_to_mb, streaming_process_payload,
    };
    use crate::sandboxes::models::ProcessInfo;
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
        writeln!(file, "FROM python:3.12-slim\nWORKDIR /app\nRUN echo hi").unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert_eq!(
            plan.registered_name,
            temp_dir.path().file_name().unwrap().to_string_lossy()
        );
        assert!(plan.ignored_instructions.is_empty());
    }

    #[test]
    fn load_dockerfile_plan_rejects_unsupported_instructions() {
        for instruction in ["ONBUILD RUN echo", "SHELL [\"/bin/bash\"]"] {
            let temp_dir = tempfile::tempdir().unwrap();
            let dockerfile_path = temp_dir.path().join("Dockerfile");
            std::fs::write(
                &dockerfile_path,
                format!("FROM python:3.12-slim\n{}\n", instruction),
            )
            .unwrap();

            let error = load_dockerfile_plan(&dockerfile_path, None).unwrap_err();
            let keyword = instruction.split_whitespace().next().unwrap();
            let expected = format!(
                "line 2: Dockerfile instruction '{}' is not supported for sandbox image creation",
                keyword
            );
            assert!(
                error.to_string().contains(&expected),
                "instruction {instruction}: expected {expected:?} in {error}",
            );
        }
    }

    #[test]
    fn load_dockerfile_plan_accepts_arg_at_top_level() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "ARG PY_TAG=3.12-slim\nFROM python:${PY_TAG}\nRUN echo hi\n",
        )
        .unwrap();

        // ARG no longer rejects the build; the variable-bearing FROM is
        // recorded for the warning channel and the lookup is skipped.
        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        assert_eq!(plan.base_image, "python:${PY_TAG}");
        assert_eq!(plan.unresolvable_image_references.len(), 1);
        assert!(matches!(
            plan.unresolvable_image_references[0].reason,
            super::UnresolvableImageReferenceReason::BuildArgExpansion
        ));
    }

    #[test]
    fn load_dockerfile_plan_warns_on_digest_pinned_from() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM python:3.12-slim@sha256:abc\nRUN echo hi\n",
        )
        .unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        assert_eq!(plan.base_image, "python:3.12-slim@sha256:abc");
        assert_eq!(plan.unresolvable_image_references.len(), 1);
        assert!(matches!(
            plan.unresolvable_image_references[0].reason,
            super::UnresolvableImageReferenceReason::DigestPin
        ));
    }

    #[test]
    fn load_dockerfile_plan_flags_final_from_stage_alias() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM ubuntu:24.04 AS base\nRUN make\nFROM base\nRUN ls\n",
        )
        .unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        // The final FROM is `FROM base`, an internal alias — flagged so
        // it is not looked up as an external template.
        assert_eq!(plan.base_image, "base");
        assert!(plan.base_image_is_internal_stage);
        assert_eq!(plan.additional_image_references, vec!["ubuntu:24.04"]);
    }

    #[test]
    fn load_dockerfile_plan_collects_ignored_instructions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM python:3.12-slim\n\
             LABEL maintainer=alice\n\
             EXPOSE 8080\n\
             HEALTHCHECK CMD echo ok\n\
             STOPSIGNAL SIGTERM\n\
             VOLUME /data\n\
             RUN echo hi\n",
        )
        .unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        let keywords: Vec<&str> = plan
            .ignored_instructions
            .iter()
            .map(|(_, kw)| kw.as_str())
            .collect();
        assert_eq!(
            keywords,
            vec!["LABEL", "EXPOSE", "HEALTHCHECK", "STOPSIGNAL", "VOLUME",]
        );
        // Dockerfile text is preserved verbatim so the rootfs builder still
        // sees the same instructions.
        assert!(plan.dockerfile_text.contains("EXPOSE 8080"));
    }

    #[test]
    fn load_dockerfile_plan_accepts_user_cmd_entrypoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM python:3.12-slim\n\
             USER app\n\
             CMD [\"python\", \"-m\", \"http.server\"]\n\
             ENTRYPOINT [\"/usr/bin/env\"]\n\
             RUN echo hi\n",
        )
        .unwrap();

        // USER, CMD, and ENTRYPOINT are now supported: the plan loads without
        // error and none of them land in the ignored set.
        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        let keywords: Vec<&str> = plan
            .ignored_instructions
            .iter()
            .map(|(_, kw)| kw.as_str())
            .collect();
        assert!(
            keywords.is_empty(),
            "expected no ignored instructions, got {keywords:?}",
        );
    }

    #[test]
    fn load_dockerfile_plan_accepts_multistage_builds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM ubuntu:24.04 AS build\n\
             RUN make\n\
             FROM python:3.12-slim\n\
             COPY --from=build /artifact /artifact\n",
        )
        .unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert_eq!(plan.additional_image_references, vec!["ubuntu:24.04"]);
    }

    #[test]
    fn load_dockerfile_plan_collects_copy_from_image_references() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM python:3.12-slim\n\
             COPY --from=tensorlake/utility:1.0 /bin/foo /usr/local/bin/foo\n\
             COPY src/ /app/\n",
        )
        .unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert_eq!(
            plan.additional_image_references,
            vec!["tensorlake/utility:1.0"]
        );
    }

    #[test]
    fn load_dockerfile_plan_treats_copy_from_stage_as_internal() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM ubuntu:24.04 AS build\n\
             FROM python:3.12-slim\n\
             COPY --from=build /artifact /artifact\n",
        )
        .unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        // `build` matches an earlier stage alias, so the COPY --from is
        // internal; only the demoted earlier-FROM ends up in the additional
        // references list.
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert_eq!(plan.additional_image_references, vec!["ubuntu:24.04"]);
    }

    #[test]
    fn load_dockerfile_plan_dedupes_image_references_across_stages() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dockerfile_path = temp_dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile_path,
            "FROM python:3.12-slim AS prep\n\
             FROM python:3.12-slim\n\
             COPY --from=python:3.12-slim /tmp/x /tmp/x\n",
        )
        .unwrap();

        let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
        // The first FROM and the COPY --from both reference the same image
        // as the final-stage FROM, so additional_image_references is empty.
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert!(
            plan.additional_image_references.is_empty(),
            "got {:?}",
            plan.additional_image_references
        );
    }

    #[test]
    fn load_dockerfile_text_plan_uses_explicit_context_without_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context_dir = temp_dir.path().join("context");
        std::fs::create_dir(&context_dir).unwrap();
        let dockerfile_path = context_dir.join("Dockerfile.generated");

        let plan = load_dockerfile_text_plan(
            &dockerfile_path,
            Some(&context_dir),
            "FROM python:3.12-slim\nRUN echo hi\n".to_string(),
            Some("generated"),
        )
        .unwrap();

        assert_eq!(plan.context_dir, context_dir);
        assert_eq!(plan.base_image, "python:3.12-slim");
        assert_eq!(plan.registered_name, "generated");
    }

    #[test]
    fn rootfs_disk_bytes_uses_default_and_validates_overflow() {
        let mut base = prepared_build("base");
        base.parent = None;
        let diff = prepared_build("diff");

        assert_eq!(
            rootfs_disk_bytes(None, &base).unwrap(),
            10 * 1024 * 1024 * 1024
        );
        assert_eq!(
            rootfs_disk_bytes(None, &diff).unwrap(),
            20 * 1024 * 1024 * 1024
        );
        assert_eq!(
            rootfs_disk_bytes(Some(2048), &diff).unwrap(),
            2048 * 1024 * 1024
        );
        assert!(rootfs_disk_bytes(Some(u64::MAX), &base).is_err());
    }

    #[test]
    fn rootfs_disk_bytes_requires_parent_size_for_diff_default() {
        let mut prepared = prepared_build("diff");
        prepared.parent.as_mut().unwrap().rootfs_disk_bytes = None;

        let error = rootfs_disk_bytes(None, &prepared).unwrap_err();
        assert!(
            error.to_string().contains("parent rootfsDiskBytes"),
            "{error}"
        );
    }

    #[test]
    fn rootfs_disk_bytes_to_mb_rounds_up() {
        assert_eq!(rootfs_disk_bytes_to_mb(1024 * 1024).unwrap(), 1);
        assert_eq!(rootfs_disk_bytes_to_mb((1024 * 1024) + 1).unwrap(), 2);
    }

    #[test]
    fn build_rootfs_spec_adds_builder_inputs() {
        let prepared_spec = json!({
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
        let prepared: PreparedSandboxTemplateBuild =
            serde_json::from_value(prepared_spec.clone()).unwrap();
        let plan = super::DockerfileBuildPlan {
            context_dir: "/tmp/context".into(),
            registered_name: "child".to_string(),
            dockerfile_text: "FROM alpine\nRUN echo hi\n".to_string(),
            base_image: "alpine".to_string(),
            additional_image_references: Vec::new(),
            base_image_is_internal_stage: false,
            unresolvable_image_references: Vec::new(),
            ignored_instructions: Vec::new(),
        };

        let spec = build_rootfs_spec(
            &prepared_spec,
            &prepared,
            &plan,
            Some(2048),
            Some("{}".to_string()),
        )
        .unwrap();
        assert_eq!(spec["dockerfile"], "FROM alpine\nRUN echo hi\n");
        assert_eq!(
            spec["contextDir"],
            "/var/lib/tensorlake/rootfs-builder/build/context"
        );
        assert_eq!(spec["rootfsDiskBytes"], 2048_u64 * 1024 * 1024);
        assert_eq!(spec["dockerConfigJson"], "{}");
    }

    #[test]
    fn build_rootfs_spec_defaults_diff_to_parent_rootfs_size() {
        let prepared = prepared_build("diff");
        let prepared_spec = serde_json::to_value(&prepared).unwrap();
        let plan = super::DockerfileBuildPlan {
            context_dir: "/tmp/context".into(),
            registered_name: "child".to_string(),
            dockerfile_text: "FROM parent\nRUN echo hi\n".to_string(),
            base_image: "parent".to_string(),
            additional_image_references: Vec::new(),
            base_image_is_internal_stage: false,
            unresolvable_image_references: Vec::new(),
            ignored_instructions: Vec::new(),
        };

        let spec = build_rootfs_spec(&prepared_spec, &prepared, &plan, None, None).unwrap();
        assert_eq!(spec["rootfsDiskBytes"], 20_u64 * 1024 * 1024 * 1024);
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
    fn streaming_process_payload_runs_as_root_when_requested() {
        let payload = streaming_process_payload(
            "tl-rootfs-build",
            vec!["--spec".to_string(), "/tmp/spec.json".to_string()],
            None,
            None,
            true,
        );

        assert_eq!(payload["command"], "tl-rootfs-build");
        assert_eq!(payload["user"], "root");
    }

    #[test]
    fn streaming_process_payload_omits_user_when_not_root() {
        // Upload-prep helpers (mkdir, /bin/true) must NOT request root,
        // otherwise the container-daemon's file API — which writes with the
        // sandbox user's fsuid — can't create temp files inside the resulting
        // root-owned directories. The absence of a `user` field lets the
        // daemon fall back to its default (sandbox user) so the dir and the
        // subsequent uploads share the same fsuid.
        let payload = streaming_process_payload(
            "mkdir",
            vec![
                "-p".to_string(),
                "/var/lib/tensorlake/rootfs-builder".to_string(),
            ],
            None,
            None,
            false,
        );

        assert_eq!(payload["command"], "mkdir");
        assert!(
            payload.get("user").is_none(),
            "non-root callers must not pin a user; got {:?}",
            payload.get("user")
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

    #[test]
    fn collect_dir_files_honors_dockerignore() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        std::fs::write(root.join(".dockerignore"), "ignored.txt\ncache/drop.txt\n").unwrap();
        std::fs::write(root.join("included.txt"), "included").unwrap();
        std::fs::write(root.join("ignored.txt"), "ignored").unwrap();
        std::fs::create_dir(root.join("cache")).unwrap();
        std::fs::write(root.join("cache/drop.txt"), "drop").unwrap();
        std::fs::write(root.join("cache/keep.txt"), "keep").unwrap();

        let mut files = collect_dir_files(root, root)
            .unwrap()
            .into_iter()
            .map(|(_, relative)| relative)
            .collect::<Vec<_>>();
        files.sort();

        assert_eq!(
            files,
            vec![".dockerignore", "cache/keep.txt", "included.txt"]
        );
    }

    #[test]
    fn process_terminal_status_detects_oom_killed_process() {
        let info = ProcessInfo {
            handle: Some(1),
            pid: 42,
            status: "oom_killed".to_string(),
            exit_code: None,
            signal: Some(9),
            oom_killed: true,
            stdin_writable: false,
            command: "/usr/local/bin/tl-rootfs-build".to_string(),
            args: Vec::new(),
            started_at: json!(123),
            ended_at: Some(json!(456)),
            managed: None,
        };

        let status = process_terminal_status(&info).unwrap();
        assert_eq!(status.code, -9);
        assert!(status.oom_killed);
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
                rootfs_disk_bytes: Some(20 * 1024 * 1024 * 1024),
            }),
        }
    }

    #[allow(dead_code)]
    fn assert_serialize(_: &CompleteSandboxTemplateBuildRequest, _: &Value) {}
}
