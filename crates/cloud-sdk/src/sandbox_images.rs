use std::{
    collections::HashMap,
    fs::File,
    io::Read,
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
        models::{
            CreateSandboxRequest, CreateSandboxResources, MultipartHint, ProcessInfo,
            RunProcessEvent, SandboxInfo, SignBlobOp, SignBlobRequest, SignBlobTarget,
        },
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
const REMOTE_CONTEXT_ARCHIVE_PATH: &str = "/var/lib/tensorlake/rootfs-builder/build/context.tar.gz";
const REMOTE_SPEC_PATH: &str = "/var/lib/tensorlake/rootfs-builder/build/spec.json";
const REMOTE_METADATA_PATH: &str = "/var/lib/tensorlake/rootfs-builder/build/metadata.json";
const ROOTFS_BUILDER_BIN_DIR: &str = "/usr/local/bin";
const ROOTFS_BUILDER_PATH: &str = "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const ROOTFS_BUILDER_COMMAND: &str = "tl-rootfs-build";
const ROOTFS_BUILDER_PROCESS_USER: &str = "root";
const DIAGNOSTIC_COMMAND_TIMEOUT_SECS: i64 = 5;
const BUILDER_DISK_USAGE_DIAGNOSTIC_THRESHOLD_PERCENT: u8 = 95;
const ARCHIVE_PROGRESS_BYTE_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessTerminalStatus {
    code: i64,
    oom_killed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContextArchiveStats {
    file_count: usize,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
}

#[derive(Debug, Clone)]
struct ContextArchiveFile {
    full_path: PathBuf,
    relative_path: String,
    bytes: u64,
}

struct ProgressReader<R, F> {
    inner: R,
    bytes_read: u64,
    on_progress: F,
}

impl<R, F> ProgressReader<R, F> {
    fn new(inner: R, on_progress: F) -> Self {
        Self {
            inner,
            bytes_read: 0,
            on_progress,
        }
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl<R, F> Read for ProgressReader<R, F>
where
    R: Read,
    F: FnMut(u64),
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        if read > 0 {
            self.bytes_read = self.bytes_read.saturating_add(read as u64);
            (self.on_progress)(self.bytes_read);
        }
        Ok(read)
    }
}

/// Dockerfile instructions that run as usual during the rootfs builder's
/// `docker build`, but have no effect when a sandbox is later run from the
/// image. The builder preserves the built image's OCI config, yet the
/// sandbox runtime only honors `ENV`/`WORKDIR`/`USER`/`ENTRYPOINT`/`CMD`; the
/// rest of the image config is never read at runtime. `ONBUILD` triggers only
/// fire in a downstream build (there is no child build here) and `SHELL` only
/// changes the shell for build-time shell-form `RUN`/`CMD`/`ENTRYPOINT`. We
/// accept all of these and warn, rather than reject, so a Dockerfile that works
/// with `docker build` also works here.
//
// ARG is not in this list; it is accepted at top-level scope so a Dockerfile
// can declare global defaults that Docker resolves at build time, but the SDK
// does not substitute ARG values at parse time.
const IGNORED_DOCKERFILE_INSTRUCTIONS: &[&str] = &[
    "ONBUILD",
    "SHELL",
    "EXPOSE",
    "HEALTHCHECK",
    "LABEL",
    "STOPSIGNAL",
    "VOLUME",
];

#[derive(Debug, Error)]
pub enum SandboxImageBuildError {
    /// A failure after the builder sandbox was created. Carries the IDs
    /// support needs to find the build: the builder sandbox ID correlates
    /// with dataplane and platform logs, the build ID with platform-api.
    #[error("{source} (builder sandbox: {builder_sandbox_id}, build: {build_id})")]
    BuildFailed {
        builder_sandbox_id: String,
        build_id: String,
        #[source]
        source: Box<SandboxImageBuildError>,
    },
    #[error("{source}\n{messages}")]
    WithDiagnostics {
        #[source]
        source: Box<SandboxImageBuildError>,
        messages: String,
    },
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

/// Auth/context and resource fields shared by every sandbox-image build mode
/// (Dockerfile build and registry import). The mode-specific public option
/// structs (`SandboxImageBuildOptions`, `SandboxImageImportOptions`) each carry
/// their own copy of these and hand them to the shared `run_build_plan` runner.
#[derive(Debug, Clone)]
pub struct CommonBuildOptions {
    pub api_url: String,
    pub bearer_token: String,
    pub use_scope_headers: bool,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub namespace: String,
    pub registered_name: Option<String>,
    pub disk_mb: Option<u64>,
    pub builder_disk_mb: Option<u64>,
    pub cpus: Option<f64>,
    pub memory_mb: Option<i64>,
    pub is_public: bool,
    pub user_agent: Option<String>,
    pub docker_compat: bool,
}

/// Options for building a sandbox image from a Dockerfile. This is the
/// Dockerfile build path only — to import a registry image directly into a
/// rootfs without a Dockerfile, use [`SandboxImageImportOptions`] /
/// [`import_sandbox_image`] instead. Keeping the two modes in separate structs
/// means a caller cannot construct an ambiguous mix of a Dockerfile and an
/// import reference.
#[derive(Debug, Clone)]
pub struct SandboxImageBuildOptions {
    pub common: CommonBuildOptions,
    pub dockerfile_path: PathBuf,
    pub dockerfile_text: Option<String>,
    pub context_dir: Option<PathBuf>,
}

/// Options for importing a registry image directly into a rootfs (no
/// Dockerfile, no Docker daemon — the builder runs
/// `indexify-rootfs-materialize oci-image-to-ext4`). The reference is always
/// pulled fresh from the registry; it is never resolved against the Tensorlake
/// template registry.
#[derive(Debug, Clone)]
pub struct SandboxImageImportOptions {
    pub common: CommonBuildOptions,
    /// The registry image reference to import, e.g.
    /// `pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime` or
    /// `ghcr.io/org/app@sha256:...`.
    pub image_reference: String,
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
    /// When set, this is an image-import build rather than a Dockerfile build:
    /// the builder pulls this registry reference directly into the rootfs with
    /// no Docker daemon. There is no build context to upload and the base is
    /// never resolved against the template registry (import always pulls from
    /// the registry, producing a fresh base rootfs).
    import_image_reference: Option<String>,
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
    /// Optional during and after the versioned-response rollout: platform-api
    /// is moving the snapshot location off of a pre-baked `snapshotUri` and
    /// onto `snapshotRelPath` (resolved client-side via
    /// `SandboxProxyClient::sign_blob`). On that new path, the signed upload
    /// response carries the final URI and the CLI copies it into the build
    /// spec before the in-sandbox builder writes `metadata.json`.
    #[serde(default)]
    snapshot_uri: Option<String>,
    rootfs_node_kind: String,
    builder: PreparedRootfsBuilder,
    parent: Option<PreparedRootfsParent>,
    /// New-path marker: when present, platform-api stopped pre-signing S3 and
    /// the CLI must call `SandboxProxyClient::sign_blob` to mint the upload
    /// spec. When absent, the response carries an embedded `upload` block in
    /// the raw passthrough `Value` (legacy path). Apart from the top-level
    /// `snapshotUri` handoff, the prepared spec stays opaque to preserve the
    /// platform-api ↔ in-sandbox-builder forward-compat property.
    #[serde(default)]
    snapshot_rel_path: Option<String>,
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

/// Build a sandbox image from a Dockerfile (path or inline text). To import a
/// registry image directly into a rootfs without a Dockerfile, use
/// [`import_sandbox_image`] instead.
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
            options.common.registered_name.as_deref(),
        )?
    } else {
        load_dockerfile_plan(
            &options.dockerfile_path,
            options.common.registered_name.as_deref(),
        )?
    };
    run_build_plan(plan, options.common, emit).await
}

/// Import a registry image directly into a rootfs (no Dockerfile, no Docker
/// daemon). The reference is always pulled fresh from the registry; it is
/// never resolved against the Tensorlake template registry.
pub async fn import_sandbox_image<F>(
    options: SandboxImageImportOptions,
    mut emit: F,
) -> Result<Value>
where
    F: FnMut(SandboxImageBuildEvent),
{
    emit(SandboxImageBuildEvent::Status(format!(
        "Importing registry image {}...",
        options.image_reference
    )));
    let plan = plan_image_import(
        &options.image_reference,
        options.common.registered_name.as_deref(),
    )?;
    run_build_plan(plan, options.common, emit).await
}

/// Shared build pipeline: provision the rootfs-builder sandbox, materialize the
/// filesystem from `plan`, and register the resulting snapshot. Both the
/// Dockerfile build and registry import paths funnel through here once they
/// have produced a [`DockerfileBuildPlan`].
async fn run_build_plan<F>(
    plan: DockerfileBuildPlan,
    options: CommonBuildOptions,
    mut emit: F,
) -> Result<Value>
where
    F: FnMut(SandboxImageBuildEvent),
{
    emit(SandboxImageBuildEvent::Status(format!(
        "Selected image name: {}",
        plan.registered_name
    )));

    for (_line_number, keyword) in &plan.ignored_instructions {
        emit(SandboxImageBuildEvent::Warning(format!(
            "Dockerfile instruction '{}' is applied during the image build but has no \
             effect when running sandboxes from this image.",
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
    if options.docker_compat {
        emit(SandboxImageBuildEvent::Warning(
            "Docker compatibility mode is enabled. This uses Docker/BuildKit export for rootfs \
             materialization, which can be slower and may require a larger builder sandbox disk."
                .to_string(),
        ));
    }

    let ctx = resolve_build_context(options.clone()).await?;

    emit(SandboxImageBuildEvent::Status(
        "Preparing rootfs build...".to_string(),
    ));
    let platform_client = platform_client(&ctx)?;
    let (prepared, mut prepared_spec) =
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
        gpu_configs: None,
    };
    emit(SandboxImageBuildEvent::Status(format!(
        "Builder resources: {:.2} CPU, {}, {} disk",
        resources.cpus,
        format_bytes(resources.memory_mb.max(0) as u64 * 1024 * 1024),
        format_bytes(builder_disk_mb * 1024 * 1024),
    )));
    emit(SandboxImageBuildEvent::Status(format!(
        "Target rootfs size: {}",
        format_bytes(rootfs_disk_bytes),
    )));

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

        let mut builder_failure_diagnostics = BuilderFailureDiagnostics::default();
        let post_proxy_result: Result<Value> = async {
            // Versioned-response bridge: on the new path, platform-api returns
            // `snapshotRelPath` instead of a pre-signed `upload` block, and we
            // ask the sandbox proxy to mint the upload spec. Splice the result
            // into the raw prepared spec so `upload_build_inputs` /
            // `build_rootfs_spec` see an `upload` key regardless of which path
            // produced it — preserving the platform-api ↔ in-sandbox-builder
            // passthrough property.
            //
            // The CLI doesn't know the final snapshot file size until the
            // in-sandbox builder finishes and writes metadata.json, so the
            // provider-neutral upload request includes a capacity hint derived
            // from the rootfs disk budget (`rootfsDiskBytes`). Clamp to
            // [1, MULTIPART_MAX_PARTS] so a 0 MB hint still produces a valid
            // request and absurd inputs don't blow past S3's 10,000-part ceiling.
            //
            // Legacy path: `snapshot_rel_path` is absent, the upload block is
            // already in `prepared_spec`, and we do nothing here.
            let signed_snapshot_uri = if let Some(rel_path) = prepared.snapshot_rel_path.clone() {
                let disk_mb = rootfs_disk_bytes_to_mb(rootfs_disk_bytes)?;
                let parts: u32 = disk_mb
                    .div_ceil(MULTIPART_PART_SIZE_MB)
                    .clamp(1, MULTIPART_MAX_PARTS as u64) as u32;
                let signed = proxy
                    .sign_blob(&SignBlobRequest {
                        target: SignBlobTarget::Artifact { rel_path },
                        op: SignBlobOp::PutArtifact {
                            multipart_hint: Some(MultipartHint {
                                max_parts: parts,
                                part_size_bytes: MULTIPART_PART_SIZE_BYTES,
                            }),
                        },
                    })
                    .await
                    .map_err(SandboxImageBuildError::Sdk)?
                    .into_inner();
                let snapshot_uri = splice_signed_upload(&mut prepared_spec, signed)?;

                if let Some(parent) = prepared.parent.as_ref() {
                    let signed = proxy
                        .sign_blob(&SignBlobRequest {
                            target: SignBlobTarget::Blob {
                                uri: parent.parent_manifest_uri.clone(),
                            },
                            op: SignBlobOp::GetBlob,
                        })
                        .await
                        .map_err(SandboxImageBuildError::Sdk)?
                        .into_inner();
                    prepared_spec
                        .as_object_mut()
                        .ok_or_else(|| {
                            SandboxImageBuildError::other("prepared spec is not a JSON object")
                        })?
                        .get_mut("parent")
                        .and_then(Value::as_object_mut)
                        .ok_or_else(|| {
                            SandboxImageBuildError::other("prepared parent is not a JSON object")
                        })?
                        .insert("download".to_string(), signed);
                }

                Some(snapshot_uri)
            } else {
                None
            };

            upload_build_inputs(
                &proxy,
                &plan,
                &prepared,
                &prepared_spec,
                options.disk_mb,
                options.docker_compat,
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
            let builder_result =
                run_rootfs_builder(&proxy, &prepared.builder.command, &mut |event| {
                    builder_failure_diagnostics.observe_build_event(&event);
                    emit(event);
                })
                .await;
            keepalive_task.abort();
            builder_result?;

            let metadata = read_build_metadata(&proxy).await?;
            let complete_request = complete_request_from_metadata(
                &prepared,
                &metadata,
                signed_snapshot_uri.as_deref(),
            )?;

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

        match post_proxy_result {
            Ok(registered) => Ok(registered),
            Err(source) => {
                Err(decorate_builder_failure(&proxy, source, &builder_failure_diagnostics).await)
            }
        }
    }
    .await;

    if let Err(error) = sandboxes.delete(&sandbox_id).await {
        emit(SandboxImageBuildEvent::Warning(format!(
            "Failed to terminate rootfs builder sandbox {} during cleanup: {}",
            sandbox_id, error
        )));
    }

    result.map_err(|source| SandboxImageBuildError::BuildFailed {
        builder_sandbox_id: sandbox_id,
        build_id: prepared.build_id.clone(),
        source: Box::new(source),
    })
}

async fn resolve_build_context(options: CommonBuildOptions) -> Result<ResolvedBuildContext> {
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

fn unscoped_client(options: &CommonBuildOptions) -> Result<Client> {
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

        // A sandbox that is still `pending` isn't routable yet, so the
        // lifecycle gateway can return a transient proxy error (502 /
        // "Failed to proxy request") until it starts — in slower environments
        // that window is a minute or two. Treat those as retryable, the same
        // way `wait_for_proxy_ready` does, and let the deadline above bound
        // the total wait. Non-transient errors still fail the build.
        let info = match sandboxes.get(sandbox_id).await {
            Ok(info) => info,
            Err(error) => {
                let error = SandboxImageBuildError::from(error);
                if is_transient_proxy_error(&error) {
                    tokio::time::sleep(SANDBOX_WAIT_POLL_INTERVAL).await;
                    continue;
                }
                return Err(error);
            }
        };
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
    // were already recorded as unresolvable and warned about. Import builds
    // never resolve a parent — they always pull a fresh base from the
    // registry, even if the reference happens to match a template name.
    let parent_template_payload = if plan.import_image_reference.is_some()
        || plan.base_image_is_internal_stage
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
    Ok(
        SandboxProxyClient::new(client.with_base_url(&proxy_base), host_override)
            .with_sandbox_id(Some(sandbox_id.to_string()))
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
    docker_compat: bool,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    // Pre-create REMOTE_BUILD_DIR with permissive mode as root so the
    // sandbox-user file API can write into it. The path lives under
    // /var/lib/tensorlake/ which is root-owned in the rootfs-builder image,
    // so a plain upload into that tree would fail without this setup.
    ensure_remote_build_root(proxy).await?;
    // Import builds have no local build context — the rootfs comes straight
    // from the registry image — so there is nothing to upload.
    if plan.import_image_reference.is_none() {
        emit(SandboxImageBuildEvent::Status(
            "Uploading build context...".to_string(),
        ));
        upload_context_archive(proxy, &plan.context_dir, emit).await?;
    }

    let docker_config_json = resolved_docker_config_json().await?;
    let spec = build_rootfs_spec(
        prepared_spec,
        prepared,
        plan,
        disk_mb,
        docker_config_json,
        docker_compat,
    )?;
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
    docker_compat: bool,
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
    // Routes the builder to `oci-image-to-ext4` instead of docker build; the
    // builder pulls this reference straight into the rootfs.
    if let Some(import_image_reference) = &plan.import_image_reference {
        object.insert(
            "importImageReference".to_string(),
            Value::String(import_image_reference.clone()),
        );
    }
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
    if docker_compat {
        object.insert("dockerCompat".to_string(), Value::Bool(true));
    }

    Ok(spec)
}

fn splice_signed_upload(prepared_spec: &mut Value, signed_upload: Value) -> Result<String> {
    let snapshot_uri = signed_upload
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            SandboxImageBuildError::other("dataplane signed upload response is missing uri")
        })?
        .to_string();

    let object = prepared_spec
        .as_object_mut()
        .ok_or_else(|| SandboxImageBuildError::other("prepared spec is not a JSON object"))?;
    object.insert(
        "snapshotUri".to_string(),
        Value::String(snapshot_uri.clone()),
    );
    object.insert("upload".to_string(), signed_upload);
    Ok(snapshot_uri)
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

/// Part size used when the new-path `sign_blob` flow sends an upload capacity
/// hint to the proxy. Used directly by the splice in `build_sandbox_image`.
const MULTIPART_PART_SIZE_MB: u64 = 64;
const MULTIPART_PART_SIZE_BYTES: u64 = MULTIPART_PART_SIZE_MB * 1024 * 1024;

/// S3 caps a multipart upload at 10,000 parts. The dataplane's `sign_blob`
/// endpoint enforces the same ceiling (`MAX_MULTIPART_PARTS` in
/// `indexify/crates/dataplane/src/sign_blob.rs`); keep these in sync.
const MULTIPART_MAX_PARTS: u32 = 10_000;

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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BuilderFailureDiagnostics {
    oom_killed: bool,
    disk_usage_percent: Option<u8>,
    disk_error_output: bool,
}

impl BuilderFailureDiagnostics {
    fn observe_build_event(&mut self, event: &SandboxImageBuildEvent) {
        if let SandboxImageBuildEvent::BuildLog { message, .. } = event
            && contains_disk_space_evidence(message)
        {
            self.disk_error_output = true;
        }
    }

    fn merge(&mut self, other: &BuilderFailureDiagnostics) {
        self.oom_killed |= other.oom_killed;
        self.disk_error_output |= other.disk_error_output;
        self.disk_usage_percent = self.disk_usage_percent.max(other.disk_usage_percent);
    }

    fn advice_messages(&self) -> Vec<&'static str> {
        let mut messages = Vec::new();
        if self.oom_killed {
            messages.push(
                "The builder sandbox ran out of memory. Retry with a larger `memory_mb` / `memoryMb` / `--memory` value.",
            );
        }
        if self
            .disk_usage_percent
            .is_some_and(|usage| usage >= BUILDER_DISK_USAGE_DIAGNOSTIC_THRESHOLD_PERCENT)
            || self.disk_error_output
        {
            messages.push(
                "The builder sandbox ran out of disk space. Retry with a larger `builder_disk_mb` / `builderDiskMb` / `--builder_disk_mb` value.",
            );
        }
        messages
    }
}

async fn decorate_builder_failure(
    proxy: &SandboxProxyClient,
    source: SandboxImageBuildError,
    observed: &BuilderFailureDiagnostics,
) -> SandboxImageBuildError {
    let mut diagnostics = diagnose_builder_failure(proxy, &source.to_string()).await;
    diagnostics.merge(observed);
    let messages = diagnostics.advice_messages();
    if messages.is_empty() {
        return source;
    }

    SandboxImageBuildError::WithDiagnostics {
        source: Box::new(source),
        messages: messages.join("\n"),
    }
}

async fn diagnose_builder_failure(
    proxy: &SandboxProxyClient,
    failure_output: &str,
) -> BuilderFailureDiagnostics {
    let dmesg_output = run_diagnostic_command_stdout(proxy, "dmesg 2>/dev/null || true").await;
    let disk_output = run_diagnostic_command_stdout(
        proxy,
        &format!(
            "for path in '{}' /var/lib/docker /; do if [ -e \"$path\" ]; then df -P \"$path\"; fi; done 2>/dev/null || true",
            REMOTE_BUILD_DIR
        ),
    )
    .await;

    BuilderFailureDiagnostics {
        oom_killed: dmesg_output
            .as_deref()
            .is_some_and(contains_oom_killer_evidence),
        disk_usage_percent: disk_output.as_deref().and_then(parse_df_max_usage_percent),
        disk_error_output: contains_disk_space_evidence(failure_output),
    }
}

async fn run_diagnostic_command_stdout(proxy: &SandboxProxyClient, script: &str) -> Option<String> {
    let mut payload = streaming_process_payload(
        "/bin/sh",
        vec!["-lc".to_string(), script.to_string()],
        None,
        None,
        true,
    );
    payload.as_object_mut()?.insert(
        "timeout".to_string(),
        json!(DIAGNOSTIC_COMMAND_TIMEOUT_SECS),
    );

    let events = proxy.run_process(&payload).await.ok()?.into_inner();
    let mut output = String::new();
    for event in events {
        if let RunProcessEvent::Output(event) = event
            && event.stream.as_deref() != Some("stderr")
        {
            output.push_str(&event.line);
            output.push('\n');
        }
    }
    Some(output)
}

fn contains_oom_killer_evidence(output: &str) -> bool {
    let output = output.to_ascii_lowercase();
    output.contains("out of memory")
        || output.contains("oom-kill")
        || output.contains("killed process")
}

fn contains_disk_space_evidence(output: &str) -> bool {
    let output = output.to_ascii_lowercase();
    output.contains("enospc") || output.contains("no space")
}

fn parse_df_max_usage_percent(output: &str) -> Option<u8> {
    output.lines().filter_map(parse_df_line_usage_percent).max()
}

fn parse_df_line_usage_percent(line: &str) -> Option<u8> {
    let mut fields = line.split_whitespace();
    let _filesystem = fields.next()?;
    let _blocks = fields.next()?;
    let _used = fields.next()?;
    let _available = fields.next()?;
    fields.next()?.strip_suffix('%')?.parse().ok()
}

async fn read_build_metadata(proxy: &SandboxProxyClient) -> Result<Value> {
    let content = proxy.read_file(REMOTE_METADATA_PATH).await?.into_inner();
    serde_json::from_slice(&content).map_err(Into::into)
}

fn complete_request_from_metadata(
    prepared: &PreparedSandboxTemplateBuild,
    metadata: &Value,
    signed_snapshot_uri: Option<&str>,
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
        snapshot_uri: completion_snapshot_uri(prepared, metadata, signed_snapshot_uri)?,
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

fn completion_snapshot_uri(
    prepared: &PreparedSandboxTemplateBuild,
    metadata: &Value,
    signed_snapshot_uri: Option<&str>,
) -> Result<String> {
    if let Some(signed_snapshot_uri) = signed_snapshot_uri {
        if let Some(metadata_snapshot_uri) =
            metadata_string(metadata, "snapshot_uri", "snapshotUri")
            && metadata_snapshot_uri != signed_snapshot_uri
        {
            return Err(SandboxImageBuildError::other(format!(
                "rootfs builder metadata snapshot_uri {} did not match dataplane signed uri {}",
                metadata_snapshot_uri, signed_snapshot_uri
            )));
        }

        return Ok(signed_snapshot_uri.to_string());
    }

    if let Some(snapshot_uri) = metadata_string(metadata, "snapshot_uri", "snapshotUri") {
        return Ok(snapshot_uri);
    }

    if let Some(snapshot_uri) = prepared.snapshot_uri.clone() {
        return Ok(snapshot_uri);
    }

    Err(SandboxImageBuildError::other(
        "rootfs build completed without snapshot_uri",
    ))
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

async fn upload_context_archive(
    proxy: &SandboxProxyClient,
    context_dir: &Path,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    if !context_dir.is_dir() {
        return Err(SandboxImageBuildError::other(format!(
            "Local build context not found: {}",
            context_dir.display()
        )));
    }

    emit(SandboxImageBuildEvent::Status(format!(
        "Creating build context archive from {}...",
        context_dir.display()
    )));
    let archive = tempfile::Builder::new()
        .prefix("tensorlake-build-context-")
        .suffix(".tar.gz")
        .tempfile()?;
    let stats = create_context_archive(context_dir, archive.path(), emit)?;
    emit(SandboxImageBuildEvent::Status(format!(
        "Build context: {} files, {} uncompressed, {} compressed",
        stats.file_count,
        format_bytes(stats.uncompressed_bytes),
        format_bytes(stats.compressed_bytes),
    )));

    ensure_remote_parent_dir(proxy, REMOTE_CONTEXT_ARCHIVE_PATH).await?;
    upload_archive_with_progress(
        proxy,
        REMOTE_CONTEXT_ARCHIVE_PATH,
        archive.path(),
        stats.compressed_bytes,
        emit,
    )
    .await?;
    extract_context_archive(proxy, emit).await
}

fn create_context_archive(
    context_dir: &Path,
    archive_path: &Path,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<ContextArchiveStats> {
    let file = File::create(archive_path)?;
    let gz = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
    let mut tar = tar::Builder::new(gz);

    let files = collect_context_archive_files(context_dir)?;
    let uncompressed_bytes = files
        .iter()
        .fold(0_u64, |total, file| total.saturating_add(file.bytes));
    emit_archive_progress(0, uncompressed_bytes, emit);

    let mut archived_bytes = 0_u64;
    let mut last_emitted_bytes = 0_u64;
    let mut last_percent = upload_percent(0, uncompressed_bytes);
    for file in &files {
        let input = File::open(&file.full_path)?;
        let metadata = input.metadata()?;
        let file_bytes = metadata.len();
        let mut header = tar::Header::new_gnu();
        header.set_metadata(&metadata);
        header.set_size(file_bytes);
        header.set_cksum();

        let bytes_read = {
            let mut reader = ProgressReader::new(input.take(file_bytes), |file_archived_bytes| {
                let current_bytes = archived_bytes
                    .saturating_add(file_archived_bytes)
                    .min(uncompressed_bytes);
                let percent = upload_percent(current_bytes, uncompressed_bytes);
                if percent == 100
                    || percent > last_percent
                    || current_bytes.saturating_sub(last_emitted_bytes)
                        >= ARCHIVE_PROGRESS_BYTE_INTERVAL_BYTES
                {
                    last_percent = percent;
                    last_emitted_bytes = current_bytes;
                    emit_archive_progress(current_bytes, uncompressed_bytes, emit);
                }
            });
            tar.append_data(&mut header, &file.relative_path, &mut reader)?;
            reader.bytes_read()
        };
        if bytes_read != file_bytes {
            return Err(SandboxImageBuildError::other(format!(
                "Build context file changed while archiving: {} (expected {}, read {})",
                file.full_path.display(),
                format_bytes(file_bytes),
                format_bytes(bytes_read),
            )));
        }

        archived_bytes = archived_bytes.saturating_add(file_bytes);
    }
    if last_percent < 100 {
        emit_archive_progress(uncompressed_bytes, uncompressed_bytes, emit);
    }

    tar.finish()?;
    tar.into_inner()?.finish()?;
    let compressed_bytes = std::fs::metadata(archive_path)?.len();
    Ok(ContextArchiveStats {
        file_count: files.len(),
        uncompressed_bytes,
        compressed_bytes,
    })
}

fn collect_context_archive_files(context_dir: &Path) -> Result<Vec<ContextArchiveFile>> {
    let mut files = Vec::new();
    for (full_path, relative_path) in collect_dir_files(context_dir, context_dir)? {
        let bytes = std::fs::metadata(&full_path)?.len();
        files.push(ContextArchiveFile {
            full_path,
            relative_path,
            bytes,
        });
    }
    Ok(files)
}

fn emit_archive_progress(
    archived_bytes: u64,
    total_bytes: u64,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    emit(SandboxImageBuildEvent::Status(format!(
        "Creating build context archive: {}% ({} / {})",
        upload_percent(archived_bytes, total_bytes),
        format_bytes(archived_bytes.min(total_bytes)),
        format_bytes(total_bytes),
    )));
}

async fn upload_archive_with_progress(
    proxy: &SandboxProxyClient,
    remote_path: &str,
    local_path: &Path,
    total_bytes: u64,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    emit_upload_progress(0, total_bytes, emit);
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let upload = proxy.upload_file_with_progress(remote_path, local_path, progress_tx);
    tokio::pin!(upload);

    let mut last_percent = 0_u64;
    loop {
        tokio::select! {
            progress = progress_rx.recv() => {
                if let Some(uploaded) = progress {
                    let percent = upload_percent(uploaded, total_bytes);
                    if percent == 100 || percent >= last_percent.saturating_add(10) {
                        last_percent = percent;
                        emit_upload_progress(uploaded, total_bytes, emit);
                    }
                }
            }
            result = &mut upload => {
                result?;
                emit_upload_progress(total_bytes, total_bytes, emit);
                return Ok(());
            }
        }
    }
}

fn emit_upload_progress(
    uploaded_bytes: u64,
    total_bytes: u64,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    emit(SandboxImageBuildEvent::Status(format!(
        "Uploading build context archive: {}% ({} / {})",
        upload_percent(uploaded_bytes, total_bytes),
        format_bytes(uploaded_bytes.min(total_bytes)),
        format_bytes(total_bytes),
    )));
}

fn upload_percent(uploaded_bytes: u64, total_bytes: u64) -> u64 {
    if total_bytes == 0 {
        return 100;
    }
    uploaded_bytes
        .saturating_mul(100)
        .saturating_div(total_bytes)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = UNITS[0];
    for next_unit in &UNITS[1..] {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next_unit;
    }

    if unit == "B" {
        format!("{bytes} B")
    } else if value < 10.0 {
        format!("{value:.2} {unit}")
    } else if value < 100.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{value:.0} {unit}")
    }
}

async fn extract_context_archive(
    proxy: &SandboxProxyClient,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    ensure_remote_parent_dir(proxy, &join_posix(REMOTE_CONTEXT_DIR, ".keep")).await?;

    emit(SandboxImageBuildEvent::Status(
        "Untarring build context archive in builder sandbox...".to_string(),
    ));
    let mut process_emit = |event| emit(event);
    run_streaming_process(
        proxy,
        "tar",
        vec![
            "-xzf".to_string(),
            REMOTE_CONTEXT_ARCHIVE_PATH.to_string(),
            "-C".to_string(),
            REMOTE_CONTEXT_DIR.to_string(),
        ],
        None,
        None,
        false,
        &mut process_emit,
    )
    .await?;
    proxy.delete_file(REMOTE_CONTEXT_ARCHIVE_PATH).await?;
    emit(SandboxImageBuildEvent::Status(
        "Build context extracted; removed remote archive".to_string(),
    ));
    Ok(())
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

/// Build a plan for importing a registry image directly (no Dockerfile).
/// The stored "dockerfile" is a synthetic `FROM <ref>` so the template
/// registry records a faithful provenance, but the build never runs Docker:
/// the spec's `importImageReference` routes the builder to
/// `oci-image-to-ext4`. Import is always a fresh base from the registry, so
/// the base is not resolved against the template registry.
fn plan_image_import(
    image_ref: &str,
    registered_name: Option<&str>,
) -> Result<DockerfileBuildPlan> {
    let image_ref = image_ref.trim();
    if image_ref.is_empty() {
        return Err(SandboxImageBuildError::usage(
            "image reference to import must not be empty",
        ));
    }
    let registered_name = registered_name
        .map(str::to_string)
        .unwrap_or_else(|| default_registered_name_from_image(image_ref));
    Ok(DockerfileBuildPlan {
        context_dir: PathBuf::new(),
        registered_name,
        dockerfile_text: format!("FROM {image_ref}\n"),
        base_image: image_ref.to_string(),
        base_image_is_internal_stage: false,
        additional_image_references: Vec::new(),
        unresolvable_image_references: Vec::new(),
        ignored_instructions: Vec::new(),
        import_image_reference: Some(image_ref.to_string()),
    })
}

/// Derive a registered name from an image reference: the last path segment
/// with any tag/digest stripped (e.g. `pytorch/pytorch:2.4.1` -> `pytorch`,
/// `ghcr.io/org/app@sha256:...` -> `app`).
fn default_registered_name_from_image(image_ref: &str) -> String {
    let without_digest = image_ref.split('@').next().unwrap_or(image_ref);
    let last_segment = without_digest.rsplit('/').next().unwrap_or(without_digest);
    let name = match last_segment.rsplit_once(':') {
        Some((repo, tag)) if !tag.is_empty() => repo,
        _ => last_segment,
    };
    if name.is_empty() {
        "imported-image".to_string()
    } else {
        name.to_string()
    }
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
        import_image_reference: None,
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

    #[test]
    fn build_failed_error_names_the_builder_sandbox_and_build() {
        let error = super::SandboxImageBuildError::BuildFailed {
            builder_sandbox_id: "rtmmkcw33uvsbep6hn03u".to_string(),
            build_id: "sandbox_template_build_123".to_string(),
            source: Box::new(super::SandboxImageBuildError::Other(
                "rootfs builder exited with status 1".to_string(),
            )),
        };
        let message = error.to_string();
        assert!(message.contains("builder sandbox: rtmmkcw33uvsbep6hn03u"));
        assert!(message.contains("build: sandbox_template_build_123"));
        assert!(message.contains("rootfs builder exited with status 1"));
    }
    use super::{
        BuilderFailureDiagnostics, CompleteSandboxTemplateBuildRequest, PreparedRootfsBuilder,
        PreparedRootfsParent, PreparedSandboxTemplateBuild, SandboxImageBuildError,
        SandboxImageBuildEvent, build_rootfs_spec, collect_dir_files,
        complete_request_from_metadata, contains_disk_space_evidence, contains_oom_killer_evidence,
        create_context_archive, default_registered_name, load_dockerfile_plan,
        load_dockerfile_text_plan, logical_dockerfile_lines, normalize_posix,
        parse_df_line_usage_percent, parse_df_max_usage_percent, process_terminal_status,
        rootfs_builder_env, rootfs_builder_executable, rootfs_disk_bytes, rootfs_disk_bytes_to_mb,
        splice_signed_upload, streaming_process_payload, upload_percent,
    };
    use crate::sandboxes::models::ProcessInfo;
    use serde_json::{Value, json};
    use std::io::Write;

    #[test]
    fn oom_dmesg_parser_detects_kernel_oom_entries() {
        assert!(contains_oom_killer_evidence(
            "[ 123.4] Out of memory: Killed process 99 (python) total-vm:1234kB"
        ));
        assert!(contains_oom_killer_evidence(
            "memory: oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null)"
        ));
        assert!(contains_oom_killer_evidence(
            "Killed process 42 (cc1plus), UID 0, total-vm:1234kB"
        ));
    }

    #[test]
    fn oom_dmesg_parser_ignores_unrelated_entries() {
        assert!(!contains_oom_killer_evidence(
            "[ 123.4] eth0: link becomes ready\n[ 124.0] EXT4-fs mounted"
        ));
    }

    #[test]
    fn df_parser_extracts_usage_percent() {
        assert_eq!(
            parse_df_line_usage_percent("/dev/vda1 10485760 9961472 524288 95% /"),
            Some(95)
        );
        assert_eq!(
            parse_df_line_usage_percent(
                "Filesystem 1024-blocks Used Available Capacity Mounted on"
            ),
            None
        );
    }

    #[test]
    fn df_parser_returns_max_usage_across_multiple_filesystems() {
        let output = "\
Filesystem 1024-blocks Used Available Capacity Mounted on
/dev/vda1 10485760 5242880 5242880 50% /
Filesystem 1024-blocks Used Available Capacity Mounted on
/dev/vdb1 10485760 10066329 419431 96% /var/lib/docker
";
        assert_eq!(parse_df_max_usage_percent(output), Some(96));
    }

    #[test]
    fn disk_space_parser_detects_enospc_and_no_space_output() {
        assert!(contains_disk_space_evidence(
            "fallocate: fallocate failed: No space left on device"
        ));
        assert!(contains_disk_space_evidence(
            "failed to write layer: ENOSPC"
        ));
        assert!(!contains_disk_space_evidence(
            "rootfs builder exited with status 1"
        ));
    }

    #[test]
    fn diagnostics_observe_disk_space_build_log_events() {
        let mut diagnostics = BuilderFailureDiagnostics::default();
        diagnostics.observe_build_event(&SandboxImageBuildEvent::BuildLog {
            stream: "stderr".to_string(),
            message: "dd: error writing '/tmp/fill': No space left on device".to_string(),
        });

        assert!(diagnostics.disk_error_output);
        assert!(
            diagnostics
                .advice_messages()
                .iter()
                .any(|message| message.contains("larger `builder_disk_mb`"))
        );
    }

    #[test]
    fn diagnostics_advice_uses_thresholds() {
        assert!(
            BuilderFailureDiagnostics {
                oom_killed: false,
                disk_usage_percent: Some(94),
                disk_error_output: false,
            }
            .advice_messages()
            .is_empty()
        );

        let messages = BuilderFailureDiagnostics {
            oom_killed: true,
            disk_usage_percent: Some(95),
            disk_error_output: false,
        }
        .advice_messages();
        assert_eq!(messages.len(), 2);
        assert!(messages[0].contains("larger `memory_mb`"));
        assert!(messages[1].contains("larger `builder_disk_mb`"));
        assert!(messages[1].contains("--builder_disk_mb"));

        let messages = BuilderFailureDiagnostics {
            oom_killed: false,
            disk_usage_percent: Some(10),
            disk_error_output: true,
        }
        .advice_messages();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("larger `builder_disk_mb`"));
    }

    #[test]
    fn diagnostic_error_wrapper_preserves_source_message() {
        let error = SandboxImageBuildError::WithDiagnostics {
            source: Box::new(SandboxImageBuildError::Other(
                "rootfs builder exited with status 1".to_string(),
            )),
            messages: BuilderFailureDiagnostics {
                oom_killed: true,
                disk_usage_percent: Some(99),
                disk_error_output: false,
            }
            .advice_messages()
            .join("\n"),
        };
        let message = error.to_string();
        assert!(message.contains("rootfs builder exited with status 1"));
        assert!(message.contains("The builder sandbox ran out of memory"));
        assert!(message.contains("The builder sandbox ran out of disk space"));
    }

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
    fn plan_image_import_synthesizes_from_and_marks_import() {
        let plan = super::plan_image_import("ubuntu:24.04", None).unwrap();
        assert_eq!(plan.dockerfile_text, "FROM ubuntu:24.04\n");
        assert_eq!(plan.base_image, "ubuntu:24.04");
        assert_eq!(plan.import_image_reference.as_deref(), Some("ubuntu:24.04"));
        assert!(plan.additional_image_references.is_empty());
        // Last path segment with the tag stripped.
        assert_eq!(plan.registered_name, "ubuntu");
    }

    #[test]
    fn plan_image_import_honors_explicit_name_and_rejects_empty() {
        let plan = super::plan_image_import("ghcr.io/org/app:v1", Some("my-image")).unwrap();
        assert_eq!(plan.registered_name, "my-image");
        assert!(super::plan_image_import("   ", None).is_err());
    }

    #[test]
    fn default_registered_name_from_image_strips_path_tag_and_digest() {
        assert_eq!(
            super::default_registered_name_from_image("pytorch/pytorch:2.4.1-runtime"),
            "pytorch"
        );
        assert_eq!(
            super::default_registered_name_from_image(&format!(
                "ghcr.io/org/app@sha256:{}",
                "a".repeat(64)
            )),
            "app"
        );
        assert_eq!(
            super::default_registered_name_from_image("ubuntu"),
            "ubuntu"
        );
    }

    #[test]
    fn build_rootfs_spec_sets_import_reference_for_import_plans() {
        let prepared_spec = json!({});
        let prepared: super::PreparedSandboxTemplateBuild = serde_json::from_value(json!({
            "buildId": "build-1",
            "snapshotId": "snap-1",
            "snapshotUri": "s3://bucket/snap-1",
            "rootfsNodeKind": "base",
            "builder": {
                "image": "tensorlake/rootfs-builder",
                "command": "tl-rootfs-build",
                "cpus": 2.0,
                "memoryMb": 2048,
                "diskMb": 20480
            },
            "parent": null
        }))
        .unwrap();
        let plan = super::plan_image_import("ubuntu:24.04", None).unwrap();

        let spec =
            super::build_rootfs_spec(&prepared_spec, &prepared, &plan, Some(10240), None, false)
                .unwrap();
        assert_eq!(spec["importImageReference"], "ubuntu:24.04");
        assert_eq!(spec["baseImage"], "ubuntu:24.04");
        assert_eq!(spec["dockerfile"], "FROM ubuntu:24.04\n");
        assert!(spec.get("dockerCompat").is_none());
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
    fn load_dockerfile_plan_accepts_onbuild_and_shell() {
        // ONBUILD and SHELL are no longer rejected: they run during the build
        // but have no runtime effect, so they land in the ignored set alongside
        // EXPOSE/LABEL/etc. rather than failing the build.
        for instruction in ["ONBUILD RUN echo", "SHELL [\"/bin/bash\", \"-c\"]"] {
            let temp_dir = tempfile::tempdir().unwrap();
            let dockerfile_path = temp_dir.path().join("Dockerfile");
            std::fs::write(
                &dockerfile_path,
                format!("FROM python:3.12-slim\n{}\n", instruction),
            )
            .unwrap();

            let plan = load_dockerfile_plan(&dockerfile_path, None).unwrap();
            let keyword = instruction.split_whitespace().next().unwrap();
            let keywords: Vec<&str> = plan
                .ignored_instructions
                .iter()
                .map(|(_, kw)| kw.as_str())
                .collect();
            assert_eq!(
                keywords,
                vec![keyword],
                "instruction {instruction}: expected {keyword:?} in the ignored set",
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
             ONBUILD RUN echo build\n\
             SHELL [\"/bin/bash\", \"-c\"]\n\
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
            vec![
                "ONBUILD",
                "SHELL",
                "LABEL",
                "EXPOSE",
                "HEALTHCHECK",
                "STOPSIGNAL",
                "VOLUME",
            ]
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
    fn prepared_deserializes_snapshot_rel_path() {
        let with_rel_path: PreparedSandboxTemplateBuild = serde_json::from_value(json!({
            "buildId": "build-1",
            "snapshotId": "snapshot-1",
            "snapshotUri": "s3://bucket/snapshot.tlsnap",
            "snapshotRelPath": "snapshots/abc.tlsnap",
            "rootfsNodeKind": "base",
            "builder": {
                "image": "tensorlake/rootfs-builder",
                "command": "tl-rootfs-build",
                "cpus": 2,
                "memoryMb": 4096,
                "diskMb": 30720
            }
        }))
        .unwrap();
        assert_eq!(
            with_rel_path.snapshot_rel_path.as_deref(),
            Some("snapshots/abc.tlsnap")
        );

        // Legacy-shape fixture (no snapshotRelPath) must still deserialize
        // and default to None — that's how the CLI tells the two paths
        // apart at runtime.
        let without_rel_path: PreparedSandboxTemplateBuild = serde_json::from_value(json!({
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
            }
        }))
        .unwrap();
        assert!(without_rel_path.snapshot_rel_path.is_none());
        assert_eq!(
            without_rel_path.snapshot_uri.as_deref(),
            Some("s3://bucket/snapshot.tlsnap")
        );
    }

    #[test]
    fn prepared_deserializes_without_snapshot_uri() {
        // Forward-compat with platform-api dropping `snapshotUri` once the
        // versioned-response rollout finishes: the CLI must still accept
        // the response and fill the final URI from dataplane signing.
        let prepared: PreparedSandboxTemplateBuild = serde_json::from_value(json!({
            "buildId": "build-1",
            "snapshotId": "snapshot-1",
            "snapshotRelPath": "snapshots/abc.tlsnap",
            "rootfsNodeKind": "base",
            "builder": {
                "image": "tensorlake/rootfs-builder",
                "command": "tl-rootfs-build",
                "cpus": 2,
                "memoryMb": 4096,
                "diskMb": 30720
            }
        }))
        .unwrap();
        assert!(prepared.snapshot_uri.is_none());
        assert_eq!(
            prepared.snapshot_rel_path.as_deref(),
            Some("snapshots/abc.tlsnap")
        );
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
            import_image_reference: None,
        };

        let spec = build_rootfs_spec(
            &prepared_spec,
            &prepared,
            &plan,
            Some(2048),
            Some("{}".to_string()),
            true,
        )
        .unwrap();
        assert_eq!(spec["dockerfile"], "FROM alpine\nRUN echo hi\n");
        assert_eq!(
            spec["contextDir"],
            "/var/lib/tensorlake/rootfs-builder/build/context"
        );
        assert_eq!(spec["rootfsDiskBytes"], 2048_u64 * 1024 * 1024);
        assert_eq!(spec["dockerConfigJson"], "{}");
        assert_eq!(spec["dockerCompat"], true);
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
            import_image_reference: None,
        };

        let spec = build_rootfs_spec(&prepared_spec, &prepared, &plan, None, None, false).unwrap();
        assert_eq!(spec["rootfsDiskBytes"], 20_u64 * 1024 * 1024 * 1024);
        assert!(spec.get("dockerCompat").is_none());
    }

    #[test]
    fn splice_signed_upload_uses_dataplane_uri_for_snapshot_uri() {
        let mut prepared_spec = json!({
            "buildId": "build-1",
            "snapshotId": "snapshot-1",
            "snapshotUri": "s3://platform/stale.tlsnap",
            "rootfsNodeKind": "base",
        });
        let signed_upload = json!({
            "kind": "s3_multipart",
            "uri": "s3://dataplane/final.tlsnap",
            "uploadId": "upload-1",
            "partSizeBytes": 67_108_864,
            "partUrls": []
        });

        let snapshot_uri = splice_signed_upload(&mut prepared_spec, signed_upload).unwrap();

        assert_eq!(snapshot_uri, "s3://dataplane/final.tlsnap");
        assert_eq!(prepared_spec["snapshotUri"], "s3://dataplane/final.tlsnap");
        assert_eq!(
            prepared_spec["upload"]["uri"],
            "s3://dataplane/final.tlsnap"
        );
        assert_eq!(prepared_spec["upload"]["uploadId"], "upload-1");
    }

    #[test]
    fn splice_signed_upload_requires_dataplane_uri() {
        let mut prepared_spec = json!({
            "buildId": "build-1",
            "snapshotId": "snapshot-1",
            "rootfsNodeKind": "base",
        });
        let signed_upload = json!({
            "kind": "s3_multipart",
            "uploadId": "upload-1",
            "partSizeBytes": 67_108_864,
            "partUrls": []
        });

        let error = splice_signed_upload(&mut prepared_spec, signed_upload).unwrap_err();
        assert!(
            error.to_string().contains("missing uri"),
            "expected signed upload uri error, got: {error}"
        );
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

        let request = complete_request_from_metadata(&prepared, &metadata, None).unwrap();
        let body = serde_json::to_value(&request).unwrap();
        assert_eq!(body["snapshotId"], "snapshot-1");
        assert_eq!(body["snapshotUri"], "s3://bucket/child.tlsnap");
        assert_eq!(body["snapshotFormatVersion"], "durable_archive_v1");
        assert_eq!(body["snapshotSizeBytes"], 1234);
        assert_eq!(body["rootfsNodeKind"], "diff");
        assert_eq!(body["parentManifestUri"], "s3://bucket/parent.tlsnap");
    }

    #[test]
    fn complete_request_uses_signed_uri_when_provided() {
        let mut prepared = prepared_build("base");
        prepared.parent = None;
        prepared.snapshot_uri = None;
        let metadata = json!({
            "snapshot_uri": "s3://bucket/from-signed.tlsnap",
            "snapshot_format_version": "durable_archive_v1",
            "snapshot_size_bytes": 1234,
            "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64
        });

        let request = complete_request_from_metadata(
            &prepared,
            &metadata,
            Some("s3://bucket/from-signed.tlsnap"),
        )
        .unwrap();
        assert_eq!(request.snapshot_uri, "s3://bucket/from-signed.tlsnap");
    }

    #[test]
    fn complete_request_uses_signed_uri_when_metadata_omits_snapshot_uri() {
        let mut prepared = prepared_build("base");
        prepared.parent = None;
        prepared.snapshot_uri = Some("s3://bucket/stale-prepared.tlsnap".to_string());
        let metadata = json!({
            "snapshot_format_version": "durable_archive_v1",
            "snapshot_size_bytes": 1234,
            "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64
        });

        let request = complete_request_from_metadata(
            &prepared,
            &metadata,
            Some("s3://bucket/from-signed.tlsnap"),
        )
        .unwrap();
        assert_eq!(request.snapshot_uri, "s3://bucket/from-signed.tlsnap");
    }

    #[test]
    fn complete_request_rejects_metadata_uri_mismatch_when_signed_uri_provided() {
        let mut prepared = prepared_build("base");
        prepared.parent = None;
        prepared.snapshot_uri = None;
        let metadata = json!({
            "snapshot_uri": "s3://bucket/from-metadata.tlsnap",
            "snapshot_format_version": "durable_archive_v1",
            "snapshot_size_bytes": 1234,
            "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64
        });

        let error = complete_request_from_metadata(
            &prepared,
            &metadata,
            Some("s3://bucket/from-signed.tlsnap"),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not match dataplane signed uri"),
            "expected signed URI mismatch error, got: {error}"
        );
    }

    #[test]
    fn complete_request_errors_when_snapshot_uri_missing_from_both_sources() {
        let mut prepared = prepared_build("base");
        prepared.parent = None;
        prepared.snapshot_uri = None;
        let metadata = json!({
            "snapshot_format_version": "durable_archive_v1",
            "snapshot_size_bytes": 1234,
            "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64
        });

        let error = complete_request_from_metadata(&prepared, &metadata, None).unwrap_err();
        assert!(
            error.to_string().contains("snapshot_uri"),
            "expected snapshot_uri error, got: {error}"
        );
    }

    #[test]
    fn complete_request_uses_prepared_parent_for_diff_when_metadata_omits_it() {
        let prepared = prepared_build("diff");
        let metadata = json!({
            "snapshot_format_version": "durable_archive_v1",
            "snapshot_size_bytes": "1234",
            "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64
        });

        let request = complete_request_from_metadata(&prepared, &metadata, None).unwrap();
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
    fn create_context_archive_honors_dockerignore() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        std::fs::write(root.join(".dockerignore"), "ignored.txt\ncache/drop.txt\n").unwrap();
        std::fs::write(root.join("included.txt"), "included").unwrap();
        std::fs::write(root.join("ignored.txt"), "ignored").unwrap();
        std::fs::create_dir(root.join("cache")).unwrap();
        std::fs::write(root.join("cache/drop.txt"), "drop").unwrap();
        std::fs::write(root.join("cache/keep.txt"), "keep").unwrap();

        let archive_file = tempfile::NamedTempFile::new().unwrap();
        let mut events = Vec::new();
        let stats =
            create_context_archive(root, archive_file.path(), &mut |event| events.push(event))
                .unwrap();

        assert_eq!(stats.file_count, 3);
        assert!(events.iter().any(|event| matches!(
            event,
            SandboxImageBuildEvent::Status(message)
                if message.starts_with("Creating build context archive:")
        )));

        let file = std::fs::File::open(archive_file.path()).unwrap();
        let decoder = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(decoder);
        let mut entries = archive
            .entries()
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        entries.sort();

        assert_eq!(
            entries,
            vec![".dockerignore", "cache/keep.txt", "included.txt"]
        );
    }

    #[test]
    fn upload_percent_handles_empty_and_partial_totals() {
        assert_eq!(upload_percent(0, 0), 100);
        assert_eq!(upload_percent(25, 100), 25);
        assert_eq!(upload_percent(100, 100), 100);
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
            snapshot_uri: Some("s3://bucket/prepared.tlsnap".to_string()),
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
            snapshot_rel_path: None,
        }
    }

    #[allow(dead_code)]
    fn assert_serialize(_: &CompleteSandboxTemplateBuildRequest, _: &Value) {}
}
