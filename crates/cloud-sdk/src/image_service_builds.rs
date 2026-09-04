//! CAS image builds through the Image Service build plane.
//!
//! The SDK owns Dockerfile parsing and reference resolution: the build
//! plan's external references are resolved against the Image Service
//! catalog here, client-side, and submitted already resolved with the build
//! request. The Image Service validates each pin against the caller's
//! project and its reconciler drives the builder sandbox, mounting one
//! read-only parent volume per pin beside the writable target and disposable
//! scratch volumes, and snapshotting the target through the CAS filesystem
//! daemon. The client
//! creates the build, uploads and seals the context, and polls it to
//! completion. While an active attempt exposes its ephemeral builder sandbox,
//! the client may also follow `tl-image-builder` output through the existing
//! project-scoped sandbox APIs; Image Service status remains authoritative.

use std::{
    collections::HashSet,
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use reqwest::{Method, StatusCode, header::CONTENT_LENGTH};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    Client,
    sandbox_images::{
        CommonBuildOptions, DockerfileBuildPlan, SandboxImageBuildError, SandboxImageBuildEvent,
        SandboxImageContextFile, client_builder, collect_dir_files, follow_started_process_output,
        is_localhost, normalize_context_file_path, resolve_build_context,
        resolved_docker_config_json, sandbox_lifecycle_client, sandbox_proxy_client,
    },
    sandboxes::{SandboxesClient, models::ProcessInfo},
};

type Result<T> = std::result::Result<T, SandboxImageBuildError>;

/// Base URL of the Image Service. Until the platform gateway routes to the
/// service, CAS builds require this to be set explicitly.
pub const IMAGE_SERVICE_URL_ENV: &str = "TENSORLAKE_IMAGE_SERVICE_URL";

/// Ingress route that fronts the Image Service. The ingress authenticates,
/// replaces any client-supplied identity headers with verified ones, and maps
/// this prefix onto the service's own routes -- so the paths below are
/// prefix-relative (`/builds`, not `/v1/builds`).
const IMAGE_SERVICE_INGRESS_PATH: &str = "/images/v4";

/// Immutable image reference prefix (`cas-v1:<64 hex sha256>`). A Dockerfile
/// reference written in this form pins the image id directly, with no
/// catalog name lookup.
const CAS_IMAGE_REF_PREFIX: &str = "cas-v1:";

/// Where the Dockerfile text is injected into the context tar when the
/// Dockerfile does not live inside the context directory (`-f` outside the
/// context, or inline Dockerfile text).
const INJECTED_DOCKERFILE_PATH: &str = ".tensorlake/Dockerfile";

/// The Image Service caps pinned parents so they always fit the sandbox
/// volume budget beside the target volume. Enforced client-side too for a
/// clearer error than a rejected build request.
const MAX_PINNED_PARENTS: usize = 2;

/// Default final filesystem capacity for CAS image builds. This is the
/// Server's current minimum sandbox/rootfs disk size; callers can override it
/// with the existing `disk_mb` / `--disk_mb` option.
const DEFAULT_CAS_IMAGE_DISK_MB: u64 = 10 * 1024;
const BYTES_PER_MIB: u64 = 1024 * 1024;

const BUILD_POLL_INTERVAL: Duration = Duration::from_secs(3);
const BUILD_POLL_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const BUILDER_LOG_CHANNEL_CAPACITY: usize = 256;
const BUILDER_LOG_DRAIN_GRACE: Duration = Duration::from_secs(2);
const BUILDER_PROCESS_DISCOVERY_ATTEMPTS: usize = 10;
const BUILDER_PROCESS_DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const IMAGE_BUILDER_COMMAND: &str = "tl-image-builder";

/// Run a build plan through the Image Service. `dockerfile_path` is the
/// local Dockerfile path for Dockerfile builds (used only to decide whether
/// the file already lives inside the context directory); import plans carry
/// no Dockerfile.
pub(crate) async fn run_image_service_build<F>(
    plan: DockerfileBuildPlan,
    dockerfile_path: Option<&Path>,
    build_args: Vec<(String, String)>,
    context_files: Vec<SandboxImageContextFile>,
    options: CommonBuildOptions,
    mut emit: F,
) -> Result<Value>
where
    F: FnMut(SandboxImageBuildEvent),
{
    warn_ignored_options(&options, &mut emit);
    let disk_mb = cas_target_disk_mb(options.disk_mb)?;
    let is_public = options.is_public;
    let builder_resources = builder_resource_overrides(&options)?;
    // Forward the local `docker login` state for the guest's registry pulls,
    // the same source the legacy path ships into its builder. The service
    // stages it write-only and the reconciler hands the guest a short-lived
    // fetch URL; the document never enters the catalog.
    let registry_credentials_json = resolved_docker_config_json().await?;
    if registry_credentials_json.is_some() {
        emit(SandboxImageBuildEvent::Status(
            "Forwarding local registry credentials to the build".to_string(),
        ));
    }
    // Ingress authenticates the bearer credential and injects the canonical project header.
    // Image Service deliberately accepts no project selector in bodies or query strings.
    let ctx = resolve_build_context(options)?;
    let image_service_url = image_service_url(&ctx.api_url);
    let client = client_builder(
        &image_service_url,
        &ctx.bearer_token,
        ctx.use_scope_headers,
        ctx.organization_id.as_deref(),
        ctx.project_id.as_deref(),
        ctx.user_agent.as_deref(),
    )
    .build()?;

    if let Some(import_reference) = plan.import_image_reference.clone() {
        let created = create_build(
            &client,
            build_request(
                json!({
                    "kind": "import",
                    "image_ref": import_reference,
                    "name": plan.registered_name,
                }),
                is_public,
                disk_mb,
                builder_resources,
                registry_credentials_json,
            ),
            &mut emit,
        )
        .await?;
        return wait_for_publication(&client, &ctx, &created, &plan.registered_name, emit).await;
    }

    let parents = resolve_parents(&client, &plan, &mut emit).await?;
    let (dockerfile_in_context, injected_dockerfile) = context_dockerfile(&plan, dockerfile_path)?;
    let created = create_build(
        &client,
        build_request(
            json!({
                "kind": "dockerfile",
                "dockerfile_path": dockerfile_in_context,
                "parents": parents,
                "build_args": build_args
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeMap<String, String>>(),
                "name": plan.registered_name,
            }),
            is_public,
            disk_mb,
            builder_resources,
            registry_credentials_json,
        ),
        &mut emit,
    )
    .await?;
    let build_id = build_id_of(&created)?;

    let upload = created.get("context_upload").cloned().ok_or_else(|| {
        SandboxImageBuildError::other(
            "Image Service build creation returned no context upload capability",
        )
    })?;
    upload_and_seal_context(
        &client,
        &build_id,
        &upload,
        &plan,
        injected_dockerfile.as_deref(),
        &context_files,
        &mut emit,
    )
    .await?;

    wait_for_publication(&client, &ctx, &created, &plan.registered_name, emit).await
}

/// Base URL for the Image Service.
///
/// Defaults to the ingress route on the configured API host, so a normal
/// client needs no extra configuration: `https://api.tensorlake.ai` yields
/// `https://api.tensorlake.ai/images/v4`, and the ingress authenticates the
/// request, injects verified identity headers, and strips the prefix before
/// the Image Service sees it. The service performs no authentication of its
/// own, so reaching it any other way in a deployed environment would bypass
/// the only auth boundary there is.
///
/// `TENSORLAKE_IMAGE_SERVICE_URL` overrides it for pointing at a local
/// Image Service directly during development.
pub(crate) fn image_service_url(api_url: &str) -> String {
    resolve_image_service_url(api_url, std::env::var(IMAGE_SERVICE_URL_ENV).ok())
}

/// Pure form, so the defaulting rule is testable without mutating process
/// environment (which races across parallel tests).
fn resolve_image_service_url(api_url: &str, override_url: Option<String>) -> String {
    if let Some(override_url) = override_url
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
    {
        return override_url;
    }
    format!(
        "{}{IMAGE_SERVICE_INGRESS_PATH}",
        api_url.trim_end_matches('/')
    )
}

/// Attach the forwarded docker config to a build-creation body. The field is
/// write-only on the service side; it is never echoed back.
fn with_registry_credentials(mut body: Value, registry_credentials_json: Option<String>) -> Value {
    if let (Some(credentials), Some(object)) = (registry_credentials_json, body.as_object_mut()) {
        object.insert(
            "registry_credentials_json".to_string(),
            Value::String(credentials),
        );
    }
    body
}

/// Build requests rely on Image Service's executor-fleet default unless the
/// user explicitly asks for a public image. Deliberately omit that private
/// scope instead of sending `executor_fleet`: omission preserves compatibility
/// with older Image Service versions, whose default is also private. Global
/// admission remains the service's responsibility and is restricted to
/// allowlisted publishers.
fn build_request(
    mut body: Value,
    is_public: bool,
    disk_mb: u64,
    builder_resources: Option<Value>,
    registry_credentials_json: Option<String>,
) -> Value {
    let object = body
        .as_object_mut()
        .expect("Image Service build request body must be an object");
    object.insert("disk_mb".to_string(), Value::Number(disk_mb.into()));
    if is_public {
        object.insert(
            "image_scope".to_string(),
            Value::String("global".to_string()),
        );
    }
    if let Some(builder_resources) = builder_resources {
        object.insert("builder_resources".to_string(), builder_resources);
    }
    with_registry_credentials(body, registry_credentials_json)
}

fn cas_target_disk_mb(requested_disk_mb: Option<u64>) -> Result<u64> {
    let disk_mb = requested_disk_mb.unwrap_or(DEFAULT_CAS_IMAGE_DISK_MB);
    if disk_mb == 0 {
        return Err(SandboxImageBuildError::usage(
            "--disk_mb must be greater than zero for CAS image builds",
        ));
    }
    disk_mb.checked_mul(BYTES_PER_MIB).ok_or_else(|| {
        SandboxImageBuildError::usage("--disk_mb is too large to convert to bytes")
    })?;
    Ok(disk_mb)
}

/// Preserve partial resource overrides so Image Service remains authoritative
/// for defaults and CPU-to-memory validation.
fn builder_resource_overrides(options: &CommonBuildOptions) -> Result<Option<Value>> {
    let mut resources = serde_json::Map::new();
    if let Some(cpus) = options.cpus {
        let cpus = serde_json::Number::from_f64(cpus)
            .ok_or_else(|| SandboxImageBuildError::usage("builder CPUs must be a finite number"))?;
        resources.insert("cpus".to_string(), Value::Number(cpus));
    }
    if let Some(memory_mb) = options.memory_mb {
        let memory_mb = u64::try_from(memory_mb)
            .map_err(|_| SandboxImageBuildError::usage("builder memory must not be negative"))?;
        resources.insert("memory_mb".to_string(), Value::Number(memory_mb.into()));
    }
    Ok((!resources.is_empty()).then_some(Value::Object(resources)))
}

/// Builder-rootfs sizing and compatibility mode belong to the legacy
/// platform-api rootfs builder. `disk_mb` is supported by Image Service and
/// sizes the final published target; `builder_disk_mb` remains legacy-only.
fn warn_ignored_options(
    options: &CommonBuildOptions,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    let mut ignored = Vec::new();
    if options.builder_disk_mb.is_some() {
        ignored.push("builder_disk_mb");
    }
    if options.docker_compat {
        ignored.push("docker_compat");
    }
    if !ignored.is_empty() {
        emit(SandboxImageBuildEvent::Warning(format!(
            "Image Service builds ignore: {}.",
            ignored.join(", ")
        )));
    }
}

/// Resolve the plan's external references against the Image Service catalog
/// and return the resolved parents the build request submits. A
/// `cas-v1:<64 hex>` reference pins the image id directly; every other
/// plain reference is looked up as a catalog name, and a miss means the
/// guest pulls the reference from a registry. References the plan already
/// marked unresolvable (`$` expansions, `@` digest pins) never reach here.
async fn resolve_parents(
    client: &Client,
    plan: &DockerfileBuildPlan,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<Vec<Value>> {
    let mut candidates: Vec<&str> = Vec::new();
    if !plan.base_image_is_internal_stage
        && !plan.base_image.eq_ignore_ascii_case("scratch")
        && !plan.base_image.contains('$')
        && !plan.base_image.contains('@')
    {
        candidates.push(plan.base_image.as_str());
    }
    candidates.extend(plan.additional_image_references.iter().map(String::as_str));

    let mut parents = Vec::new();
    let mut pinned_references = Vec::new();
    for reference in candidates {
        let catalog_reference = cas_catalog_reference(reference);
        let image_id = match direct_pin_image_id(reference)? {
            Some(image_id) => Some(image_id),
            None => lookup_catalog_name(client, catalog_reference.as_ref()).await?,
        };
        if image_id.is_none() && catalog_reference.as_ref() != reference {
            return Err(SandboxImageBuildError::usage(format!(
                "CAS base image '{}' was not found while resolving Dockerfile reference \
                 '{}'. Publish the CAS base first or use an existing tensorlake/cas image.",
                catalog_reference, reference
            )));
        }
        let Some(image_id) = image_id else {
            continue; // external registry reference, pulled by the guest
        };
        if catalog_reference.as_ref() == reference {
            emit(SandboxImageBuildEvent::Status(format!(
                "Resolved '{reference}' to registered image {CAS_IMAGE_REF_PREFIX}{image_id}"
            )));
        } else {
            emit(SandboxImageBuildEvent::Status(format!(
                "Resolved '{reference}' through CAS image '{}' to \
                 {CAS_IMAGE_REF_PREFIX}{image_id}",
                catalog_reference
            )));
        }
        pinned_references.push(reference.to_string());
        parents.push(json!({ "reference": reference, "image_id": image_id }));
    }
    if parents.len() > MAX_PINNED_PARENTS {
        return Err(SandboxImageBuildError::usage(format!(
            "the Dockerfile references {} registered images ({}) but a build may pin at most \
             {MAX_PINNED_PARENTS}",
            parents.len(),
            pinned_references.join(", "),
        )));
    }
    Ok(parents)
}

/// Map Tensorlake-managed Dockerfile references onto their CAS catalog
/// namespace. The original reference is retained in the submitted parent pin
/// because BuildKit binds the mounted context to the exact token written in
/// the Dockerfile; only the authenticated catalog lookup uses this name.
fn cas_catalog_reference(reference: &str) -> std::borrow::Cow<'_, str> {
    let Some(remainder) = reference.strip_prefix("tensorlake/") else {
        return std::borrow::Cow::Borrowed(reference);
    };
    if remainder == "cas" || remainder.starts_with("cas/") || remainder.starts_with("cas:") {
        return std::borrow::Cow::Borrowed(reference);
    }
    std::borrow::Cow::Owned(format!("tensorlake/cas/{remainder}"))
}

/// `cas-v1:<64 lowercase hex>` pins an image id directly. The prefix with a
/// malformed id is an error rather than a registry fallthrough: no registry
/// image can live under the reserved scheme.
fn direct_pin_image_id(reference: &str) -> Result<Option<String>> {
    let Some(image_id) = reference.strip_prefix(CAS_IMAGE_REF_PREFIX) else {
        return Ok(None);
    };
    if image_id.len() != 64
        || !image_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(SandboxImageBuildError::usage(format!(
            "reference '{reference}' uses the reserved {CAS_IMAGE_REF_PREFIX} scheme but is not \
             a 64-lowercase-hex image id"
        )));
    }
    Ok(Some(image_id.to_string()))
}

/// Look up a reference as a catalog name in the caller's project. `404` and
/// `400` both mean "not a registered image" (absent, or a reference shape
/// the name grammar rejects) and fall through to a registry pull, matching
/// how the legacy path treats template-registry misses.
async fn lookup_catalog_name(client: &Client, reference: &str) -> Result<Option<String>> {
    let path = format!("/names/{reference}");
    let request = client.request(Method::GET, &path).build()?;
    let response = client.execute_raw(request).await?;
    match response.status() {
        StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => Ok(None),
        status if status.is_success() => {
            let body: Value = response.json().await?;
            body.get("image_id")
                .and_then(Value::as_str)
                .map(|image_id| Some(image_id.to_string()))
                .ok_or_else(|| {
                    SandboxImageBuildError::other(format!(
                        "Image Service name lookup for '{reference}' returned no image_id"
                    ))
                })
        }
        status => {
            let body = response.text().await.unwrap_or_default();
            Err(SandboxImageBuildError::other(format!(
                "Image Service name lookup for '{reference}' failed (HTTP {status}): {body}"
            )))
        }
    }
}

/// Decide where the Dockerfile lives in the uploaded context: its own
/// relative path when the file sits inside the context directory, or an
/// injected `.tensorlake/Dockerfile` entry carrying the plan's Dockerfile
/// text otherwise (`-f` outside the context, inline text).
fn context_dockerfile(
    plan: &DockerfileBuildPlan,
    dockerfile_path: Option<&Path>,
) -> Result<(String, Option<String>)> {
    if let Some(dockerfile_path) = dockerfile_path
        && let (Ok(dockerfile), Ok(context_dir)) = (
            dockerfile_path.canonicalize(),
            plan.context_dir.canonicalize(),
        )
        && let Ok(relative) = dockerfile.strip_prefix(&context_dir)
    {
        let relative = relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        if !relative.is_empty() {
            return Ok((relative, None));
        }
    }
    Ok((
        INJECTED_DOCKERFILE_PATH.to_string(),
        Some(plan.dockerfile_text.clone()),
    ))
}

fn build_id_of(build: &Value) -> Result<String> {
    build
        .get("build_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            SandboxImageBuildError::other("Image Service build response carries no build_id")
        })
}

async fn create_build(
    client: &Client,
    body: Value,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<Value> {
    emit(SandboxImageBuildEvent::Status(
        "Creating Image Service build...".to_string(),
    ));
    let request = client
        .request(Method::POST, "/builds")
        .json(&body)
        .build()?;
    let response = client.execute_raw(request).await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "Image Service rejected the build (HTTP {status}): {body}"
        )));
    }
    let created: Value = response.json().await?;
    emit(SandboxImageBuildEvent::Status(accepted_build_message(
        &created,
    )));
    Ok(created)
}

fn accepted_build_message(created: &Value) -> String {
    let build_id = created
        .get("build_id")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let resources = created.get("builder_resources");
    match resources.and_then(|resources| {
        Some((
            resources.get("cpus")?.as_f64()?,
            resources.get("memory_mb")?.as_u64()?,
        ))
    }) {
        Some((cpus, memory_mb)) => {
            format!("Build {build_id} accepted (builder: {cpus} CPU, {memory_mb} MiB)")
        }
        None => format!("Build {build_id} accepted"),
    }
}

/// Create the plain (uncompressed) context tar, honoring `.dockerignore`,
/// upload it through the sealed capability, and seal the build with the
/// tar's sha256. The capability URL is presigned (or the service's direct
/// dev route); the upload deliberately uses a bare HTTP client so no
/// Authorization header corrupts a presigned request.
async fn upload_and_seal_context(
    client: &Client,
    build_id: &str,
    upload: &Value,
    plan: &DockerfileBuildPlan,
    injected_dockerfile: Option<&str>,
    context_files: &[SandboxImageContextFile],
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    emit(SandboxImageBuildEvent::Status(
        "Creating build context archive...".to_string(),
    ));
    let tar_file = tempfile::Builder::new()
        .prefix("tensorlake-image-context-")
        .suffix(".tar")
        .tempfile()?;
    let (tar_bytes, digest) = create_context_tar(
        &plan.context_dir,
        injected_dockerfile,
        context_files,
        tar_file.path(),
    )?;

    let max_bytes = upload.get("max_bytes").and_then(Value::as_u64);
    if let Some(max_bytes) = max_bytes
        && tar_bytes > max_bytes
    {
        return Err(SandboxImageBuildError::usage(format!(
            "build context is {tar_bytes} bytes, above the Image Service cap of {max_bytes}"
        )));
    }

    let url = upload
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| SandboxImageBuildError::other("context upload capability carries no url"))?;
    let method = upload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("PUT")
        .parse::<Method>()
        .map_err(|_| {
            SandboxImageBuildError::other("context upload capability method is invalid")
        })?;

    emit(SandboxImageBuildEvent::Status(format!(
        "Uploading build context ({tar_bytes} bytes)..."
    )));
    let file = tokio::fs::File::open(tar_file.path()).await?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let response = reqwest::Client::new()
        .request(method, url)
        .header(CONTENT_LENGTH, tar_bytes)
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "context upload failed (HTTP {status}): {body}"
        )));
    }

    emit(SandboxImageBuildEvent::Status(
        "Sealing build context...".to_string(),
    ));
    let request = client
        .request(Method::POST, &format!("/builds/{build_id}/context/seal"))
        .json(&json!({ "digest": digest }))
        .build()?;
    let response = client.execute_raw(request).await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "context seal failed (HTTP {status}): {body}"
        )));
    }
    Ok(())
}

/// Write the plain context tar to `tar_path` and return its byte length and
/// sha256. Entries honor `.dockerignore` via the shared context walker; the
/// optional injected Dockerfile is appended last and collides with nothing
/// because its reserved path is rejected if present in the context.
fn create_context_tar(
    context_dir: &Path,
    injected_dockerfile: Option<&str>,
    context_files: &[SandboxImageContextFile],
    tar_path: &Path,
) -> Result<(u64, String)> {
    let mut tar = tar::Builder::new(std::fs::File::create(tar_path)?);
    let mut archived_paths = HashSet::new();
    if context_dir.is_dir() {
        for (full_path, relative_path) in collect_dir_files(context_dir, context_dir)? {
            if injected_dockerfile.is_some() && relative_path == INJECTED_DOCKERFILE_PATH {
                return Err(SandboxImageBuildError::usage(format!(
                    "the build context already contains {INJECTED_DOCKERFILE_PATH}, which is \
                     reserved for the injected Dockerfile"
                )));
            }
            let mut file = std::fs::File::open(&full_path)?;
            tar.append_file(&relative_path, &mut file)?;
            archived_paths.insert(relative_path);
        }
    }
    if let Some(dockerfile_text) = injected_dockerfile {
        let mut header = synthetic_file_header(dockerfile_text.len() as u64, 0o644);
        tar.append_data(
            &mut header,
            INJECTED_DOCKERFILE_PATH,
            dockerfile_text.as_bytes(),
        )?;
        archived_paths.insert(INJECTED_DOCKERFILE_PATH.to_string());
    }

    let mut context_files = context_files
        .iter()
        .map(|file| Ok((normalize_context_file_path(&file.path)?, file)))
        .collect::<Result<Vec<_>>>()?;
    context_files.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, file) in context_files {
        if !archived_paths.insert(relative_path.clone()) {
            return Err(SandboxImageBuildError::usage(format!(
                "in-memory build context file '{relative_path}' conflicts with another context file"
            )));
        }
        let mut header = synthetic_file_header(file.contents.len() as u64, file.mode);
        tar.append_data(
            &mut header,
            relative_path,
            std::io::Cursor::new(&file.contents),
        )?;
    }
    tar.finish()?;
    drop(tar);

    let mut file = std::fs::File::open(tar_path)?;
    let mut hasher = Sha256::new();
    let bytes = std::io::copy(&mut file, &mut hasher)?;
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

fn synthetic_file_header(size: u64, mode: u32) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(size);
    header.set_mode(mode & 0o777);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    header
}

/// Poll the build to a terminal status, then fetch and return the published
/// image record.
async fn wait_for_publication(
    client: &Client,
    ctx: &crate::sandbox_images::ResolvedBuildContext,
    created: &Value,
    registered_name: &str,
    emit: impl FnMut(SandboxImageBuildEvent),
) -> Result<Value> {
    wait_for_publication_with_follower(
        client,
        ctx,
        created,
        registered_name,
        emit,
        BuildWaitTiming {
            poll_interval: BUILD_POLL_INTERVAL,
            poll_timeout: BUILD_POLL_TIMEOUT,
            log_drain_grace: BUILDER_LOG_DRAIN_GRACE,
        },
        |ctx, sandbox_id, sink| async move {
            let mut emit = |event| sink.try_emit(event);
            follow_builder_logs(&ctx, &sandbox_id, &mut emit).await
        },
    )
    .await
}

#[derive(Clone, Copy)]
struct BuildWaitTiming {
    poll_interval: Duration,
    poll_timeout: Duration,
    log_drain_grace: Duration,
}

#[derive(Clone)]
struct BuilderLogSink {
    sender: tokio::sync::mpsc::Sender<SandboxImageBuildEvent>,
    dropped: Arc<AtomicUsize>,
}

impl BuilderLogSink {
    fn try_emit(&self, event: SandboxImageBuildEvent) {
        match self.sender.try_send(event) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_publication_with_follower<Follow, FollowFuture>(
    client: &Client,
    ctx: &crate::sandbox_images::ResolvedBuildContext,
    created: &Value,
    registered_name: &str,
    mut emit: impl FnMut(SandboxImageBuildEvent),
    timing: BuildWaitTiming,
    follow_builder: Follow,
) -> Result<Value>
where
    Follow: Fn(crate::sandbox_images::ResolvedBuildContext, String, BuilderLogSink) -> FollowFuture
        + Clone
        + Send
        + 'static,
    FollowFuture: Future<Output = Result<()>> + Send + 'static,
{
    let build_id = build_id_of(created)?;
    let path = format!("/builds/{build_id}");
    let deadline = tokio::time::Instant::now() + timing.poll_timeout;
    let mut last_reported = String::new();
    let mut observed_builders = HashSet::new();
    let (log_sender, mut log_receiver) = tokio::sync::mpsc::channel(BUILDER_LOG_CHANNEL_CAPACITY);
    let dropped_logs = Arc::new(AtomicUsize::new(0));
    let mut followers = tokio::task::JoinSet::new();
    let image_id = loop {
        drain_ready_builder_logs(&mut log_receiver, &dropped_logs, &mut emit);
        let request = client.request(Method::GET, &path).build()?;
        let response = client.execute_raw(request).await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(SandboxImageBuildError::other(format!(
                "build status poll failed (HTTP {status}): {body}"
            )));
        }
        let build: Value = response.json().await?;
        let status = build
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let attempt = build.get("attempt_no").and_then(Value::as_u64).unwrap_or(0);
        let progress = format!("{status}/{attempt}");
        if progress != last_reported {
            emit(SandboxImageBuildEvent::Status(format!(
                "Build {build_id}: {status} (attempt {attempt})"
            )));
            last_reported = progress;
        }
        match status.as_str() {
            "succeeded" => {
                finish_builder_log_followers(
                    &mut followers,
                    &mut log_receiver,
                    &dropped_logs,
                    timing.log_drain_grace,
                    &mut emit,
                )
                .await;
                break build
                    .get("image_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        SandboxImageBuildError::other(format!(
                            "build {build_id} succeeded but reports no image_id"
                        ))
                    })?;
            }
            "failed" => {
                finish_builder_log_followers(
                    &mut followers,
                    &mut log_receiver,
                    &dropped_logs,
                    timing.log_drain_grace,
                    &mut emit,
                )
                .await;
                let error = build
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("no error recorded");
                return Err(SandboxImageBuildError::other(format!(
                    "build {build_id} failed: {error}"
                )));
            }
            "running" => {
                if let Some(builder_sandbox_id) =
                    take_unobserved_builder(&build, &mut observed_builders)
                {
                    emit(SandboxImageBuildEvent::Status(format!(
                        "Following Image Service builder logs for attempt {attempt}..."
                    )));
                    let follower = follow_builder.clone();
                    let follower_ctx = ctx.clone();
                    let follower_sandbox_id = builder_sandbox_id.clone();
                    let warning_sender = log_sender.clone();
                    let sink = BuilderLogSink {
                        sender: log_sender.clone(),
                        dropped: dropped_logs.clone(),
                    };
                    followers.spawn(async move {
                        if let Err(error) =
                            follower(follower_ctx, follower_sandbox_id, sink).await
                        {
                            let _ = warning_sender
                                .send(SandboxImageBuildEvent::Warning(format!(
                                    "Could not follow builder logs for attempt {attempt} ({builder_sandbox_id}): {error}"
                                )))
                                .await;
                        }
                    });
                }
            }
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            finish_builder_log_followers(
                &mut followers,
                &mut log_receiver,
                &dropped_logs,
                Duration::ZERO,
                &mut emit,
            )
            .await;
            return Err(build_poll_timeout(&build_id, timing.poll_timeout));
        }
        stream_builder_logs_until_next_poll(
            &mut log_receiver,
            &dropped_logs,
            (tokio::time::Instant::now() + timing.poll_interval).min(deadline),
            &mut emit,
        )
        .await;
    };

    let request = client
        .request(Method::GET, &published_image_path(&image_id))
        .build()?;
    let response = client.execute_raw(request).await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "published image lookup failed (HTTP {status}): {body}"
        )));
    }
    let image: Value = response.json().await?;
    emit(SandboxImageBuildEvent::Status(format!(
        "Image '{registered_name}' published ({CAS_IMAGE_REF_PREFIX}{image_id})"
    )));
    Ok(image)
}

fn published_image_path(image_id: &str) -> String {
    format!("/images/{image_id}")
}

fn build_poll_timeout(build_id: &str, timeout: Duration) -> SandboxImageBuildError {
    SandboxImageBuildError::other(format!(
        "build {build_id} did not finish within {}s",
        timeout.as_secs()
    ))
}

fn drain_ready_builder_logs(
    receiver: &mut tokio::sync::mpsc::Receiver<SandboxImageBuildEvent>,
    dropped: &AtomicUsize,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    while let Ok(event) = receiver.try_recv() {
        emit(event);
    }
    emit_dropped_builder_logs(dropped, emit);
}

fn emit_dropped_builder_logs(dropped: &AtomicUsize, emit: &mut impl FnMut(SandboxImageBuildEvent)) {
    let dropped = dropped.swap(0, Ordering::Relaxed);
    if dropped != 0 {
        emit(SandboxImageBuildEvent::Warning(format!(
            "Dropped {dropped} builder log events because the CLI could not keep up."
        )));
    }
}

async fn stream_builder_logs_until_next_poll(
    receiver: &mut tokio::sync::mpsc::Receiver<SandboxImageBuildEvent>,
    dropped: &AtomicUsize,
    next_poll: tokio::time::Instant,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    loop {
        tokio::select! {
            event = receiver.recv() => {
                if let Some(event) = event {
                    emit(event);
                }
            }
            _ = tokio::time::sleep_until(next_poll) => break,
        }
    }
    emit_dropped_builder_logs(dropped, emit);
}

async fn finish_builder_log_followers(
    followers: &mut tokio::task::JoinSet<()>,
    receiver: &mut tokio::sync::mpsc::Receiver<SandboxImageBuildEvent>,
    dropped: &AtomicUsize,
    grace: Duration,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        drain_ready_builder_logs(receiver, dropped, emit);
        if followers.is_empty() || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::select! {
            event = receiver.recv() => {
                if let Some(event) = event {
                    emit(event);
                }
            }
            _ = followers.join_next() => {}
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    followers.abort_all();
    while followers.join_next().await.is_some() {}
    drain_ready_builder_logs(receiver, dropped, emit);
}

async fn follow_builder_logs(
    ctx: &crate::sandbox_images::ResolvedBuildContext,
    sandbox_id: &str,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    let lifecycle_client = sandbox_lifecycle_client(ctx)?;
    let sandboxes = SandboxesClient::new(
        lifecycle_client.clone(),
        ctx.namespace.clone(),
        is_localhost(&ctx.api_url),
    );
    let sandbox = sandboxes.get(sandbox_id).await?.into_inner();
    let proxy = sandbox_proxy_client(
        ctx,
        &lifecycle_client,
        sandbox_id,
        sandbox.sandbox_url.as_deref(),
        sandbox.ingress_endpoint.as_deref(),
        sandbox.routing_hint,
    )?;
    let process = discover_image_builder_process(&proxy).await?;
    follow_started_process_output(&proxy, process.pid, emit).await
}

fn take_unobserved_builder(build: &Value, observed: &mut HashSet<(u64, String)>) -> Option<String> {
    if build.get("status").and_then(Value::as_str) != Some("running") {
        return None;
    }
    let attempt = build.get("attempt_no")?.as_u64()?;
    let sandbox_id = build.get("builder_sandbox_id")?.as_str()?.to_string();
    observed
        .insert((attempt, sandbox_id.clone()))
        .then_some(sandbox_id)
}

async fn discover_image_builder_process(
    proxy: &crate::sandboxes::SandboxProxyClient,
) -> Result<ProcessInfo> {
    let mut last_error = None;
    for attempt in 0..BUILDER_PROCESS_DISCOVERY_ATTEMPTS {
        match proxy.list_processes().await {
            Ok(processes) => {
                if let Some(process) = processes
                    .into_inner()
                    .into_iter()
                    .filter(|process| is_image_builder_command(&process.command))
                    .min_by_key(|process| process.pid)
                {
                    return Ok(process);
                }
            }
            Err(error) => last_error = Some(error),
        }
        if attempt + 1 < BUILDER_PROCESS_DISCOVERY_ATTEMPTS {
            tokio::time::sleep(BUILDER_PROCESS_DISCOVERY_INTERVAL).await;
        }
    }

    let detail = last_error
        .map(|error| format!(" after process-list errors: {error}"))
        .unwrap_or_default();
    Err(SandboxImageBuildError::other(format!(
        "{IMAGE_BUILDER_COMMAND} process was not found{detail}"
    )))
}

fn is_image_builder_command(command: &str) -> bool {
    Path::new(command)
        .file_name()
        .is_some_and(|name| name == IMAGE_BUILDER_COMMAND)
}

#[cfg(test)]
mod url_tests {
    use super::*;

    /// A normal client needs no configuration: the Image Service is reached
    /// through the API host's ingress path, which is the only place requests
    /// are authenticated.
    #[test]
    fn defaults_to_the_ingress_path_on_the_api_host() {
        assert_eq!(
            resolve_image_service_url("https://api.tensorlake.ai", None),
            "https://api.tensorlake.ai/images/v4"
        );
        // A trailing slash on the API URL must not double up.
        assert_eq!(
            resolve_image_service_url("https://api.tensorlake.ai/", None),
            "https://api.tensorlake.ai/images/v4"
        );
    }

    #[test]
    fn the_override_wins_and_is_trimmed() {
        assert_eq!(
            resolve_image_service_url(
                "https://api.tensorlake.ai",
                Some("  http://127.0.0.1:8843/  ".to_string())
            ),
            "http://127.0.0.1:8843"
        );
    }

    /// An empty or whitespace override is ignored rather than producing a
    /// client with an empty base URL.
    #[test]
    fn a_blank_override_falls_back_to_the_default() {
        for blank in ["", "   "] {
            assert_eq!(
                resolve_image_service_url("https://api.tensorlake.ai", Some(blank.to_string())),
                "https://api.tensorlake.ai/images/v4"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Read as _, pin::Pin};

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;
    use crate::ClientBuilder;

    #[test]
    fn dockerfile_and_import_requests_apply_global_scope_or_private_default() {
        for kind in ["import", "dockerfile"] {
            let body = build_request(
                serde_json::json!({ "kind": kind }),
                true,
                30_720,
                Some(serde_json::json!({"cpus": 2.5, "memory_mb": 6144})),
                Some(r#"{"auths":{}}"#.to_string()),
            );
            assert_eq!(body["image_scope"], "global");
            assert_eq!(
                body["builder_resources"],
                serde_json::json!({"cpus": 2.5, "memory_mb": 6144})
            );
            assert_eq!(body["registry_credentials_json"], r#"{"auths":{}}"#);
            assert_eq!(body["disk_mb"], 30_720);

            let private_body = build_request(
                serde_json::json!({ "kind": kind }),
                false,
                DEFAULT_CAS_IMAGE_DISK_MB,
                None,
                None,
            );
            assert_eq!(
                private_body,
                serde_json::json!({
                    "kind": kind,
                    "disk_mb": DEFAULT_CAS_IMAGE_DISK_MB,
                })
            );
        }
    }

    #[test]
    fn cas_target_disk_defaults_and_validates() {
        assert_eq!(cas_target_disk_mb(None).unwrap(), DEFAULT_CAS_IMAGE_DISK_MB);
        assert_eq!(cas_target_disk_mb(Some(30_720)).unwrap(), 30_720);
        assert!(cas_target_disk_mb(Some(0)).is_err());
        assert!(cas_target_disk_mb(Some(u64::MAX)).is_err());
    }

    #[test]
    fn builder_resource_overrides_preserve_partial_requests() {
        let mut options = common_options();
        assert_eq!(builder_resource_overrides(&options).unwrap(), None);

        options.cpus = Some(2.5);
        assert_eq!(
            builder_resource_overrides(&options).unwrap(),
            Some(serde_json::json!({"cpus": 2.5}))
        );

        options.cpus = None;
        options.memory_mb = Some(6_144);
        assert_eq!(
            builder_resource_overrides(&options).unwrap(),
            Some(serde_json::json!({"memory_mb": 6144}))
        );

        options.memory_mb = Some(-1);
        assert!(builder_resource_overrides(&options).is_err());
        options.memory_mb = None;
        options.cpus = Some(f64::NAN);
        assert!(builder_resource_overrides(&options).is_err());
    }

    #[test]
    fn accepted_build_message_reports_resolved_resources_when_available() {
        assert_eq!(
            accepted_build_message(&serde_json::json!({
                "build_id": "build-1",
                "builder_resources": {"cpus": 2.5, "memory_mb": 6144},
            })),
            "Build build-1 accepted (builder: 2.5 CPU, 6144 MiB)"
        );
        assert_eq!(
            accepted_build_message(&serde_json::json!({"build_id": "build-1"})),
            "Build build-1 accepted"
        );
    }

    #[test]
    fn image_service_warns_only_for_unsupported_legacy_options() {
        let mut options = common_options();
        options.cpus = Some(2.5);
        options.memory_mb = Some(6_144);
        options.disk_mb = Some(30_720);
        let mut events = Vec::new();
        warn_ignored_options(&options, &mut |event| events.push(event));
        assert!(events.is_empty());

        options.builder_disk_mb = Some(20_480);
        warn_ignored_options(&options, &mut |event| events.push(event));
        assert_eq!(
            events,
            vec![SandboxImageBuildEvent::Warning(
                "Image Service builds ignore: builder_disk_mb.".to_string()
            )]
        );
    }

    #[test]
    fn each_running_attempt_builder_is_observed_once() {
        let mut observed = HashSet::new();
        let pending = json!({
            "status": "pending",
            "attempt_no": 1,
            "builder_sandbox_id": "stale-builder",
        });
        assert_eq!(take_unobserved_builder(&pending, &mut observed), None);

        let pre_create = json!({ "status": "running", "attempt_no": 1 });
        assert_eq!(take_unobserved_builder(&pre_create, &mut observed), None);

        let attempt_one = json!({
            "status": "running",
            "attempt_no": 1,
            "builder_sandbox_id": "builder-1",
        });
        assert_eq!(
            take_unobserved_builder(&attempt_one, &mut observed).as_deref(),
            Some("builder-1")
        );
        assert_eq!(take_unobserved_builder(&attempt_one, &mut observed), None);

        let attempt_two = json!({
            "status": "running",
            "attempt_no": 2,
            "builder_sandbox_id": "builder-2",
        });
        assert_eq!(
            take_unobserved_builder(&attempt_two, &mut observed).as_deref(),
            Some("builder-2")
        );
    }

    #[test]
    fn builder_command_matches_only_the_command_basename() {
        assert!(is_image_builder_command("tl-image-builder"));
        assert!(is_image_builder_command("/usr/local/bin/tl-image-builder"));
        assert!(!is_image_builder_command("tl-rootfs-build"));
        assert!(!is_image_builder_command("tl-image-builder-helper"));
    }

    #[test]
    fn published_image_path_relies_on_ingress_for_project_scope() {
        assert_eq!(published_image_path("image-id"), "/images/image-id");
    }

    #[test]
    fn direct_pins_accept_only_lowercase_hex_ids() {
        let hex = "a".repeat(64);
        assert_eq!(
            direct_pin_image_id(&format!("cas-v1:{hex}")).unwrap(),
            Some(hex)
        );
        assert_eq!(direct_pin_image_id("alpine:3.20").unwrap(), None);
        assert_eq!(direct_pin_image_id("team/base").unwrap(), None);
        assert!(direct_pin_image_id("cas-v1:NOT-HEX").is_err());
        assert!(direct_pin_image_id(&format!("cas-v1:{}", "a".repeat(63))).is_err());
    }

    #[test]
    fn tensorlake_references_use_the_cas_catalog_namespace() {
        for (reference, expected) in [
            ("tensorlake/ubuntu-minimal", "tensorlake/cas/ubuntu-minimal"),
            (
                "tensorlake/ubuntu-minimal:latest",
                "tensorlake/cas/ubuntu-minimal:latest",
            ),
            (
                "tensorlake/cas/ubuntu-minimal",
                "tensorlake/cas/ubuntu-minimal",
            ),
            ("tensorlake/cas:latest", "tensorlake/cas:latest"),
            ("ubuntu:24.04", "ubuntu:24.04"),
            ("ghcr.io/team/image:v1", "ghcr.io/team/image:v1"),
        ] {
            assert_eq!(cas_catalog_reference(reference), expected);
        }
    }

    #[test]
    fn context_dockerfile_uses_the_relative_path_inside_the_context() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("build");
        std::fs::create_dir(&nested).unwrap();
        let dockerfile = nested.join("prod.Dockerfile");
        std::fs::write(&dockerfile, "FROM scratch\n").unwrap();

        let plan = plan_with_context(dir.path().to_path_buf());
        let (path, injected) = context_dockerfile(&plan, Some(&dockerfile)).unwrap();
        assert_eq!(path, "build/prod.Dockerfile");
        assert!(injected.is_none());
    }

    #[test]
    fn context_dockerfile_injects_text_when_outside_the_context() {
        let context = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let dockerfile = elsewhere.path().join("Dockerfile");
        std::fs::write(&dockerfile, "FROM scratch\n").unwrap();

        let plan = plan_with_context(context.path().to_path_buf());
        let (path, injected) = context_dockerfile(&plan, Some(&dockerfile)).unwrap();
        assert_eq!(path, INJECTED_DOCKERFILE_PATH);
        assert_eq!(injected.as_deref(), Some("FROM scratch\n"));
    }

    #[test]
    fn context_tar_is_plain_and_carries_the_injected_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("app.py"), "print('hi')\n").unwrap();

        let tar_path = scratch.path().join("out.tar");
        let (bytes, digest) =
            create_context_tar(dir.path(), Some("FROM scratch\n"), &[], &tar_path).unwrap();
        assert_eq!(bytes, std::fs::metadata(&tar_path).unwrap().len());
        assert_eq!(digest.len(), 64);

        let mut names = Vec::new();
        let mut archive = tar::Archive::new(std::fs::File::open(&tar_path).unwrap());
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().to_string();
            if name == INJECTED_DOCKERFILE_PATH {
                assert_eq!(entry.header().entry_type(), tar::EntryType::Regular);
                assert_eq!(entry.header().uid().unwrap(), 0);
                assert_eq!(entry.header().gid().unwrap(), 0);
                assert_eq!(entry.header().mode().unwrap(), 0o644);
                assert_eq!(entry.header().mtime().unwrap(), 0);
                let mut text = String::new();
                entry.read_to_string(&mut text).unwrap();
                assert_eq!(text, "FROM scratch\n");
            }
            names.push(name);
        }
        names.sort();
        assert_eq!(
            names,
            vec![INJECTED_DOCKERFILE_PATH.to_string(), "app.py".to_string()]
        );
    }

    #[test]
    fn context_tar_rejects_a_colliding_reserved_dockerfile_entry() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".tensorlake")).unwrap();
        std::fs::write(dir.path().join(INJECTED_DOCKERFILE_PATH), "FROM x\n").unwrap();

        let tar_path = scratch.path().join("out.tar");
        let error = create_context_tar(dir.path(), Some("FROM scratch\n"), &[], &tar_path)
            .expect_err("reserved path must collide");
        assert!(
            error.to_string().contains(".tensorlake/Dockerfile"),
            "{error}"
        );
    }

    #[test]
    fn context_tar_carries_validated_in_memory_files() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let tar_path = scratch.path().join("out.tar");
        let files = vec![SandboxImageContextFile {
            path: ".tensorlake/runtime.tgz".into(),
            contents: b"runtime".to_vec(),
            mode: 0o640,
        }];

        create_context_tar(dir.path(), Some("FROM scratch\n"), &files, &tar_path).unwrap();

        let mut archive = tar::Archive::new(std::fs::File::open(&tar_path).unwrap());
        let runtime = archive
            .entries()
            .unwrap()
            .map(|entry| entry.unwrap())
            .find(|entry| entry.path().unwrap() == Path::new(".tensorlake/runtime.tgz"))
            .expect("in-memory runtime file");
        assert_eq!(runtime.header().mode().unwrap(), 0o640);
    }

    #[test]
    fn context_tar_rejects_in_memory_file_collisions() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("runtime.tgz"), "disk").unwrap();
        let tar_path = scratch.path().join("out.tar");
        let files = vec![SandboxImageContextFile {
            path: "runtime.tgz".into(),
            contents: b"memory".to_vec(),
            mode: 0o644,
        }];

        let error = create_context_tar(dir.path(), None, &files, &tar_path)
            .expect_err("disk and memory paths must not collide");
        assert!(error.to_string().contains("conflicts"), "{error}");
    }

    #[tokio::test]
    async fn successful_build_wins_over_an_open_log_stream() {
        let image_id = "a".repeat(64);
        let (base_url, server) = scripted_image_service(vec![
            json!({
                "status": "running",
                "attempt_no": 1,
                "builder_sandbox_id": "builder-1",
            }),
            json!({
                "status": "succeeded",
                "attempt_no": 1,
                "image_id": image_id,
            }),
            json!({"image_id": image_id}),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let mut events = Vec::new();

        let image = wait_for_publication_with_follower(
            &client,
            &ctx,
            &json!({"build_id": "build-1"}),
            "image-1",
            |event| events.push(event),
            test_build_wait_timing(),
            |_ctx, _sandbox_id, sink| async move {
                sink.try_emit(SandboxImageBuildEvent::BuildLog {
                    stream: "stdout".to_string(),
                    message: "still building".to_string(),
                });
                std::future::pending::<()>().await;
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(image["image_id"], image_id);
        assert!(events.contains(&SandboxImageBuildEvent::BuildLog {
            stream: "stdout".to_string(),
            message: "still building".to_string(),
        }));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_attach_once_warn_once_and_bound_buffered_logs() {
        let image_id = "b".repeat(64);
        let (base_url, server) = scripted_image_service(vec![
            json!({
                "status": "running",
                "attempt_no": 1,
                "builder_sandbox_id": "builder-1",
            }),
            json!({
                "status": "running",
                "attempt_no": 1,
                "builder_sandbox_id": "builder-1",
            }),
            json!({
                "status": "pending",
                "attempt_no": 1,
                "builder_sandbox_id": "stale-builder",
            }),
            json!({
                "status": "running",
                "attempt_no": 2,
                "builder_sandbox_id": "builder-2",
            }),
            json!({
                "status": "succeeded",
                "attempt_no": 2,
                "image_id": image_id,
            }),
            json!({"image_id": image_id}),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let attempt_one_attaches = Arc::new(AtomicUsize::new(0));
        let attempt_two_attaches = Arc::new(AtomicUsize::new(0));
        let mut events = Vec::new();
        let follow = {
            let attempt_one_attaches = attempt_one_attaches.clone();
            let attempt_two_attaches = attempt_two_attaches.clone();
            move |_ctx, sandbox_id: String, sink: BuilderLogSink| {
                let attempt_one_attaches = attempt_one_attaches.clone();
                let attempt_two_attaches = attempt_two_attaches.clone();
                Box::pin(async move {
                    match sandbox_id.as_str() {
                        "builder-1" => {
                            attempt_one_attaches.fetch_add(1, Ordering::Relaxed);
                            Err(SandboxImageBuildError::other("stream unavailable"))
                        }
                        "builder-2" => {
                            attempt_two_attaches.fetch_add(1, Ordering::Relaxed);
                            for index in 0..BUILDER_LOG_CHANNEL_CAPACITY + 50 {
                                sink.try_emit(SandboxImageBuildEvent::BuildLog {
                                    stream: "stdout".to_string(),
                                    message: format!("line {index}"),
                                });
                            }
                            std::future::pending::<()>().await;
                            Ok(())
                        }
                        other => panic!("unexpected builder {other}"),
                    }
                }) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
            }
        };

        let image = wait_for_publication_with_follower(
            &client,
            &ctx,
            &json!({"build_id": "build-1"}),
            "image-1",
            |event| events.push(event),
            test_build_wait_timing(),
            follow,
        )
        .await
        .unwrap();

        assert_eq!(image["image_id"], image_id);
        assert_eq!(attempt_one_attaches.load(Ordering::Relaxed), 1);
        assert_eq!(attempt_two_attaches.load(Ordering::Relaxed), 1);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SandboxImageBuildEvent::Warning(message)
                        if message.contains("stream unavailable")
                ))
                .count(),
            1
        );
        assert!(events.iter().any(|event| matches!(
            event,
            SandboxImageBuildEvent::Warning(message) if message.starts_with("Dropped ")
        )));
        server.await.unwrap();
    }

    fn test_build_wait_timing() -> BuildWaitTiming {
        BuildWaitTiming {
            poll_interval: Duration::from_millis(5),
            poll_timeout: Duration::from_secs(1),
            log_drain_grace: Duration::from_millis(20),
        }
    }

    fn test_build_context(api_url: &str) -> crate::sandbox_images::ResolvedBuildContext {
        crate::sandbox_images::ResolvedBuildContext {
            api_url: api_url.to_string(),
            bearer_token: "token".to_string(),
            use_scope_headers: false,
            organization_id: Some("organization-1".to_string()),
            project_id: Some("project-1".to_string()),
            namespace: "default".to_string(),
            user_agent: None,
        }
    }

    async fn scripted_image_service(
        responses: Vec<Value>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let body = serde_json::to_vec(&response).unwrap();
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(headers.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        (format!("http://{address}"), server)
    }

    fn plan_with_context(context_dir: std::path::PathBuf) -> DockerfileBuildPlan {
        DockerfileBuildPlan {
            context_dir,
            registered_name: "img".to_string(),
            dockerfile_text: "FROM scratch\n".to_string(),
            base_image: "scratch".to_string(),
            base_image_is_internal_stage: false,
            additional_image_references: Vec::new(),
            unresolvable_image_references: Vec::new(),
            ignored_instructions: Vec::new(),
            import_image_reference: None,
        }
    }

    fn common_options() -> CommonBuildOptions {
        CommonBuildOptions {
            api_url: "https://api.tensorlake.dev".to_string(),
            bearer_token: "token".to_string(),
            use_scope_headers: false,
            organization_id: None,
            project_id: None,
            namespace: "default".to_string(),
            registered_name: None,
            disk_mb: None,
            builder_disk_mb: None,
            cpus: None,
            memory_mb: None,
            is_public: false,
            cas: true,
            user_agent: None,
            docker_compat: false,
        }
    }
}
