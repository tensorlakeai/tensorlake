//! CAS image builds through the Image Service build plane.
//!
//! The SDK owns Dockerfile parsing and reference resolution: the build
//! plan's external references are resolved against the Image Service
//! catalog here, client-side, and submitted already resolved with the build
//! request. The Image Service validates each pin against the caller's
//! project and its reconciler drives the builder sandbox, mounting one
//! read-only parent volume per pin beside the writable target and disposable
//! scratch volumes, and snapshotting the target through the CAS filesystem
//! daemon. The client admits the build under an opaque client request id
//! (`Idempotency-Key`), uploads and seals the context, and polls the build to
//! a terminal status, reconnecting across transient failures and honoring the
//! service-reported operation deadline. While an active attempt exposes its
//! ephemeral builder sandbox, the client may also follow `tl-image-builder`
//! output through the existing project-scoped sandbox APIs; Image Service
//! status remains authoritative. The public surface is
//! [`crate::image_builds`]; this module holds the mechanics.

use std::{
    collections::HashSet,
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use reqwest::{Method, StatusCode, header::CONTENT_LENGTH};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use crate::{
    Client,
    error::SdkError,
    image_builds::{
        CANCEL_HANDSHAKE_BUDGET, CancelBuildOutcome, CancelBuildResponse, CancelBuildSelector,
        ContextUploadCapability, DEFAULT_WAIT_LIMIT, IDEMPOTENCY_KEY_HEADER, ImageBuildClient,
        ImageBuildClientOptions, ImageBuildConflict, ImageBuildError, ImageBuildHandle,
        ImageBuildInfo, ImageBuildPhase, ImageBuildResult, ImageBuildStatus, ImageBuildWaitOptions,
        InterruptSignal, InterruptionOutcome, InterruptionReport, new_client_request_id,
    },
    sandbox_image_catalog::{
        IMAGE_SERVICE_ID_PREFIX, ImageServiceImage, RegistrationWait, is_image_service_id,
    },
    sandbox_images::{
        CommonBuildOptions, DockerfileBuildPlan, ResolvedBuildContext, SandboxImageBuildError,
        SandboxImageBuildEvent, SandboxImageContextFile, UnresolvableImageReferenceReason,
        client_builder, collect_dir_files, emit_plan_warnings, follow_started_process_output,
        is_localhost, normalize_context_file_path, open_context_file, platform_client,
        resolve_build_context, sandbox_lifecycle_client, sandbox_proxy_client,
        sandbox_templates_client_for,
    },
    sandbox_templates::SandboxTemplatesClient,
    sandboxes::{SandboxesClient, models::ProcessInfo},
};

type Result<T> = std::result::Result<T, SandboxImageBuildError>;
type BuildResult<T> = std::result::Result<T, ImageBuildError>;

/// Base URL of the Image Service. Until the platform gateway routes to the
/// service, CAS builds require this to be set explicitly.
pub const IMAGE_SERVICE_URL_ENV: &str = "TENSORLAKE_IMAGE_SERVICE_URL";

/// Ingress route that fronts the Image Service. The ingress authenticates,
/// replaces any client-supplied identity headers with verified ones, and maps
/// this prefix onto the service's own routes -- so the paths below are
/// prefix-relative (`/builds`, not `/v1/builds`).
const IMAGE_SERVICE_INGRESS_PATH: &str = "/images/v4";

/// The retired immutable reference scheme. A Dockerfile reference written in
/// this form is refused: parents are pinned by name or `image_` id (ADR 0107
/// D16, D32).
const CAS_V1_REFERENCE_PREFIX: &str = "cas-v1:";

/// Where the Dockerfile text is injected into the context tar when the
/// Dockerfile does not live inside the context directory (`-f` outside the
/// context, or inline Dockerfile text).
const INJECTED_DOCKERFILE_PATH: &str = ".tensorlake/Dockerfile";

/// The Image Service accepts three pinned parents beside its target and
/// scratch volumes, matching Server's five-volume sandbox limit.
const MAX_PINNED_PARENTS: usize = 3;

/// Default final filesystem capacity for CAS image builds. This is the
/// Server's current minimum sandbox/rootfs disk size; callers can override it
/// with the existing `disk_mb` / `--disk_mb` option.
const DEFAULT_CAS_IMAGE_DISK_MB: u64 = 10 * 1024;
const BYTES_PER_MIB: u64 = 1024 * 1024;

pub(crate) const BUILD_POLL_INTERVAL: Duration = Duration::from_secs(3);
const BUILDER_LOG_CHANNEL_CAPACITY: usize = 256;
const BUILDER_LOG_DRAIN_GRACE: Duration = Duration::from_secs(2);
const BUILDER_PROCESS_DISCOVERY_ATTEMPTS: usize = 10;
const BUILDER_PROCESS_DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const IMAGE_BUILDER_COMMAND: &str = "tl-image-builder";

/// How long an admission, seal or lookup keeps reconnecting across transient
/// failures. Admission replays carry the same `Idempotency-Key`, so a replay
/// joins the operation the first attempt may already have created.
const ADMISSION_RETRY_BUDGET: Duration = Duration::from_secs(120);
/// How long a single build lookup keeps reconnecting.
const LOOKUP_RETRY_BUDGET: Duration = Duration::from_secs(30);
/// Reconnect backoff: `0.5 s × 2^n`, capped here, with ±25% jitter.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(15);
/// Cap on a `Retry-After` the service asks for.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Run a build plan through the Image Service to publication: admit, upload
/// and seal where required, then wait. `dockerfile_path` is the local
/// Dockerfile path for Dockerfile builds (used only to decide whether the
/// file already lives inside the context directory); import plans carry no
/// Dockerfile. The result is the published image in the platform template
/// shape, as before.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_image_service_build<F>(
    plan: DockerfileBuildPlan,
    dockerfile_path: Option<&Path>,
    build_args: Vec<(String, String)>,
    context_files: Vec<SandboxImageContextFile>,
    options: CommonBuildOptions,
    registry_credentials_json: Option<String>,
    interrupt: &InterruptSignal,
    mut emit: F,
) -> Result<Value>
where
    F: FnMut(SandboxImageBuildEvent),
{
    let registered_name = plan.registered_name.clone();
    let mut handle = start_image_service_build(
        plan,
        dockerfile_path,
        build_args,
        context_files,
        options,
        registry_credentials_json,
        interrupt,
        &mut emit,
    )
    .await?;
    let result = handle
        .wait(
            ImageBuildWaitOptions {
                timeout: None,
                follow_logs: true,
            },
            interrupt,
            &mut emit,
        )
        .await?;
    // The record is returned in the platform template shape so callers of
    // both build paths receive one record kind (ADR 0107 D18, D28).
    emit(SandboxImageBuildEvent::Status(format!(
        "Image '{registered_name}' registered ({})",
        result.id.as_deref().unwrap_or("-")
    )));
    Ok(serde_json::to_value(result)?)
}

/// Admit a build plan without waiting: resolve parents, admit under a fresh
/// client request id, and for Dockerfile builds upload and seal the context.
/// Triggering `interrupt` at any point runs the cancellation handshake —
/// by client request id while the admission is unresolved, by build id once
/// admitted — and ends with [`ImageBuildError::Interrupted`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_image_service_build<F>(
    plan: DockerfileBuildPlan,
    dockerfile_path: Option<&Path>,
    build_args: Vec<(String, String)>,
    context_files: Vec<SandboxImageContextFile>,
    options: CommonBuildOptions,
    registry_credentials_json: Option<String>,
    interrupt: &InterruptSignal,
    mut emit: F,
) -> BuildResult<ImageBuildHandle>
where
    F: FnMut(SandboxImageBuildEvent),
{
    // Build args expand `FROM` references before parents are resolved, so a
    // `FROM python:${PY_TAG}` with a supplied or defaulted PY_TAG pins the
    // registered image it names; only references left with an unexpanded
    // variable remain unresolvable and are warned about.
    let plan = expand_build_arg_references(plan, &build_args);
    emit_plan_warnings(&plan, &mut emit);
    warn_ignored_options(&options, &mut emit);
    let disk_mb = cas_target_disk_mb(options.disk_mb)?;
    let is_public = options.is_public;
    let builder_resources = builder_resource_overrides(&options)?;
    // Forward credentials already resolved for this operation, using the same
    // document as the non-CAS builder. The service
    // stages it write-only and the reconciler hands the guest a short-lived
    // fetch URL; the document never enters the catalog.
    if registry_credentials_json.is_some() {
        emit(SandboxImageBuildEvent::Status(
            "Forwarding registry credentials to the build".to_string(),
        ));
    }
    // Ingress authenticates the bearer credential and injects the canonical project header.
    // Image Service deliberately accepts no project selector in bodies or query strings.
    let ctx = resolve_build_context(options)?;
    let client = image_service_client(&ctx)?;
    // Generated once, before the first admission attempt, and retained across
    // every retry of it: a replay joins the operation rather than admitting a
    // second one.
    let request_id = new_client_request_id();

    let created = if let Some(import_reference) = plan.import_image_reference.clone() {
        admit_build(
            &client,
            "/builds",
            &build_request(
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
            &request_id,
            interrupt,
            &mut emit,
        )
        .await?
        .info
    } else {
        // A legacy template named like a Dockerfile reference cannot be a CAS
        // parent; the platform registry is consulted only to say so (D32).
        let templates = sandbox_templates_client_for(&ctx, platform_client(&ctx)?)?;
        let parents = resolve_parents(&client, &templates, &plan, &mut emit).await?;
        let (dockerfile_in_context, injected_dockerfile) =
            context_dockerfile(&plan, dockerfile_path)?;
        let created = admit_build(
            &client,
            "/builds",
            &build_request(
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
            &request_id,
            interrupt,
            &mut emit,
        )
        .await?;
        let upload = created.context_upload.clone().ok_or_else(|| {
            ImageBuildError::other(
                "Image Service build creation returned no context upload capability",
            )
        })?;
        let sealed = tokio::select! {
            biased;
            _ = interrupt.triggered() => None,
            sealed = upload_and_seal_context(
                &client,
                &created.info.build_id,
                &upload,
                &plan,
                injected_dockerfile.as_deref(),
                &context_files,
                &mut emit,
            ) => Some(sealed),
        };
        match sealed {
            Some(sealed) => sealed?,
            None => {
                let report =
                    interruption_for_build(&client, &created.info.build_id, Some(&request_id))
                        .await;
                emit(SandboxImageBuildEvent::Interrupted(report.clone()));
                return Err(ImageBuildError::Interrupted(report));
            }
        }
        created.info
    };

    Ok(ImageBuildHandle {
        client: ImageBuildClient { client, ctx },
        build_id: created.build_id.clone(),
        client_request_id: Some(request_id),
        info: created,
    })
}

/// Build the lookup/cancel client behind [`ImageBuildClient::new`].
pub(crate) fn image_build_client(
    options: ImageBuildClientOptions,
) -> BuildResult<ImageBuildClient> {
    let ctx = resolve_build_context(CommonBuildOptions {
        api_url: options.api_url,
        bearer_token: options.bearer_token,
        use_scope_headers: options.use_scope_headers,
        organization_id: options.organization_id,
        project_id: options.project_id,
        namespace: options.namespace,
        registered_name: None,
        disk_mb: None,
        builder_disk_mb: None,
        cpus: None,
        memory_mb: None,
        is_public: false,
        cas: true,
        user_agent: options.user_agent,
        docker_compat: false,
    })?;
    let client = image_service_client(&ctx)?;
    Ok(ImageBuildClient { client, ctx })
}

fn image_service_client(ctx: &ResolvedBuildContext) -> BuildResult<Client> {
    let image_service_url = image_service_url(&ctx.api_url);
    Ok(client_builder(
        &image_service_url,
        &ctx.bearer_token,
        ctx.use_scope_headers,
        ctx.organization_id.as_deref(),
        ctx.project_id.as_deref(),
        ctx.user_agent.as_deref(),
    )
    .build()?)
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

/// Builder-rootfs sizing belongs to the legacy platform-api rootfs builder:
/// the Image Service sizes the builder's scratch space from `disk_mb`, which
/// also sizes the published target. Docker compatibility mode needs no
/// warning: CAS builds always run BuildKit (ADR 0107 D29).
fn warn_ignored_options(
    options: &CommonBuildOptions,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    if options.builder_disk_mb.is_some() {
        emit(SandboxImageBuildEvent::Warning(
            "builder_disk_mb is ignored for CAS builds: the builder's disk is sized from \
             disk_mb, the size of the published image."
                .to_string(),
        ));
    }
}

/// Expand `$VAR`, `${VAR}`, `${VAR:-default}` and `${VAR-default}` in the
/// plan's `FROM` references from the supplied build args, then the
/// Dockerfile's global `ARG` defaults (ADR 0107 D31). References that still
/// carry a variable afterwards stay unresolvable; references that expanded
/// completely become ordinary catalog candidates. The Dockerfile text itself
/// is untouched: BuildKit performs the same expansion in the builder.
pub(crate) fn expand_build_arg_references(
    mut plan: DockerfileBuildPlan,
    build_args: &[(String, String)],
) -> DockerfileBuildPlan {
    let lookup = |name: &str| -> Option<String> {
        build_args
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .or_else(|| {
                plan.global_build_args
                    .iter()
                    .find(|(key, _)| key == name)
                    .and_then(|(_, default)| default.clone())
            })
    };
    let expanded_base = expand_reference(&plan.base_image, &lookup);
    let expanded_additional: Vec<String> = plan
        .unresolvable_image_references
        .iter()
        .filter(|unresolvable| {
            unresolvable.reason == UnresolvableImageReferenceReason::BuildArgExpansion
                && unresolvable.reference != plan.base_image
        })
        .filter_map(|unresolvable| {
            let expanded = expand_reference(&unresolvable.reference, &lookup);
            (!expanded.contains('$')).then_some(expanded)
        })
        .collect();
    let base_expanded = expanded_base != plan.base_image && !expanded_base.contains('$');
    plan.unresolvable_image_references.retain(|unresolvable| {
        if unresolvable.reason != UnresolvableImageReferenceReason::BuildArgExpansion {
            return true;
        }
        expand_reference(&unresolvable.reference, &lookup).contains('$')
    });
    for reference in expanded_additional {
        if reference != expanded_base
            && !reference.eq_ignore_ascii_case("scratch")
            && !plan.additional_image_references.contains(&reference)
        {
            plan.additional_image_references.push(reference);
        }
    }
    if base_expanded {
        plan.base_image = expanded_base;
    }
    plan
}

/// Substitute Dockerfile-style variable references in one image reference.
fn expand_reference(reference: &str, lookup: &dyn Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(reference.len());
    let mut rest = reference;
    while let Some(index) = rest.find('$') {
        out.push_str(&rest[..index]);
        let after = &rest[index + 1..];
        if let Some(inner) = after.strip_prefix('{') {
            let Some(close) = inner.find('}') else {
                out.push_str(&rest[index..]);
                return out;
            };
            let expression = &inner[..close];
            let (name, default) = match expression.split_once(":-") {
                Some((name, default)) => (name, Some(default)),
                None => match expression.split_once('-') {
                    Some((name, default)) => (name, Some(default)),
                    None => (expression, None),
                },
            };
            match lookup(name) {
                Some(value) if !value.is_empty() || default.is_none() => out.push_str(&value),
                _ => match default {
                    Some(default) => out.push_str(default),
                    None => out.push_str(&rest[index..index + 2 + close + 1]),
                },
            }
            rest = &inner[close + 1..];
        } else {
            let end = after
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(after.len());
            let name = &after[..end];
            match lookup(name) {
                Some(value) if !name.is_empty() => out.push_str(&value),
                _ => out.push_str(&rest[index..index + 1 + end]),
            }
            rest = &after[end..];
        }
    }
    out.push_str(rest);
    out
}

/// Resolve the plan's external references against the Image Service catalog
/// and return the resolved parents the build request submits, each pinned
/// by the registered image's `image_` id. An `image_` reference pins the id
/// directly; every other plain reference is looked up as a catalog name. A
/// miss that names a legacy platform template is an error (a CAS build
/// cannot be layered on a legacy image); any other miss means the guest
/// pulls the reference from a registry. References the plan still marks
/// unresolvable (`$` expansions left after build-arg expansion, `@` digest
/// pins) never reach here.
async fn resolve_parents(
    client: &Client,
    templates: &SandboxTemplatesClient,
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
        let public_id = match direct_pin_public_id(reference)? {
            Some(public_id) => Some(public_id),
            None => lookup_catalog_name(client, catalog_reference.as_ref()).await?,
        };
        if public_id.is_none() && catalog_reference.as_ref() != reference {
            return Err(SandboxImageBuildError::usage(format!(
                "CAS base image '{}' was not found while resolving Dockerfile reference \
                 '{}'. Publish the CAS base first or use an existing tensorlake/cas image.",
                catalog_reference, reference
            )));
        }
        let Some(public_id) = public_id else {
            if templates.find_by_name(reference).await?.is_some() {
                return Err(SandboxImageBuildError::usage(format!(
                    "'{reference}' is a legacy image; a CAS build can use CAS images or \
                     registry references as a base; rebuild '{reference}' with --cas first"
                )));
            }
            continue; // external registry reference, pulled by the guest
        };
        if catalog_reference.as_ref() == reference {
            emit(SandboxImageBuildEvent::Status(format!(
                "Resolved '{reference}' to registered image {public_id}"
            )));
        } else {
            emit(SandboxImageBuildEvent::Status(format!(
                "Resolved '{reference}' through CAS image '{}' to {public_id}",
                catalog_reference
            )));
        }
        pinned_references.push(reference.to_string());
        parents.push(json!({ "reference": reference, "public_id": public_id }));
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

/// An `image_<id>` reference pins the registered image directly, with no
/// name lookup; the service validates the pin against the caller's project.
/// The retired `cas-v1:` scheme is an error rather than a registry
/// fallthrough: no registry image can live under it, and digests are no
/// longer addresses.
fn direct_pin_public_id(reference: &str) -> Result<Option<String>> {
    if reference.starts_with(CAS_V1_REFERENCE_PREFIX) {
        return Err(SandboxImageBuildError::usage(format!(
            "reference '{reference}' pins a content digest, which is no longer supported; pin \
             the image by name or its image_ id"
        )));
    }
    if !reference.starts_with(IMAGE_SERVICE_ID_PREFIX) {
        return Ok(None);
    }
    if !is_image_service_id(reference) {
        return Err(SandboxImageBuildError::usage(format!(
            "reference '{reference}' uses the reserved {IMAGE_SERVICE_ID_PREFIX} prefix but is \
             not an image id"
        )));
    }
    Ok(Some(reference.to_string()))
}

/// Look up a reference as a catalog name in the caller's project and return
/// the registered image's `image_` id. `404` and `400` both mean "not a
/// registered image" (absent, or a reference shape the name grammar
/// rejects) and fall through to a registry pull, matching how the legacy
/// path treats template-registry misses.
async fn lookup_catalog_name(client: &Client, reference: &str) -> Result<Option<String>> {
    let path = format!("/images/name/{reference}");
    let request = client.request(Method::GET, &path).build()?;
    let response = client.execute_raw(request).await?;
    match response.status() {
        StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => Ok(None),
        status if status.is_success() => {
            let image: ImageServiceImage = response.json().await?;
            Ok(Some(image.id))
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

// ---------------------------------------------------------------------------
// Requests, reconnects and error classification
// ---------------------------------------------------------------------------

/// A response read to completion, with the one header the reconnect policy
/// honors.
struct RawResponse {
    status: StatusCode,
    retry_after: Option<Duration>,
    body: String,
}

/// A failed attempt: the classified error plus the service's `Retry-After`,
/// which only the reconnect loop needs. Boxed so the `Err` arm stays small
/// on the hot request path.
struct RequestFailure {
    error: Box<ImageBuildError>,
    retry_after: Option<Duration>,
}

impl RequestFailure {
    fn new(error: ImageBuildError, retry_after: Option<Duration>) -> Self {
        Self {
            error: Box::new(error),
            retry_after,
        }
    }
}

impl From<ImageBuildError> for RequestFailure {
    fn from(error: ImageBuildError) -> Self {
        Self::new(error, None)
    }
}

impl From<SdkError> for RequestFailure {
    fn from(error: SdkError) -> Self {
        ImageBuildError::Sdk(error).into()
    }
}

/// The Image Service error body: `{"error": ...}` plus the typed conflict
/// fields of `409 image_build_in_progress`.
#[derive(Debug, Default, Deserialize)]
struct ImageServiceErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    code: Option<String>,
    /// The build an error concerns (`build_failed`, `build_cancelled`,
    /// `client_request_id_mismatch`, `publication_pending`), flattened into
    /// the envelope by the service.
    #[serde(default)]
    build_id: Option<String>,
    #[serde(default)]
    image_name: Option<String>,
    #[serde(default)]
    active_build_id: Option<String>,
    #[serde(default)]
    active_kind: Option<String>,
    #[serde(default)]
    active_status: Option<String>,
}

async fn send_raw(
    client: &Client,
    request: reqwest::Request,
) -> std::result::Result<RawResponse, RequestFailure> {
    let response = client.execute_raw(request).await?;
    let status = response.status();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let body = response
        .text()
        .await
        .map_err(|error| RequestFailure::from(SdkError::Http(error)))?;
    Ok(RawResponse {
        status,
        retry_after,
        body,
    })
}

/// Map a non-success response onto the typed error model. 5xx keep the
/// [`SdkError::ServerError`] shape so the shared transport classification
/// (`retry::is_transient`, `never_reached_server`) still applies to them.
fn service_error(status: StatusCode, body: &str, request_id: Option<&str>) -> ImageBuildError {
    let parsed: ImageServiceErrorBody = serde_json::from_str(body).unwrap_or_default();
    let message = parsed
        .error
        .clone()
        .or_else(|| parsed.message.clone())
        .map(|message| message.trim().to_string())
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| {
            let text = body.trim();
            if text.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("unexpected response")
                    .to_string()
            } else {
                text.to_string()
            }
        });
    match status {
        StatusCode::BAD_REQUEST => ImageBuildError::Usage(message),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ImageBuildError::Auth(message),
        StatusCode::CONFLICT => match parsed.code.as_deref() {
            Some("image_build_in_progress") => ImageBuildError::InProgress(ImageBuildConflict {
                image_name: parsed.image_name,
                active_build_id: parsed.active_build_id,
                active_kind: parsed.active_kind.map(Into::into),
                active_status: parsed.active_status.map(Into::into),
                message,
            }),
            Some("client_request_id_mismatch") => ImageBuildError::IdempotencyKeyReused {
                client_request_id: request_id.unwrap_or("-").to_string(),
                message,
            },
            _ => ImageBuildError::Service {
                status: status.as_u16(),
                message,
            },
        },
        status if status.is_server_error() => {
            ImageBuildError::Sdk(SdkError::ServerError { status, message })
        }
        status => ImageBuildError::Service {
            status: status.as_u16(),
            message,
        },
    }
}

/// Whether an attempt is worth repeating with the same request: transport
/// failures (including timeouts: every retried request here is idempotent or
/// keyed), 5xx, 408 and 429. Authorization, validation, not-found and
/// conflicts are answers, not outages.
fn is_retryable(error: &ImageBuildError) -> bool {
    match error {
        ImageBuildError::Sdk(error) => {
            error.transport_failure().is_some()
                || crate::retry::is_transient(error)
                || matches!(error, SdkError::ServerError { status, .. } if status.is_server_error())
                // A connection dropped before or during the response
                // (reset, incomplete message); every request here is
                // idempotent or keyed, so sending it again is safe.
                || error
                    .as_reqwest()
                    .is_some_and(|error| error.is_request() || error.is_body())
        }
        ImageBuildError::Service { status, .. } => {
            *status == StatusCode::REQUEST_TIMEOUT.as_u16()
                || *status == StatusCode::TOO_MANY_REQUESTS.as_u16()
        }
        _ => false,
    }
}

/// Wait before the `attempt`-th reconnect: `0.5 s × 2^(attempt-1)` capped
/// at [`MAX_RECONNECT_BACKOFF`] with ±25% jitter, or the service's
/// `Retry-After` (capped) when it sent one.
fn reconnect_delay(attempt: usize, retry_after: Option<Duration>) -> Duration {
    if let Some(retry_after) = retry_after {
        return retry_after.min(MAX_RETRY_AFTER);
    }
    let exponent = attempt.saturating_sub(1).min(8) as u32;
    let base = Duration::from_millis(500).saturating_mul(1 << exponent);
    let capped = base.min(MAX_RECONNECT_BACKOFF);
    capped.mul_f64(0.75 + rand::random::<f64>() * 0.5)
}

/// How a reconnecting request loop ended.
enum Attempted<T> {
    Done(T),
    /// A definitive answer that is not worth retrying.
    Failed(ImageBuildError),
    /// The interrupt signal fired; the last request, if any, was abandoned.
    Interrupted,
    /// The budget or deadline ran out while still failing transiently.
    Expired(ImageBuildError),
}

/// Repeat `attempt` across retryable failures until `deadline`, with
/// backoff, jitter and `Retry-After`, reporting each reconnect through
/// `emit`. An `interrupt` cuts the loop (and an in-flight request) short.
async fn with_reconnects<T, F, Fut>(
    label: &str,
    deadline: Instant,
    interrupt: Option<&InterruptSignal>,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
    mut attempt: F,
) -> Attempted<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, RequestFailure>>,
{
    let mut reconnects = 0usize;
    loop {
        if interrupt.is_some_and(InterruptSignal::is_triggered) {
            return Attempted::Interrupted;
        }
        let outcome = match interrupt {
            Some(signal) => tokio::select! {
                biased;
                _ = signal.triggered() => return Attempted::Interrupted,
                outcome = attempt() => outcome,
            },
            None => attempt().await,
        };
        let failure = match outcome {
            Ok(value) => return Attempted::Done(value),
            Err(failure) => failure,
        };
        if !is_retryable(&failure.error) {
            return Attempted::Failed(*failure.error);
        }
        reconnects += 1;
        let now = Instant::now();
        if now >= deadline {
            return Attempted::Expired(*failure.error);
        }
        let delay = reconnect_delay(reconnects, failure.retry_after).min(deadline - now);
        emit(SandboxImageBuildEvent::Status(format!(
            "{label} unavailable ({}); reconnecting in {:.1}s (attempt {reconnects})...",
            failure.error,
            delay.as_secs_f64()
        )));
        match interrupt {
            Some(signal) => tokio::select! {
                biased;
                _ = signal.triggered() => return Attempted::Interrupted,
                _ = tokio::time::sleep(delay) => {}
            },
            None => tokio::time::sleep(delay).await,
        }
    }
}

fn decode_build(body: &str) -> BuildResult<ImageBuildInfo> {
    let value: Value = serde_json::from_str(body)?;
    Ok(ImageBuildInfo::from_json(value)?)
}

/// An admission response: the build plus, for a dockerfile build whose
/// context is unsealed, the upload capability. The capability lives only
/// here, for the upload and seal step; [`ImageBuildInfo`] never carries it.
#[derive(Debug)]
pub(crate) struct AdmittedBuild {
    pub(crate) info: ImageBuildInfo,
    pub(crate) context_upload: Option<ContextUploadCapability>,
}

fn decode_admission(body: &str) -> BuildResult<AdmittedBuild> {
    let mut value: Value = serde_json::from_str(body)?;
    let context_upload = match value
        .as_object_mut()
        .and_then(|object| object.remove("context_upload"))
    {
        Some(Value::Null) | None => None,
        Some(capability) => Some(serde_json::from_value(capability)?),
    };
    Ok(AdmittedBuild {
        info: ImageBuildInfo::from_json(value)?,
        context_upload,
    })
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

/// Admit an operation (`POST /builds`, `POST /images/from-snapshot`) under
/// `request_id`, reconnecting across transient failures with the same key.
/// An interruption while the admission is unresolved stops the retries and
/// resolves the outcome through the cancel route's request-id selector; it
/// never replays the admission to discover an abandoned build.
pub(crate) async fn admit_build(
    client: &Client,
    path: &str,
    body: &Value,
    request_id: &str,
    interrupt: &InterruptSignal,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> BuildResult<AdmittedBuild> {
    emit(SandboxImageBuildEvent::Status(
        "Creating Image Service build...".to_string(),
    ));
    let deadline = Instant::now() + ADMISSION_RETRY_BUDGET;
    let attempted = with_reconnects("Image Service", deadline, Some(interrupt), emit, || async {
        let request = client
            .request(Method::POST, path)
            .header(IDEMPOTENCY_KEY_HEADER, request_id)
            .json(body)
            .build()
            .map_err(SdkError::Http)?;
        let response = send_raw(client, request).await?;
        match response.status {
            StatusCode::OK | StatusCode::CREATED | StatusCode::ACCEPTED => {
                decode_admission(&response.body).map_err(RequestFailure::from)
            }
            status => Err(RequestFailure::new(
                service_error(status, &response.body, Some(request_id)),
                response.retry_after,
            )),
        }
    })
    .await;
    let created = match attempted {
        Attempted::Done(created) => created,
        Attempted::Failed(error) | Attempted::Expired(error) => return Err(error),
        Attempted::Interrupted => {
            let report = interruption_for_unresolved_admission(client, request_id).await;
            emit(SandboxImageBuildEvent::Interrupted(report.clone()));
            return Err(ImageBuildError::Interrupted(report));
        }
    };
    emit(SandboxImageBuildEvent::Accepted {
        build_id: created.info.build_id.clone(),
        client_request_id: Some(request_id.to_string()),
    });
    emit(SandboxImageBuildEvent::Status(accepted_build_message(
        &created.info,
    )));
    Ok(created)
}

/// Admit a direct publication (`POST /images/from-snapshot`) under
/// `request_id`: 201 is the published image, `503 publication_pending` the
/// operation to keep retrying under the same key. Conflicts and validation
/// answers are typed like build admission; an interruption while unresolved
/// resolves through the request id. Every request is bounded by `wait`'s
/// absolute registration deadline: the per-request reconnect budget is
/// clamped to it and a request still in flight at the deadline is abandoned
/// and reported as the registration timeout.
pub(crate) async fn admit_direct(
    client: &Client,
    path: &str,
    body: &Value,
    request_id: &str,
    wait: &RegistrationWait,
    interrupt: &InterruptSignal,
) -> BuildResult<crate::sandbox_image_catalog::SnapshotAdoption> {
    use crate::sandbox_image_catalog::SnapshotAdoption;
    let deadline = (Instant::now() + ADMISSION_RETRY_BUDGET).min(wait.deadline);
    let mut silent = |_event| {};
    let attempted = with_reconnects(
        "Image Service",
        deadline,
        Some(interrupt),
        &mut silent,
        || async {
            let request = client
                .request(Method::POST, path)
                .header(IDEMPOTENCY_KEY_HEADER, request_id)
                .json(body)
                .build()
                .map_err(SdkError::Http)?;
            let remaining = wait.deadline.saturating_duration_since(Instant::now());
            let response = match tokio::time::timeout(remaining, send_raw(client, request)).await {
                Ok(response) => response?,
                // The registration deadline passed while the request was in
                // flight: abandon it. A 408 is retryable, so the reconnect
                // loop reports expiry rather than a definitive failure.
                Err(_elapsed) => {
                    return Err(RequestFailure::new(
                        ImageBuildError::Service {
                            status: StatusCode::REQUEST_TIMEOUT.as_u16(),
                            message: "the registration wait expired during the request".to_string(),
                        },
                        None,
                    ));
                }
            };
            match response.status {
                StatusCode::SERVICE_UNAVAILABLE => {
                    let parsed: ImageServiceErrorBody =
                        serde_json::from_str(&response.body).unwrap_or_default();
                    match (parsed.code.as_deref(), parsed.build_id) {
                        (Some("publication_pending"), Some(build_id)) => {
                            Ok(SnapshotAdoption::Pending { build_id })
                        }
                        _ => Err(RequestFailure::new(
                            service_error(response.status, &response.body, Some(request_id)),
                            response.retry_after,
                        )),
                    }
                }
                status if status.is_success() => {
                    let image: ImageServiceImage =
                        serde_json::from_str(&response.body).map_err(ImageBuildError::Json)?;
                    Ok(SnapshotAdoption::Published(Box::new(image)))
                }
                status => Err(RequestFailure::new(
                    service_error(status, &response.body, Some(request_id)),
                    response.retry_after,
                )),
            }
        },
    )
    .await;
    match attempted {
        Attempted::Done(adoption) => Ok(adoption),
        Attempted::Failed(error) => Err(error),
        Attempted::Expired(error) => {
            if Instant::now() >= wait.deadline {
                Err(wait.timeout(request_id))
            } else {
                Err(error)
            }
        }
        Attempted::Interrupted => Err(ImageBuildError::Interrupted(
            interruption_for_unresolved_admission(client, request_id).await,
        )),
    }
}

fn accepted_build_message(created: &ImageBuildInfo) -> String {
    format!("Build {} accepted", created.build_id)
}

/// Upper bound on a storage error body kept in an upload failure message.
const MAX_UPLOAD_ERROR_BODY: usize = 2048;

/// Remove every trace of the context-upload capability from `text`: the
/// URL itself, its query string, every `X-Amz-*` parameter (name and value,
/// wherever a storage error echoes them) and the signing material an S3
/// `SignatureDoesNotMatch` body carries. Bounded so a huge body cannot
/// become the message.
fn redact_capability(text: &str, capability_url: &str) -> String {
    let mut text = text.to_string();
    if !capability_url.is_empty() {
        text = text.replace(capability_url, "<capability-url>");
        if let Some((_, query)) = capability_url.split_once('?')
            && !query.is_empty()
        {
            text = text.replace(query, "<capability-query>");
        }
    }
    for element in [
        "StringToSign",
        "CanonicalRequest",
        "AWSAccessKeyId",
        "SignatureProvided",
    ] {
        text = redact_xml_element(&text, element);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_str();
    while let Some(index) = rest.find("X-Amz-") {
        out.push_str(&rest[..index]);
        let after = &rest[index..];
        let end = after
            .find(|c: char| {
                c == '&' || c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | ',' | '}')
            })
            .unwrap_or(after.len());
        out.push_str("<redacted>");
        rest = &after[end..];
    }
    out.push_str(rest);
    if out.len() > MAX_UPLOAD_ERROR_BODY {
        let mut cut = MAX_UPLOAD_ERROR_BODY;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
        out.push_str("...");
    }
    out
}

/// Replace the content of every `<element>...</element>` with a marker.
fn redact_xml_element(text: &str, element: &str) -> String {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(&open) {
        let content_start = start + open.len();
        match rest[content_start..].find(&close) {
            Some(length) => {
                out.push_str(&rest[..content_start]);
                out.push_str("<redacted>");
                rest = &rest[content_start + length..];
            }
            None => break,
        }
    }
    out.push_str(rest);
    out
}

// ---------------------------------------------------------------------------
// Lookup and cancellation
// ---------------------------------------------------------------------------

/// `GET /builds/{build_id}` with a short reconnect budget. 404 is
/// [`ImageBuildError::NotFound`] for unknown and foreign builds alike.
pub(crate) async fn get_build(client: &Client, build_id: &str) -> BuildResult<ImageBuildInfo> {
    let deadline = Instant::now() + LOOKUP_RETRY_BUDGET;
    let mut silent = |_event| {};
    match with_reconnects("Image Service", deadline, None, &mut silent, || {
        get_build_attempt(client, build_id)
    })
    .await
    {
        Attempted::Done(info) => Ok(info),
        Attempted::Failed(error) | Attempted::Expired(error) => Err(error),
        Attempted::Interrupted => Err(ImageBuildError::other("build lookup interrupted")),
    }
}

async fn get_build_attempt(
    client: &Client,
    build_id: &str,
) -> std::result::Result<ImageBuildInfo, RequestFailure> {
    let request = client
        .request(Method::GET, &format!("/builds/{build_id}"))
        .build()
        .map_err(SdkError::Http)?;
    let response = send_raw(client, request).await?;
    match response.status {
        status if status.is_success() => decode_build(&response.body).map_err(Into::into),
        StatusCode::NOT_FOUND => Err(ImageBuildError::NotFound {
            build_id: build_id.to_string(),
        }
        .into()),
        status => Err(RequestFailure::new(
            service_error(status, &response.body, None),
            response.retry_after,
        )),
    }
}

/// `POST /builds/cancel` with exactly one selector, repeated across
/// transient failures for `budget`. The cancel route never admits anything,
/// so repeating it is safe; a budget that runs out is reported as an error
/// the caller turns into "acknowledgement unknown".
pub(crate) async fn cancel_build(
    client: &Client,
    selector: &CancelBuildSelector,
    budget: Duration,
) -> BuildResult<CancelBuildResponse> {
    let deadline = Instant::now() + budget;
    let mut silent = |_event| {};
    let body = selector.body();
    let attempted = with_reconnects("Image Service", deadline, None, &mut silent, || async {
        let request = client
            .request(Method::POST, "/builds/cancel")
            .json(&body)
            .build()
            .map_err(SdkError::Http)?;
        let response = send_raw(client, request).await?;
        match response.status {
            status if status.is_success() => {
                let decoded: CancelBuildResponse =
                    serde_json::from_str(&response.body).map_err(ImageBuildError::Json)?;
                Ok(decoded)
            }
            StatusCode::NOT_FOUND => Err(match selector {
                CancelBuildSelector::BuildId(build_id) => ImageBuildError::NotFound {
                    build_id: build_id.clone(),
                },
                CancelBuildSelector::ClientRequestId(_) => {
                    service_error(response.status, &response.body, None)
                }
            }
            .into()),
            status => Err(RequestFailure::new(
                service_error(status, &response.body, None),
                response.retry_after,
            )),
        }
    })
    .await;
    match attempted {
        Attempted::Done(response) => Ok(response),
        Attempted::Failed(error) => Err(error),
        Attempted::Expired(error) => Err(ImageBuildError::other(format!(
            "no cancellation acknowledgement within {}s: {error}",
            budget.as_secs()
        ))),
        Attempted::Interrupted => Err(ImageBuildError::other("cancellation interrupted")),
    }
}

fn interruption_outcome(response: CancelBuildResponse) -> InterruptionOutcome {
    match response.outcome {
        CancelBuildOutcome::Cancelled => InterruptionOutcome::Cancelled,
        CancelBuildOutcome::AlreadyCancelled => InterruptionOutcome::AlreadyCancelled,
        CancelBuildOutcome::AlreadyCompleted => InterruptionOutcome::AlreadyCompleted(
            response.build.and_then(|build| build.result).map(Box::new),
        ),
        CancelBuildOutcome::AlreadyFailed => {
            InterruptionOutcome::AlreadyFailed(response.build.and_then(|build| build.error))
        }
        CancelBuildOutcome::NotAdmitted => InterruptionOutcome::NotAdmitted,
        CancelBuildOutcome::Unknown(other) => {
            InterruptionOutcome::Unconfirmed(format!("unknown cancel outcome {other:?}"))
        }
    }
}

/// The handshake for an admitted build: cancel by build id within the
/// budget and report what the service observed.
pub(crate) async fn interruption_for_build(
    client: &Client,
    build_id: &str,
    client_request_id: Option<&str>,
) -> InterruptionReport {
    let outcome = match cancel_build(
        client,
        &CancelBuildSelector::BuildId(build_id.to_string()),
        CANCEL_HANDSHAKE_BUDGET,
    )
    .await
    {
        Ok(response) => interruption_outcome(response),
        Err(error) => InterruptionOutcome::Unconfirmed(error.to_string()),
    };
    InterruptionReport {
        build_id: Some(build_id.to_string()),
        client_request_id: client_request_id.map(str::to_string),
        outcome,
    }
}

/// The handshake while the admission's answer is unknown: resolve through
/// the request-id index. `not_admitted` is reported as such, with the
/// request id, and is not a confirmation.
async fn interruption_for_unresolved_admission(
    client: &Client,
    request_id: &str,
) -> InterruptionReport {
    match cancel_build(
        client,
        &CancelBuildSelector::ClientRequestId(request_id.to_string()),
        CANCEL_HANDSHAKE_BUDGET,
    )
    .await
    {
        Ok(response) => InterruptionReport {
            build_id: response.build.as_ref().map(|build| build.build_id.clone()),
            client_request_id: Some(request_id.to_string()),
            outcome: interruption_outcome(response),
        },
        Err(error) => InterruptionReport {
            build_id: None,
            client_request_id: Some(request_id.to_string()),
            outcome: InterruptionOutcome::Unconfirmed(error.to_string()),
        },
    }
}

/// Create the plain (uncompressed) context tar, honoring `.dockerignore`,
/// upload it through the sealed capability, and seal the build with the
/// tar's sha256. The capability URL is presigned (or the service's direct
/// dev route); the upload deliberately uses a bare HTTP client so no
/// Authorization header corrupts a presigned request. The seal is keyed by
/// the digest, so it is replayed across transient failures.
async fn upload_and_seal_context(
    client: &Client,
    build_id: &str,
    upload: &ContextUploadCapability,
    plan: &DockerfileBuildPlan,
    injected_dockerfile: Option<&str>,
    context_files: &[SandboxImageContextFile],
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> BuildResult<()> {
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

    if let Some(max_bytes) = upload.max_bytes
        && tar_bytes > max_bytes
    {
        return Err(ImageBuildError::usage(format!(
            "build context is {tar_bytes} bytes, above the Image Service cap of {max_bytes}"
        )));
    }

    // The capability is always a PUT; the service no longer spells it out.
    let method = Method::PUT;

    emit(SandboxImageBuildEvent::Status(format!(
        "Uploading build context ({tar_bytes} bytes)..."
    )));
    let file = tokio::fs::File::open(tar_file.path()).await?;
    let stream = tokio_util::io::ReaderStream::new(file);
    // The capability URL is a signed credential (presigned `X-Amz-*` query
    // parameters): no error leaving this function may carry it. A transport
    // error drops its URL and a storage error body is redacted before either
    // becomes a message a CLI, a binding or a log can print.
    let response = crate::http_transport::https_builder()
        .build()
        .map_err(SdkError::Http)?
        .request(method, &upload.url)
        .header(CONTENT_LENGTH, tar_bytes)
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await
        .map_err(|error| {
            ImageBuildError::other(format!(
                "context upload failed: {}",
                redact_capability(&error.without_url().to_string(), &upload.url)
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(ImageBuildError::other(format!(
            "context upload failed (HTTP {status}): {}",
            redact_capability(&body, &upload.url)
        )));
    }

    emit(SandboxImageBuildEvent::Status(
        "Sealing build context...".to_string(),
    ));
    let deadline = Instant::now() + ADMISSION_RETRY_BUDGET;
    let seal_body = json!({ "digest": digest });
    let path = format!("/builds/{build_id}/context/seal");
    let attempted = with_reconnects("Image Service", deadline, None, emit, || async {
        let request = client
            .request(Method::POST, &path)
            .json(&seal_body)
            .build()
            .map_err(SdkError::Http)?;
        let response = send_raw(client, request).await?;
        if response.status.is_success() {
            return Ok(());
        }
        let error = match service_error(response.status, &response.body, None) {
            ImageBuildError::Service { status, message } => ImageBuildError::Service {
                status,
                message: format!("context seal failed: {message}"),
            },
            other => other,
        };
        Err(RequestFailure::new(error, response.retry_after))
    })
    .await;
    match attempted {
        Attempted::Done(()) => Ok(()),
        Attempted::Failed(error) | Attempted::Expired(error) => Err(error),
        Attempted::Interrupted => Err(ImageBuildError::other("context seal interrupted")),
    }
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
        // The walker never follows symlinks, and the files are re-opened
        // without following them either (see `open_context_file`).
        let collected = collect_dir_files(context_dir, context_dir)?;
        for (full_path, relative_path) in collected.files {
            if injected_dockerfile.is_some() && relative_path == INJECTED_DOCKERFILE_PATH {
                return Err(SandboxImageBuildError::usage(format!(
                    "the build context already contains {INJECTED_DOCKERFILE_PATH}, which is \
                     reserved for the injected Dockerfile"
                )));
            }
            let mut file = open_context_file(&full_path)?;
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

// ---------------------------------------------------------------------------
// Waiting
// ---------------------------------------------------------------------------

/// Poll `build_id` to a terminal status and return that terminal record.
/// `initial` is the last known state (the admission response), used to seed
/// the deadline. See the `image_builds` module docs for the policy.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn wait_for_terminal<F>(
    client: &Client,
    ctx: Option<&ResolvedBuildContext>,
    build_id: &str,
    client_request_id: Option<&str>,
    initial: Option<ImageBuildInfo>,
    options: &ImageBuildWaitOptions,
    interrupt: &InterruptSignal,
    emit: F,
) -> BuildResult<ImageBuildInfo>
where
    F: FnMut(SandboxImageBuildEvent),
{
    wait_for_terminal_with_follower(
        client,
        ctx,
        build_id,
        client_request_id,
        initial,
        options,
        interrupt,
        emit,
        BuildWaitTiming {
            poll_interval: BUILD_POLL_INTERVAL,
            log_drain_grace: BUILDER_LOG_DRAIN_GRACE,
        },
        |ctx, sandbox_id, sink| async move {
            let mut emit = |event| sink.try_emit(event);
            follow_builder_logs(&ctx, &sandbox_id, &mut emit).await
        },
    )
    .await
}

/// The result of a terminal build: the immutable publication record, or the
/// typed failure/cancellation outcome.
pub(crate) fn terminal_result(info: &ImageBuildInfo) -> BuildResult<ImageBuildResult> {
    match &info.status {
        ImageBuildStatus::Succeeded => info.result.clone().ok_or_else(|| {
            ImageBuildError::other(format!(
                "build {} succeeded but reports no result",
                info.build_id
            ))
        }),
        ImageBuildStatus::Failed => Err(ImageBuildError::Failed {
            build_id: info.build_id.clone(),
            error: info
                .error
                .clone()
                .unwrap_or_else(|| "no error recorded".to_string()),
            info: Box::new(info.clone()),
        }),
        ImageBuildStatus::Cancelled => Err(ImageBuildError::Cancelled {
            build_id: info.build_id.clone(),
            info: Box::new(info.clone()),
        }),
        other => Err(ImageBuildError::other(format!(
            "build {} ended in unexpected status {other}",
            info.build_id
        ))),
    }
}

#[derive(Clone, Copy)]
struct BuildWaitTiming {
    poll_interval: Duration,
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

/// The instant the wait gives up: the service-informed operation deadline
/// plus the recovery allowance and/or the caller's timeout, whichever comes
/// first; [`DEFAULT_WAIT_LIMIT`] when neither is known. Recomputed from each
/// observation so a service that extends the deadline (a new attempt) is
/// honored.
fn wait_deadline(
    info: Option<&ImageBuildInfo>,
    options: &ImageBuildWaitOptions,
    started: Instant,
) -> Instant {
    let caller = options.timeout.map(|timeout| started + timeout);
    let server = info
        .and_then(ImageBuildInfo::operation_deadline)
        .map(|deadline| {
            let remaining = deadline
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO);
            Instant::now() + remaining
        });
    match (server, caller) {
        (Some(server), Some(caller)) => server.min(caller),
        (Some(server), None) => server,
        (None, Some(caller)) => caller,
        (None, None) => started + DEFAULT_WAIT_LIMIT,
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_terminal_with_follower<F, Follow, FollowFuture>(
    client: &Client,
    ctx: Option<&ResolvedBuildContext>,
    build_id: &str,
    client_request_id: Option<&str>,
    initial: Option<ImageBuildInfo>,
    options: &ImageBuildWaitOptions,
    interrupt: &InterruptSignal,
    mut emit: F,
    timing: BuildWaitTiming,
    follow_builder: Follow,
) -> BuildResult<ImageBuildInfo>
where
    F: FnMut(SandboxImageBuildEvent),
    Follow:
        Fn(ResolvedBuildContext, String, BuilderLogSink) -> FollowFuture + Clone + Send + 'static,
    FollowFuture: Future<Output = Result<()>> + Send + 'static,
{
    let started = Instant::now();
    // A record already known to be terminal needs no poll: report it.
    if let Some(info) = initial.as_ref().filter(|info| info.status.is_terminal()) {
        emit(SandboxImageBuildEvent::Progress {
            build_id: info.build_id.clone(),
            status: info.status.clone(),
            phase: info.phase.clone(),
            attempt: info.attempt_no,
        });
        emit(SandboxImageBuildEvent::Status(progress_message(info)));
        emit(SandboxImageBuildEvent::Terminal {
            build_id: info.build_id.clone(),
            status: info.status.clone(),
            phase: info.phase.clone(),
            attempt: info.attempt_no,
        });
        return Ok(info.clone());
    }
    let mut deadline = wait_deadline(initial.as_ref(), options, started);
    let mut last_progress: Option<(ImageBuildStatus, Option<ImageBuildPhase>, u64)> = None;
    let mut observed_builders = HashSet::new();
    let (log_sender, mut log_receiver) = tokio::sync::mpsc::channel(BUILDER_LOG_CHANNEL_CAPACITY);
    let dropped_logs = Arc::new(AtomicUsize::new(0));
    let mut followers = tokio::task::JoinSet::new();

    /// Stop following logs, run the cancellation handshake and report it.
    async fn interrupted<F: FnMut(SandboxImageBuildEvent)>(
        client: &Client,
        build_id: &str,
        client_request_id: Option<&str>,
        followers: &mut tokio::task::JoinSet<()>,
        receiver: &mut tokio::sync::mpsc::Receiver<SandboxImageBuildEvent>,
        dropped: &AtomicUsize,
        emit: &mut F,
    ) -> ImageBuildError {
        finish_builder_log_followers(followers, receiver, dropped, Duration::ZERO, emit).await;
        let report = interruption_for_build(client, build_id, client_request_id).await;
        emit(SandboxImageBuildEvent::Interrupted(report.clone()));
        ImageBuildError::Interrupted(report)
    }

    loop {
        drain_ready_builder_logs(&mut log_receiver, &dropped_logs, &mut emit);
        let attempted = with_reconnects(
            "Image Service",
            deadline,
            Some(interrupt),
            &mut emit,
            || get_build_attempt(client, build_id),
        )
        .await;
        let info = match attempted {
            Attempted::Done(info) => info,
            Attempted::Failed(error) => {
                finish_builder_log_followers(
                    &mut followers,
                    &mut log_receiver,
                    &dropped_logs,
                    Duration::ZERO,
                    &mut emit,
                )
                .await;
                return Err(error);
            }
            Attempted::Expired(_) => {
                finish_builder_log_followers(
                    &mut followers,
                    &mut log_receiver,
                    &dropped_logs,
                    Duration::ZERO,
                    &mut emit,
                )
                .await;
                return Err(ImageBuildError::Timeout {
                    build_id: build_id.to_string(),
                    waited: started.elapsed(),
                });
            }
            Attempted::Interrupted => {
                return Err(interrupted(
                    client,
                    build_id,
                    client_request_id,
                    &mut followers,
                    &mut log_receiver,
                    &dropped_logs,
                    &mut emit,
                )
                .await);
            }
        };
        deadline = wait_deadline(Some(&info), options, started);

        let progress = (info.status.clone(), info.phase.clone(), info.attempt_no);
        if last_progress.as_ref() != Some(&progress) {
            emit(SandboxImageBuildEvent::Progress {
                build_id: info.build_id.clone(),
                status: info.status.clone(),
                phase: info.phase.clone(),
                attempt: info.attempt_no,
            });
            emit(SandboxImageBuildEvent::Status(progress_message(&info)));
            last_progress = Some(progress);
        }

        if info.status.is_terminal() {
            finish_builder_log_followers(
                &mut followers,
                &mut log_receiver,
                &dropped_logs,
                timing.log_drain_grace,
                &mut emit,
            )
            .await;
            emit(SandboxImageBuildEvent::Terminal {
                build_id: info.build_id.clone(),
                status: info.status.clone(),
                phase: info.phase.clone(),
                attempt: info.attempt_no,
            });
            return Ok(info);
        }

        if options.follow_logs
            && let Some(ctx) = ctx
            && let Some(builder_sandbox_id) = take_unobserved_builder(&info, &mut observed_builders)
        {
            let attempt = info.attempt_no;
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
                if let Err(error) = follower(follower_ctx, follower_sandbox_id, sink).await {
                    let _ = warning_sender
                        .send(SandboxImageBuildEvent::Warning(format!(
                            "Could not follow builder logs for attempt {attempt} ({builder_sandbox_id}): {error}"
                        )))
                        .await;
                }
            });
        }

        let now = Instant::now();
        if now >= deadline {
            finish_builder_log_followers(
                &mut followers,
                &mut log_receiver,
                &dropped_logs,
                Duration::ZERO,
                &mut emit,
            )
            .await;
            return Err(ImageBuildError::Timeout {
                build_id: build_id.to_string(),
                waited: started.elapsed(),
            });
        }
        let next_poll = (now + timing.poll_interval).min(deadline);
        if !stream_builder_logs_until_next_poll(
            &mut log_receiver,
            &dropped_logs,
            next_poll,
            interrupt,
            &mut emit,
        )
        .await
        {
            return Err(interrupted(
                client,
                build_id,
                client_request_id,
                &mut followers,
                &mut log_receiver,
                &dropped_logs,
                &mut emit,
            )
            .await);
        }
    }
}

fn progress_message(info: &ImageBuildInfo) -> String {
    match &info.phase {
        Some(phase) => format!(
            "Build {}: {} (attempt {}, phase {phase})",
            info.build_id, info.status, info.attempt_no
        ),
        None => format!(
            "Build {}: {} (attempt {})",
            info.build_id, info.status, info.attempt_no
        ),
    }
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

/// Stream buffered builder logs until `next_poll`. Returns `false` when the
/// interrupt fired first.
async fn stream_builder_logs_until_next_poll(
    receiver: &mut tokio::sync::mpsc::Receiver<SandboxImageBuildEvent>,
    dropped: &AtomicUsize,
    next_poll: Instant,
    interrupt: &InterruptSignal,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> bool {
    let interrupted = loop {
        tokio::select! {
            biased;
            _ = interrupt.triggered() => break true,
            event = receiver.recv() => {
                if let Some(event) = event {
                    emit(event);
                }
            }
            _ = tokio::time::sleep_until(next_poll) => break false,
        }
    };
    emit_dropped_builder_logs(dropped, emit);
    !interrupted
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

fn take_unobserved_builder(
    build: &ImageBuildInfo,
    observed: &mut HashSet<(u64, String)>,
) -> Option<String> {
    if build.status != ImageBuildStatus::Running {
        return None;
    }
    let sandbox_id = build.builder_sandbox_id.clone()?;
    observed
        .insert((build.attempt_no, sandbox_id.clone()))
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
    use crate::{
        ClientBuilder,
        image_builds::ImageBuildKind,
        sandbox_image_catalog::{ImageServiceCatalog, RegistrationWaitPolicy},
    };

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
    fn accepted_build_message_names_the_build_only() {
        // The service no longer echoes the resolved builder resources.
        assert_eq!(
            accepted_build_message(&info_from(serde_json::json!({
                "build_id": "build-1",
                "builder_resources": {"cpus": 2.5, "memory_mb": 6144},
            }))),
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

        // Docker compatibility mode means nothing for a BuildKit-only path
        // and is accepted silently (ADR 0107 D29).
        options.docker_compat = true;
        warn_ignored_options(&options, &mut |event| events.push(event));
        assert!(events.is_empty());

        options.builder_disk_mb = Some(20_480);
        warn_ignored_options(&options, &mut |event| events.push(event));
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            SandboxImageBuildEvent::Warning(message)
                if message.contains("builder_disk_mb is ignored for CAS builds")
        ));
    }

    #[test]
    fn each_running_attempt_builder_is_observed_once() {
        let mut observed = HashSet::new();
        let pending = info_from(build_json("pending", 1, Some("stale-builder"), None));
        assert_eq!(take_unobserved_builder(&pending, &mut observed), None);

        let pre_create = info_from(build_json("running", 1, None, None));
        assert_eq!(take_unobserved_builder(&pre_create, &mut observed), None);

        let attempt_one = info_from(build_json("running", 1, Some("builder-1"), None));
        assert_eq!(
            take_unobserved_builder(&attempt_one, &mut observed).as_deref(),
            Some("builder-1")
        );
        assert_eq!(take_unobserved_builder(&attempt_one, &mut observed), None);

        let attempt_two = info_from(build_json("running", 2, Some("builder-2"), None));
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
    fn direct_pins_accept_image_ids_and_refuse_digests() {
        assert_eq!(
            direct_pin_public_id("image_abc123").unwrap(),
            Some("image_abc123".to_string())
        );
        assert_eq!(direct_pin_public_id("alpine:3.20").unwrap(), None);
        assert_eq!(direct_pin_public_id("team/base").unwrap(), None);
        let hex = "a".repeat(64);
        let error = direct_pin_public_id(&format!("cas-v1:{hex}"))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("pin the image by name or its image_ id"),
            "{error}"
        );
        assert!(direct_pin_public_id("image_NOT-AN-ID").is_err());
    }

    #[test]
    fn build_args_expand_from_references_before_resolution() {
        let mut plan = plan_with_context(std::env::temp_dir());
        plan.base_image = "python:${PY_TAG}".to_string();
        plan.global_build_args = vec![
            ("PY_TAG".to_string(), Some("3.12-slim".to_string())),
            ("TOOLS".to_string(), None),
        ];
        plan.unresolvable_image_references = vec![
            crate::sandbox_images::UnresolvableImageReference {
                line_number: 2,
                reference: "python:${PY_TAG}".to_string(),
                reason: UnresolvableImageReferenceReason::BuildArgExpansion,
            },
            crate::sandbox_images::UnresolvableImageReference {
                line_number: 3,
                reference: "$TOOLS".to_string(),
                reason: UnresolvableImageReferenceReason::BuildArgExpansion,
            },
            crate::sandbox_images::UnresolvableImageReference {
                line_number: 4,
                reference: "team/${SIDE:-base}:v1".to_string(),
                reason: UnresolvableImageReferenceReason::BuildArgExpansion,
            },
            crate::sandbox_images::UnresolvableImageReference {
                line_number: 5,
                reference: "pinned@sha256:abc".to_string(),
                reason: UnresolvableImageReferenceReason::DigestPin,
            },
        ];

        // Defaults alone: the base and the defaulted side reference expand,
        // the ARG without a default stays unresolvable, the digest pin stays.
        let expanded = expand_build_arg_references(plan.clone(), &[]);
        assert_eq!(expanded.base_image, "python:3.12-slim");
        assert_eq!(
            expanded.additional_image_references,
            vec!["team/base:v1".to_string()]
        );
        let remaining: Vec<&str> = expanded
            .unresolvable_image_references
            .iter()
            .map(|unresolvable| unresolvable.reference.as_str())
            .collect();
        assert_eq!(remaining, vec!["$TOOLS", "pinned@sha256:abc"]);

        // Supplied build args win over the Dockerfile defaults.
        let expanded = expand_build_arg_references(
            plan,
            &[
                ("PY_TAG".to_string(), "3.13".to_string()),
                ("TOOLS".to_string(), "team/tools".to_string()),
            ],
        );
        assert_eq!(expanded.base_image, "python:3.13");
        assert_eq!(
            expanded.additional_image_references,
            vec!["team/tools".to_string(), "team/base:v1".to_string()]
        );
        assert_eq!(expanded.unresolvable_image_references.len(), 1);
    }

    #[tokio::test]
    async fn parents_pin_by_public_id_and_refuse_legacy_templates() {
        // The Image Service knows `team/base`; `old-base` is unknown there
        // but registered as a legacy platform template.
        let (image_service_url, image_service) = scripted_image_service_with_status(vec![
            (
                200,
                published_image_record("image_base1", "team/base", &"c".repeat(64)),
            ),
            (404, json!({"error": "name \"old-base\" not found"})),
        ])
        .await;
        let (platform_url, platform) = scripted_image_service_with_status(vec![(
            200,
            json!({"id": "sandbox_template_1", "name": "old-base", "snapshotId": "snap-1"}),
        )])
        .await;
        let client = ClientBuilder::new(&image_service_url).build().unwrap();
        let templates = SandboxTemplatesClient::new_api_key_scoped(
            ClientBuilder::new(&platform_url).build().unwrap(),
        );
        let mut plan = plan_with_context(std::env::temp_dir());
        plan.base_image = "team/base".to_string();
        plan.additional_image_references = vec!["old-base".to_string()];
        let mut events = Vec::new();
        let error = resolve_parents(&client, &templates, &plan, &mut |event| events.push(event))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("'old-base' is a legacy image"), "{error}");
        assert!(
            error.contains("rebuild 'old-base' with --cas first"),
            "{error}"
        );
        assert!(events.contains(&SandboxImageBuildEvent::Status(
            "Resolved 'team/base' to registered image image_base1".to_string()
        )));
        image_service.await.unwrap();
        platform.await.unwrap();

        // A registry reference nobody registered pins nothing and pulls; an
        // `image_` reference is pinned directly, with no catalog lookup.
        let (image_service_url, image_service) =
            scripted_image_service_with_status(vec![(404, json!({"error": "not found"}))]).await;
        let (platform_url, platform) =
            scripted_image_service_with_status(vec![(404, json!({"message": "not found"}))]).await;
        let client = ClientBuilder::new(&image_service_url).build().unwrap();
        let templates = SandboxTemplatesClient::new_api_key_scoped(
            ClientBuilder::new(&platform_url).build().unwrap(),
        );
        let mut plan = plan_with_context(std::env::temp_dir());
        plan.base_image = "alpine:3.20".to_string();
        plan.additional_image_references = vec!["image_direct".to_string()];
        let parents = resolve_parents(&client, &templates, &plan, &mut |_| {})
            .await
            .unwrap();
        assert_eq!(
            parents,
            vec![json!({"reference": "image_direct", "public_id": "image_direct"})]
        );
        image_service.await.unwrap();
        platform.await.unwrap();
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
            build_json("running", 1, Some("builder-1"), None),
            build_json(
                "succeeded",
                1,
                None,
                Some(published_image_record("image_abc123", "image-1", &image_id)),
            ),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let mut events = Vec::new();

        let info = wait_for_terminal_with_follower(
            &client,
            Some(&ctx),
            "build-1",
            None,
            None,
            &follow_logs_options(),
            &InterruptSignal::new(),
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

        // The result is the build's own immutable record; the template shape
        // is derived from it (ADR 0107 D18/D28), never re-read.
        let result = terminal_result(&info).unwrap();
        assert_eq!(result.id.as_deref(), Some("image_abc123"));
        assert_eq!(result.name.as_deref(), Some("image-1"));
        let image = serde_json::to_value(result).unwrap();
        assert_eq!(image["snapshotId"], "snap123");
        assert_eq!(
            image["snapshotFormatVersion"],
            "content_addressed_streaming_v1"
        );
        assert!(
            image.get("imageId").is_none(),
            "no digest extra on the derived record: {image}"
        );
        assert!(events.contains(&SandboxImageBuildEvent::BuildLog {
            stream: "stdout".to_string(),
            message: "still building".to_string(),
        }));
        assert!(events.contains(&SandboxImageBuildEvent::Progress {
            build_id: "build-1".to_string(),
            status: ImageBuildStatus::Running,
            phase: None,
            attempt: 1,
        }));
        assert!(events.contains(&SandboxImageBuildEvent::Terminal {
            build_id: "build-1".to_string(),
            status: ImageBuildStatus::Succeeded,
            phase: None,
            attempt: 1,
        }));
        assert!(events.contains(&SandboxImageBuildEvent::Status(
            "Build build-1: succeeded (attempt 1)".to_string()
        )));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_attach_once_warn_once_and_bound_buffered_logs() {
        let image_id = "b".repeat(64);
        let (base_url, server) = scripted_image_service(vec![
            build_json("running", 1, Some("builder-1"), None),
            build_json("running", 1, Some("builder-1"), None),
            build_json("pending", 1, Some("stale-builder"), None),
            build_json("running", 2, Some("builder-2"), None),
            build_json(
                "succeeded",
                2,
                None,
                Some(published_image_record("image_def456", "image-1", &image_id)),
            ),
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

        let info = wait_for_terminal_with_follower(
            &client,
            Some(&ctx),
            "build-1",
            None,
            None,
            &follow_logs_options(),
            &InterruptSignal::new(),
            |event| events.push(event),
            test_build_wait_timing(),
            follow,
        )
        .await
        .unwrap();

        let result = terminal_result(&info).unwrap();
        assert_eq!(result.id.as_deref(), Some("image_def456"));
        assert_eq!(
            result.snapshot_format_version.as_deref(),
            Some("content_addressed_streaming_v1"),
            "the template shape is derived from the trimmed record"
        );
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

    /// Failure and cancellation are distinct typed outcomes, and a service
    /// that reports neither a result nor a terminal status is an error.
    #[test]
    fn terminal_results_are_typed() {
        let failed = info_from(build_json("failed", 2, None, None));
        assert!(matches!(
            terminal_result(&failed),
            Err(ImageBuildError::Failed { build_id, error, .. })
                if build_id == "build-1" && error == "no error recorded"
        ));
        let cancelled = info_from(build_json("cancelled", 1, None, None));
        assert!(matches!(
            terminal_result(&cancelled),
            Err(ImageBuildError::Cancelled { build_id, .. }) if build_id == "build-1"
        ));
        let no_result = info_from(build_json("succeeded", 1, None, None));
        assert!(
            terminal_result(&no_result)
                .unwrap_err()
                .to_string()
                .contains("reports no result")
        );
    }

    /// The same request is replayed under the same `Idempotency-Key` across
    /// a transient failure; the key is never re-minted.
    #[tokio::test]
    async fn admission_keeps_one_idempotency_key_across_transient_retries() {
        let (base_url, server) = recording_image_service(vec![
            Reply::Status(503, json!({"error": "deploying"})),
            Reply::Reset,
            Reply::Status(201, build_json("pending", 0, None, None)),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let request_id = new_client_request_id();
        let mut events = Vec::new();
        let created = admit_build(
            &client,
            "/builds",
            &json!({"kind": "import", "image_ref": "alpine:3.20", "name": "img"}),
            &request_id,
            &InterruptSignal::new(),
            &mut |event| events.push(event),
        )
        .await
        .unwrap();
        assert_eq!(created.info.build_id, "build-1");
        let requests = server.finish().await;
        assert_eq!(requests.len(), 3);
        for request in &requests {
            assert_eq!(request.line, "POST /builds HTTP/1.1");
            assert_eq!(request.header("idempotency-key"), Some(request_id.as_str()));
            assert_eq!(request.json()["image_ref"], "alpine:3.20");
        }
        assert!(events.contains(&SandboxImageBuildEvent::Accepted {
            build_id: "build-1".to_string(),
            client_request_id: Some(request_id.clone()),
        }));
        assert!(events.iter().any(|event| matches!(
            event,
            SandboxImageBuildEvent::Status(message) if message.contains("reconnecting in")
        )));
    }

    #[tokio::test]
    async fn admission_conflicts_are_typed() {
        let (base_url, server) = recording_image_service(vec![
            Reply::Status(
                409,
                json!({
                    "error": "Image 'img' already has an unfinished build 'build-9'. Cancel it before starting another.",
                    "code": "image_build_in_progress",
                    "image_name": "img",
                    "active_build_id": "build-9",
                    "active_kind": "dockerfile",
                    "active_status": "running",
                }),
            ),
            Reply::Status(
                409,
                json!({"error": "key reused", "code": "client_request_id_mismatch"}),
            ),
            Reply::Status(400, json!({"error": "Idempotency-Key header is required"})),
            Reply::Status(403, json!({"error": "forbidden"})),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let body = json!({"kind": "import", "image_ref": "alpine:3.20", "name": "img"});
        let mut emit = |_event| {};
        let conflict = admit_build(
            &client,
            "/builds",
            &body,
            "rid-1",
            &InterruptSignal::new(),
            &mut emit,
        )
        .await
        .unwrap_err();
        let ImageBuildError::InProgress(conflict) = conflict else {
            panic!("expected a conflict, got {conflict}");
        };
        assert_eq!(conflict.image_name.as_deref(), Some("img"));
        assert_eq!(conflict.active_build_id.as_deref(), Some("build-9"));
        assert_eq!(conflict.active_kind, Some(ImageBuildKind::Dockerfile));
        assert_eq!(conflict.active_status, Some(ImageBuildStatus::Running));
        assert!(
            conflict
                .message
                .contains("Cancel it before starting another")
        );

        let reused = admit_build(
            &client,
            "/builds",
            &body,
            "rid-1",
            &InterruptSignal::new(),
            &mut emit,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            reused,
            ImageBuildError::IdempotencyKeyReused { client_request_id, .. }
                if client_request_id == "rid-1"
        ));
        let usage = admit_build(
            &client,
            "/builds",
            &body,
            "rid-1",
            &InterruptSignal::new(),
            &mut emit,
        )
        .await
        .unwrap_err();
        assert!(matches!(usage, ImageBuildError::Usage(message) if message.contains("required")));
        let auth = admit_build(
            &client,
            "/builds",
            &body,
            "rid-1",
            &InterruptSignal::new(),
            &mut emit,
        )
        .await
        .unwrap_err();
        assert!(matches!(auth, ImageBuildError::Auth(message) if message == "forbidden"));
        // Answers are not outages: one request each, no retries.
        assert_eq!(server.finish().await.len(), 4);
    }

    #[tokio::test]
    async fn cancel_outcomes_are_typed_including_not_admitted() {
        let image_id = "c".repeat(64);
        let (base_url, server) = recording_image_service(vec![
            Reply::Status(
                200,
                json!({"outcome": "cancelled", "build": build_json("cancelled", 1, None, None)}),
            ),
            Reply::Status(
                200,
                json!({
                    "outcome": "already_completed",
                    "build": build_json(
                        "succeeded",
                        1,
                        None,
                        Some(published_image_record("image_won", "img", &image_id)),
                    ),
                }),
            ),
            Reply::Status(200, json!({"outcome": "not_admitted", "build": null})),
            Reply::Status(404, json!({"error": "build 'build-x' not found"})),
            Reply::Status(400, json!({"error": "exactly one selector"})),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let by_id = CancelBuildSelector::BuildId("build-1".to_string());

        let cancelled = cancel_build(&client, &by_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(cancelled.outcome, CancelBuildOutcome::Cancelled);
        assert_eq!(cancelled.build.unwrap().status, ImageBuildStatus::Cancelled);

        let completed = cancel_build(&client, &by_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(completed.outcome, CancelBuildOutcome::AlreadyCompleted);
        assert_eq!(
            completed.build.unwrap().result.unwrap().id.as_deref(),
            Some("image_won")
        );

        let not_admitted = cancel_build(
            &client,
            &CancelBuildSelector::ClientRequestId("rid-lost".to_string()),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(not_admitted.outcome, CancelBuildOutcome::NotAdmitted);
        assert!(not_admitted.build.is_none());
        // The report distinguishes it from a confirmation.
        let report = InterruptionReport {
            build_id: None,
            client_request_id: Some("rid-lost".to_string()),
            outcome: interruption_outcome(not_admitted),
        };
        assert!(!report.is_confirmed_cancelled());

        let missing = cancel_build(
            &client,
            &CancelBuildSelector::BuildId("build-x".to_string()),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert!(matches!(missing, ImageBuildError::NotFound { build_id } if build_id == "build-x"));
        let usage = cancel_build(&client, &by_id, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(usage, ImageBuildError::Usage(_)));

        let requests = server.finish().await;
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0].line, "POST /builds/cancel HTTP/1.1");
        assert_eq!(requests[0].json(), json!({"build_id": "build-1"}));
        assert_eq!(requests[2].json(), json!({"client_request_id": "rid-lost"}));
        assert!(requests[0].header("idempotency-key").is_none());
    }

    /// A caller timeout reports the build id and leaves the build alone: no
    /// cancellation, no deletion.
    #[tokio::test]
    async fn caller_timeout_carries_the_build_id_and_sends_no_cancel() {
        let replies =
            std::iter::repeat_n(Reply::Status(200, build_json("running", 1, None, None)), 40)
                .collect();
        let (base_url, server) = recording_image_service(replies).await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let options = ImageBuildWaitOptions {
            timeout: Some(Duration::from_millis(60)),
            follow_logs: false,
        };
        let error = wait_for_terminal_with_follower(
            &client,
            Some(&ctx),
            "build-1",
            None,
            None,
            &options,
            &InterruptSignal::new(),
            |_event| {},
            test_build_wait_timing(),
            no_follower,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            &error,
            ImageBuildError::Timeout { build_id, .. } if build_id == "build-1"
        ));
        assert!(error.to_string().contains("may still be running"));
        drop(client);
        let requests = server.finish().await;
        assert!(!requests.is_empty());
        assert!(
            requests
                .iter()
                .all(|request| request.line == "GET /builds/build-1 HTTP/1.1")
        );
    }

    /// The service-informed deadline bounds the wait when the caller sets no
    /// timeout; an already expired deadline (plus allowance) ends it at once.
    #[tokio::test]
    async fn server_deadline_bounds_the_wait() {
        let twenty_minutes_ago = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 20 * 60 * 1_000;
        let mut expired = build_json("running", 1, None, None);
        expired["operation_deadline_ms"] = json!(twenty_minutes_ago);
        let replies = std::iter::repeat_n(Reply::Status(200, expired), 4).collect();
        let (base_url, server) = recording_image_service(replies).await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let error = wait_for_terminal_with_follower(
            &client,
            Some(&ctx),
            "build-1",
            None,
            None,
            &ImageBuildWaitOptions::default(),
            &InterruptSignal::new(),
            |_event| {},
            test_build_wait_timing(),
            no_follower,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ImageBuildError::Timeout { .. }));
        drop(client);
        assert!(server.finish().await.len() <= 2);

        // The pure deadline rule: the caller's bound applies on top of the
        // service's, and a wait without either is still bounded.
        let started = Instant::now();
        let caller = ImageBuildWaitOptions {
            timeout: Some(Duration::from_secs(5)),
            follow_logs: false,
        };
        let mut far = info_from(build_json("running", 1, None, None));
        far.operation_deadline_ms = Some(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 3_600_000,
        );
        assert_eq!(
            wait_deadline(Some(&far), &caller, started),
            started + Duration::from_secs(5)
        );
        assert_eq!(
            wait_deadline(None, &ImageBuildWaitOptions::default(), started),
            started + DEFAULT_WAIT_LIMIT
        );
        assert!(
            wait_deadline(Some(&far), &ImageBuildWaitOptions::default(), started)
                > started + Duration::from_secs(3_500)
        );
    }

    /// Status polling survives a 503, a dropped connection and a 429 with
    /// Retry-After, keeps the same build id, and reports the reconnects.
    #[tokio::test]
    async fn wait_reconnects_across_transient_failures() {
        let image_id = "d".repeat(64);
        let (base_url, server) = recording_image_service(vec![
            Reply::Status(503, json!({"error": "deploying"})),
            Reply::Reset,
            Reply::StatusWithHeaders(
                429,
                json!({"error": "slow down"}),
                vec![("Retry-After".to_string(), "0".to_string())],
            ),
            Reply::Status(200, build_json("running", 1, None, None)),
            Reply::Status(
                200,
                build_json(
                    "succeeded",
                    1,
                    None,
                    Some(published_image_record("image_ok", "img", &image_id)),
                ),
            ),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let mut events = Vec::new();
        let info = wait_for_terminal_with_follower(
            &client,
            Some(&ctx),
            "build-1",
            None,
            None,
            &ImageBuildWaitOptions::default(),
            &InterruptSignal::new(),
            |event| events.push(event),
            test_build_wait_timing(),
            no_follower,
        )
        .await
        .unwrap();
        assert_eq!(info.status, ImageBuildStatus::Succeeded);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SandboxImageBuildEvent::Status(message) if message.contains("reconnecting in")
                ))
                .count(),
            3
        );
        let requests = server.finish().await;
        assert_eq!(requests.len(), 5);
        assert!(
            requests
                .iter()
                .all(|request| request.line == "GET /builds/build-1 HTTP/1.1")
        );
    }

    /// Not-found, authorization and validation answers end the wait at once.
    #[tokio::test]
    async fn wait_does_not_retry_definitive_answers() {
        for (status, body, check) in [
            (
                404,
                json!({"error": "not found"}),
                (|error: &ImageBuildError| matches!(error, ImageBuildError::NotFound { .. }))
                    as fn(&ImageBuildError) -> bool,
            ),
            (403, json!({"error": "forbidden"}), |error| {
                matches!(error, ImageBuildError::Auth(_))
            }),
            (400, json!({"error": "bad id"}), |error| {
                matches!(error, ImageBuildError::Usage(_))
            }),
        ] {
            let (base_url, server) =
                recording_image_service(vec![Reply::Status(status, body)]).await;
            let client = ClientBuilder::new(&base_url).build().unwrap();
            let ctx = test_build_context(&base_url);
            let error = wait_for_terminal_with_follower(
                &client,
                Some(&ctx),
                "build-1",
                None,
                None,
                &ImageBuildWaitOptions::default(),
                &InterruptSignal::new(),
                |_event| {},
                test_build_wait_timing(),
                no_follower,
            )
            .await
            .unwrap_err();
            assert!(check(&error), "{status}: {error}");
            assert_eq!(server.finish().await.len(), 1);
        }
    }

    /// An interrupt during the wait cancels by build id and reports the
    /// confirmed outcome; the client request id is never needed.
    #[tokio::test]
    async fn interrupt_during_wait_cancels_by_build_id() {
        let (base_url, server) = recording_image_service(vec![
            Reply::Status(200, build_json("running", 1, None, None)),
            Reply::Status(503, json!({"error": "blip"})),
            Reply::Status(
                200,
                json!({"outcome": "cancelled", "build": build_json("cancelled", 1, None, None)}),
            ),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let interrupt = InterruptSignal::new();
        let trigger = interrupt.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            trigger.trigger();
        });
        let mut events = Vec::new();
        let error = wait_for_terminal_with_follower(
            &client,
            Some(&ctx),
            "build-1",
            None,
            None,
            &ImageBuildWaitOptions::default(),
            &interrupt,
            |event| events.push(event),
            BuildWaitTiming {
                poll_interval: Duration::from_secs(10),
                log_drain_grace: Duration::ZERO,
            },
            no_follower,
        )
        .await
        .unwrap_err();
        let ImageBuildError::Interrupted(report) = error else {
            panic!("expected an interruption, got {error}");
        };
        assert_eq!(report.build_id.as_deref(), Some("build-1"));
        assert_eq!(report.outcome, InterruptionOutcome::Cancelled);
        assert!(report.is_confirmed_cancelled());
        assert!(events.contains(&SandboxImageBuildEvent::Interrupted(report)));
        let requests = server.finish().await;
        assert_eq!(requests.len(), 3);
        // The handshake retried the transient cancel failure with the same
        // selector and never touched admission.
        assert_eq!(requests[1].line, "POST /builds/cancel HTTP/1.1");
        assert_eq!(requests[2].json(), json!({"build_id": "build-1"}));
    }

    fn pending_reply(build_id: &str) -> Reply {
        Reply::Status(
            503,
            json!({
                "error": "registration is still pending",
                "code": "publication_pending",
                "build_id": build_id,
            }),
        )
    }

    fn registration_policy(limit: Duration, poll_interval: Duration) -> RegistrationWaitPolicy {
        RegistrationWaitPolicy {
            limit,
            poll_interval,
        }
    }

    /// A `publication_pending` answer is progress, not unavailability: the
    /// registration keeps re-POSTing the identical request under the same
    /// key until the image publishes, announcing the pending build once.
    #[tokio::test]
    async fn registration_retries_pending_answers_under_one_key_until_published() {
        let (base_url, server) = recording_image_service(vec![
            pending_reply("build-9"),
            pending_reply("build-9"),
            pending_reply("build-9"),
            Reply::Status(
                201,
                published_image_record("image_reg", "img", &"a".repeat(64)),
            ),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let catalog = ImageServiceCatalog::new(&client, &base_url);
        let mut events = Vec::new();
        let template = catalog
            .adopt_snapshot_and_wait_with(
                "img",
                "snap-cas",
                false,
                registration_policy(Duration::from_secs(10), Duration::from_millis(10)),
                &InterruptSignal::new(),
                |event| events.push(event),
            )
            .await
            .unwrap();
        assert_eq!(template.id.as_deref(), Some("image_reg"));
        assert_eq!(
            template.snapshot_format_version.as_deref(),
            Some("content_addressed_streaming_v1")
        );
        let requests = server.completed().await;
        assert_eq!(requests.len(), 4);
        let keys: HashSet<String> = requests
            .iter()
            .map(|request| request.header("idempotency-key").unwrap().to_string())
            .collect();
        assert_eq!(keys.len(), 1, "one key across every retry");
        assert!(
            requests
                .iter()
                .all(|request| request.line.ends_with("/images/from-snapshot HTTP/1.1")),
            "{:?}",
            requests.iter().map(|r| &r.line).collect::<Vec<_>>()
        );
        let key = keys.into_iter().next().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SandboxImageBuildEvent::Accepted { .. }))
                .count(),
            1,
            "the pending build is announced once"
        );
        assert!(events.contains(&SandboxImageBuildEvent::Accepted {
            build_id: "build-9".to_string(),
            client_request_id: Some(key),
        }));
        assert!(events.iter().any(|event| matches!(
            event,
            SandboxImageBuildEvent::Status(message) if message.contains("pending (build build-9)")
        )));
    }

    /// A pending streak past the registration wait bound is a timeout naming
    /// the pending build; nothing is cancelled and no fresh key is minted.
    #[tokio::test]
    async fn registration_pending_past_the_bound_times_out_naming_the_build() {
        let replies: Vec<Reply> = (0..40).map(|_| pending_reply("build-9")).collect();
        let (base_url, server) = recording_image_service(replies).await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let catalog = ImageServiceCatalog::new(&client, &base_url);
        let started = Instant::now();
        let error = catalog
            .adopt_snapshot_and_wait_with(
                "img",
                "snap-cas",
                false,
                registration_policy(Duration::from_millis(250), Duration::from_millis(40)),
                &InterruptSignal::new(),
                |_| {},
            )
            .await
            .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        let ImageBuildError::Timeout { build_id, .. } = error else {
            panic!("expected a timeout, got {error}");
        };
        assert_eq!(build_id, "build-9");
        let requests = server.finish().await;
        assert!(requests.len() >= 2, "{}", requests.len());
        assert!(
            requests
                .iter()
                .all(|request| request.line.ends_with("/images/from-snapshot HTTP/1.1")),
            "no cancel and nothing but the same POST: {:?}",
            requests.iter().map(|r| &r.line).collect::<Vec<_>>()
        );
        let keys: HashSet<String> = requests
            .iter()
            .map(|request| request.header("idempotency-key").unwrap().to_string())
            .collect();
        assert_eq!(keys.len(), 1);
    }

    /// A POST still in flight when the registration deadline passes is
    /// abandoned at the deadline rather than running its own reconnect
    /// budget to the end; no answer ever named a build, so the timeout
    /// names the request id. The server holds the connection for far longer
    /// than the deadline and never answers, so returning at all before the
    /// hold ends proves the abandonment; the recording server counts only
    /// complete requests, so the dropped attempt leaves exactly one.
    #[tokio::test]
    async fn registration_abandons_an_in_flight_request_at_the_deadline() {
        const HOLD: Duration = Duration::from_secs(60);
        let (base_url, server) = recording_image_service(vec![Reply::Hang(HOLD)]).await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let catalog = ImageServiceCatalog::new(&client, &base_url);
        let started = Instant::now();
        let error = catalog
            .adopt_snapshot_and_wait_with(
                "img",
                "snap-cas",
                false,
                registration_policy(Duration::from_millis(300), Duration::from_millis(40)),
                &InterruptSignal::new(),
                |_| {},
            )
            .await
            .unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(300) && elapsed < HOLD / 2,
            "{elapsed:?}"
        );
        let ImageBuildError::Timeout { build_id, waited } = error else {
            panic!("expected a timeout, got {error}");
        };
        assert!(build_id.starts_with("(request "), "{build_id}");
        assert!(waited >= Duration::from_millis(300));
        let requests = server.finish().await;
        assert_eq!(
            requests.len(),
            1,
            "one complete POST, no second attempt and no cancel: {:?}",
            requests.iter().map(|r| &r.line).collect::<Vec<_>>()
        );
        assert!(requests[0].line.ends_with("/images/from-snapshot HTTP/1.1"));
    }

    #[test]
    fn upload_failures_carry_no_capability_material() {
        let url = "https://bucket.s3.amazonaws.com/contexts/p/b.tar?X-Amz-Algorithm=AWS4-HMAC-SHA256\
                   &X-Amz-Credential=AKIATESTKEY%2F20261015%2Fus-east-1%2Fs3%2Faws4_request\
                   &X-Amz-Signature=deadbeefcafef00d";
        let body = format!(
            "<Error><Code>SignatureDoesNotMatch</Code><AWSAccessKeyId>AKIATESTKEY</AWSAccessKeyId>\
             <StringToSign>AWS4-HMAC-SHA256 {url}</StringToSign><Resource>/contexts/p/b.tar</Resource>\
             </Error> also url={url} json={{\"q\": \"X-Amz-Signature=deadbeefcafef00d\"}}"
        );
        let redacted = redact_capability(&body, url);
        // Fixture markers standing in for signing material; none may survive.
        for marker in [
            "deadbeefcafef00d",
            "AKIATESTKEY",
            "X-Amz-",
            "aws4_request",
            "bucket.s3.amazonaws.com",
        ] {
            assert!(!redacted.contains(marker), "{marker} leaked: {redacted}");
        }
        assert!(redacted.contains("SignatureDoesNotMatch"), "{redacted}");
        assert!(redacted.contains("<capability-url>"), "{redacted}");
        // A transport error loses its URL before it is formatted.
        let big = "x".repeat(10_000);
        assert!(redact_capability(&big, url).len() <= MAX_UPLOAD_ERROR_BODY + 3);
    }

    /// Interrupted before the admission answered: the admission is not
    /// replayed; the request id resolves it through the cancel route, and
    /// `not_admitted` is reported as exactly that.
    #[tokio::test]
    async fn interrupt_before_admission_response_resolves_by_request_id() {
        let (base_url, server) = recording_image_service(vec![
            Reply::Hang(Duration::from_millis(200)),
            Reply::Status(200, json!({"outcome": "not_admitted", "build": null})),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let interrupt = InterruptSignal::new();
        let trigger = interrupt.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            trigger.trigger();
        });
        let request_id = new_client_request_id();
        let mut events = Vec::new();
        let error = admit_build(
            &client,
            "/builds",
            &json!({"kind": "import", "image_ref": "alpine:3.20", "name": "img"}),
            &request_id,
            &interrupt,
            &mut |event| events.push(event),
        )
        .await
        .unwrap_err();
        let ImageBuildError::Interrupted(report) = error else {
            panic!("expected an interruption, got {error}");
        };
        assert_eq!(report.build_id, None);
        assert_eq!(
            report.client_request_id.as_deref(),
            Some(request_id.as_str())
        );
        assert_eq!(report.outcome, InterruptionOutcome::NotAdmitted);
        assert!(!report.is_confirmed_cancelled());
        assert!(report.to_string().contains("No build was admitted"));
        let requests = server.finish().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].line, "POST /builds HTTP/1.1");
        assert_eq!(requests[1].line, "POST /builds/cancel HTTP/1.1");
        assert_eq!(requests[1].json(), json!({"client_request_id": request_id}));
    }

    /// When publication wins the race the interruption says so and carries
    /// the published result instead of claiming cancellation.
    #[tokio::test]
    async fn interrupt_racing_success_reports_already_completed() {
        let image_id = "e".repeat(64);
        let (base_url, server) = recording_image_service(vec![
            Reply::Status(200, build_json("running", 1, None, None)),
            Reply::Status(
                200,
                json!({
                    "outcome": "already_completed",
                    "build": build_json(
                        "succeeded",
                        1,
                        None,
                        Some(published_image_record("image_won", "img", &image_id)),
                    ),
                }),
            ),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let ctx = test_build_context(&base_url);
        let interrupt = InterruptSignal::new();
        let trigger = interrupt.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            trigger.trigger();
        });
        let error = wait_for_terminal_with_follower(
            &client,
            Some(&ctx),
            "build-1",
            None,
            None,
            &ImageBuildWaitOptions::default(),
            &interrupt,
            |_event| {},
            BuildWaitTiming {
                poll_interval: Duration::from_secs(10),
                log_drain_grace: Duration::ZERO,
            },
            no_follower,
        )
        .await
        .unwrap_err();
        let ImageBuildError::Interrupted(report) = error else {
            panic!("expected an interruption, got {error}");
        };
        match &report.outcome {
            InterruptionOutcome::AlreadyCompleted(Some(result)) => {
                assert_eq!(result.id.as_deref(), Some("image_won"));
            }
            other => panic!("unexpected outcome {other:?}"),
        }
        assert!(!report.is_confirmed_cancelled());
        assert!(report.to_string().contains("already completed"));
        server.finish().await;
    }

    /// A build lookup maps 404 to `NotFound` and reconnects across a 503.
    #[tokio::test]
    async fn get_build_reconnects_and_maps_not_found() {
        let (base_url, server) = recording_image_service(vec![
            Reply::Status(503, json!({"error": "deploying"})),
            Reply::Status(200, build_json("running", 1, Some("builder-1"), None)),
            Reply::Status(404, json!({"error": "unknown"})),
        ])
        .await;
        let client = ClientBuilder::new(&base_url).build().unwrap();
        let info = get_build(&client, "build-1").await.unwrap();
        assert_eq!(info.builder_sandbox_id.as_deref(), Some("builder-1"));
        let missing = get_build(&client, "build-2").await.unwrap_err();
        assert!(matches!(missing, ImageBuildError::NotFound { build_id } if build_id == "build-2"));
        assert_eq!(server.finish().await.len(), 3);
    }

    #[test]
    fn retryable_failures_are_outages_not_answers() {
        let server = |status: u16| {
            ImageBuildError::Sdk(SdkError::ServerError {
                status: StatusCode::from_u16(status).unwrap(),
                message: String::new(),
            })
        };
        assert!(is_retryable(&server(500)));
        assert!(is_retryable(&server(503)));
        assert!(is_retryable(&ImageBuildError::Service {
            status: 429,
            message: String::new()
        }));
        assert!(is_retryable(&ImageBuildError::Service {
            status: 408,
            message: String::new()
        }));
        assert!(!is_retryable(&ImageBuildError::Service {
            status: 409,
            message: String::new()
        }));
        assert!(!is_retryable(&ImageBuildError::Auth(String::new())));
        assert!(!is_retryable(&ImageBuildError::NotFound {
            build_id: "b".to_string()
        }));
        assert!(!is_retryable(&ImageBuildError::Usage(String::new())));

        // Retry-After wins over the schedule; the schedule is capped and
        // jittered.
        assert_eq!(
            reconnect_delay(1, Some(Duration::from_secs(3))),
            Duration::from_secs(3)
        );
        assert_eq!(
            reconnect_delay(1, Some(Duration::from_secs(600))),
            MAX_RETRY_AFTER
        );
        for attempt in 1..=12 {
            let delay = reconnect_delay(attempt, None);
            assert!(delay >= Duration::from_millis(375), "{delay:?}");
            assert!(delay <= MAX_RECONNECT_BACKOFF.mul_f64(1.25), "{delay:?}");
        }
    }

    fn no_follower(
        _ctx: ResolvedBuildContext,
        _sandbox_id: String,
        _sink: BuilderLogSink,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn follow_logs_options() -> ImageBuildWaitOptions {
        ImageBuildWaitOptions {
            timeout: Some(Duration::from_secs(5)),
            follow_logs: true,
        }
    }

    fn test_build_wait_timing() -> BuildWaitTiming {
        BuildWaitTiming {
            poll_interval: Duration::from_millis(5),
            log_drain_grace: Duration::from_millis(20),
        }
    }

    fn test_build_context(api_url: &str) -> ResolvedBuildContext {
        ResolvedBuildContext {
            api_url: api_url.to_string(),
            bearer_token: "token".to_string(),
            use_scope_headers: false,
            organization_id: Some("organization-1".to_string()),
            project_id: Some("project-1".to_string()),
            namespace: "default".to_string(),
            user_agent: None,
        }
    }

    /// A build record as the service reports it.
    fn build_json(
        status: &str,
        attempt: u64,
        builder_sandbox_id: Option<&str>,
        result: Option<Value>,
    ) -> Value {
        json!({
            "build_id": "build-1",
            "client_request_id": "rid-1",
            "kind": "dockerfile",
            "name": "img",
            "status": status,
            "phase": null,
            "attempt_no": attempt,
            "builder_sandbox_id": builder_sandbox_id,
            "result": result,
            "cleanup_pending": false,
            "operation_deadline_ms": null,
        })
    }

    fn info_from(value: Value) -> ImageBuildInfo {
        ImageBuildInfo::from_json(value).unwrap()
    }

    /// The Image Service's tenant record for a published image: the trimmed
    /// eight fields. `_image_id` is accepted for the callers that still
    /// name the content; the record carries no digest.
    fn published_image_record(public_id: &str, name: &str, _image_id: &str) -> Value {
        json!({
            "id": public_id,
            "name": name,
            "snapshot_id": "snap123",
            "global": false,
            "physical_size_bytes": 42,
            "logical_size_bytes": 10_737_418_240_u64,
            "created_at": "2026-10-15T00:00:00.000Z",
            "updated_at": "2026-10-15T00:00:00.000Z",
        })
    }

    async fn scripted_image_service(
        responses: Vec<Value>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        scripted_image_service_with_status(
            responses
                .into_iter()
                .map(|response| (200, response))
                .collect(),
        )
        .await
    }

    async fn scripted_image_service_with_status(
        responses: Vec<(u16, Value)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let expected = responses.len();
        let (url, server) = recording_image_service(
            responses
                .into_iter()
                .map(|(status, body)| Reply::Status(status, body))
                .collect(),
        )
        .await;
        (
            url,
            tokio::spawn(async move {
                assert_eq!(server.completed().await.len(), expected);
            }),
        )
    }

    /// One scripted answer of the recording Image Service.
    #[derive(Clone)]
    enum Reply {
        Status(u16, Value),
        StatusWithHeaders(u16, Value, Vec<(String, String)>),
        /// Read the request, then drop the connection without answering.
        Reset,
        /// Read the request, hold the connection for the duration, then drop
        /// it: an admission whose answer the client never sees.
        Hang(Duration),
    }

    struct RecordedRequest {
        line: String,
        headers: Vec<(String, String)>,
        body: String,
    }

    impl RecordedRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }

        fn json(&self) -> Value {
            serde_json::from_str(&self.body).unwrap_or(Value::Null)
        }
    }

    /// A scripted Image Service that records every request it answers, in
    /// arrival order, so tests can assert on methods, paths, headers and
    /// bodies — including which requests were never made. `finish` returns
    /// the record whether or not every scripted reply was consumed.
    struct RecordingServer {
        recorded: Arc<std::sync::Mutex<Vec<RecordedRequest>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl RecordingServer {
        /// Wait until every scripted reply has been consumed.
        async fn completed(self) -> Vec<RecordedRequest> {
            self.task.await.unwrap();
            std::mem::take(&mut *self.recorded.lock().unwrap())
        }

        async fn finish(self) -> Vec<RecordedRequest> {
            // Give an in-flight accept a moment, then stop listening.
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.task.abort();
            let _ = self.task.await;
            std::mem::take(&mut *self.recorded.lock().unwrap())
        }
    }

    async fn recording_image_service(replies: Vec<Reply>) -> (String, RecordingServer) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let recorded: Arc<std::sync::Mutex<Vec<RecordedRequest>>> = Arc::default();
        let record = recorded.clone();
        let task = tokio::spawn(async move {
            for reply in replies {
                // Accept until one complete request (head and body) arrives.
                // A connection the client opened and dropped before or while
                // sending -- an attempt abandoned at a deadline, a pooled
                // connection never used -- is not a request: it is neither
                // recorded nor answered with this reply.
                let (mut stream, line, headers, body) = loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut raw = Vec::new();
                    let mut byte = [0_u8];
                    while !raw.ends_with(b"\r\n\r\n") {
                        if stream.read_exact(&mut byte).await.is_err() {
                            break;
                        }
                        raw.push(byte[0]);
                    }
                    if !raw.ends_with(b"\r\n\r\n") {
                        drop(stream);
                        continue;
                    }
                    let head = String::from_utf8_lossy(&raw).to_string();
                    let mut lines = head.lines();
                    let line = lines.next().unwrap_or_default().to_string();
                    if line.is_empty() {
                        drop(stream);
                        continue;
                    }
                    let headers: Vec<(String, String)> = lines
                        .filter_map(|header| header.split_once(':'))
                        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
                        .collect();
                    let length = headers
                        .iter()
                        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                        .and_then(|(_, value)| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    let mut body = vec![0_u8; length];
                    if length > 0 && stream.read_exact(&mut body).await.is_err() {
                        drop(stream);
                        continue;
                    }
                    break (stream, line, headers, body);
                };
                record.lock().unwrap().push(RecordedRequest {
                    line,
                    headers,
                    body: String::from_utf8_lossy(&body).to_string(),
                });
                let (status, response, extra) = match reply {
                    Reply::Status(status, response) => (status, response, Vec::new()),
                    Reply::StatusWithHeaders(status, response, extra) => (status, response, extra),
                    Reply::Reset => {
                        drop(stream);
                        continue;
                    }
                    Reply::Hang(duration) => {
                        tokio::time::sleep(duration).await;
                        drop(stream);
                        continue;
                    }
                };
                let body = serde_json::to_vec(&response).unwrap();
                let mut head = format!(
                    "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                    body.len()
                );
                for (key, value) in extra {
                    head.push_str(&format!("{key}: {value}\r\n"));
                }
                head.push_str("\r\n");
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            }
        });
        (
            format!("http://{address}"),
            RecordingServer { recorded, task },
        )
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
            global_build_args: Vec::new(),
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
