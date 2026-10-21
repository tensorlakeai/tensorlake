//! napi-rs bindings for the Tensorlake Rust cloud SDK, consumed by the
//! TypeScript SDK.
//!
//! Mirrors the surface exposed by `crates/rust-cloud-sdk-py` to Python so
//! both language SDKs delegate to the same Rust implementation for cloud,
//! sandbox, repository, filesystem and sandbox-image operations.

#![deny(clippy::all)]

mod cloud;
mod function_agent;
mod repositories;
mod sandbox;
mod tunnel;

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use napi::{
    Result,
    bindgen_prelude::*,
    threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode},
};
use napi_derive::napi;
use tensorlake::{
    image_builds::{
        CancelBuildSelector, ImageBuildClient, ImageBuildClientOptions, ImageBuildError,
        ImageBuildWaitOptions, InterruptSignal,
    },
    retry::{RetryPolicy, retry},
    sandbox_image_catalog::SandboxImageCatalog,
    sandbox_images::{
        CommonBuildOptions, SandboxImageBuildError, SandboxImageBuildEvent,
        SandboxImageBuildOptions, SandboxImageImportOptions,
        build_sandbox_image as rust_build_sandbox_image,
        build_sandbox_image_with_interrupt as rust_build_sandbox_image_with_interrupt,
        import_sandbox_image as rust_import_sandbox_image,
        import_sandbox_image_with_interrupt as rust_import_sandbox_image_with_interrupt,
    },
};

/// Capture Docker config contents at CLI startup, before asynchronous work.
#[napi]
pub fn load_docker_config_json() -> Result<String> {
    tensorlake::docker_config::load_docker_config_json()
        .map_err(|error| Error::from_reason(error.to_string()))
}

#[napi(object)]
pub struct SandboxImageBuildOptionsJs {
    pub api_url: String,
    pub bearer_token: String,
    pub dockerfile_path: String,
    pub registered_name: Option<String>,
    /// Root disk size for the generated sandbox image in MB. Capped at u32
    /// max (~4 PiB) for napi compatibility — well above the 10 GiB default.
    pub disk_mb: Option<u32>,
    pub builder_disk_mb: Option<u32>,
    pub cpus: Option<f64>,
    pub memory_mb: Option<i64>,
    pub is_public: Option<bool>,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub namespace: Option<String>,
    pub use_scope_headers: Option<bool>,
    pub user_agent: Option<String>,
    pub docker_compat: Option<bool>,
    /// Docker config JSON contents. Omission discovers local configuration.
    pub docker_config_json: Option<String>,
    pub dockerfile_text: Option<String>,
    pub context_dir: Option<String>,
    /// Build a CAS (content-addressed streaming) image, the default.
    /// `false` builds a legacy image.
    pub cas: Option<bool>,
    /// Dockerfile build args forwarded to BuildKit inside the builder. CAS
    /// images only.
    pub build_args: Option<std::collections::HashMap<String, String>>,
}

#[napi(object)]
pub struct SandboxImageImportOptionsJs {
    pub api_url: String,
    pub bearer_token: String,
    /// Registry image reference to import directly into a rootfs (no
    /// Dockerfile, no Docker daemon), e.g. `pytorch/pytorch:2.4.1` or
    /// `ghcr.io/org/app@sha256:...`.
    pub image_reference: String,
    pub registered_name: Option<String>,
    /// Root disk size for the generated sandbox image in MB. Capped at u32
    /// max (~4 PiB) for napi compatibility — well above the 10 GiB default.
    pub disk_mb: Option<u32>,
    pub builder_disk_mb: Option<u32>,
    pub cpus: Option<f64>,
    pub memory_mb: Option<i64>,
    pub is_public: Option<bool>,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub namespace: Option<String>,
    pub use_scope_headers: Option<bool>,
    pub user_agent: Option<String>,
    pub docker_compat: Option<bool>,
    /// Docker config JSON contents. Omission discovers local configuration.
    pub docker_config_json: Option<String>,
    /// Build a CAS (content-addressed streaming) image, the default.
    /// `false` builds a legacy image.
    pub cas: Option<bool>,
}

#[napi(object)]
#[derive(Default)]
pub struct SandboxImageBuildEventJs {
    /// One of `"status"`, `"build_log"`, `"warning"`, and for CAS builds
    /// `"build_accepted"`, `"build_progress"`, `"build_terminal"`,
    /// `"interrupted"`. Matches the event vocabulary the PyO3 binding uses
    /// on the Python side.
    pub event_type: String,
    /// Only set for `"build_log"` events; carries `"stdout"` or `"stderr"`.
    pub stream: Option<String>,
    pub message: String,
    /// CAS build events: the build this event is about.
    pub build_id: Option<String>,
    pub client_request_id: Option<String>,
    /// `"build_progress"` / `"build_terminal"`: the observed state.
    pub status: Option<String>,
    pub phase: Option<String>,
    pub attempt: Option<u32>,
    /// `"interrupted"`: the cancellation handshake's outcome and whether it
    /// confirms the remote build will not publish.
    pub outcome: Option<String>,
    pub confirmed: Option<bool>,
}

fn event_to_js(event: SandboxImageBuildEvent) -> SandboxImageBuildEventJs {
    match event {
        SandboxImageBuildEvent::Status(message) => SandboxImageBuildEventJs {
            event_type: "status".to_string(),
            message,
            ..Default::default()
        },
        SandboxImageBuildEvent::BuildLog { stream, message } => SandboxImageBuildEventJs {
            event_type: "build_log".to_string(),
            stream: Some(stream),
            message,
            ..Default::default()
        },
        SandboxImageBuildEvent::Warning(message) => SandboxImageBuildEventJs {
            event_type: "warning".to_string(),
            message,
            ..Default::default()
        },
        SandboxImageBuildEvent::Accepted {
            build_id,
            client_request_id,
        } => SandboxImageBuildEventJs {
            event_type: "build_accepted".to_string(),
            message: format!("Build {build_id} accepted"),
            build_id: Some(build_id),
            client_request_id,
            ..Default::default()
        },
        SandboxImageBuildEvent::Progress {
            build_id,
            status,
            phase,
            attempt,
        }
        | SandboxImageBuildEvent::Terminal {
            build_id,
            status,
            phase,
            attempt,
        } => SandboxImageBuildEventJs {
            event_type: if status.is_terminal() {
                "build_terminal".to_string()
            } else {
                "build_progress".to_string()
            },
            message: format!("Build {build_id}: {status} (attempt {attempt})"),
            build_id: Some(build_id),
            status: Some(status.as_str().to_string()),
            phase: phase.map(|phase| phase.as_str().to_string()),
            attempt: Some(u32::try_from(attempt).unwrap_or(u32::MAX)),
            ..Default::default()
        },
        SandboxImageBuildEvent::Interrupted(report) => SandboxImageBuildEventJs {
            event_type: "interrupted".to_string(),
            message: report.to_string(),
            build_id: report.build_id.clone(),
            client_request_id: report.client_request_id.clone(),
            outcome: Some(report.outcome.name().to_string()),
            confirmed: Some(report.is_confirmed_cancelled()),
            ..Default::default()
        },
    }
}

/// Build a sandbox image. Returns a JSON-encoded string with the registered
/// sandbox-template metadata; the TS SDK is expected to parse it.
///
/// `emit`, if provided, is invoked for each progress event. The TypeScript
/// wrapper must catch user callback exceptions before they cross this napi
/// threadsafe callback boundary.
///
/// `interrupt_id` (from [`create_image_build_interrupt`]) is honored only
/// for CAS image builds: triggering it cancels the remote CAS image build
/// through the bounded handshake. A legacy image build ignores it and keeps its
/// existing error path; nothing is cancelled remotely on its behalf.
#[napi]
pub async fn build_sandbox_image(
    options: SandboxImageBuildOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
    interrupt_id: Option<u32>,
) -> Result<String> {
    let cas = options.cas.unwrap_or(true);
    let docker_config_json = options.docker_config_json.clone();
    let build_options = build_options_from_js(options);
    if cas {
        let interrupt = interrupt_for(interrupt_id);
        let result = rust_build_sandbox_image_with_interrupt(
            build_options,
            docker_config_json,
            &interrupt,
            move |event| {
                if let Some(tsfn) = emit.as_ref() {
                    tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
                }
            },
        )
        .await
        .map_err(cas_build_napi_error)?;
        return serde_json::to_string(&result)
            .map_err(|error| Error::from_reason(error.to_string()));
    }

    let result = rust_build_sandbox_image(build_options, docker_config_json, move |event| {
        if let Some(tsfn) = emit.as_ref() {
            tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
        }
    })
    .await
    .map_err(|error| Error::from_reason(error.to_string()))?;

    serde_json::to_string(&result).map_err(|error| Error::from_reason(error.to_string()))
}

fn build_options_from_js(options: SandboxImageBuildOptionsJs) -> SandboxImageBuildOptions {
    SandboxImageBuildOptions {
        common: CommonBuildOptions {
            api_url: options.api_url,
            bearer_token: options.bearer_token,
            use_scope_headers: options.use_scope_headers.unwrap_or(false),
            organization_id: options.organization_id,
            project_id: options.project_id,
            namespace: options.namespace.unwrap_or_else(|| "default".to_string()),
            registered_name: options.registered_name,
            disk_mb: options.disk_mb.map(u64::from),
            builder_disk_mb: options.builder_disk_mb.map(u64::from),
            cpus: options.cpus,
            memory_mb: options.memory_mb,
            is_public: options.is_public.unwrap_or(false),
            cas: options.cas.unwrap_or(true),
            user_agent: options.user_agent,
            docker_compat: options.docker_compat.unwrap_or(false),
        },
        dockerfile_path: PathBuf::from(options.dockerfile_path),
        dockerfile_text: options.dockerfile_text,
        context_dir: options.context_dir.map(PathBuf::from),
        context_files: Vec::new(),
        build_args: options
            .build_args
            .map(|args| {
                let mut args: Vec<(String, String)> = args.into_iter().collect();
                args.sort();
                args
            })
            .unwrap_or_default(),
    }
}

/// Import a registry image directly into a sandbox image (no Dockerfile, no
/// Docker daemon). Returns a JSON-encoded string with the registered
/// sandbox-template metadata; the TS SDK is expected to parse it.
///
/// `emit`, if provided, is invoked for each progress event. The TypeScript
/// wrapper must catch user callback exceptions before they cross this napi
/// threadsafe callback boundary.
///
/// `interrupt_id` is honored for CAS imports only; see
/// [`build_sandbox_image`].
#[napi]
pub async fn import_sandbox_image(
    options: SandboxImageImportOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
    interrupt_id: Option<u32>,
) -> Result<String> {
    let cas = options.cas.unwrap_or(true);
    let docker_config_json = options.docker_config_json.clone();
    let import_options = import_options_from_js(options);
    if cas {
        let interrupt = interrupt_for(interrupt_id);
        let result = rust_import_sandbox_image_with_interrupt(
            import_options,
            docker_config_json,
            &interrupt,
            move |event| {
                if let Some(tsfn) = emit.as_ref() {
                    tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
                }
            },
        )
        .await
        .map_err(cas_build_napi_error)?;
        return serde_json::to_string(&result)
            .map_err(|error| Error::from_reason(error.to_string()));
    }

    let result = rust_import_sandbox_image(import_options, docker_config_json, move |event| {
        if let Some(tsfn) = emit.as_ref() {
            tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
        }
    })
    .await
    .map_err(|error| Error::from_reason(error.to_string()))?;

    serde_json::to_string(&result).map_err(|error| Error::from_reason(error.to_string()))
}

fn import_options_from_js(options: SandboxImageImportOptionsJs) -> SandboxImageImportOptions {
    SandboxImageImportOptions {
        common: CommonBuildOptions {
            api_url: options.api_url,
            bearer_token: options.bearer_token,
            use_scope_headers: options.use_scope_headers.unwrap_or(false),
            organization_id: options.organization_id,
            project_id: options.project_id,
            namespace: options.namespace.unwrap_or_else(|| "default".to_string()),
            registered_name: options.registered_name,
            disk_mb: options.disk_mb.map(u64::from),
            builder_disk_mb: options.builder_disk_mb.map(u64::from),
            cpus: options.cpus,
            memory_mb: options.memory_mb,
            is_public: options.is_public.unwrap_or(false),
            cas: options.cas.unwrap_or(true),
            user_agent: options.user_agent,
            docker_compat: options.docker_compat.unwrap_or(false),
        },
        image_reference: options.image_reference,
    }
}

// ---- Image Service build operations (CAS only) ----------------------------
//
// Everything below addresses the Image Service build plane. Legacy builds,
// lookups and deletes keep the functions above and their error path; no
// legacy call reaches an interrupt or a cancellation.

/// Interrupt signals the TypeScript side can trigger from an `AbortSignal`.
/// Registered by id so a trigger can arrive from any JS callback while the
/// Rust future runs; released by the caller once the operation ends.
static INTERRUPTS: LazyLock<Mutex<HashMap<u32, InterruptSignal>>> = LazyLock::new(Mutex::default);
static NEXT_INTERRUPT_ID: AtomicU32 = AtomicU32::new(1);

/// Create an interrupt signal and return its id.
#[napi]
pub fn create_image_build_interrupt() -> u32 {
    let id = NEXT_INTERRUPT_ID.fetch_add(1, Ordering::Relaxed);
    INTERRUPTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(id, InterruptSignal::new());
    id
}

/// Request cancellation of the operation running under `id`. Unknown ids
/// are ignored.
#[napi]
pub fn trigger_image_build_interrupt(id: u32) {
    if let Some(signal) = INTERRUPTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&id)
    {
        signal.trigger();
    }
}

/// Forget an interrupt signal once its operation has ended.
#[napi]
pub fn release_image_build_interrupt(id: u32) {
    INTERRUPTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&id);
}

fn interrupt_for(id: Option<u32>) -> InterruptSignal {
    id.and_then(|id| {
        INTERRUPTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()
    })
    .unwrap_or_default()
}

/// Typed build outcomes become `{category: "image_build", code, message,
/// build_id, ...}` payloads (see `ImageBuildError::to_json`); other failures
/// keep the plain message the legacy path uses.
fn cas_build_napi_error(error: SandboxImageBuildError) -> napi::Error {
    match error {
        SandboxImageBuildError::Build(error) => image_build_napi_error(error),
        other => Error::from_reason(other.to_string()),
    }
}

fn image_build_napi_error(error: ImageBuildError) -> napi::Error {
    let mut payload = error.to_json();
    if let Some(object) = payload.as_object_mut() {
        object.insert("category".into(), serde_json::json!("image_build"));
        object.entry("status").or_insert(serde_json::Value::Null);
    }
    Error::from_reason(payload.to_string())
}

fn json_string<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|error| Error::from_reason(error.to_string()))
}

/// Credentials and scope for build lookups, waits and cancellations.
#[napi(object)]
pub struct ImageBuildClientOptionsJs {
    pub api_url: String,
    pub bearer_token: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub namespace: Option<String>,
    pub use_scope_headers: Option<bool>,
    pub user_agent: Option<String>,
}

fn image_build_client(options: ImageBuildClientOptionsJs) -> Result<ImageBuildClient> {
    ImageBuildClient::new(ImageBuildClientOptions {
        api_url: options.api_url,
        bearer_token: options.bearer_token,
        use_scope_headers: options.use_scope_headers.unwrap_or(false),
        organization_id: options.organization_id,
        project_id: options.project_id,
        namespace: options.namespace.unwrap_or_else(|| "default".to_string()),
        user_agent: options.user_agent,
    })
    .map_err(image_build_napi_error)
}

/// Bounds for a wait.
#[napi(object)]
pub struct ImageBuildWaitOptionsJs {
    /// Caller bound in milliseconds on top of the service-informed deadline.
    pub timeout_ms: Option<f64>,
    /// Follow builder logs through the sandbox API.
    pub follow_logs: Option<bool>,
}

/// Exactly one of `build_id` (sufficient) or `client_request_id` (recovery
/// of a lost admission response).
#[napi(object)]
pub struct CancelImageBuildSelectorJs {
    pub build_id: Option<String>,
    pub client_request_id: Option<String>,
}

/// Admit a CAS Dockerfile build (upload and seal its context) without
/// waiting. Returns JSON `{"build": BuildInfo, "client_request_id": ...}`.
#[napi]
pub async fn start_image_build(
    options: SandboxImageBuildOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
    interrupt_id: Option<u32>,
) -> Result<String> {
    let docker_config_json = options.docker_config_json.clone();
    let mut build_options = build_options_from_js(options);
    build_options.common.cas = true;
    let interrupt = interrupt_for(interrupt_id);
    let handle = tensorlake::image_builds::start_build(
        build_options,
        docker_config_json,
        &interrupt,
        move |event| {
            if let Some(tsfn) = emit.as_ref() {
                tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
            }
        },
    )
    .await
    .map_err(image_build_napi_error)?;
    json_string(&serde_json::json!({
        "build": handle.info(),
        "client_request_id": handle.client_request_id(),
    }))
}

/// Admit a CAS registry import without waiting.
#[napi]
pub async fn start_image_import(
    options: SandboxImageImportOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
    interrupt_id: Option<u32>,
) -> Result<String> {
    let docker_config_json = options.docker_config_json.clone();
    let mut import_options = import_options_from_js(options);
    import_options.common.cas = true;
    let interrupt = interrupt_for(interrupt_id);
    let handle = tensorlake::image_builds::start_import(
        import_options,
        docker_config_json,
        &interrupt,
        move |event| {
            if let Some(tsfn) = emit.as_ref() {
                tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
            }
        },
    )
    .await
    .map_err(image_build_napi_error)?;
    json_string(&serde_json::json!({
        "build": handle.info(),
        "client_request_id": handle.client_request_id(),
    }))
}

/// `GET /builds/{build_id}` as BuildInfo JSON.
#[napi]
pub async fn get_image_build(
    options: ImageBuildClientOptionsJs,
    build_id: String,
) -> Result<String> {
    let client = image_build_client(options)?;
    let handle = client
        .get(&build_id)
        .await
        .map_err(image_build_napi_error)?;
    json_string(handle.info())
}

/// Wait for a build and return its immutable result JSON. A timeout leaves
/// the build running; a triggered interrupt cancels it remotely.
#[napi]
pub async fn wait_image_build(
    options: ImageBuildClientOptionsJs,
    build_id: String,
    wait: ImageBuildWaitOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
    interrupt_id: Option<u32>,
) -> Result<String> {
    let client = image_build_client(options)?;
    let timeout = match wait.timeout_ms {
        Some(ms) if ms.is_finite() && ms >= 0.0 => Some(Duration::from_secs_f64(ms / 1_000.0)),
        Some(_) => {
            return Err(sandbox::usage_error(
                "timeoutMs must be a non-negative number".to_string(),
            ));
        }
        None => None,
    };
    let interrupt = interrupt_for(interrupt_id);
    // The lookup runs under the interrupt too: an abort during a slow or
    // stalled first request cancels the build by its id.
    let result = client
        .wait_build(
            &build_id,
            ImageBuildWaitOptions {
                timeout,
                follow_logs: wait.follow_logs.unwrap_or(false),
            },
            &interrupt,
            move |event| {
                if let Some(tsfn) = emit.as_ref() {
                    tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
                }
            },
        )
        .await
        .map_err(image_build_napi_error)?;
    json_string(&result)
}

/// Cancel a build. Returns JSON `{"outcome": ..., "build": BuildInfo|null}`.
#[napi]
pub async fn cancel_image_build(
    options: ImageBuildClientOptionsJs,
    selector: CancelImageBuildSelectorJs,
) -> Result<String> {
    let selector = match (selector.build_id, selector.client_request_id) {
        (Some(build_id), None) => CancelBuildSelector::BuildId(build_id),
        (None, Some(request_id)) => CancelBuildSelector::ClientRequestId(request_id),
        _ => {
            return Err(sandbox::usage_error(
                "exactly one of buildId or clientRequestId is required".to_string(),
            ));
        }
    };
    let client = image_build_client(options)?;
    let response = client
        .cancel(selector)
        .await
        .map_err(image_build_napi_error)?;
    json_string(&response)
}

/// Credentials and scope for the merged image catalog (ADR 0107): the
/// legacy image registry plus the CAS image registry behind one lookup,
/// delete and list.
#[napi(object)]
pub struct SandboxImageCatalogOptionsJs {
    pub api_url: String,
    pub bearer_token: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub use_scope_headers: Option<bool>,
    pub user_agent: Option<String>,
}

fn sandbox_image_catalog(options: SandboxImageCatalogOptionsJs) -> Result<SandboxImageCatalog> {
    SandboxImageCatalog::from_credentials(
        &options.api_url,
        &options.bearer_token,
        options.use_scope_headers.unwrap_or(false),
        options.organization_id.as_deref(),
        options.project_id.as_deref(),
        options.user_agent.as_deref(),
    )
    .map_err(sandbox::into_napi_error)
}

/// List every registered sandbox image of both families, newest first, as a
/// JSON array in the platform template shape.
#[napi]
pub async fn list_sandbox_images(options: SandboxImageCatalogOptionsJs) -> Result<String> {
    let catalog = sandbox_image_catalog(options)?;
    let images =
        sandbox::with_retry(catalog, 5, |catalog| async move { catalog.list().await }).await?;
    serde_json::to_string(&images).map_err(|error| Error::from_reason(error.to_string()))
}

/// Look up one sandbox image by name or id. Returns the record JSON in the
/// platform template shape, or `null` when neither catalog knows it.
#[napi]
pub async fn find_sandbox_image(
    options: SandboxImageCatalogOptionsJs,
    reference: String,
) -> Result<Option<String>> {
    let catalog = sandbox_image_catalog(options)?;
    let image = sandbox::with_retry(catalog, 5, |catalog| {
        let reference = reference.clone();
        async move { catalog.find(&reference).await }
    })
    .await?;
    image
        .map(|image| serde_json::to_string(&image))
        .transpose()
        .map_err(|error| Error::from_reason(error.to_string()))
}

/// The same-named legacy image a name's CAS delete left behind and could not
/// remove.
#[napi(object)]
pub struct SandboxImageLegacyCleanupFailureJs {
    /// The legacy image's `sandbox_template_` id, when known.
    pub legacy_image_id: Option<String>,
    pub error: String,
}

/// Delete one sandbox image by name or id. A name whose CAS image is deleted
/// also loses its same-named legacy image, which can no longer start
/// sandboxes. Not found in either catalog is a 404 `remote_api` error.
/// Replayed only when the request provably never reached a server. Resolves
/// to the legacy image that could not be deleted, or null; the delete still
/// succeeded and the TypeScript layer turns it into a warning.
#[napi]
pub async fn delete_sandbox_image(
    options: SandboxImageCatalogOptionsJs,
    reference: String,
) -> Result<Option<SandboxImageLegacyCleanupFailureJs>> {
    let catalog = sandbox_image_catalog(options)?;
    retry(catalog, RetryPolicy::non_idempotent(), |catalog| {
        let reference = reference.clone();
        async move {
            let deletion = catalog.delete_reporting(&reference).await?;
            Ok(deletion.failed_legacy_cleanup().map(|(id, error)| {
                SandboxImageLegacyCleanupFailureJs {
                    legacy_image_id: id.map(str::to_string),
                    error: error.to_string(),
                }
            }))
        }
    })
    .await
    .map_err(sandbox::into_napi_error)
}
