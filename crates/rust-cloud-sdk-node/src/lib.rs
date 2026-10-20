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

use std::path::PathBuf;

use napi::{
    Result,
    bindgen_prelude::*,
    threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode},
};
use napi_derive::napi;
use tensorlake::{
    retry::{RetryPolicy, retry},
    sandbox_image_catalog::SandboxImageCatalog,
    sandbox_images::{
        CommonBuildOptions, SandboxImageBuildEvent, SandboxImageBuildOptions,
        SandboxImageImportOptions, build_sandbox_image as rust_build_sandbox_image,
        import_sandbox_image as rust_import_sandbox_image,
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
    /// Opt into the non-default CAS (content-addressed streaming) image
    /// format.
    pub cas: Option<bool>,
    /// Dockerfile build args forwarded to BuildKit inside the builder. CAS
    /// builds only.
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
    /// Opt into the non-default CAS (content-addressed streaming) image
    /// format.
    pub cas: Option<bool>,
}

#[napi(object)]
pub struct SandboxImageBuildEventJs {
    /// One of `"status"`, `"build_log"`, `"warning"`. Matches the event
    /// vocabulary the PyO3 binding uses on the Python side.
    pub event_type: String,
    /// Only set for `"build_log"` events; carries `"stdout"` or `"stderr"`.
    pub stream: Option<String>,
    pub message: String,
}

fn event_to_js(event: SandboxImageBuildEvent) -> SandboxImageBuildEventJs {
    match event {
        SandboxImageBuildEvent::Status(message) => SandboxImageBuildEventJs {
            event_type: "status".to_string(),
            stream: None,
            message,
        },
        SandboxImageBuildEvent::BuildLog { stream, message } => SandboxImageBuildEventJs {
            event_type: "build_log".to_string(),
            stream: Some(stream),
            message,
        },
        SandboxImageBuildEvent::Warning(message) => SandboxImageBuildEventJs {
            event_type: "warning".to_string(),
            stream: None,
            message,
        },
    }
}

/// Build a sandbox image. Returns a JSON-encoded string with the registered
/// sandbox-template metadata; the TS SDK is expected to parse it.
///
/// `emit`, if provided, is invoked for each progress event. The TypeScript
/// wrapper must catch user callback exceptions before they cross this napi
/// threadsafe callback boundary.
#[napi]
pub async fn build_sandbox_image(
    options: SandboxImageBuildOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
) -> Result<String> {
    let build_options = SandboxImageBuildOptions {
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
            cas: options.cas.unwrap_or(false),
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
    };

    let result =
        rust_build_sandbox_image(build_options, options.docker_config_json, move |event| {
            if let Some(tsfn) = emit.as_ref() {
                tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
            }
        })
        .await
        .map_err(|error| Error::from_reason(error.to_string()))?;

    serde_json::to_string(&result).map_err(|error| Error::from_reason(error.to_string()))
}

/// Import a registry image directly into a sandbox image (no Dockerfile, no
/// Docker daemon). Returns a JSON-encoded string with the registered
/// sandbox-template metadata; the TS SDK is expected to parse it.
///
/// `emit`, if provided, is invoked for each progress event. The TypeScript
/// wrapper must catch user callback exceptions before they cross this napi
/// threadsafe callback boundary.
#[napi]
pub async fn import_sandbox_image(
    options: SandboxImageImportOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
) -> Result<String> {
    let import_options = SandboxImageImportOptions {
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
            cas: options.cas.unwrap_or(false),
            user_agent: options.user_agent,
            docker_compat: options.docker_compat.unwrap_or(false),
        },
        image_reference: options.image_reference,
    };

    let result =
        rust_import_sandbox_image(import_options, options.docker_config_json, move |event| {
            if let Some(tsfn) = emit.as_ref() {
                tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
            }
        })
        .await
        .map_err(|error| Error::from_reason(error.to_string()))?;

    serde_json::to_string(&result).map_err(|error| Error::from_reason(error.to_string()))
}

/// Credentials and scope for the merged image catalog (ADR 0107): the
/// platform template registry plus the Image Service behind one lookup,
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

/// Delete one sandbox image by name or id. Not found in either catalog is a
/// 404 `remote_api` error. Replayed only when the request provably never
/// reached a server.
#[napi]
pub async fn delete_sandbox_image(
    options: SandboxImageCatalogOptionsJs,
    reference: String,
) -> Result<()> {
    let catalog = sandbox_image_catalog(options)?;
    retry(catalog, RetryPolicy::non_idempotent(), |catalog| {
        let reference = reference.clone();
        async move { catalog.delete(&reference).await }
    })
    .await
    .map_err(sandbox::into_napi_error)
}
