//! napi-rs bindings for the Tensorlake Rust cloud SDK, consumed by the
//! TypeScript SDK.
//!
//! Mirrors the surface exposed by `crates/rust-cloud-sdk-py` to Python so
//! both language SDKs delegate to the same Rust implementation. Today this
//! exposes only the sandbox-image build entry point — additional functions
//! can be added as further TS code paths are consolidated.

#![deny(clippy::all)]

use std::path::PathBuf;

use napi::{
    Result,
    bindgen_prelude::*,
    threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode},
};
use napi_derive::napi;
use tensorlake::sandbox_images::{
    SandboxImageBuildEvent, SandboxImageBuildOptions, build_sandbox_image as rust_build_sandbox_image,
};

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
    pub dockerfile_text: Option<String>,
    pub context_dir: Option<String>,
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
/// `emit`, if provided, is invoked for each progress event. Errors thrown
/// from inside the callback are swallowed (`ErrorStrategy::Fatal`).
#[napi]
pub async fn build_sandbox_image(
    options: SandboxImageBuildOptionsJs,
    emit: Option<ThreadsafeFunction<SandboxImageBuildEventJs, ErrorStrategy::Fatal>>,
) -> Result<String> {
    let build_options = SandboxImageBuildOptions {
        api_url: options.api_url,
        bearer_token: options.bearer_token,
        use_scope_headers: options.use_scope_headers.unwrap_or(false),
        organization_id: options.organization_id,
        project_id: options.project_id,
        namespace: options.namespace.unwrap_or_else(|| "default".to_string()),
        dockerfile_path: PathBuf::from(options.dockerfile_path),
        dockerfile_text: options.dockerfile_text,
        context_dir: options.context_dir.map(PathBuf::from),
        registered_name: options.registered_name,
        disk_mb: options.disk_mb.map(u64::from),
        builder_disk_mb: options.builder_disk_mb.map(u64::from),
        cpus: options.cpus,
        memory_mb: options.memory_mb,
        is_public: options.is_public.unwrap_or(false),
        user_agent: options.user_agent,
    };

    let result = rust_build_sandbox_image(build_options, move |event| {
        if let Some(tsfn) = emit.as_ref() {
            tsfn.call(event_to_js(event), ThreadsafeFunctionCallMode::Blocking);
        }
    })
    .await
    .map_err(|error| Error::from_reason(error.to_string()))?;

    serde_json::to_string(&result).map_err(|error| Error::from_reason(error.to_string()))
}
