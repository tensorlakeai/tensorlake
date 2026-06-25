//! napi-rs bindings for the Tensorlake sandbox lifecycle + proxy clients.
//!
//! Mirrors the surface `crates/rust-cloud-sdk-py` exposes to Python so both
//! language SDKs delegate to the same Rust implementation (and therefore share
//! its `reqwest` connection pool and HTTP/2 connection coalescing). Every
//! method is `async` here — Node has no use for the sync variants the Python
//! binding also exposes.
//!
//! Conventions (kept identical to the Python binding):
//! - JSON in / JSON out: request bodies and structured responses cross the
//!   boundary as JSON strings; `serde_json` handles (de)serialization.
//! - Bytes use `Buffer`.
//! - Every call surfaces the W3C `trace_id` alongside its payload.
//! - Errors are encoded as a JSON `{category, status, message}` string in the
//!   napi error reason so the TypeScript layer can rethrow typed errors
//!   (`SandboxNotFoundError`, `PoolInUseError`, ...).

use std::future::Future;
use std::time::Duration;

use napi::bindgen_prelude::Buffer;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::de::DeserializeOwned;
use serde_json::Value;

use tensorlake::sandboxes::models::{
    ArchivedSandboxesPaginationDirection, CreateSandboxRequest, ListArchivedSandboxesParams,
    SandboxPoolRequest, SnapshotType, UpdateSandboxRequest,
};
use tensorlake::sandboxes::{SandboxProxyClient, SandboxesClient};
use tensorlake::{ClientBuilder, error::SdkError};

// ---- Return value objects -------------------------------------------------

/// A JSON payload paired with the request's W3C trace id. `json` is the
/// JSON-encoded response body; the TS layer parses it.
#[napi(object)]
pub struct TracedJson {
    pub trace_id: String,
    pub json: String,
}

/// A binary payload paired with the request's W3C trace id.
#[napi(object)]
pub struct TracedBytes {
    pub trace_id: String,
    pub data: Buffer,
}

/// A list of JSON-encoded events paired with the request's W3C trace id.
#[napi(object)]
pub struct TracedEvents {
    pub trace_id: String,
    pub events: Vec<String>,
}

// ---- Error encoding -------------------------------------------------------

fn make_napi_error(category: &str, status: Option<u16>, message: String) -> napi::Error {
    let payload = serde_json::json!({
        "category": category,
        "status": status,
        "message": message,
    });
    napi::Error::from_reason(payload.to_string())
}

/// Map an [`SdkError`] to a structured napi error. Mirrors the Python binding's
/// `into_sandbox_py_error` so both SDKs classify failures identically.
fn into_napi_error(error: SdkError) -> napi::Error {
    match error {
        SdkError::Authentication(message) => make_napi_error("sdk_usage", Some(401), message),
        SdkError::Authorization(message) => make_napi_error("sdk_usage", Some(403), message),
        SdkError::ServerError { status, message } => {
            make_napi_error("remote_api", Some(status.as_u16()), message)
        }
        SdkError::Http(http_error) => {
            if http_error.is_timeout() {
                make_napi_error("connection", Some(504), http_error.to_string())
            } else if http_error.is_connect() {
                make_napi_error("connection", Some(503), http_error.to_string())
            } else {
                make_napi_error("internal", None, http_error.to_string())
            }
        }
        SdkError::Middleware(middleware_error) => {
            let message = middleware_error.to_string();
            let lower = message.to_lowercase();
            if lower.contains("timeout") || lower.contains("connect") {
                make_napi_error("connection", None, message)
            } else {
                make_napi_error("internal", None, message)
            }
        }
        other => make_napi_error("internal", None, other.to_string()),
    }
}

fn usage_error(message: String) -> napi::Error {
    make_napi_error("sdk_usage", None, message)
}

// ---- Retry helpers (ported from the Python binding) -----------------------

fn is_retryable(error: &SdkError) -> bool {
    match error {
        SdkError::ServerError { status, .. } => {
            *status == reqwest::StatusCode::BAD_GATEWAY
                || *status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                || *status == reqwest::StatusCode::GATEWAY_TIMEOUT
        }
        SdkError::Http(http_error) => http_error.is_connect() || http_error.is_timeout(),
        SdkError::Middleware(middleware_error) => {
            let source = middleware_error.to_string().to_lowercase();
            source.contains("timeout") || source.contains("connect")
        }
        SdkError::EventSourceError(_) => true,
        _ => false,
    }
}

fn calculate_sleep_time(retries: usize) -> f64 {
    let initial_delay_seconds: f64 = 0.1;
    let max_delay_seconds: f64 = 15.0;
    let jitter_multiplier: f64 = 0.75;
    let base_delay = initial_delay_seconds * 2f64.powi(retries as i32);
    base_delay.min(max_delay_seconds) * jitter_multiplier
}

async fn retry_async_op<C, T, F, Fut>(client: C, max_retries: usize, op: F) -> Result<T, SdkError>
where
    C: Clone,
    F: Fn(C) -> Fut,
    Fut: Future<Output = Result<T, SdkError>>,
{
    let mut retries = 0usize;
    loop {
        match op(client.clone()).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !is_retryable(&err) || retries >= max_retries {
                    return Err(err);
                }
                retries += 1;
                let sleep_time = calculate_sleep_time(retries);
                tokio::time::sleep(Duration::from_secs_f64(sleep_time)).await;
            }
        }
    }
}

/// Run `op` with bounded retries, mapping any terminal error to a napi error.
async fn with_retry<C, T, F, Fut>(client: C, max_retries: usize, op: F) -> napi::Result<T>
where
    C: Clone,
    F: Fn(C) -> Fut,
    Fut: Future<Output = Result<T, SdkError>>,
{
    retry_async_op(client, max_retries, op)
        .await
        .map_err(into_napi_error)
}

// ---- Misc helpers (ported from the Python binding) ------------------------

fn duration_from_seconds(name: &str, seconds: f64) -> napi::Result<Duration> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(usage_error(format!(
            "{name} must be a positive finite number"
        )));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn parse_json_payload<T: DeserializeOwned>(request_json: &str) -> napi::Result<T> {
    serde_json::from_str(request_json)
        .map_err(|error| usage_error(format!("invalid JSON payload: {error}")))
}

fn is_localhost_api_url(api_url: &str) -> bool {
    reqwest::Url::parse(api_url)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
        .is_some_and(|host| host == "localhost" || host == "127.0.0.1")
}

/// Resolve the proxy base URL and routing headers for a sandbox connection.
///
/// Returns `(base_url, host_override, sandbox_id_header)`:
/// - localhost: `base_url` is the proxy URL as-is; `host_override` is
///   `{sandbox_id}.local`; no sandbox-id header.
/// - cloud: `base_url` is the apex ingress; the sandbox id is sent via the
///   `X-Tensorlake-Sandbox-Id` header.
fn resolve_proxy_target(
    proxy_url: &str,
    sandbox_id: &str,
) -> napi::Result<(String, Option<String>, Option<String>)> {
    let parsed = reqwest::Url::parse(proxy_url)
        .map_err(|error| usage_error(format!("invalid proxy url `{proxy_url}`: {error}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| usage_error(format!("proxy url `{proxy_url}` is missing a host")))?;

    if host == "localhost" || host == "127.0.0.1" {
        return Ok((
            proxy_url.trim_end_matches('/').to_string(),
            Some(format!("{sandbox_id}.local")),
            None,
        ));
    }

    let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    let base_url = format!("{}://{host}{port}", parsed.scheme());
    Ok((base_url, None, Some(sandbox_id.to_string())))
}

fn resolve_sandbox_lifecycle_url(api_url: &str) -> String {
    if is_localhost_api_url(api_url) {
        return api_url.to_string();
    }
    if let Ok(mut parsed) = reqwest::Url::parse(api_url)
        && let Some(host) = parsed.host_str()
        && let Some(rest) = host.strip_prefix("api.")
    {
        let new_host = format!("sandbox.{rest}");
        if parsed.set_host(Some(&new_host)).is_ok() {
            let mut result = parsed.to_string();
            if result.ends_with('/') {
                result.pop();
            }
            return result;
        }
    }
    "https://sandbox.tensorlake.ai".to_string()
}

fn parse_archived_sandboxes_params(
    limit: Option<u32>,
    cursor: Option<String>,
    direction: Option<String>,
) -> napi::Result<ListArchivedSandboxesParams> {
    let direction = match direction.as_deref() {
        None => None,
        Some("forward") => Some(ArchivedSandboxesPaginationDirection::Forward),
        Some("backward") => Some(ArchivedSandboxesPaginationDirection::Backward),
        Some(other) => {
            return Err(usage_error(format!(
                "invalid direction '{other}': expected 'forward' or 'backward'"
            )));
        }
    };
    Ok(ListArchivedSandboxesParams {
        limit: limit.map(|l| l as usize),
        cursor,
        direction,
    })
}

fn parse_snapshot_type(snapshot_type: Option<String>) -> napi::Result<Option<SnapshotType>> {
    match snapshot_type.as_deref() {
        None => Ok(None),
        Some("memory") => Ok(Some(SnapshotType::Memory)),
        Some("filesystem") => Ok(Some(SnapshotType::Filesystem)),
        Some(other) => Err(usage_error(format!(
            "invalid snapshot_type '{other}': expected 'memory' or 'filesystem'"
        ))),
    }
}

// ---- Sandbox lifecycle client ---------------------------------------------

/// Sandbox lifecycle, pool, and snapshot client. Owns the `reqwest`
/// connection pool; proxy clients minted via [`connect_proxy`] share it so
/// HTTP/2 connections are coalesced across every sandbox in a session.
#[napi]
pub struct NativeSandboxClient {
    client: SandboxesClient,
}

#[napi]
impl NativeSandboxClient {
    #[napi(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_url: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        namespace: Option<String>,
        user_agent: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> napi::Result<Self> {
        let lifecycle_url = resolve_sandbox_lifecycle_url(&api_url);
        let mut builder = ClientBuilder::new(&lifecycle_url);
        if let Some(token) = api_key.as_deref() {
            builder = builder.bearer_token(token);
        }
        if let (Some(org_id), Some(project_id)) =
            (organization_id.as_deref(), project_id.as_deref())
        {
            builder = builder.scope(org_id, project_id);
        }
        if let Some(ua) = user_agent.as_deref() {
            builder = builder.user_agent(ua);
        }
        if let Some(seconds) = request_timeout_sec {
            builder = builder.timeout(duration_from_seconds("request_timeout_sec", seconds)?);
        }

        let client = builder.build().map_err(into_napi_error)?;
        let use_namespaced_endpoints = is_localhost_api_url(&api_url);
        let sandboxes_client = SandboxesClient::new(
            client,
            namespace.unwrap_or_else(|| "default".to_string()),
            use_namespaced_endpoints,
        );

        Ok(Self {
            client: sandboxes_client,
        })
    }

    /// Create a proxy client for `sandbox_id` that shares this client's
    /// connection pool. Only the first sandbox in a session pays the TCP+TLS
    /// handshake; subsequent ones reuse the coalesced HTTP/2 connection.
    #[napi]
    pub fn connect_proxy(
        &self,
        proxy_url: String,
        sandbox_id: String,
        routing_hint: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> napi::Result<NativeSandboxProxyClient> {
        let (base_url, host_override, sandbox_id_header) =
            resolve_proxy_target(&proxy_url, &sandbox_id)?;
        let shared_client = if let Some(seconds) = request_timeout_sec {
            self.client
                .http_client()
                .with_base_url_and_timeout(
                    &base_url,
                    Some(duration_from_seconds("request_timeout_sec", seconds)?),
                )
                .map_err(into_napi_error)?
        } else {
            self.client
                .http_client()
                .with_base_url_without_timeout(&base_url)
                .map_err(into_napi_error)?
        };
        let proxy = SandboxProxyClient::new(shared_client, host_override)
            .with_sandbox_id(sandbox_id_header)
            .with_routing_hint(routing_hint);
        Ok(NativeSandboxProxyClient {
            client: proxy,
            base_url,
        })
    }

    #[napi]
    pub async fn create_sandbox(&self, request_json: String) -> napi::Result<TracedJson> {
        let request: CreateSandboxRequest = parse_json_payload(&request_json)?;
        let client = self.client.clone();
        // Create is not retried: it is not idempotent.
        let traced = client.create(&request).await.map_err(into_napi_error)?;
        let json =
            serde_json::to_string(&*traced).map_err(|e| into_napi_error(SdkError::from(e)))?;
        Ok(TracedJson {
            trace_id: traced.trace_id.clone(),
            json,
        })
    }

    #[napi]
    pub async fn claim_sandbox(&self, pool_id: String) -> napi::Result<TracedJson> {
        let client = self.client.clone();
        let traced = client.claim(&pool_id).await.map_err(into_napi_error)?;
        let json =
            serde_json::to_string(&*traced).map_err(|e| into_napi_error(SdkError::from(e)))?;
        Ok(TracedJson {
            trace_id: traced.trace_id.clone(),
            json,
        })
    }

    #[napi]
    pub async fn copy_sandbox(&self, sandbox_id: String, times: u32) -> napi::Result<TracedJson> {
        let client = self.client.clone();
        let traced = client
            .copy(&sandbox_id, times as usize)
            .await
            .map_err(into_napi_error)?;
        let json =
            serde_json::to_string(&*traced).map_err(|e| into_napi_error(SdkError::from(e)))?;
        Ok(TracedJson {
            trace_id: traced.trace_id.clone(),
            json,
        })
    }

    #[napi]
    pub async fn get_sandbox(&self, sandbox_id: String) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let traced = c.get(&sandbox_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn list_sandboxes(&self) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.list().await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&serde_json::json!({ "sandboxes": *traced }))?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn list_archived_sandboxes(
        &self,
        limit: Option<u32>,
        cursor: Option<String>,
        direction: Option<String>,
    ) -> napi::Result<TracedJson> {
        let params = parse_archived_sandboxes_params(limit, cursor, direction)?;
        with_retry(self.client.clone(), 5, move |c| {
            let params = params.clone();
            async move {
                let traced = c.list_archived(&params).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn get_archived_sandbox(&self, sandbox_id: String) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let traced = c.get_archived(&sandbox_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn update_sandbox(
        &self,
        sandbox_id: String,
        request_json: String,
    ) -> napi::Result<TracedJson> {
        let request: UpdateSandboxRequest = parse_json_payload(&request_json)?;
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            let request = request.clone();
            async move {
                let traced = c.update(&sandbox_id, &request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn delete_sandbox(&self, sandbox_id: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            async move { c.delete(&sandbox_id).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn suspend_sandbox(&self, sandbox_id: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            async move { c.suspend(&sandbox_id).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn resume_sandbox(&self, sandbox_id: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            async move { c.resume(&sandbox_id).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn attach_file_system(
        &self,
        sandbox_id: String,
        file_system_id: String,
        mount_path: String,
    ) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            let file_system_id = file_system_id.clone();
            let mount_path = mount_path.clone();
            async move {
                let traced = c
                    .attach_file_system(&sandbox_id, &file_system_id, &mount_path)
                    .await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn detach_file_system(
        &self,
        sandbox_id: String,
        mount_path: String,
    ) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            let mount_path = mount_path.clone();
            async move {
                let traced = c.detach_file_system(&sandbox_id, &mount_path).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn create_snapshot(
        &self,
        sandbox_id: String,
        snapshot_type: Option<String>,
    ) -> napi::Result<TracedJson> {
        let parsed_type = parse_snapshot_type(snapshot_type)?;
        with_retry(self.client.clone(), 5, move |c| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let traced = c.snapshot(&sandbox_id, parsed_type).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn get_snapshot(&self, snapshot_id: String) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| {
            let snapshot_id = snapshot_id.clone();
            async move {
                let traced = c.get_snapshot(&snapshot_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn list_snapshots(&self) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.list_snapshots().await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&serde_json::json!({ "snapshots": *traced }))?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn delete_snapshot(&self, snapshot_id: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let snapshot_id = snapshot_id.clone();
            async move { c.delete_snapshot(&snapshot_id).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn create_pool(&self, request_json: String) -> napi::Result<TracedJson> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        with_retry(self.client.clone(), 5, move |c| {
            let request = request.clone();
            async move {
                let traced = c.create_pool(&request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn get_pool(&self, pool_id: String) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| {
            let pool_id = pool_id.clone();
            async move {
                let traced = c.get_pool(&pool_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn list_pools(&self) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.list_pools().await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&serde_json::json!({ "pools": *traced }))?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn update_pool(
        &self,
        pool_id: String,
        request_json: String,
    ) -> napi::Result<TracedJson> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        with_retry(self.client.clone(), 5, move |c| {
            let pool_id = pool_id.clone();
            let request = request.clone();
            async move {
                let traced = c.update_pool(&pool_id, &request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn delete_pool(&self, pool_id: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let pool_id = pool_id.clone();
            async move { c.delete_pool(&pool_id).await.map(|t| t.trace_id) }
        })
        .await
    }
}

// ---- Sandbox proxy client -------------------------------------------------

/// Process / file / PTY client for a single running sandbox. Prefer
/// [`NativeSandboxClient::connect_proxy`] (shared pool) over the direct
/// constructor.
#[napi]
pub struct NativeSandboxProxyClient {
    client: SandboxProxyClient,
    base_url: String,
}

#[napi]
impl NativeSandboxProxyClient {
    #[napi(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        proxy_url: String,
        sandbox_id: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        routing_hint: Option<String>,
        user_agent: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> napi::Result<Self> {
        let (base_url, host_override, sandbox_id_header) =
            resolve_proxy_target(&proxy_url, &sandbox_id)?;

        let mut builder = ClientBuilder::new(&base_url);
        if let Some(token) = api_key.as_deref() {
            builder = builder.bearer_token(token);
        }
        if let (Some(org_id), Some(project_id)) =
            (organization_id.as_deref(), project_id.as_deref())
        {
            builder = builder.scope(org_id, project_id);
        }
        if let Some(ua) = user_agent.as_deref() {
            builder = builder.user_agent(ua);
        }
        if let Some(seconds) = request_timeout_sec {
            builder = builder.timeout(duration_from_seconds("request_timeout_sec", seconds)?);
        }

        let client = builder.build().map_err(into_napi_error)?;
        let proxy = SandboxProxyClient::new(client, host_override)
            .with_sandbox_id(sandbox_id_header)
            .with_routing_hint(routing_hint);

        Ok(Self {
            client: proxy,
            base_url,
        })
    }

    #[napi]
    pub fn base_url(&self) -> String {
        self.base_url.clone()
    }

    // -- Process management --

    #[napi]
    pub async fn start_process(&self, payload_json: String) -> napi::Result<TracedJson> {
        let payload: Value = parse_json_payload(&payload_json)?;
        with_retry(self.client.clone(), 5, move |c| {
            let payload = payload.clone();
            async move {
                let traced = c.start_process(&payload).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn list_processes(&self) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.list_processes().await?;
            let trace_id = traced.trace_id.clone();
            let processes = traced.into_inner();
            let json = serde_json::to_string(&serde_json::json!({ "processes": processes }))?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn get_process(&self, pid: i32) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.get_process(pid as i64).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn kill_process(&self, pid: i32) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| async move {
            c.kill_process(pid as i64).await.map(|t| t.trace_id)
        })
        .await
    }

    #[napi]
    pub async fn restart_process(&self, pid: i32) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.restart_process(pid as i64).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn send_signal(&self, pid: i32, signal: i32) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.send_signal(pid as i64, signal as i64).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn write_stdin(&self, pid: i32, data: Buffer) -> napi::Result<String> {
        let bytes = data.to_vec();
        with_retry(self.client.clone(), 5, move |c| {
            let bytes = bytes.clone();
            async move { c.write_stdin(pid as i64, bytes).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn close_stdin(&self, pid: i32) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| async move {
            c.close_stdin(pid as i64).await.map(|t| t.trace_id)
        })
        .await
    }

    #[napi]
    pub async fn get_stdout(&self, pid: i32) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.get_stdout(pid as i64).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn get_stderr(&self, pid: i32) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.get_stderr(pid as i64).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn get_output(&self, pid: i32) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.get_output(pid as i64).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    // -- Streaming output (events delivered live via `emit`) --

    #[napi]
    pub async fn follow_stdout(
        &self,
        pid: i32,
        emit: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    ) -> napi::Result<String> {
        self.client
            .clone()
            .follow_stdout_streaming(pid as i64, move |event| emit_event(&emit, event))
            .await
            .map_err(into_napi_error)
    }

    #[napi]
    pub async fn follow_stderr(
        &self,
        pid: i32,
        emit: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    ) -> napi::Result<String> {
        self.client
            .clone()
            .follow_stderr_streaming(pid as i64, move |event| emit_event(&emit, event))
            .await
            .map_err(into_napi_error)
    }

    #[napi]
    pub async fn follow_output(
        &self,
        pid: i32,
        emit: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    ) -> napi::Result<String> {
        self.client
            .clone()
            .follow_output_streaming(pid as i64, move |event| emit_event(&emit, event))
            .await
            .map_err(into_napi_error)
    }

    /// Start a process and stream lifecycle events live via `emit`. Resolves
    /// with the trace id once the process exits.
    #[napi]
    pub async fn run_process_streaming(
        &self,
        payload_json: String,
        emit: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    ) -> napi::Result<String> {
        let payload: Value = parse_json_payload(&payload_json)?;
        self.client
            .clone()
            .run_process_streaming(&payload, move |event| emit_event(&emit, event))
            .await
            .map_err(into_napi_error)
    }

    /// Start a process and buffer all lifecycle events, returning them once the
    /// process exits. Convenience for `run()`-style "to completion" callers.
    #[napi]
    pub async fn run_process(&self, payload_json: String) -> napi::Result<TracedEvents> {
        let payload: Value = parse_json_payload(&payload_json)?;
        // Not retried: running a process is not idempotent.
        let traced = self
            .client
            .clone()
            .run_process(&payload)
            .await
            .map_err(into_napi_error)?;
        let trace_id = traced.trace_id.clone();
        let events = traced
            .into_inner()
            .into_iter()
            .map(|event| serde_json::to_string(&event))
            .collect::<Result<Vec<String>, serde_json::Error>>()
            .map_err(|e| into_napi_error(SdkError::from(e)))?;
        Ok(TracedEvents { trace_id, events })
    }

    // -- File operations --

    #[napi]
    pub async fn read_file(&self, path: String) -> napi::Result<TracedBytes> {
        let (trace_id, data): (String, Vec<u8>) = with_retry(self.client.clone(), 5, move |c| {
            let path = path.clone();
            async move {
                let traced = c.read_file(&path).await?;
                Ok((traced.trace_id.clone(), traced.into_inner()))
            }
        })
        .await?;
        Ok(TracedBytes {
            trace_id,
            data: data.into(),
        })
    }

    #[napi]
    pub async fn write_file(&self, path: String, content: Buffer) -> napi::Result<String> {
        let bytes = content.to_vec();
        with_retry(self.client.clone(), 5, move |c| {
            let path = path.clone();
            let bytes = bytes.clone();
            async move { c.write_file(&path, bytes).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn upload_file(&self, path: String, local_path: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let path = path.clone();
            let local_path = local_path.clone();
            async move { c.upload_file(&path, local_path).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn delete_file(&self, path: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let path = path.clone();
            async move { c.delete_file(&path).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn list_directory(&self, path: String) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| {
            let path = path.clone();
            async move {
                let traced = c.list_directory(&path).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    // -- PTY + system --

    #[napi]
    pub async fn create_pty_session(&self, payload_json: String) -> napi::Result<TracedJson> {
        let payload: Value = parse_json_payload(&payload_json)?;
        with_retry(self.client.clone(), 5, move |c| {
            let payload = payload.clone();
            async move {
                let traced = c.create_pty_session(&payload).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn delete_pty_session(&self, session_id: String) -> napi::Result<String> {
        with_retry(self.client.clone(), 5, move |c| {
            let session_id = session_id.clone();
            async move { c.delete_pty_session(&session_id).await.map(|t| t.trace_id) }
        })
        .await
    }

    #[napi]
    pub async fn health(&self) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.health().await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }

    #[napi]
    pub async fn info(&self) -> napi::Result<TracedJson> {
        with_retry(self.client.clone(), 5, move |c| async move {
            let traced = c.info().await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        })
        .await
    }
}

/// Serialize one streaming event and push it across the napi threadsafe
/// boundary. Serialization failures (which should not occur for the SDK's own
/// event types) drop the event rather than aborting the stream.
fn emit_event<E: serde::Serialize>(
    emit: &ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    event: E,
) {
    if let Ok(serialized) = serde_json::to_string(&event) {
        emit.call(serialized, ThreadsafeFunctionCallMode::Blocking);
    }
}
