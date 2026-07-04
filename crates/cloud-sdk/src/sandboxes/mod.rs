pub mod desktop;
pub mod models;

use eventsource_stream::Eventsource;
use futures::{StreamExt, TryStreamExt};
use reqwest::Method;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, CONTENT_LENGTH};
use serde_json::Value;
use std::path::Path;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::{
    client::{
        Client, Traced, build_bytes_post_request_from_builder,
        build_empty_post_request_from_builder,
    },
    error::SdkError,
};
pub use desktop::SandboxDesktopClient;

use models::{
    ArchivedSandboxInfo, ArchivedSandboxesPaginationDirection, CopySandboxResponse,
    CreateSandboxPoolResponse, CreateSandboxRequest, CreateSandboxResponse, CreateSnapshotRequest,
    CreateSnapshotResponse, DaemonInfo, DetachFileSystemRequest, FileSystemMount,
    GetSandboxLogsRequest, HealthResponse, ListArchivedSandboxesParams,
    ListArchivedSandboxesResponse, ListDirectoryResponse, ListProcessesResponse,
    ListSandboxPoolsResponse, ListSandboxesResponse, ListSnapshotsResponse, OutputEvent,
    OutputResponse, ProcessInfo, RunProcessEvent, SandboxInfo, SandboxLogsResponse,
    SandboxPoolInfo, SandboxPoolRequest, SandboxProcessLogFiltersResponse, SendSignalResponse,
    SignBlobRequest, SnapshotInfo, SnapshotType, UpdateSandboxRequest,
};

pub const DEFAULT_SANDBOX_PROXY_URL: &str = "https://sandbox.tensorlake.ai";
pub const SANDBOX_MANAGEMENT_PORT: u16 = 9501;

/// A reference to a sandbox process: either its OS **pid** or a managed-process **name**
/// given at creation. This is the single place the pid/name path segment is built, reused by
/// the Rust SDK, the Python/Node bindings, and the CLI.
#[derive(Debug, Clone)]
pub enum ProcessRef {
    Pid(u64),
    Name(String),
}

impl ProcessRef {
    /// The `{pid_or_name}` path segment. A pid is a bare decimal; a name is **percent-encoded**
    /// so arbitrary characters (spaces, punctuation) survive as a single path segment. `/`
    /// can't appear in a valid name (see [`validate_managed_name`]), so encoded slashes never
    /// arise. The daemon's `Path<String>` extractor percent-decodes before matching the name.
    pub fn to_path_segment(&self) -> String {
        match self {
            ProcessRef::Pid(pid) => pid.to_string(),
            ProcessRef::Name(name) => urlencoding::encode(name).into_owned(),
        }
    }
}

impl From<u64> for ProcessRef {
    fn from(pid: u64) -> Self {
        ProcessRef::Pid(pid)
    }
}
impl From<u32> for ProcessRef {
    fn from(pid: u32) -> Self {
        ProcessRef::Pid(pid as u64)
    }
}
impl From<i64> for ProcessRef {
    fn from(pid: i64) -> Self {
        ProcessRef::Pid(pid as u64)
    }
}
impl From<&str> for ProcessRef {
    fn from(s: &str) -> Self {
        // An all-ASCII-digit string is a pid; anything else is a managed name. This mirrors
        // the daemon's route disambiguation, so a stringified pid still hits the pid branch.
        if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(pid) = s.parse::<u64>() {
                return ProcessRef::Pid(pid);
            }
        }
        ProcessRef::Name(s.to_string())
    }
}
impl From<String> for ProcessRef {
    fn from(s: String) -> Self {
        ProcessRef::from(s.as_str())
    }
}
impl From<&String> for ProcessRef {
    fn from(s: &String) -> Self {
        ProcessRef::from(s.as_str())
    }
}

/// Validate a user-supplied managed-process name. **Single source of truth** for the rule
/// across the Rust SDK, the Python/Node bindings, and the CLI (the daemon keeps a
/// byte-identical copy in its own repo). Permissive on purpose -- a name may contain any
/// characters (spaces, punctuation, unicode; clients percent-encode the path segment) with
/// only three rejections:
///   1. empty (no path segment),
///   2. contains `/` (an encoded slash is unreliable across the proxy/router chain), and
///   3. all ASCII digits (reserved for PID addressing on the shared `/processes/{pid_or_name}`
///      route -- this is the one residual backward-incompatibility; see release notes).
pub fn validate_managed_name(name: &str) -> Result<(), SdkError> {
    if name.is_empty() {
        return Err(SdkError::ClientError(
            "managed process name must not be empty".to_string(),
        ));
    }
    if name.contains('/') {
        return Err(SdkError::ClientError(
            "managed process name must not contain '/'".to_string(),
        ));
    }
    if name.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SdkError::ClientError(
            "managed process name must not be all digits; numeric strings are reserved for PID addressing"
                .to_string(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxProxyTarget {
    pub base_url: String,
    pub host_override: Option<String>,
    pub sandbox_id_header: Option<String>,
}

pub fn sandbox_url_from_ingress_endpoint(
    ingress_endpoint: &str,
    sandbox_id: &str,
    port: Option<u16>,
) -> Result<String, SdkError> {
    let parsed = reqwest::Url::parse(ingress_endpoint).map_err(|error| {
        SdkError::ClientError(format!(
            "invalid ingress endpoint `{ingress_endpoint}`: {error}"
        ))
    })?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(SdkError::ClientError(
            "ingress_endpoint must be an absolute http(s) URL".to_string(),
        ));
    }
    let host = parsed.host_str().ok_or_else(|| {
        SdkError::ClientError(format!(
            "ingress endpoint `{ingress_endpoint}` is missing a host"
        ))
    })?;
    let label = match port {
        Some(port) if port != SANDBOX_MANAGEMENT_PORT => format!("{port}-{sandbox_id}"),
        _ => sandbox_id.to_string(),
    };
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let port_part = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    Ok(format!("{}://{label}.{host}{port_part}", parsed.scheme()))
}

pub fn resolve_default_sandbox_proxy_url(api_url: &str) -> String {
    if let Ok(url) = std::env::var("TENSORLAKE_SANDBOX_PROXY_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if is_localhost_api_url(api_url) {
        return "http://localhost:9443".to_string();
    }
    if let Ok(parsed) = reqwest::Url::parse(api_url)
        && let Some(host) = parsed.host_str()
        && let Some(rest) = host.strip_prefix("api.")
    {
        return format!("{}://sandbox.{rest}", parsed.scheme());
    }
    DEFAULT_SANDBOX_PROXY_URL.to_string()
}

pub fn select_sandbox_proxy_url(
    _api_url: &str,
    _sandbox_id: &str,
    server_sandbox_url: Option<&str>,
    _server_ingress_endpoint: Option<&str>,
    explicit_proxy_url: Option<&str>,
) -> Result<String, SdkError> {
    if let Some(url) = non_empty(server_sandbox_url) {
        return Ok(url.to_string());
    }
    if let Some(url) = non_empty(explicit_proxy_url) {
        return Ok(url.to_string());
    }
    Err(SdkError::ClientError(
        "server response did not include sandbox_url; refusing to derive a proxy URL".to_string(),
    ))
}

pub fn resolve_sandbox_proxy_target(
    proxy_url: &str,
    sandbox_id: &str,
) -> Result<SandboxProxyTarget, SdkError> {
    let parsed = reqwest::Url::parse(proxy_url).map_err(|error| {
        SdkError::ClientError(format!("invalid proxy url `{proxy_url}`: {error}"))
    })?;
    let host = parsed.host_str().ok_or_else(|| {
        SdkError::ClientError(format!("proxy url `{proxy_url}` is missing a host"))
    })?;

    if host == "localhost" || host == "127.0.0.1" {
        return Ok(SandboxProxyTarget {
            base_url: proxy_url.trim_end_matches('/').to_string(),
            host_override: Some(format!("{sandbox_id}.local")),
            sandbox_id_header: None,
        });
    }

    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    Ok(SandboxProxyTarget {
        base_url: format!("{}://{host}{port}", parsed.scheme()),
        host_override: None,
        sandbox_id_header: Some(sandbox_id.to_string()),
    })
}

pub fn sandbox_proxy_hostname(proxy_url: &str) -> Result<String, SdkError> {
    let parsed = reqwest::Url::parse(proxy_url).map_err(|error| {
        SdkError::ClientError(format!("invalid sandbox url `{proxy_url}`: {error}"))
    })?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(SdkError::ClientError(
            "sandbox_url must be an absolute http(s) URL".to_string(),
        ));
    }
    parsed.host_str().map(str::to_string).ok_or_else(|| {
        SdkError::ClientError(format!("sandbox url `{proxy_url}` is missing a host"))
    })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn is_localhost_api_url(api_url: &str) -> bool {
    reqwest::Url::parse(api_url)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
        .is_some_and(|host| host == "localhost" || host == "127.0.0.1")
}

/// A client for managing sandbox lifecycle, pool, and snapshot APIs.
#[derive(Clone)]
pub struct SandboxesClient {
    client: Client,
    log_client: Client,
    namespace: String,
    use_namespaced_endpoints: bool,
}

impl SandboxesClient {
    /// Create a new sandboxes client.
    ///
    /// If `use_namespaced_endpoints` is true, requests are sent to
    /// `/v1/namespaces/{namespace}/...`; otherwise to `/{endpoint}`.
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        use_namespaced_endpoints: bool,
    ) -> Self {
        Self {
            log_client: client.clone(),
            client,
            namespace: namespace.into(),
            use_namespaced_endpoints,
        }
    }

    /// Use a separate client/base URL for log-reader endpoints.
    pub fn with_log_client(mut self, log_client: Client) -> Self {
        self.log_client = log_client;
        self
    }

    pub fn http_client(&self) -> &Client {
        &self.client
    }

    fn endpoint(&self, endpoint: &str) -> String {
        if self.use_namespaced_endpoints {
            format!("/v1/namespaces/{}/{}", self.namespace, endpoint)
        } else {
            format!("/{endpoint}")
        }
    }

    fn log_endpoint(&self, endpoint: &str) -> String {
        format!("/v1/namespaces/{}/{}", self.namespace, endpoint)
    }

    pub async fn create(
        &self,
        request: &CreateSandboxRequest,
    ) -> Result<Traced<CreateSandboxResponse>, SdkError> {
        let uri = self.endpoint("sandboxes");
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        self.client
            .execute_json_allow_status(req, &[StatusCode::GATEWAY_TIMEOUT])
            .await
    }

    pub async fn claim(&self, pool_id: &str) -> Result<Traced<CreateSandboxResponse>, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}/sandboxes"));
        let req = self.client.build_empty_post_request(&uri)?;
        self.client
            .execute_json_allow_status(req, &[StatusCode::GATEWAY_TIMEOUT])
            .await
    }

    pub async fn copy(
        &self,
        sandbox_id: &str,
        times: usize,
    ) -> Result<Traced<CopySandboxResponse>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/copy"));
        let req = build_empty_post_request_from_builder(
            self.client
                .request(Method::POST, &uri)
                .query(&[("times", times)]),
        )?;
        self.client
            .execute_json_allow_status(
                req,
                &[
                    StatusCode::UNPROCESSABLE_ENTITY,
                    StatusCode::GATEWAY_TIMEOUT,
                ],
            )
            .await
    }

    pub async fn get(&self, sandbox_id: &str) -> Result<Traced<SandboxInfo>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        self.client.execute_json(req).await
    }

    pub async fn list(&self) -> Result<Traced<Vec<SandboxInfo>>, SdkError> {
        let uri = self.endpoint("sandboxes");
        let req = self.client.request(Method::GET, &uri).build()?;
        Ok(self
            .client
            .execute_json::<ListSandboxesResponse>(req)
            .await?
            .map(|r| r.sandboxes))
    }

    /// List archived (terminated) sandboxes in the namespace. Archived sandboxes
    /// live in their own column family until the server-configured TTL expires.
    pub async fn list_archived(
        &self,
        params: &ListArchivedSandboxesParams,
    ) -> Result<Traced<ListArchivedSandboxesResponse>, SdkError> {
        let uri = self.endpoint("archived-sandboxes");
        let mut request_builder = self.client.request(Method::GET, &uri);
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(limit) = params.limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(cursor) = params.cursor.as_deref() {
            query.push(("cursor", cursor.to_string()));
        }
        if let Some(direction) = params.direction {
            let value = match direction {
                ArchivedSandboxesPaginationDirection::Forward => "forward",
                ArchivedSandboxesPaginationDirection::Backward => "backward",
            };
            query.push(("direction", value.to_string()));
        }
        if !query.is_empty() {
            request_builder = request_builder.query(&query);
        }
        let req = request_builder.build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_archived(
        &self,
        sandbox_id: &str,
    ) -> Result<Traced<ArchivedSandboxInfo>, SdkError> {
        let uri = self.endpoint(&format!("archived-sandboxes/{sandbox_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_logs(
        &self,
        request: &GetSandboxLogsRequest,
    ) -> Result<Traced<SandboxLogsResponse>, SdkError> {
        let uri = self.log_endpoint(&format!("sandboxes/{}/logs", request.sandbox_id));
        let mut req_builder = self.log_client.request(Method::GET, &uri);

        for level in &request.levels {
            req_builder = req_builder.query(&[("level", level.as_i8())]);
        }
        for process_id in &request.process_ids {
            req_builder = req_builder.query(&[("processId", process_id)]);
        }
        if let Some(ref param_value) = request.next_token {
            req_builder = req_builder.query(&[("nextToken", param_value)]);
        }
        if let Some(param_value) = request.head {
            req_builder = req_builder.query(&[("head", param_value)]);
        }
        if let Some(param_value) = request.tail {
            req_builder = req_builder.query(&[("tail", param_value)]);
        }
        if let Some(ref param_value) = request.body {
            req_builder = req_builder.query(&[("body", param_value)]);
        }

        let req = req_builder.build()?;
        self.log_client.execute_json(req).await
    }

    pub async fn list_log_processes(
        &self,
        sandbox_id: &str,
    ) -> Result<Traced<SandboxProcessLogFiltersResponse>, SdkError> {
        let uri = self.log_endpoint(&format!("sandboxes/{sandbox_id}/processes"));
        let req = self.log_client.request(Method::GET, &uri).build()?;
        self.log_client.execute_json(req).await
    }

    pub async fn update(
        &self,
        sandbox_id: &str,
        request: &UpdateSandboxRequest,
    ) -> Result<Traced<SandboxInfo>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self
            .client
            .build_post_json_request(Method::PATCH, &uri, request)?;
        self.client.execute_json(req).await
    }

    pub async fn delete(&self, sandbox_id: &str) -> Result<Traced<()>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn suspend(&self, sandbox_id: &str) -> Result<Traced<()>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/suspend"));
        let req = self.client.build_empty_post_request(&uri)?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn resume(&self, sandbox_id: &str) -> Result<Traced<()>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/resume"));
        let req = self.client.build_empty_post_request(&uri)?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    /// Attach a registered file system to a running sandbox at `mount_path`.
    ///
    /// The mount completes asynchronously on the dataplane; the returned
    /// [`SandboxInfo`] already reflects the new entry in `file_systems`.
    pub async fn attach_file_system(
        &self,
        sandbox_id: &str,
        file_system_id: &str,
        mount_path: &str,
    ) -> Result<Traced<SandboxInfo>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/file_systems"));
        let body = FileSystemMount {
            file_system_id: file_system_id.to_string(),
            mount_path: mount_path.to_string(),
        };
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, &body)?;
        self.client.execute_json(req).await
    }

    /// Detach the file system mounted at `mount_path` from a running sandbox.
    ///
    /// The unmount completes asynchronously on the dataplane; the returned
    /// [`SandboxInfo`] already reflects the removed `file_systems` entry.
    pub async fn detach_file_system(
        &self,
        sandbox_id: &str,
        mount_path: &str,
    ) -> Result<Traced<SandboxInfo>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/file_systems"));
        let body = DetachFileSystemRequest {
            mount_path: mount_path.to_string(),
        };
        let req = self
            .client
            .build_post_json_request(Method::DELETE, &uri, &body)?;
        self.client.execute_json(req).await
    }

    pub async fn snapshot(
        &self,
        sandbox_id: &str,
        snapshot_type: Option<SnapshotType>,
    ) -> Result<Traced<CreateSnapshotResponse>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/snapshot"));
        let req = if snapshot_type.is_some() {
            let body = CreateSnapshotRequest { snapshot_type };
            self.client
                .build_post_json_request(Method::POST, &uri, &body)?
        } else {
            // Preserve today's wire shape (no body) for callers that don't set a snapshot type.
            self.client.build_empty_post_request(&uri)?
        };
        self.client.execute_json(req).await
    }

    pub async fn get_snapshot(&self, snapshot_id: &str) -> Result<Traced<SnapshotInfo>, SdkError> {
        let uri = self.endpoint(&format!("snapshots/{snapshot_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        self.client.execute_json(req).await
    }

    pub async fn list_snapshots(&self) -> Result<Traced<Vec<SnapshotInfo>>, SdkError> {
        let uri = self.endpoint("snapshots");
        let req = self.client.request(Method::GET, &uri).build()?;
        Ok(self
            .client
            .execute_json::<ListSnapshotsResponse>(req)
            .await?
            .map(|r| r.snapshots))
    }

    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<Traced<()>, SdkError> {
        let uri = self.endpoint(&format!("snapshots/{snapshot_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn create_pool(
        &self,
        request: &SandboxPoolRequest,
    ) -> Result<Traced<CreateSandboxPoolResponse>, SdkError> {
        let uri = self.endpoint("sandbox-pools");
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        self.client.execute_json(req).await
    }

    pub async fn get_pool(&self, pool_id: &str) -> Result<Traced<SandboxPoolInfo>, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        self.client.execute_json(req).await
    }

    pub async fn list_pools(&self) -> Result<Traced<Vec<SandboxPoolInfo>>, SdkError> {
        let uri = self.endpoint("sandbox-pools");
        let req = self.client.request(Method::GET, &uri).build()?;
        Ok(self
            .client
            .execute_json::<ListSandboxPoolsResponse>(req)
            .await?
            .map(|r| r.pools))
    }

    pub async fn update_pool(
        &self,
        pool_id: &str,
        request: &SandboxPoolRequest,
    ) -> Result<Traced<SandboxPoolInfo>, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self
            .client
            .build_post_json_request(Method::PUT, &uri, request)?;
        self.client.execute_json(req).await
    }

    pub async fn delete_pool(&self, pool_id: &str) -> Result<Traced<()>, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }
}

/// A client for interacting with a running sandbox proxy (process/file/PTY APIs).
#[derive(Clone)]
pub struct SandboxProxyClient {
    client: Client,
    host_override: Option<String>,
    routing_hint: Option<String>,
    sandbox_id: Option<String>,
}

impl SandboxProxyClient {
    pub fn new(client: Client, host_override: Option<String>) -> Self {
        Self {
            client,
            host_override,
            routing_hint: None,
            sandbox_id: None,
        }
    }

    pub fn with_routing_hint(mut self, hint: Option<String>) -> Self {
        self.routing_hint = hint;
        self
    }

    pub fn with_sandbox_id(mut self, id: Option<String>) -> Self {
        self.sandbox_id = id;
        self
    }

    pub fn http_client(&self) -> &Client {
        &self.client
    }

    pub fn host_override(&self) -> Option<&str> {
        self.host_override.as_deref()
    }

    pub fn sandbox_id(&self) -> Option<&str> {
        self.sandbox_id.as_deref()
    }

    fn request(&self, method: Method, path: &str) -> reqwest_middleware::RequestBuilder {
        let mut request_builder = self.client.request(method, path);
        if let Some(host) = self.host_override.as_deref() {
            request_builder = request_builder.header("Host", host);
        }
        if let Some(id) = self.sandbox_id.as_deref() {
            request_builder = request_builder.header("X-Tensorlake-Sandbox-Id", id);
        }
        if let Some(hint) = self.routing_hint.as_deref() {
            request_builder = request_builder.header("X-Tensorlake-Route-Hint", hint);
        }
        request_builder
    }

    pub async fn start_process(&self, payload: &Value) -> Result<Traced<ProcessInfo>, SdkError> {
        // Validate a managed name up front (clear client-side error before the round-trip).
        // The daemon re-validates as the final authority for any path that bypasses this.
        if let Some(name) = payload.get("name").and_then(Value::as_str) {
            validate_managed_name(name)?;
        }
        let req = self
            .request(Method::POST, "/api/v1/processes")
            .json(payload)
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn list_processes(&self) -> Result<Traced<Vec<ProcessInfo>>, SdkError> {
        let req = self.request(Method::GET, "/api/v1/processes").build()?;
        Ok(self
            .client
            .execute_json::<ListProcessesResponse>(req)
            .await?
            .map(|r| r.processes))
    }

    pub async fn get_process(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<ProcessInfo>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{seg}"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn kill_process(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<()>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = self
            .request(Method::DELETE, &format!("/api/v1/processes/{seg}"))
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn restart_process(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<ProcessInfo>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = build_empty_post_request_from_builder(
            self.request(Method::POST, &format!("/api/v1/processes/{seg}/restart")),
        )?;
        self.client.execute_json(req).await
    }

    pub async fn send_signal(
        &self,
        process: impl Into<ProcessRef>,
        signal: i64,
    ) -> Result<Traced<SendSignalResponse>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = self
            .request(Method::POST, &format!("/api/v1/processes/{seg}/signal"))
            .json(&serde_json::json!({ "signal": signal }))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn write_stdin(
        &self,
        process: impl Into<ProcessRef>,
        data: Vec<u8>,
    ) -> Result<Traced<()>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = build_bytes_post_request_from_builder(
            self.request(Method::POST, &format!("/api/v1/processes/{seg}/stdin")),
            data,
        )?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn close_stdin(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<()>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = build_empty_post_request_from_builder(self.request(
            Method::POST,
            &format!("/api/v1/processes/{seg}/stdin/close"),
        ))?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn get_stdout(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<OutputResponse>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{seg}/stdout"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_stderr(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<OutputResponse>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{seg}/stderr"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_output(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<OutputResponse>, SdkError> {
        let seg = process.into().to_path_segment();
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{seg}/output"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn follow_stdout(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<Vec<OutputEvent>>, SdkError> {
        let mut events = Vec::new();
        let trace_id = self
            .follow_stdout_streaming(process, |event| events.push(event))
            .await?;
        Ok(Traced::new(trace_id, events))
    }

    pub async fn follow_stderr(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<Vec<OutputEvent>>, SdkError> {
        let mut events = Vec::new();
        let trace_id = self
            .follow_stderr_streaming(process, |event| events.push(event))
            .await?;
        Ok(Traced::new(trace_id, events))
    }

    pub async fn follow_output(
        &self,
        process: impl Into<ProcessRef>,
    ) -> Result<Traced<Vec<OutputEvent>>, SdkError> {
        let mut events = Vec::new();
        let trace_id = self
            .follow_output_streaming(process, |event| events.push(event))
            .await?;
        Ok(Traced::new(trace_id, events))
    }

    /// Stream stdout output events to `on_event` as they arrive, without
    /// buffering. Returns the request `trace_id` once the upstream stream
    /// closes. This is the streaming counterpart to [`follow_stdout`] used by
    /// language bindings that surface a live event stream to the caller.
    pub async fn follow_stdout_streaming(
        &self,
        process: impl Into<ProcessRef>,
        on_event: impl FnMut(OutputEvent),
    ) -> Result<String, SdkError> {
        let seg = process.into().to_path_segment();
        self.follow_stream_cb(&format!("/api/v1/processes/{seg}/stdout/follow"), on_event)
            .await
    }

    pub async fn follow_stderr_streaming(
        &self,
        process: impl Into<ProcessRef>,
        on_event: impl FnMut(OutputEvent),
    ) -> Result<String, SdkError> {
        let seg = process.into().to_path_segment();
        self.follow_stream_cb(&format!("/api/v1/processes/{seg}/stderr/follow"), on_event)
            .await
    }

    pub async fn follow_output_streaming(
        &self,
        process: impl Into<ProcessRef>,
        on_event: impl FnMut(OutputEvent),
    ) -> Result<String, SdkError> {
        let seg = process.into().to_path_segment();
        self.follow_stream_cb(&format!("/api/v1/processes/{seg}/output/follow"), on_event)
            .await
    }

    pub async fn run_process(
        &self,
        payload: &Value,
    ) -> Result<Traced<Vec<RunProcessEvent>>, SdkError> {
        let mut events = Vec::new();
        let trace_id = self
            .run_process_streaming(payload, |event| events.push(event))
            .await?;
        Ok(Traced::new(trace_id, events))
    }

    /// Start a process and stream its lifecycle events to `on_event` as they
    /// arrive. Returns the request `trace_id` once the process exits and the
    /// stream closes. This is the streaming counterpart to [`run_process`].
    pub async fn run_process_streaming(
        &self,
        payload: &Value,
        mut on_event: impl FnMut(RunProcessEvent),
    ) -> Result<String, SdkError> {
        let path = "/api/v1/processes/run";
        let req = self
            .request(Method::POST, path)
            .header(ACCEPT, "text/event-stream")
            .json(payload)
            .build()?;
        let response = self.client.execute_traced(req).await?;
        let trace_id = response.trace_id.clone();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if !content_type.contains("text/event-stream") {
            return Err(SdkError::ClientError(format!(
                "expected text/event-stream response from {path}, got content-type: {content_type}"
            )));
        }
        let stream = response
            .into_inner()
            .bytes_stream()
            .eventsource()
            .filter_map(move |event| async move {
                match event {
                    Ok(msg) => {
                        // The Exited variant has all-optional fields so it acts as a
                        // catch-all for unrecognised JSON. Discard Exited{None, None}
                        // — a real exit always has at least one of exit_code or signal.
                        match serde_json::from_str::<RunProcessEvent>(&msg.data) {
                            Ok(RunProcessEvent::Exited {
                                exit_code: None,
                                signal: None,
                                ..
                            }) => None,
                            Ok(evt) => Some(Ok(evt)),
                            Err(_) => None,
                        }
                    }
                    Err(error) => Some(Err(SdkError::EventSourceError(error.to_string()))),
                }
            });
        futures::pin_mut!(stream);
        while let Some(event) = stream.next().await {
            on_event(event?);
        }
        Ok(trace_id)
    }

    async fn follow_stream_cb(
        &self,
        path: &str,
        mut on_event: impl FnMut(OutputEvent),
    ) -> Result<String, SdkError> {
        let req = self
            .request(Method::GET, path)
            .header(ACCEPT, "text/event-stream")
            .build()?;
        let response = self.client.execute_traced(req).await?;
        let trace_id = response.trace_id.clone();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if !content_type.contains("text/event-stream") {
            return Err(SdkError::ClientError(format!(
                "expected text/event-stream response from {path}, got content-type: {content_type}"
            )));
        }
        let stream = response
            .into_inner()
            .bytes_stream()
            .eventsource()
            .filter_map(move |event| async move {
                match event {
                    Ok(msg) => match serde_json::from_str::<OutputEvent>(&msg.data) {
                        Ok(evt) => Some(Ok(evt)),
                        Err(_) => None, // skip heartbeats / non-output events
                    },
                    Err(error) => Some(Err(SdkError::EventSourceError(error.to_string()))),
                }
            });
        futures::pin_mut!(stream);
        while let Some(event) = stream.next().await {
            on_event(event?);
        }
        Ok(trace_id)
    }

    pub async fn read_file(&self, path: &str) -> Result<Traced<Vec<u8>>, SdkError> {
        let req = self
            .request(Method::GET, "/api/v1/files")
            .query(&[("path", path)])
            .build()?;
        let resp = self.client.execute_traced(req).await?;
        let trace_id = resp.trace_id.clone();
        let bytes = resp.into_inner().bytes().await?;
        Ok(Traced::new(trace_id, bytes.to_vec()))
    }

    pub async fn write_file(&self, path: &str, content: Vec<u8>) -> Result<Traced<()>, SdkError> {
        let req = self
            .request(Method::PUT, "/api/v1/files")
            .query(&[("path", path)])
            .body(content)
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn upload_file(
        &self,
        path: &str,
        local_path: impl AsRef<Path>,
    ) -> Result<Traced<()>, SdkError> {
        let file = File::open(local_path.as_ref()).await?;
        let size = file.metadata().await?.len();
        let stream = ReaderStream::new(file);
        let req = self
            .request(Method::PUT, "/api/v1/files")
            .query(&[("path", path)])
            .header(CONTENT_LENGTH, size)
            .body(reqwest::Body::wrap_stream(stream))
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn upload_file_with_progress(
        &self,
        path: &str,
        local_path: impl AsRef<Path>,
        progress_tx: tokio::sync::mpsc::UnboundedSender<u64>,
    ) -> Result<Traced<()>, SdkError> {
        let file = File::open(local_path.as_ref()).await?;
        let size = file.metadata().await?.len();
        let mut uploaded = 0_u64;
        let stream = ReaderStream::new(file).map_ok(move |chunk| {
            uploaded = uploaded.saturating_add(chunk.len() as u64);
            let _ = progress_tx.send(uploaded);
            chunk
        });
        let req = self
            .request(Method::PUT, "/api/v1/files")
            .query(&[("path", path)])
            .header(CONTENT_LENGTH, size)
            .body(reqwest::Body::wrap_stream(stream))
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn delete_file(&self, path: &str) -> Result<Traced<()>, SdkError> {
        let req = self
            .request(Method::DELETE, "/api/v1/files")
            .query(&[("path", path)])
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn list_directory(
        &self,
        path: &str,
    ) -> Result<Traced<ListDirectoryResponse>, SdkError> {
        let req = self
            .request(Method::GET, "/api/v1/files/list")
            .query(&[("path", path)])
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn create_pty_session(&self, payload: &Value) -> Result<Traced<Value>, SdkError> {
        let req = self
            .request(Method::POST, "/api/v1/pty")
            .json(payload)
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn delete_pty_session(&self, session_id: &str) -> Result<Traced<()>, SdkError> {
        let req = self
            .request(Method::DELETE, &format!("/api/v1/pty/{session_id}"))
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn health(&self) -> Result<Traced<HealthResponse>, SdkError> {
        let req = self.request(Method::GET, "/api/v1/health").build()?;
        self.client.execute_json(req).await
    }

    pub async fn info(&self) -> Result<Traced<DaemonInfo>, SdkError> {
        let req = self.request(Method::GET, "/api/v1/info").build()?;
        self.client.execute_json(req).await
    }

    /// Ask the sandbox proxy to mint a builder-compatible blob signing spec.
    ///
    /// Used during the versioned-response rollout: platform-api now returns
    /// `snapshotRelPath` instead of a pre-signed `upload` block, and the CLI
    /// calls this endpoint to resolve the artifact rel-path into a concrete
    /// upload spec. Parent snapshot downloads use the same endpoint with a
    /// full blob URI. The response is a `serde_json::Value` because most
    /// fields splice verbatim into the rootfs build spec consumed by the
    /// in-sandbox builder.
    pub async fn sign_blob(&self, request: &SignBlobRequest) -> Result<Traced<Value>, SdkError> {
        let req = self
            .request(Method::POST, "/api/v1/blob/sign")
            .json(request)
            .build()?;
        self.client.execute_json(req).await
    }
}

#[cfg(test)]
mod process_ref_tests {
    use super::{
        ProcessRef, resolve_sandbox_proxy_target, sandbox_proxy_hostname,
        sandbox_url_from_ingress_endpoint, select_sandbox_proxy_url, validate_managed_name,
    };

    #[test]
    fn validate_managed_name_rules() {
        // Only three rejections: empty, contains '/', and all-digits (reserved for PID).
        let long_digits = "9".repeat(100);
        for bad in ["", "123", "0", "a/b", "worker/api", &long_digits] {
            assert!(
                validate_managed_name(bad).is_err(),
                "expected {bad:?} rejected"
            );
        }
        // Permissive: spaces, punctuation, leading/trailing whitespace, and unicode are all
        // allowed now (clients percent-encode the path segment).
        for ok in [
            "web",
            "web1",
            "1web",
            "my-app_v.2",
            "web 1",
            " web ",
            "a?b",
            "a%b",
            "a#b",
            "café",
        ] {
            assert!(
                validate_managed_name(ok).is_ok(),
                "expected {ok:?} accepted"
            );
        }
    }

    #[test]
    fn process_ref_path_segments() {
        assert_eq!(ProcessRef::from(1234u64).to_path_segment(), "1234");
        assert_eq!(ProcessRef::from(42i64).to_path_segment(), "42");
        // A numeric string is treated as a pid; a name is percent-encoded.
        assert_eq!(ProcessRef::from("1234").to_path_segment(), "1234");
        assert!(matches!(ProcessRef::from("1234"), ProcessRef::Pid(1234)));
        assert_eq!(ProcessRef::from("web").to_path_segment(), "web");
        assert!(matches!(ProcessRef::from("web"), ProcessRef::Name(_)));
        // Spaces and reserved characters in a name are percent-encoded into one segment.
        assert_eq!(ProcessRef::from("web 1").to_path_segment(), "web%201");
        assert_eq!(ProcessRef::from("a?b").to_path_segment(), "a%3Fb");
    }

    #[test]
    fn select_proxy_url_prefers_server_sandbox_url() {
        let selected = select_sandbox_proxy_url(
            "https://api.tensorlake.ai",
            "sbx-1",
            Some("https://sbx-1.sandbox.gcp-use4.tensorlake.ai"),
            Some("https://sandbox.us-east-1.aws.tensorlake.ai"),
            Some("https://override.example.com"),
        )
        .unwrap();

        assert_eq!(selected, "https://sbx-1.sandbox.gcp-use4.tensorlake.ai");
    }

    #[test]
    fn select_proxy_url_uses_explicit_override_when_server_url_missing() {
        let selected = select_sandbox_proxy_url(
            "https://api.tensorlake.ai",
            "sbx-1",
            None,
            Some("https://sandbox.gcp-use4.tensorlake.ai"),
            Some("https://override.example.com"),
        )
        .unwrap();

        assert_eq!(selected, "https://override.example.com");
    }

    #[test]
    fn select_proxy_url_errors_without_server_url_or_explicit_override() {
        let error = select_sandbox_proxy_url(
            "https://api.tensorlake.ai",
            "sbx-1",
            None,
            Some("https://sandbox.gcp-use4.tensorlake.ai"),
            None,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("server response did not include sandbox_url"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn ingress_endpoint_url_builder_preserves_custom_port() {
        let selected = sandbox_url_from_ingress_endpoint(
            "https://sandbox.gcp-use4.tensorlake.ai:9443",
            "sbx-1",
            None,
        )
        .unwrap();

        assert_eq!(
            selected,
            "https://sbx-1.sandbox.gcp-use4.tensorlake.ai:9443"
        );
    }

    #[test]
    fn proxy_target_preserves_server_sandbox_host() {
        let target =
            resolve_sandbox_proxy_target("https://sbx-1.sandbox.gcp-use4.tensorlake.ai", "sbx-1")
                .unwrap();

        assert_eq!(
            target.base_url,
            "https://sbx-1.sandbox.gcp-use4.tensorlake.ai"
        );
        assert_eq!(target.host_override, None);
        assert_eq!(target.sandbox_id_header.as_deref(), Some("sbx-1"));
    }

    #[test]
    fn sandbox_proxy_hostname_uses_server_sandbox_url_host() {
        let hostname =
            sandbox_proxy_hostname("https://sbx-1.sandbox.gcp-use4.tensorlake.ai").unwrap();

        assert_eq!(hostname, "sbx-1.sandbox.gcp-use4.tensorlake.ai");
    }
}
