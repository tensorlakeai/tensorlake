pub mod desktop;
pub mod models;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::Method;
use reqwest::header::ACCEPT;
use serde_json::Value;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};
pub use desktop::SandboxDesktopClient;

use models::{
    CreateSandboxPoolResponse, CreateSandboxRequest, CreateSandboxResponse, CreateSnapshotRequest,
    CreateSnapshotResponse, DaemonInfo, HealthResponse, ListDirectoryResponse,
    ListProcessesResponse, ListSandboxPoolsResponse, ListSandboxesResponse, ListSnapshotsResponse,
    OutputEvent, OutputResponse, ProcessInfo, RunProcessEvent, SandboxInfo, SandboxPoolInfo,
    SandboxPoolRequest, SendSignalResponse, SnapshotContentMode, SnapshotInfo,
    UpdateSandboxRequest,
};

/// A client for managing sandbox lifecycle, pool, and snapshot APIs.
#[derive(Clone)]
pub struct SandboxesClient {
    client: Client,
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
            client,
            namespace: namespace.into(),
            use_namespaced_endpoints,
        }
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

    pub async fn create(
        &self,
        request: &CreateSandboxRequest,
    ) -> Result<Traced<CreateSandboxResponse>, SdkError> {
        let uri = self.endpoint("sandboxes");
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        self.client.execute_json(req).await
    }

    pub async fn claim(&self, pool_id: &str) -> Result<Traced<CreateSandboxResponse>, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}/sandboxes"));
        let req = self.client.request(Method::POST, &uri).build()?;
        self.client.execute_json(req).await
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
        let req = self.client.request(Method::POST, &uri).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn resume(&self, sandbox_id: &str) -> Result<Traced<()>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/resume"));
        let req = self.client.request(Method::POST, &uri).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn snapshot(
        &self,
        sandbox_id: &str,
        content_mode: Option<SnapshotContentMode>,
    ) -> Result<Traced<CreateSnapshotResponse>, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/snapshot"));
        let req = if content_mode.is_some() {
            let body = CreateSnapshotRequest {
                snapshot_content_mode: content_mode,
            };
            self.client
                .build_post_json_request(Method::POST, &uri, &body)?
        } else {
            // Preserve today's wire shape (no body) for callers that don't set a content mode.
            self.client.request(Method::POST, &uri).build()?
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
}

impl SandboxProxyClient {
    pub fn new(client: Client, host_override: Option<String>) -> Self {
        Self {
            client,
            host_override,
            routing_hint: None,
        }
    }

    pub fn with_routing_hint(mut self, hint: Option<String>) -> Self {
        self.routing_hint = hint;
        self
    }

    fn request(&self, method: Method, path: &str) -> reqwest_middleware::RequestBuilder {
        let mut request_builder = self.client.request(method, path);
        if let Some(host) = self.host_override.as_deref() {
            request_builder = request_builder.header("Host", host);
        }
        if let Some(hint) = self.routing_hint.as_deref() {
            request_builder = request_builder.header("X-Tensorlake-Route-Hint", hint);
        }
        request_builder
    }

    pub async fn start_process(&self, payload: &Value) -> Result<Traced<ProcessInfo>, SdkError> {
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

    pub async fn get_process(&self, pid: i64) -> Result<Traced<ProcessInfo>, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn kill_process(&self, pid: i64) -> Result<Traced<()>, SdkError> {
        let req = self
            .request(Method::DELETE, &format!("/api/v1/processes/{pid}"))
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn send_signal(
        &self,
        pid: i64,
        signal: i64,
    ) -> Result<Traced<SendSignalResponse>, SdkError> {
        let req = self
            .request(Method::POST, &format!("/api/v1/processes/{pid}/signal"))
            .json(&serde_json::json!({ "signal": signal }))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn write_stdin(&self, pid: i64, data: Vec<u8>) -> Result<Traced<()>, SdkError> {
        let req = self
            .request(Method::POST, &format!("/api/v1/processes/{pid}/stdin"))
            .body(data)
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn close_stdin(&self, pid: i64) -> Result<Traced<()>, SdkError> {
        let req = self
            .request(
                Method::POST,
                &format!("/api/v1/processes/{pid}/stdin/close"),
            )
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn get_stdout(&self, pid: i64) -> Result<Traced<OutputResponse>, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}/stdout"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_stderr(&self, pid: i64) -> Result<Traced<OutputResponse>, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}/stderr"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_output(&self, pid: i64) -> Result<Traced<OutputResponse>, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}/output"))
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn follow_stdout(&self, pid: i64) -> Result<Traced<Vec<OutputEvent>>, SdkError> {
        self.follow_stream(&format!("/api/v1/processes/{pid}/stdout/follow"))
            .await
    }

    pub async fn follow_stderr(&self, pid: i64) -> Result<Traced<Vec<OutputEvent>>, SdkError> {
        self.follow_stream(&format!("/api/v1/processes/{pid}/stderr/follow"))
            .await
    }

    pub async fn follow_output(&self, pid: i64) -> Result<Traced<Vec<OutputEvent>>, SdkError> {
        self.follow_stream(&format!("/api/v1/processes/{pid}/output/follow"))
            .await
    }

    pub async fn run_process(
        &self,
        payload: &Value,
    ) -> Result<Traced<Vec<RunProcessEvent>>, SdkError> {
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
                            }) => None,
                            Ok(evt) => Some(Ok(evt)),
                            Err(_) => None,
                        }
                    }
                    Err(error) => Some(Err(SdkError::EventSourceError(error.to_string()))),
                }
            });
        futures::pin_mut!(stream);
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event?);
        }
        Ok(Traced::new(trace_id, events))
    }

    async fn follow_stream(&self, path: &str) -> Result<Traced<Vec<OutputEvent>>, SdkError> {
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
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event?);
        }
        Ok(Traced::new(trace_id, events))
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

    pub async fn health(&self) -> Result<Traced<HealthResponse>, SdkError> {
        let req = self.request(Method::GET, "/api/v1/health").build()?;
        self.client.execute_json(req).await
    }

    pub async fn info(&self) -> Result<Traced<DaemonInfo>, SdkError> {
        let req = self.request(Method::GET, "/api/v1/info").build()?;
        self.client.execute_json(req).await
    }
}
