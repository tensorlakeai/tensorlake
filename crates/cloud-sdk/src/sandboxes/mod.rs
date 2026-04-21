pub mod desktop;
pub mod models;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::Method;
use reqwest::header::ACCEPT;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{client::Client, error::SdkError};
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

    async fn parse_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, SdkError> {
        let bytes = response.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_slice(bytes.as_ref());
        let parsed = serde_path_to_error::deserialize(jd)?;
        Ok(parsed)
    }

    pub async fn create(
        &self,
        request: &CreateSandboxRequest,
    ) -> Result<CreateSandboxResponse, SdkError> {
        let uri = self.endpoint("sandboxes");
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn claim(&self, pool_id: &str) -> Result<CreateSandboxResponse, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}/sandboxes"));
        let req = self.client.request(Method::POST, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get(&self, sandbox_id: &str) -> Result<SandboxInfo, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn list(&self) -> Result<Vec<SandboxInfo>, SdkError> {
        let uri = self.endpoint("sandboxes");
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        let list: ListSandboxesResponse = Self::parse_json(resp).await?;
        Ok(list.sandboxes)
    }

    pub async fn update(
        &self,
        sandbox_id: &str,
        request: &UpdateSandboxRequest,
    ) -> Result<SandboxInfo, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self
            .client
            .build_post_json_request(Method::PATCH, &uri, request)?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn delete(&self, sandbox_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn suspend(&self, sandbox_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/suspend"));
        let req = self.client.request(Method::POST, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn resume(&self, sandbox_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/resume"));
        let req = self.client.request(Method::POST, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn snapshot(
        &self,
        sandbox_id: &str,
        content_mode: Option<SnapshotContentMode>,
    ) -> Result<CreateSnapshotResponse, SdkError> {
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
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get_snapshot(&self, snapshot_id: &str) -> Result<SnapshotInfo, SdkError> {
        let uri = self.endpoint(&format!("snapshots/{snapshot_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, SdkError> {
        let uri = self.endpoint("snapshots");
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        let list: ListSnapshotsResponse = Self::parse_json(resp).await?;
        Ok(list.snapshots)
    }

    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("snapshots/{snapshot_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn create_pool(
        &self,
        request: &SandboxPoolRequest,
    ) -> Result<CreateSandboxPoolResponse, SdkError> {
        let uri = self.endpoint("sandbox-pools");
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get_pool(&self, pool_id: &str) -> Result<SandboxPoolInfo, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn list_pools(&self) -> Result<Vec<SandboxPoolInfo>, SdkError> {
        let uri = self.endpoint("sandbox-pools");
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        let list: ListSandboxPoolsResponse = Self::parse_json(resp).await?;
        Ok(list.pools)
    }

    pub async fn update_pool(
        &self,
        pool_id: &str,
        request: &SandboxPoolRequest,
    ) -> Result<SandboxPoolInfo, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self
            .client
            .build_post_json_request(Method::PUT, &uri, request)?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn delete_pool(&self, pool_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
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

    async fn parse_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, SdkError> {
        let bytes = response.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_slice(bytes.as_ref());
        let parsed = serde_path_to_error::deserialize(jd)?;
        Ok(parsed)
    }

    pub async fn start_process(&self, payload: &Value) -> Result<ProcessInfo, SdkError> {
        let req = self
            .request(Method::POST, "/api/v1/processes")
            .json(payload)
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn list_processes(&self) -> Result<Vec<ProcessInfo>, SdkError> {
        let req = self.request(Method::GET, "/api/v1/processes").build()?;
        let resp = self.client.execute(req).await?;
        let list: ListProcessesResponse = Self::parse_json(resp).await?;
        Ok(list.processes)
    }

    pub async fn get_process(&self, pid: i64) -> Result<ProcessInfo, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}"))
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn kill_process(&self, pid: i64) -> Result<(), SdkError> {
        let req = self
            .request(Method::DELETE, &format!("/api/v1/processes/{pid}"))
            .build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn send_signal(&self, pid: i64, signal: i64) -> Result<SendSignalResponse, SdkError> {
        let req = self
            .request(Method::POST, &format!("/api/v1/processes/{pid}/signal"))
            .json(&serde_json::json!({ "signal": signal }))
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn write_stdin(&self, pid: i64, data: Vec<u8>) -> Result<(), SdkError> {
        let req = self
            .request(Method::POST, &format!("/api/v1/processes/{pid}/stdin"))
            .body(data)
            .build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn close_stdin(&self, pid: i64) -> Result<(), SdkError> {
        let req = self
            .request(
                Method::POST,
                &format!("/api/v1/processes/{pid}/stdin/close"),
            )
            .build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn get_stdout(&self, pid: i64) -> Result<OutputResponse, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}/stdout"))
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get_stderr(&self, pid: i64) -> Result<OutputResponse, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}/stderr"))
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get_output(&self, pid: i64) -> Result<OutputResponse, SdkError> {
        let req = self
            .request(Method::GET, &format!("/api/v1/processes/{pid}/output"))
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn follow_stdout(&self, pid: i64) -> Result<Vec<OutputEvent>, SdkError> {
        self.follow_stream(
            Method::GET,
            &format!("/api/v1/processes/{pid}/stdout/follow"),
            None,
            |data| -> Option<Result<OutputEvent, SdkError>> {
                serde_json::from_str(data).ok().map(Ok)
            },
        )
        .await
    }

    pub async fn follow_stderr(&self, pid: i64) -> Result<Vec<OutputEvent>, SdkError> {
        self.follow_stream(
            Method::GET,
            &format!("/api/v1/processes/{pid}/stderr/follow"),
            None,
            |data| -> Option<Result<OutputEvent, SdkError>> {
                serde_json::from_str(data).ok().map(Ok)
            },
        )
        .await
    }

    pub async fn follow_output(&self, pid: i64) -> Result<Vec<OutputEvent>, SdkError> {
        self.follow_stream(
            Method::GET,
            &format!("/api/v1/processes/{pid}/output/follow"),
            None,
            |data| -> Option<Result<OutputEvent, SdkError>> {
                serde_json::from_str(data).ok().map(Ok)
            },
        )
        .await
    }

    pub async fn run_process(&self, payload: &Value) -> Result<Vec<RunProcessEvent>, SdkError> {
        self.follow_stream(
            Method::POST,
            "/api/v1/processes/run",
            Some(payload),
            |data| -> Option<Result<RunProcessEvent, SdkError>> {
                match serde_json::from_str(data) {
                    Ok(RunProcessEvent::Exited {
                        exit_code: None,
                        signal: None,
                    }) => None,
                    Ok(evt) => Some(Ok(evt)),
                    Err(_) => None,
                }
            },
        )
        .await
    }

    async fn follow_stream<T, E, F>(
        &self,
        method: Method,
        path: &str,
        payload: Option<&Value>,
        filter_map: F,
    ) -> Result<Vec<T>, SdkError>
    where
        E: Into<SdkError>,
        F: Fn(&str) -> Option<Result<T, E>>,
    {
        let mut req = self
            .request(method, path)
            .header(ACCEPT, "text/event-stream");
        if let Some(payload) = payload {
            req = req.json(payload);
        }

        let response = self.client.execute(req.build()?).await?;

        if !response.status().is_success() {
            return Err(SdkError::ClientError(format!(
                "expected success response from {path}, got status: {}",
                response.status()
            )));
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");

        if !content_type
            .to_ascii_lowercase()
            .contains("text/event-stream")
        {
            return Err(SdkError::ClientError(format!(
                "expected text/event-stream response from {path}, got content-type: {content_type}"
            )));
        }
        let stream = response.bytes_stream().eventsource().filter_map(|event| {
            let result = match event {
                Ok(msg) => filter_map(&msg.data).map(|r| r.map_err(Into::into)),
                Err(error) => Some(Err(SdkError::EventSourceError(error.to_string()))),
            };
            async move { result }
        });
        futures::pin_mut!(stream);
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event?);
        }
        Ok(events)
    }

    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>, SdkError> {
        let req = self
            .request(Method::GET, "/api/v1/files")
            .query(&[("path", path)])
            .build()?;
        let resp = self.client.execute(req).await?;
        let bytes = resp.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub async fn write_file(&self, path: &str, content: Vec<u8>) -> Result<(), SdkError> {
        let req = self
            .request(Method::PUT, "/api/v1/files")
            .query(&[("path", path)])
            .body(content)
            .build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn delete_file(&self, path: &str) -> Result<(), SdkError> {
        let req = self
            .request(Method::DELETE, "/api/v1/files")
            .query(&[("path", path)])
            .build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn list_directory(&self, path: &str) -> Result<ListDirectoryResponse, SdkError> {
        let req = self
            .request(Method::GET, "/api/v1/files/list")
            .query(&[("path", path)])
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn create_pty_session(&self, payload: &Value) -> Result<Value, SdkError> {
        let req = self
            .request(Method::POST, "/api/v1/pty")
            .json(payload)
            .build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn health(&self) -> Result<HealthResponse, SdkError> {
        let req = self.request(Method::GET, "/api/v1/health").build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn info(&self) -> Result<DaemonInfo, SdkError> {
        let req = self.request(Method::GET, "/api/v1/info").build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }
}
