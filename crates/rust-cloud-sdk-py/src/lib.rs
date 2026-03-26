#![allow(unexpected_cfgs)]
#![allow(unsafe_op_in_unsafe_fn)]
// PyO3 macro expansion currently triggers false-positive `useless_conversion` lints.
#![allow(clippy::useless_conversion)]
#![allow(clippy::too_many_arguments)]

use std::error::Error as StdError;
use std::future::Future;
use std::io::Write;
use std::time::Duration;

use futures::StreamExt;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use reqwest::Method;
use reqwest::multipart::{Form, Part};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tensorlake_cloud_sdk::document_ai::DocumentAiClient;
use tensorlake_cloud_sdk::images::ImagesClient;
use tensorlake_cloud_sdk::images::models::{
    ApplicationBuildContext, CreateApplicationBuildRequest,
};
use tensorlake_cloud_sdk::sandboxes::models::{
    CreateSandboxRequest, SandboxPoolRequest, SnapshotContentMode,
};
use tensorlake_cloud_sdk::sandboxes::{SandboxProxyClient, SandboxesClient};
use tensorlake_cloud_sdk::{Client, ClientBuilder, error::SdkError};
use tokio::runtime::Runtime;

create_exception!(_cloud_sdk, CloudApiClientError, PyException);
create_exception!(_cloud_sdk, CloudSandboxClientError, PyException);
create_exception!(_cloud_sdk, CloudDocumentAIClientError, PyException);

const DEFAULT_HTTP_REQUEST_TIMEOUT_SEC: f64 = 5.0;

#[pyclass]
pub struct CloudApiClient {
    client: Client,
    runtime: Runtime,
    api_url: String,
    namespace: String,
}

#[pymethods]
impl CloudApiClient {
    #[new]
    #[pyo3(signature = (api_url, api_key=None, organization_id=None, project_id=None, namespace=None))]
    fn new(
        api_url: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        namespace: Option<String>,
    ) -> PyResult<Self> {
        let mut builder = ClientBuilder::new(&api_url);
        if let Some(token) = api_key.as_deref() {
            builder = builder.bearer_token(token);
        }

        if let (Some(org_id), Some(project_id)) =
            (organization_id.as_deref(), project_id.as_deref())
        {
            builder = builder.scope(org_id, project_id);
        }

        let client = builder.build().map_err(into_py_error)?;
        let runtime = Runtime::new().map_err(|e| {
            CloudApiClientError::new_err((
                "internal",
                Option::<u16>::None,
                format!("failed to create tokio runtime: {e}"),
            ))
        })?;

        Ok(Self {
            client,
            runtime,
            api_url,
            namespace: namespace.unwrap_or_else(|| "default".to_string()),
        })
    }

    fn close(&self) {
        // reqwest clients are closed when dropped; this is a no-op for API parity.
    }

    fn upsert_application(
        &self,
        manifest_json: String,
        code_zip: Vec<u8>,
        upgrade_running_requests: bool,
    ) -> PyResult<()> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let namespace = namespace.clone();
            let manifest_json = manifest_json.clone();
            let code_zip = code_zip.clone();
            async move {
                let form = Form::new()
                    .part("code", Part::bytes(code_zip).file_name("code.zip"))
                    .text("code_content_type", "application/zip")
                    .text("application", manifest_json)
                    .text(
                        "upgrade_requests_to_latest_code",
                        upgrade_running_requests.to_string(),
                    );

                let request = client.build_multipart_request(
                    Method::POST,
                    &format!("/v1/namespaces/{namespace}/applications"),
                    form,
                )?;

                let _response = client.execute(request).await?;
                Ok(())
            }
        })
    }

    fn delete_application(&self, application_name: String) -> PyResult<()> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let path = format!("/v1/namespaces/{namespace}/applications/{application_name}");
            async move {
                let request = client.request(Method::DELETE, &path).build()?;
                let _response = client.execute(request).await?;
                Ok(())
            }
        })
    }

    fn applications_json(&self) -> PyResult<String> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let path = format!("/v1/namespaces/{namespace}/applications");
            async move {
                let request = client.request(Method::GET, &path).build()?;
                let response = client.execute(request).await?;
                let text = response.text().await?;
                Ok(text)
            }
        })
    }

    fn application_manifest_json(&self, application_name: String) -> PyResult<String> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let path = format!("/v1/namespaces/{namespace}/applications/{application_name}");
            async move {
                let request = client.request(Method::GET, &path).build()?;
                let response = client.execute(request).await?;
                let text = response.text().await?;
                Ok(text)
            }
        })
    }

    fn run_request(
        &self,
        application_name: String,
        inputs: Vec<(String, Vec<u8>, String)>,
    ) -> PyResult<String> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let namespace = namespace.clone();
            let application_name = application_name.clone();
            let inputs = inputs.clone();
            async move {
                let path = format!("/v1/namespaces/{namespace}/applications/{application_name}");
                let request = if inputs.is_empty() {
                    client
                        .request(Method::POST, &path)
                        .header("Accept", "application/json")
                        .body(Vec::<u8>::new())
                        .build()?
                } else if inputs.len() == 1 && inputs[0].0 == "0" {
                    let (_, data, content_type) = inputs[0].clone();
                    client
                        .request(Method::POST, &path)
                        .header("Accept", "application/json")
                        .header("Content-Type", content_type)
                        .body(data)
                        .build()?
                } else {
                    let mut form = Form::new();
                    for (name, data, content_type) in inputs {
                        let part = Part::bytes(data)
                            .file_name(name.clone())
                            .mime_str(&content_type)
                            .map_err(|e| SdkError::ClientError(e.to_string()))?;
                        form = form.part(name, part);
                    }
                    client
                        .request(Method::POST, &path)
                        .header("Accept", "application/json")
                        .multipart(form)
                        .build()?
                };

                let response = client.execute(request).await?;
                let text = response.text().await?;
                let body: Value = serde_json::from_str(&text)?;
                let request_id = body["request_id"].as_str().ok_or_else(|| {
                    SdkError::ClientError(format!(
                        "missing request_id in run request response body: {text}"
                    ))
                })?;
                Ok(request_id.to_string())
            }
        })
    }

    fn wait_on_request_completion(
        &self,
        application_name: String,
        request_id: String,
    ) -> PyResult<()> {
        let namespace = self.namespace.clone();
        self.run_with_retry(10, move |client| {
            let path =
                format!("/v1/namespaces/{namespace}/applications/{application_name}/requests/{request_id}/progress");
            async move {
                let mut stream = client.build_event_source_request::<Value>(&path).await?;
                while let Some(event) = stream.next().await {
                    let event = event?;
                    if event.get("RequestFinished").is_some() {
                        return Ok(());
                    }
                }

                Err(SdkError::EventSourceError(
                    "progress stream ended before request completion".to_string(),
                ))
            }
        })
    }

    fn request_metadata_json(
        &self,
        application_name: String,
        request_id: String,
    ) -> PyResult<String> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let path = format!(
                "/v1/namespaces/{namespace}/applications/{application_name}/requests/{request_id}"
            );
            async move {
                let request = client.request(Method::GET, &path).build()?;
                let response = client.execute(request).await?;
                let text = response.text().await?;
                Ok(text)
            }
        })
    }

    fn request_output_bytes(
        &self,
        application_name: String,
        request_id: String,
    ) -> PyResult<(Vec<u8>, String)> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let path = format!(
                "/v1/namespaces/{namespace}/applications/{application_name}/requests/{request_id}/output"
            );
            async move {
                let request = client
                    .request(Method::GET, &path)
                    .timeout(Duration::from_secs_f64(DEFAULT_HTTP_REQUEST_TIMEOUT_SEC))
                    .build()?;
                let response = client.execute(request).await?;
                let content_type = response
                    .headers()
                    .get("Content-Type")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let bytes = response.bytes().await?;
                Ok((bytes.to_vec(), content_type))
            }
        })
    }

    fn introspect_api_key_json(&self) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let request = client
                .request(Method::POST, "/platform/v1/keys/introspect")
                .build()?;
            let response = client.execute(request).await?;
            Ok(response.text().await?)
        })
    }

    #[pyo3(signature = (organization_id, project_id, page_size=100))]
    fn list_secrets_json(
        &self,
        organization_id: String,
        project_id: String,
        page_size: i32,
    ) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let path = format!(
                "/platform/v1/organizations/{organization_id}/projects/{project_id}/secrets"
            );
            async move {
                let request = client
                    .request(Method::GET, &path)
                    .query(&[("pageSize", page_size)])
                    .build()?;
                let response = client.execute(request).await?;
                Ok(response.text().await?)
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn start_image_build(
        &self,
        build_service_path: String,
        graph_name: String,
        graph_version: String,
        graph_function_name: String,
        image_name: String,
        image_id: String,
        context_tar_gz: Vec<u8>,
    ) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let build_service_path = build_service_path.clone();
            let graph_name = graph_name.clone();
            let graph_version = graph_version.clone();
            let graph_function_name = graph_function_name.clone();
            let image_name = image_name.clone();
            let image_id = image_id.clone();
            let context_tar_gz = context_tar_gz.clone();
            async move {
                let endpoint = format!("{}/builds", build_service_path.trim_end_matches('/'));
                let form = Form::new()
                    .text("graph_name", graph_name)
                    .text("graph_version", graph_version)
                    .text("graph_function_name", graph_function_name)
                    .text("image_name", image_name)
                    .text("image_id", image_id)
                    .part(
                        "context",
                        Part::bytes(context_tar_gz).file_name("context.tar.gz"),
                    );
                let request = client.build_multipart_request(Method::PUT, &endpoint, form)?;
                let response = client.execute(request).await?;
                Ok(response.text().await?)
            }
        })
    }

    fn create_application_build(
        &self,
        build_service_path: String,
        request_json: String,
        image_contexts: Vec<(String, Vec<u8>)>,
    ) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let build_service_path = build_service_path.clone();
            let request_json = request_json.clone();
            let image_contexts = image_contexts.clone();
            async move {
                let request: CreateApplicationBuildRequest = serde_json::from_str(&request_json)?;
                let image_contexts: Vec<ApplicationBuildContext> = image_contexts
                    .into_iter()
                    .map(
                        |(context_tar_part_name, context_tar_gz)| ApplicationBuildContext {
                            context_tar_part_name,
                            context_tar_gz,
                        },
                    )
                    .collect();
                let images_client = ImagesClient::new(client.clone());
                let response = images_client
                    .create_application_build(&build_service_path, &request, &image_contexts)
                    .await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn application_build_info_json(
        &self,
        build_service_path: String,
        application_build_id: String,
    ) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let build_service_path = build_service_path.clone();
            let application_build_id = application_build_id.clone();
            async move {
                let images_client = ImagesClient::new(client.clone());
                let response = images_client
                    .application_build_info(&build_service_path, &application_build_id)
                    .await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn cancel_application_build(
        &self,
        build_service_path: String,
        application_build_id: String,
    ) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let build_service_path = build_service_path.clone();
            let application_build_id = application_build_id.clone();
            async move {
                let images_client = ImagesClient::new(client.clone());
                let response = images_client
                    .cancel_application_build(&build_service_path, &application_build_id)
                    .await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn build_info_json(&self, build_service_path: String, build_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let path = format!(
                "{}/builds/{build_id}",
                build_service_path.trim_end_matches('/')
            );
            async move {
                let request = client.request(Method::GET, &path).build()?;
                let response = client.execute(request).await?;
                Ok(response.text().await?)
            }
        })
    }

    fn cancel_build(&self, build_service_path: String, build_id: String) -> PyResult<()> {
        self.run_with_retry(5, move |client| {
            let path = format!(
                "{}/builds/{build_id}/cancel",
                build_service_path.trim_end_matches('/')
            );
            async move {
                let request = client.request(Method::POST, &path).build()?;
                let _response = client.execute(request).await?;
                Ok(())
            }
        })
    }

    fn stream_build_logs_json(
        &self,
        py: Python<'_>,
        build_service_path: String,
        build_id: String,
    ) -> PyResult<Vec<String>> {
        py.detach(|| {
            self.run_with_retry(2, move |client| {
                let path = build_logs_path(&build_service_path, &build_id);
                async move {
                    let mut events: Vec<String> = Vec::new();
                    stream_build_log_events(client, &path, |event| {
                        events.push(serde_json::to_string(&event)?);
                        Ok(())
                    })
                    .await?;
                    Ok(events)
                }
            })
        })
    }

    fn stream_build_logs_to_stderr(
        &self,
        py: Python<'_>,
        build_service_path: String,
        build_id: String,
    ) -> PyResult<()> {
        py.detach(|| {
            self.run_with_retry(2, move |client| {
                let path = build_logs_path(&build_service_path, &build_id);
                async move {
                    stream_build_log_events(client, &path, |event| {
                        print_build_log_event_to_stderr(event, None);
                        Ok(())
                    })
                    .await?;
                    Ok(())
                }
            })
        })
    }

    #[pyo3(signature = (build_service_path, build_id, prefix, color=None))]
    fn stream_build_logs_to_stderr_prefixed(
        &self,
        py: Python<'_>,
        build_service_path: String,
        build_id: String,
        prefix: String,
        color: Option<String>,
    ) -> PyResult<()> {
        py.detach(|| {
            self.run_with_retry(2, move |client| {
                let path = build_logs_path(&build_service_path, &build_id);
                let log_prefix = LogPrefix::new(prefix.clone(), color.clone());
                async move {
                    stream_build_log_events(client, &path, |event| {
                        print_build_log_event_to_stderr(event, Some(&log_prefix));
                        Ok(())
                    })
                    .await?;
                    Ok(())
                }
            })
        })
    }

    fn endpoint_url(&self, endpoint: String) -> String {
        format!("{}/{}", self.api_url.trim_end_matches('/'), endpoint)
    }
}

#[pyclass]
pub struct CloudSandboxClient {
    client: SandboxesClient,
    runtime: Runtime,
}

#[pymethods]
impl CloudSandboxClient {
    #[new]
    #[pyo3(signature = (api_url, api_key=None, organization_id=None, project_id=None, namespace=None))]
    fn new(
        api_url: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        namespace: Option<String>,
    ) -> PyResult<Self> {
        let mut builder = ClientBuilder::new(&api_url);
        if let Some(token) = api_key.as_deref() {
            builder = builder.bearer_token(token);
        }

        if let (Some(org_id), Some(project_id)) =
            (organization_id.as_deref(), project_id.as_deref())
        {
            builder = builder.scope(org_id, project_id);
        }

        let client = builder.build().map_err(into_sandbox_py_error)?;
        let runtime = Runtime::new().map_err(|e| {
            CloudSandboxClientError::new_err((
                "internal",
                Option::<u16>::None,
                format!("failed to create tokio runtime: {e}"),
            ))
        })?;
        let use_namespaced_endpoints = is_localhost_api_url(&api_url);
        let sandboxes_client = SandboxesClient::new(
            client,
            namespace.unwrap_or_else(|| "default".to_string()),
            use_namespaced_endpoints,
        );

        Ok(Self {
            client: sandboxes_client,
            runtime,
        })
    }

    fn close(&self) {
        // reqwest clients are closed when dropped; this is a no-op for API parity.
    }

    fn create_sandbox(&self, request_json: String) -> PyResult<String> {
        let request: CreateSandboxRequest = parse_json_payload(&request_json)?;
        self.run_with_retry(5, move |client| {
            let request = request.clone();
            async move {
                let response = client.create(&request).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn claim_sandbox(&self, pool_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let pool_id = pool_id.clone();
            async move {
                let response = client.claim(&pool_id).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn get_sandbox_json(&self, sandbox_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let response = client.get(&sandbox_id).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn list_sandboxes_json(&self) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let sandboxes = client.list().await?;
            let response = serde_json::json!({ "sandboxes": sandboxes });
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn delete_sandbox(&self, sandbox_id: String) -> PyResult<()> {
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move { client.delete(&sandbox_id).await }
        })
    }

    fn create_snapshot(
        &self,
        sandbox_id: String,
        snapshot_content_mode: String,
    ) -> PyResult<String> {
        let content_mode = match snapshot_content_mode.as_str() {
            "full" => SnapshotContentMode::Full,
            "filesystem_only" => SnapshotContentMode::FilesystemOnly,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid snapshot_content_mode: '{}', expected 'full' or 'filesystem_only'",
                    other
                )));
            }
        };
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let response = client.snapshot(&sandbox_id, content_mode).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn get_snapshot_json(&self, snapshot_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let snapshot_id = snapshot_id.clone();
            async move {
                let response = client.get_snapshot(&snapshot_id).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn list_snapshots_json(&self) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let snapshots = client.list_snapshots().await?;
            let response = serde_json::json!({ "snapshots": snapshots });
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn delete_snapshot(&self, snapshot_id: String) -> PyResult<()> {
        self.run_with_retry(5, move |client| {
            let snapshot_id = snapshot_id.clone();
            async move { client.delete_snapshot(&snapshot_id).await }
        })
    }

    fn create_pool(&self, request_json: String) -> PyResult<String> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        self.run_with_retry(5, move |client| {
            let request = request.clone();
            async move {
                let response = client.create_pool(&request).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn get_pool_json(&self, pool_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let pool_id = pool_id.clone();
            async move {
                let response = client.get_pool(&pool_id).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn list_pools_json(&self) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let pools = client.list_pools().await?;
            let response = serde_json::json!({ "pools": pools });
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn update_pool(&self, pool_id: String, request_json: String) -> PyResult<String> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        self.run_with_retry(5, move |client| {
            let pool_id = pool_id.clone();
            let request = request.clone();
            async move {
                let response = client.update_pool(&pool_id, &request).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn delete_pool(&self, pool_id: String) -> PyResult<()> {
        self.run_with_retry(5, move |client| {
            let pool_id = pool_id.clone();
            async move { client.delete_pool(&pool_id).await }
        })
    }
}

impl CloudSandboxProxyClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, mut operation: F) -> PyResult<T>
    where
        F: FnMut(SandboxProxyClient) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        let mut retries = 0usize;
        loop {
            match self.runtime.block_on(operation(self.client.clone())) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !is_retryable(&err) || retries >= max_retries {
                        return Err(into_sandbox_py_error(err));
                    }

                    retries += 1;
                    let sleep_time = calculate_sleep_time(retries);
                    eprintln!(
                        "Retrying rust sandbox proxy request after {sleep_time:.2} seconds. Retry count: {retries}. Retryable exception: {err}"
                    );
                    std::thread::sleep(Duration::from_secs_f64(sleep_time));
                }
            }
        }
    }
}

#[pyclass]
pub struct CloudSandboxProxyClient {
    client: SandboxProxyClient,
    runtime: Runtime,
    base_url: String,
}

#[pymethods]
impl CloudSandboxProxyClient {
    #[new]
    #[pyo3(signature = (proxy_url, sandbox_id, api_key=None, organization_id=None, project_id=None))]
    fn new(
        proxy_url: String,
        sandbox_id: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
    ) -> PyResult<Self> {
        let (base_url, host_override) = resolve_proxy_target(&proxy_url, &sandbox_id)?;

        let mut builder = ClientBuilder::new(&base_url);
        if let Some(token) = api_key.as_deref() {
            builder = builder.bearer_token(token);
        }

        if let (Some(org_id), Some(project_id)) =
            (organization_id.as_deref(), project_id.as_deref())
        {
            builder = builder.scope(org_id, project_id);
        }

        let client = builder.build().map_err(into_sandbox_py_error)?;
        let runtime = Runtime::new().map_err(|e| {
            CloudSandboxClientError::new_err((
                "internal",
                Option::<u16>::None,
                format!("failed to create tokio runtime: {e}"),
            ))
        })?;
        let sandbox_proxy_client = SandboxProxyClient::new(client, host_override);

        Ok(Self {
            client: sandbox_proxy_client,
            runtime,
            base_url,
        })
    }

    fn close(&self) {
        // reqwest clients are closed when dropped; this is a no-op for API parity.
    }

    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    fn start_process_json(&self, payload_json: String) -> PyResult<String> {
        let payload: Value = parse_json_payload(&payload_json)?;
        self.run_with_retry(5, move |client| {
            let payload = payload.clone();
            async move {
                let response = client.start_process(&payload).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn list_processes_json(&self) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let processes = client.list_processes().await?;
            let response = serde_json::json!({ "processes": processes });
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn get_process_json(&self, pid: i64) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let response = client.get_process(pid).await?;
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn kill_process(&self, pid: i64) -> PyResult<()> {
        self.run_with_retry(
            5,
            move |client| async move { client.kill_process(pid).await },
        )
    }

    fn send_signal_json(&self, pid: i64, signal: i64) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let response = client.send_signal(pid, signal).await?;
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn write_stdin(&self, pid: i64, data: Vec<u8>) -> PyResult<()> {
        self.run_with_retry(5, move |client| {
            let data = data.clone();
            async move { client.write_stdin(pid, data).await }
        })
    }

    fn close_stdin(&self, pid: i64) -> PyResult<()> {
        self.run_with_retry(
            5,
            move |client| async move { client.close_stdin(pid).await },
        )
    }

    fn get_stdout_json(&self, pid: i64) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let response = client.get_stdout(pid).await?;
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn get_stderr_json(&self, pid: i64) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let response = client.get_stderr(pid).await?;
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn get_output_json(&self, pid: i64) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let response = client.get_output(pid).await?;
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn follow_stdout_json(&self, pid: i64) -> PyResult<Vec<String>> {
        self.run_with_retry(10, move |client| async move {
            let events = client.follow_stdout(pid).await?;
            events
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                .collect()
        })
    }

    fn follow_stderr_json(&self, pid: i64) -> PyResult<Vec<String>> {
        self.run_with_retry(10, move |client| async move {
            let events = client.follow_stderr(pid).await?;
            events
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                .collect()
        })
    }

    fn follow_output_json(&self, pid: i64) -> PyResult<Vec<String>> {
        self.run_with_retry(10, move |client| async move {
            let events = client.follow_output(pid).await?;
            events
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                .collect()
        })
    }

    fn read_file_bytes(&self, py: Python<'_>, path: String) -> PyResult<Py<pyo3::types::PyBytes>> {
        let data: Vec<u8> = self.run_with_retry(5, move |client| {
            let path = path.clone();
            async move { client.read_file(&path).await }
        })?;
        Ok(pyo3::types::PyBytes::new(py, &data).into())
    }

    fn write_file(&self, path: String, content: Vec<u8>) -> PyResult<()> {
        self.run_with_retry(5, move |client| {
            let path = path.clone();
            let content = content.clone();
            async move { client.write_file(&path, content).await }
        })
    }

    fn delete_file(&self, path: String) -> PyResult<()> {
        self.run_with_retry(5, move |client| {
            let path = path.clone();
            async move { client.delete_file(&path).await }
        })
    }

    fn list_directory_json(&self, path: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let path = path.clone();
            async move {
                let response = client.list_directory(&path).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn create_pty_session_json(&self, payload_json: String) -> PyResult<String> {
        let payload: Value = parse_json_payload(&payload_json)?;
        self.run_with_retry(5, move |client| {
            let payload = payload.clone();
            async move {
                let response = client.create_pty_session(&payload).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn health_json(&self) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let response = client.health().await?;
            Ok(serde_json::to_string(&response)?)
        })
    }

    fn info_json(&self) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let response = client.info().await?;
            Ok(serde_json::to_string(&response)?)
        })
    }
}

#[pyclass]
pub struct CloudDocumentAIClient {
    client: DocumentAiClient,
    runtime: Runtime,
}

#[pymethods]
impl CloudDocumentAIClient {
    #[new]
    #[pyo3(signature = (api_url, api_key))]
    fn new(api_url: String, api_key: String) -> PyResult<Self> {
        let client = ClientBuilder::new(&api_url)
            .bearer_token(&api_key)
            .build()
            .map_err(into_document_ai_py_error)?;
        let runtime = Runtime::new().map_err(|e| {
            CloudDocumentAIClientError::new_err((
                "internal",
                Option::<u16>::None,
                format!("failed to create tokio runtime: {e}"),
            ))
        })?;
        let document_ai_client = DocumentAiClient::new(client);

        Ok(Self {
            client: document_ai_client,
            runtime,
        })
    }

    fn close(&self) {
        // reqwest clients are closed when dropped; this is a no-op for API parity.
    }

    #[pyo3(signature = (method, path, body_json=None))]
    fn request_json(
        &self,
        method: String,
        path: String,
        body_json: Option<String>,
    ) -> PyResult<String> {
        let method = Method::from_bytes(method.as_bytes()).map_err(|error| {
            CloudDocumentAIClientError::new_err((
                "sdk_usage",
                Option::<u16>::None,
                format!("invalid HTTP method `{method}`: {error}"),
            ))
        })?;
        let body_json = body_json
            .as_deref()
            .map(serde_json::from_str::<Value>)
            .transpose()
            .map_err(|error| {
                CloudDocumentAIClientError::new_err((
                    "sdk_usage",
                    Option::<u16>::None,
                    format!("invalid JSON payload: {error}"),
                ))
            })?;

        self.run_with_retry(5, move |client| {
            let method = method.clone();
            let path = path.clone();
            let body_json = body_json.clone();
            async move {
                let response = client.request(method, &path, body_json.as_ref()).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn upload_file_json(&self, file_name: String, content: Vec<u8>) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let file_name = file_name.clone();
            let content = content.clone();
            async move {
                let response = client.upload_file(&file_name, content).await?;
                Ok(serde_json::to_string(&response)?)
            }
        })
    }

    fn parse_events_json(&self, parse_id: String) -> PyResult<Vec<String>> {
        self.run_with_retry(10, move |client| {
            let parse_id = parse_id.clone();
            async move {
                let events = client.parse_events(&parse_id).await?;
                events
                    .into_iter()
                    .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                    .collect()
            }
        })
    }
}

impl CloudApiClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, mut operation: F) -> PyResult<T>
    where
        F: FnMut(Client) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        let mut retries = 0usize;
        loop {
            match self.runtime.block_on(operation(self.client.clone())) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !is_retryable(&err) || retries >= max_retries {
                        return Err(into_py_error(err));
                    }

                    retries += 1;
                    let sleep_time = calculate_sleep_time(retries);
                    eprintln!(
                        "Retrying rust cloud API request after {sleep_time:.2} seconds. Retry count: {retries}. Retryable exception: {err}"
                    );
                    std::thread::sleep(Duration::from_secs_f64(sleep_time));
                }
            }
        }
    }
}

impl CloudSandboxClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, mut operation: F) -> PyResult<T>
    where
        F: FnMut(SandboxesClient) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        let mut retries = 0usize;
        loop {
            match self.runtime.block_on(operation(self.client.clone())) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !is_retryable(&err) || retries >= max_retries {
                        return Err(into_sandbox_py_error(err));
                    }

                    retries += 1;
                    let sleep_time = calculate_sleep_time(retries);
                    eprintln!(
                        "Retrying rust sandbox API request after {sleep_time:.2} seconds. Retry count: {retries}. Retryable exception: {err}"
                    );
                    std::thread::sleep(Duration::from_secs_f64(sleep_time));
                }
            }
        }
    }
}

impl CloudDocumentAIClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, mut operation: F) -> PyResult<T>
    where
        F: FnMut(DocumentAiClient) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        let mut retries = 0usize;
        loop {
            match self.runtime.block_on(operation(self.client.clone())) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !is_retryable(&err) || retries >= max_retries {
                        return Err(into_document_ai_py_error(err));
                    }

                    retries += 1;
                    let sleep_time = calculate_sleep_time(retries);
                    eprintln!(
                        "Retrying rust document-ai request after {sleep_time:.2} seconds. Retry count: {retries}. Retryable exception: {err}"
                    );
                    std::thread::sleep(Duration::from_secs_f64(sleep_time));
                }
            }
        }
    }
}

fn calculate_sleep_time(retries: usize) -> f64 {
    let initial_delay_seconds: f64 = 0.1;
    let max_delay_seconds: f64 = 15.0;
    let jitter_multiplier: f64 = 0.75;
    let base_delay = initial_delay_seconds * 2f64.powi(retries as i32);
    base_delay.min(max_delay_seconds) * jitter_multiplier
}

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

fn into_py_error(error: SdkError) -> PyErr {
    match error {
        SdkError::Authentication(message) => {
            CloudApiClientError::new_err(("sdk_usage", Some(401u16), message))
        }
        SdkError::Authorization(message) => {
            CloudApiClientError::new_err(("sdk_usage", Some(403u16), message))
        }
        SdkError::ServerError { status, message } => {
            CloudApiClientError::new_err(("remote_api", Some(status.as_u16()), message))
        }
        SdkError::Http(http_error) => {
            let message = format_error_chain(&http_error);
            if http_error.is_timeout() {
                CloudApiClientError::new_err(("connection", Option::<u16>::None, message))
            } else if http_error.is_connect() {
                CloudApiClientError::new_err(("connection", Option::<u16>::None, message))
            } else {
                CloudApiClientError::new_err(("internal", Option::<u16>::None, message))
            }
        }
        SdkError::Middleware(middleware_error) => {
            let message = format_error_chain(&middleware_error);
            let lower = message.to_lowercase();
            if lower.contains("connect")
                || lower.contains("connection refused")
                || lower.contains("dns")
                || lower.contains("timed out")
                || lower.contains("timeout")
            {
                CloudApiClientError::new_err(("connection", Option::<u16>::None, message))
            } else {
                CloudApiClientError::new_err(("internal", Option::<u16>::None, message))
            }
        }
        other => CloudApiClientError::new_err(("internal", Option::<u16>::None, other.to_string())),
    }
}

fn format_error_chain(error: &dyn StdError) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_message = cause.to_string();
        if !cause_message.is_empty() {
            message.push_str(": ");
            message.push_str(&cause_message);
        }
        source = cause.source();
    }
    message
}

fn into_sandbox_py_error(error: SdkError) -> PyErr {
    match error {
        SdkError::Authentication(message) => {
            CloudSandboxClientError::new_err(("sdk_usage", Some(401u16), message))
        }
        SdkError::Authorization(message) => {
            CloudSandboxClientError::new_err(("sdk_usage", Some(403u16), message))
        }
        SdkError::ServerError { status, message } => {
            CloudSandboxClientError::new_err(("remote_api", Some(status.as_u16()), message))
        }
        SdkError::Http(http_error) => {
            if http_error.is_timeout() {
                CloudSandboxClientError::new_err((
                    "connection",
                    Some(504u16),
                    http_error.to_string(),
                ))
            } else if http_error.is_connect() {
                CloudSandboxClientError::new_err((
                    "connection",
                    Some(503u16),
                    http_error.to_string(),
                ))
            } else {
                CloudSandboxClientError::new_err((
                    "internal",
                    Option::<u16>::None,
                    http_error.to_string(),
                ))
            }
        }
        SdkError::Middleware(middleware_error) => {
            let message = middleware_error.to_string();
            let lower = message.to_lowercase();
            if lower.contains("timeout") || lower.contains("connect") {
                CloudSandboxClientError::new_err(("connection", Option::<u16>::None, message))
            } else {
                CloudSandboxClientError::new_err(("internal", Option::<u16>::None, message))
            }
        }
        other => {
            CloudSandboxClientError::new_err(("internal", Option::<u16>::None, other.to_string()))
        }
    }
}

fn into_document_ai_py_error(error: SdkError) -> PyErr {
    match error {
        SdkError::Authentication(message) => {
            CloudDocumentAIClientError::new_err(("sdk_usage", Some(401u16), message))
        }
        SdkError::Authorization(message) => {
            CloudDocumentAIClientError::new_err(("sdk_usage", Some(403u16), message))
        }
        SdkError::ServerError { status, message } => {
            CloudDocumentAIClientError::new_err(("remote_api", Some(status.as_u16()), message))
        }
        SdkError::Http(http_error) => {
            if http_error.is_timeout() {
                CloudDocumentAIClientError::new_err((
                    "connection",
                    Some(504u16),
                    http_error.to_string(),
                ))
            } else if http_error.is_connect() {
                CloudDocumentAIClientError::new_err((
                    "connection",
                    Some(503u16),
                    http_error.to_string(),
                ))
            } else {
                CloudDocumentAIClientError::new_err((
                    "internal",
                    Option::<u16>::None,
                    http_error.to_string(),
                ))
            }
        }
        SdkError::Middleware(middleware_error) => {
            let message = middleware_error.to_string();
            let lower = message.to_lowercase();
            if lower.contains("timeout") || lower.contains("connect") {
                CloudDocumentAIClientError::new_err(("connection", Option::<u16>::None, message))
            } else {
                CloudDocumentAIClientError::new_err(("internal", Option::<u16>::None, message))
            }
        }
        other => CloudDocumentAIClientError::new_err((
            "internal",
            Option::<u16>::None,
            other.to_string(),
        )),
    }
}

fn build_logs_path(build_service_path: &str, build_id: &str) -> String {
    format!(
        "{}/builds/{build_id}/logs",
        build_service_path.trim_end_matches('/')
    )
}

async fn stream_build_log_events<F>(
    client: Client,
    path: &str,
    mut on_event: F,
) -> Result<(), SdkError>
where
    F: FnMut(&Value) -> Result<(), SdkError>,
{
    let mut stream = client.build_event_source_request::<Value>(path).await?;
    while let Some(event) = stream.next().await {
        let event = event?;
        on_event(&event)?;
    }
    Ok(())
}

#[derive(Clone)]
struct LogPrefix {
    label: String,
    color_code: Option<&'static str>,
}

impl LogPrefix {
    fn new(label: String, color: Option<String>) -> Self {
        Self {
            label,
            color_code: color.as_deref().and_then(ansi_color_code),
        }
    }

    fn render(&self) -> String {
        match self.color_code {
            Some(color_code) => format!("{color_code}{}:\x1b[0m ", self.label),
            None => format!("{}: ", self.label),
        }
    }
}

fn ansi_color_code(color: &str) -> Option<&'static str> {
    match color {
        "magenta" => Some("\x1b[35m"),
        "cyan" => Some("\x1b[36m"),
        "green" => Some("\x1b[32m"),
        "yellow" => Some("\x1b[33m"),
        "blue" => Some("\x1b[34m"),
        "white" => Some("\x1b[37m"),
        "red" => Some("\x1b[31m"),
        "bright_magenta" => Some("\x1b[95m"),
        "bright_cyan" => Some("\x1b[96m"),
        "bright_green" => Some("\x1b[92m"),
        "bright_yellow" => Some("\x1b[93m"),
        "bright_blue" => Some("\x1b[94m"),
        "bright_white" => Some("\x1b[97m"),
        "bright_red" => Some("\x1b[91m"),
        _ => None,
    }
}

fn print_build_log_event_to_stderr(event: &Value, prefix: Option<&LogPrefix>) {
    let build_status = event
        .get("build_status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if build_status == "pending" {
        print_prefixed_to_stderr("Build waiting in queue...", prefix, true);
        return;
    }

    let message = event
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let stream = event
        .get("stream")
        .and_then(Value::as_str)
        .unwrap_or_default();

    match stream {
        "stdout" => print_prefixed_to_stderr(message, prefix, false),
        "stderr" => print_prefixed_to_stderr(message, prefix, true),
        "info" => {
            let timestamp = event
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if timestamp.is_empty() {
                print_prefixed_to_stderr(message, prefix, true);
            } else {
                print_prefixed_to_stderr(&format!("{timestamp}: {message}"), prefix, true);
            }
        }
        _ => {
            if !message.is_empty() {
                print_prefixed_to_stderr(message, prefix, true);
            }
        }
    }
}

fn print_prefixed_to_stderr(message: &str, prefix: Option<&LogPrefix>, newline: bool) {
    let rendered_prefix = prefix.map(LogPrefix::render).unwrap_or_default();
    let mut stderr = std::io::stderr();

    let chunks: Vec<&str> = if message.is_empty() {
        vec![""]
    } else {
        message.split_inclusive('\n').collect()
    };

    for chunk in chunks {
        if rendered_prefix.is_empty() {
            let _ = write!(stderr, "{chunk}");
        } else {
            let _ = write!(stderr, "{rendered_prefix}{chunk}");
        }
    }

    if newline && !message.ends_with('\n') {
        let _ = writeln!(stderr);
    } else {
        let _ = stderr.flush();
    }
}

fn parse_json_payload<T: DeserializeOwned>(request_json: &str) -> PyResult<T> {
    serde_json::from_str(request_json).map_err(|error| {
        CloudSandboxClientError::new_err((
            "sdk_usage",
            Option::<u16>::None,
            format!("invalid JSON payload: {error}"),
        ))
    })
}

fn resolve_proxy_target(proxy_url: &str, sandbox_id: &str) -> PyResult<(String, Option<String>)> {
    let parsed = reqwest::Url::parse(proxy_url).map_err(|error| {
        CloudSandboxClientError::new_err((
            "sdk_usage",
            Option::<u16>::None,
            format!("invalid proxy url `{proxy_url}`: {error}"),
        ))
    })?;
    let host = parsed.host_str().ok_or_else(|| {
        CloudSandboxClientError::new_err((
            "sdk_usage",
            Option::<u16>::None,
            format!("proxy url `{proxy_url}` is missing a host"),
        ))
    })?;

    if host == "localhost" || host == "127.0.0.1" {
        return Ok((
            proxy_url.trim_end_matches('/').to_string(),
            Some(format!("{sandbox_id}.local")),
        ));
    }

    let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    let base_url = format!("{}://{sandbox_id}.{host}{port}", parsed.scheme());
    Ok((base_url, None))
}

fn is_localhost_api_url(api_url: &str) -> bool {
    reqwest::Url::parse(api_url)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
        .is_some_and(|host| host == "localhost" || host == "127.0.0.1")
}

/// Create a Docker build context tar.gz for a Tensorlake image definition.
///
/// Args:
///     base_image: The base Docker image (e.g. "python:3.11-slim-bookworm").
///     sdk_version: The tensorlake SDK version to install in the image.
///     operations_json: JSON array of build operations, each with keys
///         "op" ("RUN"|"COPY"|"ADD"|"ENV"), "args" (list of str), "options" (dict of str).
///     file_path: Destination path for the resulting tar.gz file.
#[pyfunction]
fn create_image_context_file(
    base_image: String,
    sdk_version: String,
    operations_json: String,
    file_path: String,
) -> PyResult<()> {
    use tensorlake_cloud_sdk::images::models::{Image, ImageBuildOperation};

    let operations: Vec<ImageBuildOperation> =
        serde_json::from_str(&operations_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to parse operations JSON: {e}"))
        })?;

    let image = Image::builder()
        .name(String::new())
        .base_image(base_image)
        .build_operations(operations)
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to build image: {e}"))
        })?;

    let mut file = std::fs::File::create(&file_path).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to create file '{}': {e}", file_path))
    })?;

    image
        .create_context_archive(&mut file, &sdk_version, None)
        .map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to write context archive: {e}"))
        })?;

    Ok(())
}

#[pymodule]
fn _cloud_sdk(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("CloudApiClientError", _py.get_type::<CloudApiClientError>())?;
    module.add(
        "CloudSandboxClientError",
        _py.get_type::<CloudSandboxClientError>(),
    )?;
    module.add(
        "CloudDocumentAIClientError",
        _py.get_type::<CloudDocumentAIClientError>(),
    )?;
    module.add_class::<CloudApiClient>()?;
    module.add_class::<CloudSandboxClient>()?;
    module.add_class::<CloudSandboxProxyClient>()?;
    module.add_class::<CloudDocumentAIClient>()?;
    module.add_function(wrap_pyfunction!(create_image_context_file, module)?)?;
    Ok(())
}
