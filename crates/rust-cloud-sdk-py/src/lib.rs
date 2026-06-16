#![allow(unexpected_cfgs)]
#![allow(unsafe_op_in_unsafe_fn)]
// PyO3 macro expansion currently triggers false-positive `useless_conversion` lints.
#![allow(clippy::useless_conversion)]
#![allow(clippy::too_many_arguments)]

use std::error::Error as StdError;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use reqwest::Method;
use reqwest::multipart::{Form, Part};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tensorlake::document_ai::DocumentAiClient;
use tensorlake::images::ImagesClient;
use tensorlake::images::models::{ApplicationBuildContext, CreateApplicationBuildRequest};
use tensorlake::sandbox_images::SandboxImageBuildEvent;
use tensorlake::sandbox_templates::SandboxTemplatesClient;
use tensorlake::sandboxes::models::{
    ArchivedSandboxesPaginationDirection, CreateSandboxRequest, ListArchivedSandboxesParams,
    SandboxPoolRequest, SnapshotType, UpdateSandboxRequest,
};
use tensorlake::sandboxes::{
    SandboxDesktopClient as RustSandboxDesktopClient, SandboxProxyClient, SandboxesClient,
};
use tensorlake::{Client, ClientBuilder, error::SdkError};
use tokio::runtime::Runtime;

create_exception!(_cloud_sdk, CloudApiClientError, PyException);
create_exception!(_cloud_sdk, CloudSandboxClientError, PyException);
create_exception!(_cloud_sdk, CloudDocumentAIClientError, PyException);

const DEFAULT_HTTP_REQUEST_TIMEOUT_SEC: f64 = 300.0;

// Single tokio runtime for the whole process: pyo3-async-runtimes' runtime,
// which `future_into_py` already drives. Routing sync `block_on` through here
// too keeps every client (sync and async) on the same runtime instead of each
// pyclass spawning its own.
fn shared_runtime() -> &'static Runtime {
    pyo3_async_runtimes::tokio::get_runtime()
}

#[pyclass]
pub struct CloudApiClient {
    client: Client,
    api_url: String,
    namespace: String,
}

#[pymethods]
impl CloudApiClient {
    #[new]
    #[pyo3(signature = (api_url, api_key=None, organization_id=None, project_id=None, namespace=None, user_agent=None))]
    fn new(
        api_url: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        namespace: Option<String>,
        user_agent: Option<String>,
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

        if let Some(ua) = user_agent.as_deref() {
            builder = builder.user_agent(ua);
        }

        let client = builder.build().map_err(into_py_error)?;

        Ok(Self {
            client,
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

    fn delete_sandbox_image(&self, image_name: String) -> PyResult<()> {
        let namespace = self.namespace.clone();
        self.run_with_retry(5, move |client| {
            let encoded_image = urlencoding::encode(&image_name).into_owned();
            let path = format!("/v1/namespaces/{namespace}/sandbox-images/{encoded_image}");
            async move {
                let request = client.request(Method::DELETE, &path).build()?;
                let _response = client.execute(request).await?;
                Ok(())
            }
        })
    }

    /// Look up a registered sandbox image (template) by name.
    ///
    /// Returns the template JSON, or `None` when no image with that name
    /// exists. Routed through the platform sandbox-templates API, which
    /// requires the organization/project scope passed here.
    fn find_sandbox_image_by_name(
        &self,
        organization_id: String,
        project_id: String,
        image_name: String,
    ) -> PyResult<Option<String>> {
        self.run_with_retry(5, move |client| {
            let organization_id = organization_id.clone();
            let project_id = project_id.clone();
            let image_name = image_name.clone();
            async move {
                let templates = SandboxTemplatesClient::new(client, organization_id, project_id);
                match templates.find_by_name(&image_name).await? {
                    Some(traced) => {
                        let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                        Ok(Some(json))
                    }
                    None => Ok(None),
                }
            }
        })
    }

    /// List all registered sandbox images (templates) for the given scope.
    ///
    /// Returns a JSON array of templates. Routed through the platform
    /// sandbox-templates API, which requires the organization/project scope.
    fn list_sandbox_images(&self, organization_id: String, project_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let organization_id = organization_id.clone();
            let project_id = project_id.clone();
            async move {
                let templates = SandboxTemplatesClient::new(client, organization_id, project_id);
                let traced = templates.list().await?;
                serde_json::to_string(&*traced).map_err(SdkError::from)
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
                serde_json::to_string(&*response).map_err(SdkError::from)
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
                serde_json::to_string(&*response).map_err(SdkError::from)
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
                serde_json::to_string(&*response).map_err(SdkError::from)
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
                        events.push(serde_json::to_string(&event).map_err(SdkError::from)?);
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
}

#[pymethods]
impl CloudSandboxClient {
    #[new]
    #[pyo3(signature = (api_url, api_key=None, organization_id=None, project_id=None, namespace=None, user_agent=None, request_timeout_sec=None))]
    fn new(
        api_url: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        namespace: Option<String>,
        user_agent: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> PyResult<Self> {
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

        let client = builder.build().map_err(into_sandbox_py_error)?;
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

    fn close(&self) {
        // reqwest clients are closed when dropped; this is a no-op for API parity.
    }

    fn create_sandbox(&self, request_json: String) -> PyResult<(String, String)> {
        let request: CreateSandboxRequest = parse_json_payload(&request_json)?;
        let client = self.client.clone();
        shared_runtime()
            .block_on(async move {
                let traced = client.create(&request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            })
            .map_err(into_sandbox_py_error)
    }

    fn claim_sandbox(&self, pool_id: String) -> PyResult<(String, String)> {
        let client = self.client.clone();
        shared_runtime()
            .block_on(async move {
                let traced = client.claim(&pool_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            })
            .map_err(into_sandbox_py_error)
    }

    fn copy_sandbox(&self, sandbox_id: String, times: usize) -> PyResult<(String, String)> {
        let client = self.client.clone();
        shared_runtime()
            .block_on(async move {
                let traced = client.copy(&sandbox_id, times).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            })
            .map_err(into_sandbox_py_error)
    }

    fn get_sandbox_json(&self, sandbox_id: String) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let traced = client.get(&sandbox_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn list_sandboxes_json(&self) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.list().await?;
            let trace_id = traced.trace_id.clone();
            let response = serde_json::json!({ "sandboxes": *traced });
            let json = serde_json::to_string(&response).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    #[pyo3(signature = (limit=None, cursor=None, direction=None))]
    fn list_archived_sandboxes_json(
        &self,
        limit: Option<usize>,
        cursor: Option<String>,
        direction: Option<String>,
    ) -> PyResult<(String, String)> {
        let params = parse_archived_sandboxes_params(limit, cursor, direction)?;
        self.run_with_retry(5, move |client| {
            let params = params.clone();
            async move {
                let traced = client.list_archived(&params).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn get_archived_sandbox_json(&self, sandbox_id: String) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let traced = client.get_archived(&sandbox_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn update_sandbox(
        &self,
        sandbox_id: String,
        request_json: String,
    ) -> PyResult<(String, String)> {
        let request: UpdateSandboxRequest = parse_json_payload(&request_json)?;
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            let request = request.clone();
            async move {
                let traced = client.update(&sandbox_id, &request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn delete_sandbox(&self, sandbox_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move { client.delete(&sandbox_id).await.map(|t| t.trace_id) }
        })
    }

    fn suspend_sandbox(&self, sandbox_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move { client.suspend(&sandbox_id).await.map(|t| t.trace_id) }
        })
    }

    fn resume_sandbox(&self, sandbox_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move { client.resume(&sandbox_id).await.map(|t| t.trace_id) }
        })
    }

    #[pyo3(signature = (sandbox_id, snapshot_type=None))]
    fn create_snapshot(
        &self,
        sandbox_id: String,
        snapshot_type: Option<String>,
    ) -> PyResult<(String, String)> {
        let parsed_type = match snapshot_type.as_deref() {
            None => None,
            Some("memory") => Some(SnapshotType::Memory),
            Some("filesystem") => Some(SnapshotType::Filesystem),
            Some(other) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid snapshot_type '{other}': expected 'memory' or 'filesystem'"
                )));
            }
        };
        self.run_with_retry(5, move |client| {
            let sandbox_id = sandbox_id.clone();
            async move {
                let traced = client.snapshot(&sandbox_id, parsed_type).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn get_snapshot_json(&self, snapshot_id: String) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| {
            let snapshot_id = snapshot_id.clone();
            async move {
                let traced = client.get_snapshot(&snapshot_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn list_snapshots_json(&self) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.list_snapshots().await?;
            let trace_id = traced.trace_id.clone();
            let response = serde_json::json!({ "snapshots": *traced });
            let json = serde_json::to_string(&response).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn delete_snapshot(&self, snapshot_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let snapshot_id = snapshot_id.clone();
            async move {
                client
                    .delete_snapshot(&snapshot_id)
                    .await
                    .map(|t| t.trace_id)
            }
        })
    }

    fn create_pool(&self, request_json: String) -> PyResult<(String, String)> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        self.run_with_retry(5, move |client| {
            let request = request.clone();
            async move {
                let traced = client.create_pool(&request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn get_pool_json(&self, pool_id: String) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| {
            let pool_id = pool_id.clone();
            async move {
                let traced = client.get_pool(&pool_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn list_pools_json(&self) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.list_pools().await?;
            let trace_id = traced.trace_id.clone();
            let response = serde_json::json!({ "pools": *traced });
            let json = serde_json::to_string(&response).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn update_pool(&self, pool_id: String, request_json: String) -> PyResult<(String, String)> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        self.run_with_retry(5, move |client| {
            let pool_id = pool_id.clone();
            let request = request.clone();
            async move {
                let traced = client.update_pool(&pool_id, &request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn delete_pool(&self, pool_id: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let pool_id = pool_id.clone();
            async move { client.delete_pool(&pool_id).await.map(|t| t.trace_id) }
        })
    }

    // ---- Async variants (Python awaitables backed by future_into_py) ----

    fn create_sandbox_async<'py>(
        &self,
        py: Python<'py>,
        request_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request: CreateSandboxRequest = parse_json_payload(&request_json)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = client
                .create(&request)
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn claim_sandbox_async<'py>(
        &self,
        py: Python<'py>,
        pool_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = client
                .claim(&pool_id)
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn copy_sandbox_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
        times: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = client
                .copy(&sandbox_id, times)
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn get_sandbox_json_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let sandbox_id = sandbox_id.clone();
                async move { c.get(&sandbox_id).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn list_sandboxes_json_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move { c.list().await })
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let response = serde_json::json!({ "sandboxes": *traced });
            let json = serde_json::to_string(&response).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    #[pyo3(signature = (limit=None, cursor=None, direction=None))]
    fn list_archived_sandboxes_json_async<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
        cursor: Option<String>,
        direction: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = parse_archived_sandboxes_params(limit, cursor, direction)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let params = params.clone();
                async move { c.list_archived(&params).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn get_archived_sandbox_json_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let sandbox_id = sandbox_id.clone();
                async move { c.get_archived(&sandbox_id).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn update_sandbox_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
        request_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request: UpdateSandboxRequest = parse_json_payload(&request_json)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let sandbox_id = sandbox_id.clone();
                let request = request.clone();
                async move { c.update(&sandbox_id, &request).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn delete_sandbox_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let sandbox_id = sandbox_id.clone();
                async move { c.delete(&sandbox_id).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn suspend_sandbox_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let sandbox_id = sandbox_id.clone();
                async move { c.suspend(&sandbox_id).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn resume_sandbox_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let sandbox_id = sandbox_id.clone();
                async move { c.resume(&sandbox_id).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    #[pyo3(signature = (sandbox_id, snapshot_type=None))]
    fn create_snapshot_async<'py>(
        &self,
        py: Python<'py>,
        sandbox_id: String,
        snapshot_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let parsed_type = match snapshot_type.as_deref() {
            None => None,
            Some("memory") => Some(SnapshotType::Memory),
            Some("filesystem") => Some(SnapshotType::Filesystem),
            Some(other) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid snapshot_type '{other}': expected 'memory' or 'filesystem'"
                )));
            }
        };
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let sandbox_id = sandbox_id.clone();
                async move { c.snapshot(&sandbox_id, parsed_type).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn get_snapshot_json_async<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let snapshot_id = snapshot_id.clone();
                async move { c.get_snapshot(&snapshot_id).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn list_snapshots_json_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced =
                retry_async_op(client, 5, move |c| async move { c.list_snapshots().await })
                    .await
                    .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let response = serde_json::json!({ "snapshots": *traced });
            let json = serde_json::to_string(&response).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn delete_snapshot_async<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let snapshot_id = snapshot_id.clone();
                async move { c.delete_snapshot(&snapshot_id).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn create_pool_async<'py>(
        &self,
        py: Python<'py>,
        request_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let request = request.clone();
                async move { c.create_pool(&request).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn get_pool_json_async<'py>(
        &self,
        py: Python<'py>,
        pool_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let pool_id = pool_id.clone();
                async move { c.get_pool(&pool_id).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn list_pools_json_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move { c.list_pools().await })
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let response = serde_json::json!({ "pools": *traced });
            let json = serde_json::to_string(&response).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn update_pool_async<'py>(
        &self,
        py: Python<'py>,
        pool_id: String,
        request_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request: SandboxPoolRequest = parse_json_payload(&request_json)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let pool_id = pool_id.clone();
                let request = request.clone();
                async move { c.update_pool(&pool_id, &request).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn delete_pool_async<'py>(
        &self,
        py: Python<'py>,
        pool_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let pool_id = pool_id.clone();
                async move { c.delete_pool(&pool_id).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    /// Create a proxy client for the given sandbox that shares this client's HTTP connection
    /// pool. All proxy clients created this way reuse the same underlying reqwest::Client,
    /// so HTTP/2 connections can be coalesced: only the first sandbox in a session pays the
    /// TCP+TLS handshake cost.
    #[pyo3(signature = (proxy_url, sandbox_id, routing_hint=None, request_timeout_sec=None))]
    fn connect_proxy(
        &self,
        proxy_url: String,
        sandbox_id: String,
        routing_hint: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> PyResult<CloudSandboxProxyClient> {
        let (base_url, host_override, sandbox_id_header) =
            resolve_proxy_target(&proxy_url, &sandbox_id)?;
        let shared_client = if let Some(seconds) = request_timeout_sec {
            self.client
                .http_client()
                .with_base_url_and_timeout(
                    &base_url,
                    Some(duration_from_seconds("request_timeout_sec", seconds)?),
                )
                .map_err(into_sandbox_py_error)?
        } else {
            self.client
                .http_client()
                .with_base_url_without_timeout(&base_url)
                .map_err(into_sandbox_py_error)?
        };
        let proxy = SandboxProxyClient::new(shared_client, host_override)
            .with_sandbox_id(sandbox_id_header)
            .with_routing_hint(routing_hint);
        Ok(CloudSandboxProxyClient {
            client: proxy,
            base_url,
        })
    }
}

impl CloudSandboxProxyClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, operation: F) -> PyResult<T>
    where
        F: FnMut(SandboxProxyClient) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        run_with_retry_blocking(
            self.client.clone(),
            max_retries,
            "rust sandbox proxy request",
            into_sandbox_py_error,
            operation,
        )
    }
}

#[pyclass]
pub struct CloudSandboxProxyClient {
    client: SandboxProxyClient,
    base_url: String,
}

#[pymethods]
impl CloudSandboxProxyClient {
    #[new]
    #[pyo3(signature = (proxy_url, sandbox_id, api_key=None, organization_id=None, project_id=None, routing_hint=None, user_agent=None, request_timeout_sec=None))]
    fn new(
        proxy_url: String,
        sandbox_id: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        routing_hint: Option<String>,
        user_agent: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> PyResult<Self> {
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

        let client = builder.build().map_err(into_sandbox_py_error)?;
        let sandbox_proxy_client = SandboxProxyClient::new(client, host_override)
            .with_sandbox_id(sandbox_id_header)
            .with_routing_hint(routing_hint);

        Ok(Self {
            client: sandbox_proxy_client,
            base_url,
        })
    }

    fn close(&self) {
        // reqwest clients are closed when dropped; this is a no-op for API parity.
    }

    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    fn start_process_json(&self, payload_json: String) -> PyResult<(String, String)> {
        let payload: Value = parse_json_payload(&payload_json)?;
        self.run_with_retry(5, move |client| {
            let payload = payload.clone();
            async move {
                let traced = client.start_process(&payload).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn list_processes_json(&self) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.list_processes().await?;
            let trace_id = traced.trace_id.clone();
            let processes = traced.into_inner();
            let response = serde_json::json!({ "processes": processes });
            Ok((
                trace_id,
                serde_json::to_string(&response).map_err(SdkError::from)?,
            ))
        })
    }

    fn get_process_json(&self, pid: i64) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.get_process(pid).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn kill_process(&self, pid: i64) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.kill_process(pid).await?;
            Ok(traced.trace_id)
        })
    }

    fn restart_process_json(&self, pid: i64) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.restart_process(pid).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn send_signal_json(&self, pid: i64, signal: i64) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.send_signal(pid, signal).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn write_stdin(&self, pid: i64, data: Vec<u8>) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let data = data.clone();
            async move {
                let traced = client.write_stdin(pid, data).await?;
                Ok(traced.trace_id)
            }
        })
    }

    fn close_stdin(&self, pid: i64) -> PyResult<String> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.close_stdin(pid).await?;
            Ok(traced.trace_id)
        })
    }

    fn get_stdout_json(&self, pid: i64) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.get_stdout(pid).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn get_stderr_json(&self, pid: i64) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.get_stderr(pid).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn get_output_json(&self, pid: i64) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.get_output(pid).await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn follow_stdout_json(&self, pid: i64) -> PyResult<(String, Vec<String>)> {
        self.run_with_retry(10, move |client| async move {
            let traced = client.follow_stdout(pid).await?;
            let trace_id = traced.trace_id.clone();
            let events = traced
                .into_inner()
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                .collect::<Result<Vec<String>, SdkError>>()?;
            Ok((trace_id, events))
        })
    }

    fn follow_stderr_json(&self, pid: i64) -> PyResult<(String, Vec<String>)> {
        self.run_with_retry(10, move |client| async move {
            let traced = client.follow_stderr(pid).await?;
            let trace_id = traced.trace_id.clone();
            let events = traced
                .into_inner()
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                .collect::<Result<Vec<String>, SdkError>>()?;
            Ok((trace_id, events))
        })
    }

    fn follow_output_json(&self, pid: i64) -> PyResult<(String, Vec<String>)> {
        self.run_with_retry(10, move |client| async move {
            let traced = client.follow_output(pid).await?;
            let trace_id = traced.trace_id.clone();
            let events = traced
                .into_inner()
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                .collect::<Result<Vec<String>, SdkError>>()?;
            Ok((trace_id, events))
        })
    }

    fn read_file_bytes(
        &self,
        py: Python<'_>,
        path: String,
    ) -> PyResult<(String, Py<pyo3::types::PyBytes>)> {
        let (trace_id, data): (String, Vec<u8>) = self.run_with_retry(5, move |client| {
            let path = path.clone();
            async move {
                let traced = client.read_file(&path).await?;
                Ok((traced.trace_id.clone(), traced.into_inner()))
            }
        })?;
        Ok((trace_id, pyo3::types::PyBytes::new(py, &data).into()))
    }

    fn write_file(&self, path: String, content: Vec<u8>) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let path = path.clone();
            let content = content.clone();
            async move {
                let traced = client.write_file(&path, content).await?;
                Ok(traced.trace_id)
            }
        })
    }

    fn upload_file(&self, path: String, local_path: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let path = path.clone();
            let local_path = local_path.clone();
            async move {
                let traced = client.upload_file(&path, local_path).await?;
                Ok(traced.trace_id)
            }
        })
    }

    fn delete_file(&self, path: String) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let path = path.clone();
            async move {
                let traced = client.delete_file(&path).await?;
                Ok(traced.trace_id)
            }
        })
    }

    fn list_directory_json(&self, path: String) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| {
            let path = path.clone();
            async move {
                let traced = client.list_directory(&path).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn create_pty_session_json(&self, payload_json: String) -> PyResult<(String, String)> {
        let payload: Value = parse_json_payload(&payload_json)?;
        self.run_with_retry(5, move |client| {
            let payload = payload.clone();
            async move {
                let traced = client.create_pty_session(&payload).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
                Ok((trace_id, json))
            }
        })
    }

    fn run_process_json(&self, payload_json: String) -> PyResult<(String, Vec<String>)> {
        let payload: Value = parse_json_payload(&payload_json)?;
        self.run_with_retry(5, move |client| {
            let payload = payload.clone();
            async move {
                let traced = client.run_process(&payload).await?;
                let trace_id = traced.trace_id.clone();
                let events = traced
                    .into_inner()
                    .into_iter()
                    .map(|event| serde_json::to_string(&event).map_err(SdkError::from))
                    .collect::<Result<Vec<String>, SdkError>>()?;
                Ok((trace_id, events))
            }
        })
    }

    fn health_json(&self) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.health().await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    fn info_json(&self) -> PyResult<(String, String)> {
        self.run_with_retry(5, move |client| async move {
            let traced = client.info().await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(SdkError::from)?;
            Ok((trace_id, json))
        })
    }

    // ---- Async variants (Python awaitables backed by future_into_py) ----

    fn start_process_json_async<'py>(
        &self,
        py: Python<'py>,
        payload_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload: Value = parse_json_payload(&payload_json)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let payload = payload.clone();
                async move { c.start_process(&payload).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn list_processes_json_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced =
                retry_async_op(client, 5, move |c| async move { c.list_processes().await })
                    .await
                    .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let processes = traced.into_inner();
            let response = serde_json::json!({ "processes": processes });
            let json = serde_json::to_string(&response).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn get_process_json_async<'py>(
        &self,
        py: Python<'py>,
        pid: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced =
                retry_async_op(client, 5, move |c| async move { c.get_process(pid).await })
                    .await
                    .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn kill_process_async<'py>(&self, py: Python<'py>, pid: i64) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| async move {
                c.kill_process(pid).await.map(|t| t.trace_id)
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn restart_process_json_async<'py>(
        &self,
        py: Python<'py>,
        pid: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(
                client,
                5,
                move |c| async move { c.restart_process(pid).await },
            )
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn send_signal_json_async<'py>(
        &self,
        py: Python<'py>,
        pid: i64,
        signal: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move {
                c.send_signal(pid, signal).await
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn write_stdin_async<'py>(
        &self,
        py: Python<'py>,
        pid: i64,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let data = data.clone();
                async move { c.write_stdin(pid, data).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn close_stdin_async<'py>(&self, py: Python<'py>, pid: i64) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| async move {
                c.close_stdin(pid).await.map(|t| t.trace_id)
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn get_stdout_json_async<'py>(&self, py: Python<'py>, pid: i64) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move { c.get_stdout(pid).await })
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn get_stderr_json_async<'py>(&self, py: Python<'py>, pid: i64) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move { c.get_stderr(pid).await })
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn get_output_json_async<'py>(&self, py: Python<'py>, pid: i64) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move { c.get_output(pid).await })
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn follow_stdout_json_async<'py>(
        &self,
        py: Python<'py>,
        pid: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(
                client,
                10,
                move |c| async move { c.follow_stdout(pid).await },
            )
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let events: Vec<String> = traced
                .into_inner()
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(sandbox_serde_err))
                .collect::<Result<Vec<String>, _>>()?;
            Ok((trace_id, events))
        })
    }

    fn follow_stderr_json_async<'py>(
        &self,
        py: Python<'py>,
        pid: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(
                client,
                10,
                move |c| async move { c.follow_stderr(pid).await },
            )
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let events: Vec<String> = traced
                .into_inner()
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(sandbox_serde_err))
                .collect::<Result<Vec<String>, _>>()?;
            Ok((trace_id, events))
        })
    }

    fn follow_output_json_async<'py>(
        &self,
        py: Python<'py>,
        pid: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(
                client,
                10,
                move |c| async move { c.follow_output(pid).await },
            )
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let events: Vec<String> = traced
                .into_inner()
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(sandbox_serde_err))
                .collect::<Result<Vec<String>, _>>()?;
            Ok((trace_id, events))
        })
    }

    fn read_file_bytes_async<'py>(
        &self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let (trace_id, data): (String, Vec<u8>) = retry_async_op(client, 5, move |c| {
                let path = path.clone();
                async move {
                    let traced = c.read_file(&path).await?;
                    Ok((traced.trace_id.clone(), traced.into_inner()))
                }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok((trace_id, data))
        })
    }

    fn write_file_async<'py>(
        &self,
        py: Python<'py>,
        path: String,
        content: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let path = path.clone();
                let content = content.clone();
                async move { c.write_file(&path, content).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn upload_file_async<'py>(
        &self,
        py: Python<'py>,
        path: String,
        local_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let path = path.clone();
                let local_path = local_path.clone();
                async move { c.upload_file(&path, local_path).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn delete_file_async<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let trace_id = retry_async_op(client, 5, move |c| {
                let path = path.clone();
                async move { c.delete_file(&path).await.map(|t| t.trace_id) }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            Ok(trace_id)
        })
    }

    fn list_directory_json_async<'py>(
        &self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let path = path.clone();
                async move { c.list_directory(&path).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn create_pty_session_json_async<'py>(
        &self,
        py: Python<'py>,
        payload_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload: Value = parse_json_payload(&payload_json)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let payload = payload.clone();
                async move { c.create_pty_session(&payload).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn run_process_json_async<'py>(
        &self,
        py: Python<'py>,
        payload_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload: Value = parse_json_payload(&payload_json)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| {
                let payload = payload.clone();
                async move { c.run_process(&payload).await }
            })
            .await
            .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let events: Vec<String> = traced
                .into_inner()
                .into_iter()
                .map(|event| serde_json::to_string(&event).map_err(sandbox_serde_err))
                .collect::<Result<Vec<String>, _>>()?;
            Ok((trace_id, events))
        })
    }

    fn health_json_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move { c.health().await })
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }

    fn info_json_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let traced = retry_async_op(client, 5, move |c| async move { c.info().await })
                .await
                .map_err(into_sandbox_py_error)?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced).map_err(sandbox_serde_err)?;
            Ok((trace_id, json))
        })
    }
}

#[pyclass]
pub struct CloudSandboxDesktopClient {
    client: RustSandboxDesktopClient,
}

#[pymethods]
impl CloudSandboxDesktopClient {
    #[new]
    #[pyo3(signature = (proxy_url, sandbox_id, port=5901, password=None, shared=true, connect_timeout_sec=10.0, api_key=None, organization_id=None, project_id=None, user_agent=None))]
    fn new(
        proxy_url: String,
        sandbox_id: String,
        port: u16,
        password: Option<String>,
        shared: bool,
        connect_timeout_sec: f64,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        user_agent: Option<String>,
    ) -> PyResult<Self> {
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

        let client = builder.build().map_err(into_sandbox_py_error)?;
        let proxy_client =
            SandboxProxyClient::new(client, host_override).with_sandbox_id(sandbox_id_header);
        let connect_timeout = duration_from_seconds("connect_timeout_sec", connect_timeout_sec)?;
        let desktop_client = shared_runtime()
            .block_on(RustSandboxDesktopClient::connect(
                proxy_client,
                port,
                password,
                shared,
                connect_timeout,
            ))
            .map_err(into_sandbox_py_error)?;

        Ok(Self {
            client: desktop_client,
        })
    }

    fn close(&self) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.close())
            .map_err(into_sandbox_py_error)
    }

    fn screenshot_png(
        &self,
        py: Python<'_>,
        timeout_sec: f64,
    ) -> PyResult<Py<pyo3::types::PyBytes>> {
        let timeout_sec = duration_from_seconds("timeout_sec", timeout_sec)?;
        let png = shared_runtime()
            .block_on(self.client.screenshot(timeout_sec))
            .map_err(into_sandbox_py_error)?;
        Ok(pyo3::types::PyBytes::new(py, &png).into())
    }

    fn move_mouse(&self, x: u16, y: u16) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.move_mouse(x, y))
            .map_err(into_sandbox_py_error)
    }

    #[pyo3(signature = (button="left", x=None, y=None))]
    fn mouse_press(&self, button: &str, x: Option<u16>, y: Option<u16>) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.mouse_press(button, x, y))
            .map_err(into_sandbox_py_error)
    }

    #[pyo3(signature = (button="left", x=None, y=None))]
    fn mouse_release(&self, button: &str, x: Option<u16>, y: Option<u16>) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.mouse_release(button, x, y))
            .map_err(into_sandbox_py_error)
    }

    #[pyo3(signature = (button="left", x=None, y=None))]
    fn click(&self, button: &str, x: Option<u16>, y: Option<u16>) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.click(button, x, y))
            .map_err(into_sandbox_py_error)
    }

    #[pyo3(signature = (button="left", x=None, y=None, delay_ms=50))]
    fn double_click(
        &self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
        delay_ms: u64,
    ) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.double_click(button, x, y, delay_ms))
            .map_err(into_sandbox_py_error)
    }

    #[pyo3(signature = (steps, x=None, y=None))]
    fn scroll(&self, steps: i32, x: Option<u16>, y: Option<u16>) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.scroll(steps, x, y))
            .map_err(into_sandbox_py_error)
    }

    fn key_down(&self, key: &str) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.key_down(key))
            .map_err(into_sandbox_py_error)
    }

    fn key_up(&self, key: &str) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.key_up(key))
            .map_err(into_sandbox_py_error)
    }

    fn press(&self, keys: Vec<String>) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.press(&keys))
            .map_err(into_sandbox_py_error)
    }

    fn type_text(&self, text: String) -> PyResult<()> {
        shared_runtime()
            .block_on(self.client.type_text(&text))
            .map_err(into_sandbox_py_error)
    }

    #[getter]
    fn width(&self) -> PyResult<u16> {
        shared_runtime()
            .block_on(async { self.client.dimensions().await.map(|(width, _)| width) })
            .map_err(into_sandbox_py_error)
    }

    #[getter]
    fn height(&self) -> PyResult<u16> {
        shared_runtime()
            .block_on(async { self.client.dimensions().await.map(|(_, height)| height) })
            .map_err(into_sandbox_py_error)
    }
}

#[pyclass]
pub struct CloudDocumentAIClient {
    client: DocumentAiClient,
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
        let document_ai_client = DocumentAiClient::new(client);

        Ok(Self {
            client: document_ai_client,
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
                serde_json::to_string(&response).map_err(SdkError::from)
            }
        })
    }

    fn upload_file_json(&self, file_name: String, content: Vec<u8>) -> PyResult<String> {
        self.run_with_retry(5, move |client| {
            let file_name = file_name.clone();
            let content = content.clone();
            async move {
                let response = client.upload_file(&file_name, content).await?;
                serde_json::to_string(&response).map_err(SdkError::from)
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

fn run_with_retry_blocking<C, T, F, Fut>(
    client: C,
    max_retries: usize,
    label: &str,
    into_err: fn(SdkError) -> PyErr,
    mut operation: F,
) -> PyResult<T>
where
    C: Clone,
    F: FnMut(C) -> Fut,
    Fut: Future<Output = Result<T, SdkError>>,
{
    let mut retries = 0usize;
    loop {
        match shared_runtime().block_on(operation(client.clone())) {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !is_retryable(&err) || retries >= max_retries {
                    return Err(into_err(err));
                }

                retries += 1;
                let sleep_time = calculate_sleep_time(retries);
                eprintln!(
                    "Retrying {label} after {sleep_time:.2} seconds. Retry count: {retries}. Retryable exception: {err}"
                );
                std::thread::sleep(Duration::from_secs_f64(sleep_time));
            }
        }
    }
}

impl CloudApiClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, operation: F) -> PyResult<T>
    where
        F: FnMut(Client) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        run_with_retry_blocking(
            self.client.clone(),
            max_retries,
            "rust cloud API request",
            into_py_error,
            operation,
        )
    }
}

impl CloudSandboxClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, operation: F) -> PyResult<T>
    where
        F: FnMut(SandboxesClient) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        run_with_retry_blocking(
            self.client.clone(),
            max_retries,
            "rust sandbox API request",
            into_sandbox_py_error,
            operation,
        )
    }
}

impl CloudDocumentAIClient {
    fn run_with_retry<T, F, Fut>(&self, max_retries: usize, operation: F) -> PyResult<T>
    where
        F: FnMut(DocumentAiClient) -> Fut,
        Fut: Future<Output = Result<T, SdkError>>,
    {
        run_with_retry_blocking(
            self.client.clone(),
            max_retries,
            "rust document-ai request",
            into_document_ai_py_error,
            operation,
        )
    }
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

fn sandbox_serde_err(err: serde_json::Error) -> PyErr {
    into_sandbox_py_error(SdkError::from(err))
}

fn calculate_sleep_time(retries: usize) -> f64 {
    let initial_delay_seconds: f64 = 0.1;
    let max_delay_seconds: f64 = 15.0;
    let jitter_multiplier: f64 = 0.75;
    let base_delay = initial_delay_seconds * 2f64.powi(retries as i32);
    base_delay.min(max_delay_seconds) * jitter_multiplier
}

fn duration_from_seconds(name: &str, seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(CloudSandboxClientError::new_err((
            "sdk_usage",
            Option::<u16>::None,
            format!("{name} must be a positive finite number"),
        )));
    }
    Ok(Duration::from_secs_f64(seconds))
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
            if http_error.is_timeout() || http_error.is_connect() {
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

fn parse_archived_sandboxes_params(
    limit: Option<usize>,
    cursor: Option<String>,
    direction: Option<String>,
) -> PyResult<ListArchivedSandboxesParams> {
    let direction = match direction.as_deref() {
        None => None,
        Some("forward") => Some(ArchivedSandboxesPaginationDirection::Forward),
        Some("backward") => Some(ArchivedSandboxesPaginationDirection::Backward),
        Some(other) => {
            return Err(CloudSandboxClientError::new_err((
                "sdk_usage",
                Option::<u16>::None,
                format!("invalid direction '{other}': expected 'forward' or 'backward'"),
            )));
        }
    };
    Ok(ListArchivedSandboxesParams {
        limit,
        cursor,
        direction,
    })
}

/// Resolve the proxy base URL and routing headers for a sandbox connection.
///
/// Returns `(base_url, host_override, sandbox_id_header)`:
/// - localhost: `base_url` = proxy URL as-is; `host_override` = `{sandbox_id}.local`; no sandbox_id header
/// - cloud: `base_url` = the server-selected ingress endpoint; `sandbox_id_header` = sandbox_id for `X-Tensorlake-Sandbox-Id` header
fn resolve_proxy_target(
    proxy_url: &str,
    sandbox_id: &str,
) -> PyResult<(String, Option<String>, Option<String>)> {
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
            None,
        ));
    }

    // Cloud: use apex domain with X-Tensorlake-Sandbox-Id header for routing.
    let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    let base_url = format!("{}://{host}{port}", parsed.scheme());
    Ok((base_url, None, Some(sandbox_id.to_string())))
}

fn is_localhost_api_url(api_url: &str) -> bool {
    reqwest::Url::parse(api_url)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
        .is_some_and(|host| host == "localhost" || host == "127.0.0.1")
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
    use tensorlake::images::models::{Image, ImageBuildOperation};

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

#[pyfunction]
#[pyo3(signature = (
    api_url,
    token,
    dockerfile_path,
    registered_name=None,
    disk_mb=None,
    builder_disk_mb=None,
    cpus=None,
    memory_mb=None,
    is_public=false,
    organization_id=None,
    project_id=None,
    namespace=None,
    use_scope_headers=false,
    user_agent=None,
    docker_compat=false,
    dockerfile_text=None,
    context_dir=None,
    emit=None,
))]
fn build_sandbox_image(
    py: Python<'_>,
    api_url: String,
    token: String,
    dockerfile_path: String,
    registered_name: Option<String>,
    disk_mb: Option<u64>,
    builder_disk_mb: Option<u64>,
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    is_public: bool,
    organization_id: Option<String>,
    project_id: Option<String>,
    namespace: Option<String>,
    use_scope_headers: bool,
    user_agent: Option<String>,
    docker_compat: bool,
    dockerfile_text: Option<String>,
    context_dir: Option<String>,
    emit: Option<Py<PyAny>>,
) -> PyResult<String> {
    let options = tensorlake::sandbox_images::SandboxImageBuildOptions {
        common: common_build_options(
            api_url,
            token,
            use_scope_headers,
            organization_id,
            project_id,
            namespace,
            registered_name,
            disk_mb,
            builder_disk_mb,
            cpus,
            memory_mb,
            is_public,
            user_agent,
            docker_compat,
        ),
        dockerfile_path: PathBuf::from(dockerfile_path),
        dockerfile_text,
        context_dir: context_dir.map(PathBuf::from),
    };

    let result = py
        .detach(move || {
            shared_runtime().block_on(async move {
                tensorlake::sandbox_images::build_sandbox_image(options, |event| {
                    if let Some(callback) = emit.as_ref() {
                        emit_sandbox_image_event(callback, event);
                    }
                })
                .await
            })
        })
        .map_err(|error| {
            CloudSandboxClientError::new_err((
                "sandbox_image_build",
                Option::<u16>::None,
                error.to_string(),
            ))
        })?;

    serde_json::to_string(&result).map_err(|error| {
        CloudSandboxClientError::new_err((
            "sandbox_image_build",
            Option::<u16>::None,
            error.to_string(),
        ))
    })
}

#[pyfunction]
#[pyo3(signature = (
    api_url,
    token,
    image_reference,
    registered_name=None,
    disk_mb=None,
    builder_disk_mb=None,
    cpus=None,
    memory_mb=None,
    is_public=false,
    organization_id=None,
    project_id=None,
    namespace=None,
    use_scope_headers=false,
    user_agent=None,
    docker_compat=false,
    emit=None,
))]
fn import_sandbox_image(
    py: Python<'_>,
    api_url: String,
    token: String,
    image_reference: String,
    registered_name: Option<String>,
    disk_mb: Option<u64>,
    builder_disk_mb: Option<u64>,
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    is_public: bool,
    organization_id: Option<String>,
    project_id: Option<String>,
    namespace: Option<String>,
    use_scope_headers: bool,
    user_agent: Option<String>,
    docker_compat: bool,
    emit: Option<Py<PyAny>>,
) -> PyResult<String> {
    let options = tensorlake::sandbox_images::SandboxImageImportOptions {
        common: common_build_options(
            api_url,
            token,
            use_scope_headers,
            organization_id,
            project_id,
            namespace,
            registered_name,
            disk_mb,
            builder_disk_mb,
            cpus,
            memory_mb,
            is_public,
            user_agent,
            docker_compat,
        ),
        image_reference,
    };

    let result = py
        .detach(move || {
            shared_runtime().block_on(async move {
                tensorlake::sandbox_images::import_sandbox_image(options, |event| {
                    if let Some(callback) = emit.as_ref() {
                        emit_sandbox_image_event(callback, event);
                    }
                })
                .await
            })
        })
        .map_err(|error| {
            CloudSandboxClientError::new_err((
                "sandbox_image_import",
                Option::<u16>::None,
                error.to_string(),
            ))
        })?;

    serde_json::to_string(&result).map_err(|error| {
        CloudSandboxClientError::new_err((
            "sandbox_image_import",
            Option::<u16>::None,
            error.to_string(),
        ))
    })
}

/// Assemble the auth/context + resource fields shared by the Dockerfile build
/// and registry import paths.
#[allow(clippy::too_many_arguments)]
fn common_build_options(
    api_url: String,
    token: String,
    use_scope_headers: bool,
    organization_id: Option<String>,
    project_id: Option<String>,
    namespace: Option<String>,
    registered_name: Option<String>,
    disk_mb: Option<u64>,
    builder_disk_mb: Option<u64>,
    cpus: Option<f64>,
    memory_mb: Option<i64>,
    is_public: bool,
    user_agent: Option<String>,
    docker_compat: bool,
) -> tensorlake::sandbox_images::CommonBuildOptions {
    tensorlake::sandbox_images::CommonBuildOptions {
        api_url,
        bearer_token: token,
        use_scope_headers,
        organization_id,
        project_id,
        namespace: namespace.unwrap_or_else(|| "default".to_string()),
        registered_name,
        disk_mb,
        builder_disk_mb,
        cpus,
        memory_mb,
        is_public,
        user_agent,
        docker_compat,
    }
}

fn emit_sandbox_image_event(callback: &Py<PyAny>, event: SandboxImageBuildEvent) {
    let _ = Python::attach(|py| -> PyResult<()> {
        let dict = PyDict::new(py);
        match event {
            SandboxImageBuildEvent::Status(message) => {
                dict.set_item("type", "status")?;
                dict.set_item("message", message)?;
            }
            SandboxImageBuildEvent::BuildLog { stream, message } => {
                dict.set_item("type", "build_log")?;
                dict.set_item("stream", stream)?;
                dict.set_item("message", message)?;
            }
            SandboxImageBuildEvent::Warning(message) => {
                dict.set_item("type", "warning")?;
                dict.set_item("message", message)?;
            }
        }
        callback.call1(py, (dict,))?;
        Ok(())
    });
}

#[pymodule]
fn _cloud_sdk(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Eager-init the shared tokio runtime at import time so the first sync
    // call (e.g. sandbox.create()) doesn't pay worker-thread spawn cost.
    // TTI benchmarks measure from create() to first runCommand(), so this
    // cost must land outside that window.
    let _ = shared_runtime();
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
    module.add_class::<CloudSandboxDesktopClient>()?;
    module.add_class::<CloudDocumentAIClient>()?;
    module.add_function(wrap_pyfunction!(create_image_context_file, module)?)?;
    module.add_function(wrap_pyfunction!(build_sandbox_image, module)?)?;
    module.add_function(wrap_pyfunction!(import_sandbox_image, module)?)?;
    Ok(())
}
