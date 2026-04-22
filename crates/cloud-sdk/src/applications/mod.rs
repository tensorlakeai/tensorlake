//! # Tensorlake Cloud SDK - Applications
//!
//! This module provides a high-level, ergonomic interface for interacting with Tensorlake Cloud applications.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use tensorlake_cloud_sdk::{ClientBuilder, applications::{ApplicationsClient, models::{ListApplicationsRequest, GetApplicationRequest}}};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = ClientBuilder::new("https://api.tensorlake.ai")
//!         .bearer_token("your-api-key")
//!         .build()?;
//!     let apps_client = ApplicationsClient::new(client);
//!
//! // List applications in a namespace
//! let request = ListApplicationsRequest::builder()
//!     .namespace("default".to_string())
//!     .build()?;
//! let apps = apps_client.list(&request).await?;
//!
//! // Get a specific application
//! let app = apps_client.get(&GetApplicationRequest::builder()
//!     .namespace("default".to_string())
//!     .application("my-app".to_string())
//!     .build()?).await?;
//!
//! Ok(())
//! }
//! ```

pub mod error;
pub mod models;

use bytes::Bytes;
use reqwest::{
    Method, StatusCode,
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE},
    multipart::{Form, Part},
};

use crate::{
    applications::models::RequestStateChangeEvent,
    client::{Client, Traced},
    error::SdkError,
};

/// A client for interacting with Tensorlake Cloud applications.
#[derive(Clone)]
pub struct ApplicationsClient {
    client: Client,
}

impl ApplicationsClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn list(
        &self,
        request: &models::ListApplicationsRequest,
    ) -> Result<Traced<models::ApplicationsList>, SdkError> {
        let uri_str = format!("/v1/namespaces/{}/applications", request.namespace);
        let mut req_builder = self.client.request(Method::GET, &uri_str);

        if let Some(ref param_value) = request.limit {
            req_builder = req_builder.query(&[("limit", param_value)]);
        }
        if let Some(ref param_value) = request.cursor {
            req_builder = req_builder.query(&[("cursor", param_value)]);
        }
        if let Some(ref param_value) = request.direction {
            req_builder = req_builder.query(&[("direction", param_value)]);
        }

        let req = req_builder.build()?;
        self.client.execute_json(req).await
    }

    pub async fn get(
        &self,
        request: &models::GetApplicationRequest,
    ) -> Result<Traced<models::Application>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}",
            request.namespace, request.application
        );
        let req = self.client.request(Method::GET, &uri_str).build()?;
        self.client.execute_json(req).await
    }

    pub async fn upsert(
        &self,
        request: &models::UpsertApplicationRequest,
    ) -> Result<Traced<()>, SdkError> {
        let mut multipart_form = Form::new();

        let manifest_json = serde_json::to_string(&request.application_manifest)?;
        multipart_form = multipart_form.text("application", manifest_json);

        let file_part = Part::bytes(request.code_zip.clone()).file_name("code.zip");
        multipart_form = multipart_form.part("code", file_part);

        let uri_str = format!("/v1/namespaces/{}/applications", request.namespace);
        let req = self
            .client
            .build_multipart_request(Method::POST, &uri_str, multipart_form)?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn delete(
        &self,
        request: &models::DeleteApplicationRequest,
    ) -> Result<Traced<()>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}",
            request.namespace, request.application
        );
        let req = self.client.request(Method::DELETE, &uri_str).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn invoke(
        &self,
        request: &models::InvokeApplicationRequest,
    ) -> Result<Traced<models::InvokeResponse>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}",
            request.namespace, request.application
        );
        let req = self
            .client
            .request(Method::POST, &uri_str)
            .header(ACCEPT, "application/json")
            .json(&request.body)
            .build()?;
        let resp = self.client.execute_json::<serde_json::Value>(req).await?;
        let trace_id = resp.trace_id.clone();
        let request_id = resp["request_id"]
            .as_str()
            .ok_or_else(|| SdkError::ServerError {
                status: reqwest::StatusCode::OK,
                message: "Missing request_id in response".to_string(),
            })?
            .to_string();
        Ok(Traced::new(
            trace_id,
            models::InvokeResponse::RequestId(request_id),
        ))
    }

    pub async fn list_requests(
        &self,
        request: &models::ListRequestsRequest,
    ) -> Result<Traced<models::ApplicationRequests>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/requests",
            request.namespace, request.application
        );
        let mut req_builder = self.client.request(Method::GET, &uri_str);

        if let Some(ref param_value) = request.limit {
            req_builder = req_builder.query(&[("limit", &param_value.to_string())]);
        }
        if let Some(ref param_value) = request.cursor {
            req_builder = req_builder.query(&[("cursor", &param_value)]);
        }
        if let Some(ref param_value) = request.direction {
            req_builder = req_builder.query(&[("direction", &param_value.to_string())]);
        }

        let req = req_builder.build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_request(
        &self,
        request: &models::GetRequestRequest,
    ) -> Result<Traced<models::Request>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/requests/{}",
            request.namespace, request.application, request.request_id
        );
        let mut req_builder = self.client.request(Method::GET, &uri_str);
        if let Some(token) = &request.updates_pagination_token {
            req_builder = req_builder.query(&["nextToken", token]);
        }
        let req = req_builder.build()?;
        self.client.execute_json(req).await
    }

    pub async fn delete_request(
        &self,
        request: &models::DeleteRequestRequest,
    ) -> Result<Traced<()>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/requests/{}",
            request.namespace, request.application, request.request_id
        );
        let req = self.client.request(Method::DELETE, &uri_str).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn download_function_output(
        &self,
        request: &models::DownloadFunctionOutputRequest,
    ) -> Result<Traced<models::DownloadOutput>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/requests/{}/output/{}",
            request.namespace, request.application, request.request_id, request.function_call_id
        );
        let req = self
            .client
            .request(reqwest::Method::GET, &uri_str)
            .build()?;
        let resp = self.client.execute_traced(req).await?;
        let trace_id = resp.trace_id.clone();
        let content_type = resp.headers().get(CONTENT_TYPE).cloned();
        let content_length = resp.headers().get(CONTENT_LENGTH).cloned();
        let is_success = resp.status().is_success();
        let content = if is_success {
            resp.into_inner().bytes().await?
        } else {
            Bytes::new()
        };
        Ok(Traced::new(
            trace_id,
            models::DownloadOutput {
                content_type,
                content_length,
                content,
            },
        ))
    }

    pub async fn check_function_output(
        &self,
        request: &models::CheckFunctionOutputRequest,
    ) -> Result<Traced<Option<models::DownloadOutput>>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/requests/{}/output",
            request.namespace, request.application, request.request_id
        );
        let req = self.client.request(Method::HEAD, &uri_str).build()?;
        let resp = self.client.execute_traced(req).await?;
        let trace_id = resp.trace_id.clone();
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(Traced::new(trace_id, None));
        }
        Ok(Traced::new(
            trace_id,
            Some(models::DownloadOutput {
                content_type: resp.headers().get(CONTENT_TYPE).cloned(),
                content_length: resp.headers().get(CONTENT_LENGTH).cloned(),
                content: Bytes::new(),
            }),
        ))
    }

    pub async fn download_request_output(
        &self,
        request: &models::DownloadRequestOutputRequest,
    ) -> Result<Traced<models::DownloadOutput>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/requests/{}/output",
            request.namespace, request.application, request.request_id
        );
        let req = self.client.request(Method::GET, &uri_str).build()?;
        let resp = self.client.execute_traced(req).await?;
        let trace_id = resp.trace_id.clone();
        let content_type = resp.headers().get(CONTENT_TYPE).cloned();
        let content_length = resp.headers().get(CONTENT_LENGTH).cloned();
        let is_success = resp.status().is_success();
        let content = if is_success {
            resp.into_inner().bytes().await?
        } else {
            Bytes::new()
        };
        Ok(Traced::new(
            trace_id,
            models::DownloadOutput {
                content_type,
                content_length,
                content,
            },
        ))
    }

    pub async fn get_logs(
        &self,
        request: &models::GetLogsRequest,
    ) -> Result<Traced<models::EventsResponse>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/logs",
            request.namespace, request.application
        );
        let mut req_builder = self.client.request(Method::GET, &uri_str);

        if let Some(ref param_value) = request.request_id {
            req_builder = req_builder.query(&[("requestId", param_value)]);
        }
        if let Some(ref param_value) = request.container_id {
            req_builder = req_builder.query(&[("containerId", param_value)]);
        }
        if let Some(ref param_value) = request.function {
            req_builder = req_builder.query(&[("function", param_value)]);
        }
        if let Some(ref param_value) = request.next_token {
            req_builder = req_builder.query(&[("nextToken", param_value)]);
        }
        if let Some(param_value) = request.head {
            req_builder = req_builder.query(&[("head", &param_value.to_string())]);
        }
        if let Some(param_value) = request.tail {
            req_builder = req_builder.query(&[("tail", &param_value.to_string())]);
        }
        if let Some(ref param_value) = request.ignore {
            req_builder = req_builder.query(&[("ignore", param_value)]);
        }
        if let Some(ref param_value) = request.function_executor {
            req_builder = req_builder.query(&[("functionExecutor", param_value)]);
        }

        let req = req_builder.build()?;
        self.client.execute_json(req).await
    }

    pub async fn get_progress_updates(
        &self,
        request: &models::ProgressUpdatesRequest,
    ) -> Result<Traced<models::ProgressUpdatesResponse>, SdkError> {
        let uri_str = format!(
            "/v1/namespaces/{}/applications/{}/requests/{}/updates",
            request.namespace, request.application, request.request_id
        );

        match request.mode {
            models::ProgressUpdatesRequestMode::Stream => {
                let stream = self
                    .client
                    .build_event_source_request::<RequestStateChangeEvent>(&uri_str)
                    .await?;
                let trace_id = stream.trace_id.clone();
                Ok(Traced::new(
                    trace_id,
                    models::ProgressUpdatesResponse::Stream(stream.into_inner()),
                ))
            }
            models::ProgressUpdatesRequestMode::Paginated(ref token) => {
                let query = token
                    .as_ref()
                    .map(|token| [("nextToken", token.as_str())].to_vec());
                let req = self
                    .client
                    .build_get_json_request(&uri_str, query.as_deref())?;
                let response = self
                    .client
                    .execute_json::<models::ProgressUpdatesJson>(req)
                    .await?;
                let trace_id = response.trace_id.clone();
                Ok(Traced::new(
                    trace_id,
                    models::ProgressUpdatesResponse::Json(response.into_inner()),
                ))
            }
        }
    }
}
