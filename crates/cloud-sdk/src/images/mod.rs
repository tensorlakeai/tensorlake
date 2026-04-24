//! # Tensorlake Cloud SDK - Images
//!
//! This module provides functionality for building and managing container images
//! in the Tensorlake Cloud platform.
//!
//! ## Usage
//!
//! ```rust
//! use tensorlake_cloud_sdk::{Sdk, images::models::{ImageBuildRequest, Image}};
//!
//! let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key").unwrap();
//! let images_client = sdk.images();
//!
//! // Define an image
//! let image = Image::builder()
//!     .name("my-app")
//!     .base_image("python:3.9")
//!     .build().unwrap();
//!
//! // Build the image
//! let build_request = ImageBuildRequest::builder()
//!     .image(image)
//!     .image_tag("latest")
//!     .application_name("my-app")
//!     .application_version("1.0.0")
//!     .function_name("main")
//!     .sdk_version("0.2")
//!     .build().unwrap();
//!
//! images_client.build_image(build_request);
//! ```

use std::{collections::HashMap, pin::Pin, time::Duration};

use crate::{
    client::{Client, Traced},
    error::SdkError,
    images::error::ImagesError,
};
use futures::stream::Stream;
use reqwest::{
    Method,
    multipart::{Form, Part},
};

pub mod error;
pub mod models;
use models::*;

/// A client for managing image builds in Tensorlake Cloud.
#[derive(Clone)]
pub struct ImagesClient {
    client: Client,
}

impl ImagesClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Build a container image, poll for completion, and return the result.
    pub async fn build_image(
        &self,
        request: ImageBuildRequest,
    ) -> Result<ImageBuildResult, SdkError> {
        let build_info = self.submit_build_request(&request).await?;
        self.poll_build_status(&build_info.id).await
    }

    pub async fn create_application_build(
        &self,
        build_service_path: &str,
        request: &CreateApplicationBuildRequest,
        image_contexts: &[ApplicationBuildContext],
    ) -> Result<Traced<ApplicationBuildResponse>, SdkError> {
        let form = create_application_build_form(request, image_contexts)?;
        let req = self
            .client
            .build_multipart_request(Method::POST, build_service_path, form)?;
        self.client.execute_json(req).await
    }

    pub async fn application_build_info(
        &self,
        build_service_path: &str,
        application_build_id: &str,
    ) -> Result<Traced<ApplicationBuildResponse>, SdkError> {
        let path = format!(
            "{}/{}",
            build_service_path.trim_end_matches('/'),
            application_build_id
        );
        let req = self.client.request(Method::GET, &path).build()?;
        self.client.execute_json(req).await
    }

    pub async fn cancel_application_build(
        &self,
        build_service_path: &str,
        application_build_id: &str,
    ) -> Result<Traced<ApplicationBuildResponse>, SdkError> {
        let path = format!(
            "{}/{}/cancel",
            build_service_path.trim_end_matches('/'),
            application_build_id
        );
        let req = self.client.request(Method::POST, &path).build()?;
        self.client.execute_json(req).await
    }

    async fn submit_build_request(
        &self,
        request: &ImageBuildRequest,
    ) -> Result<BuildInfo, SdkError> {
        let mut context_data = Vec::new();
        request
            .image
            .create_context_archive(&mut context_data, &request.sdk_version, None)?;
        let image_hash = request.image.image_hash(&request.sdk_version)?;
        let form = Form::new()
            .text("graph_name", request.application_name.clone())
            .text("graph_version", request.application_version.clone())
            .text("graph_function_name", request.function_name.clone())
            .text("image_hash", image_hash)
            .text("image_name", request.image.name.clone())
            .part(
                "context",
                Part::bytes(context_data).file_name("context.tar.gz"),
            );

        let req = self
            .client
            .build_multipart_request(Method::PUT, "/images/v2/builds", form)?;
        Ok(self
            .client
            .execute_json::<BuildInfo>(req)
            .await?
            .into_inner())
    }

    async fn poll_build_status(&self, build_id: &str) -> Result<ImageBuildResult, SdkError> {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let uri_str = format!("/images/v2/builds/{build_id}");
            let req = self.client.request(Method::GET, &uri_str).build()?;
            let build_info = self
                .client
                .execute_json::<BuildInfo>(req)
                .await?
                .into_inner();

            match build_info.status.as_str() {
                "completed" | "succeeded" => {
                    return Ok(ImageBuildResult {
                        id: build_info.id,
                        status: BuildStatus::Succeeded,
                        created_at: build_info.created_at,
                        finished_at: build_info.finished_at,
                        error_message: None,
                    });
                }
                "failed" => {
                    return Ok(ImageBuildResult {
                        id: build_info.id,
                        status: BuildStatus::Failed,
                        created_at: build_info.created_at,
                        finished_at: build_info.finished_at,
                        error_message: build_info.error_message,
                    });
                }
                _ => continue,
            }
        }
    }

    pub async fn list_builds(
        &self,
        request: &models::ListBuildsRequest,
    ) -> Result<Traced<Page<BuildListResponse>>, SdkError> {
        let mut query_params = Vec::new();
        if let Some(p) = request.page {
            query_params.push(("page", p.to_string()));
        }
        if let Some(ps) = request.page_size {
            query_params.push(("page_size", ps.to_string()));
        }
        if let Some(s) = &request.status {
            let status_str = match s {
                BuildStatus::Pending => "pending",
                BuildStatus::Enqueued => "enqueued",
                BuildStatus::Building => "building",
                BuildStatus::Succeeded => "succeeded",
                BuildStatus::Failed => "failed",
                BuildStatus::Canceling => "canceling",
                BuildStatus::Canceled => "canceled",
            };
            query_params.push(("status", status_str.to_string()));
        }
        if let Some(gn) = &request.application_name {
            query_params.push(("graph_name", gn.to_string()));
        }
        if let Some(iname) = &request.image_name {
            query_params.push(("image_name", iname.to_string()));
        }
        if let Some(gfn) = &request.function_name {
            query_params.push(("graph_function_name", gfn.to_string()));
        }

        let req = self
            .client
            .request(Method::GET, "/images/v2/builds")
            .query(&query_params)
            .build()?;
        self.client.execute_json(req).await
    }

    pub async fn cancel_build(
        &self,
        request: &models::CancelBuildRequest,
    ) -> Result<Traced<()>, SdkError> {
        let uri_str = format!("/images/v2/builds/{}/cancel", request.build_id);
        let req = self.client.request(Method::POST, &uri_str).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }

    pub async fn get_build_info(
        &self,
        request: &models::GetBuildInfoRequest,
    ) -> Result<Traced<BuildInfoResponse>, SdkError> {
        let uri_str = format!("/images/v2/builds/{}", request.build_id);
        let req = self.client.request(Method::GET, &uri_str).build()?;
        self.client.execute_json(req).await
    }

    pub async fn stream_logs(
        &self,
        request: &models::StreamLogsRequest,
    ) -> Result<Traced<ImageBuildLogStream>, SdkError> {
        let uri_str = format!("/images/v2/builds/{}/logs", request.build_id);
        let stream = self
            .client
            .build_event_source_request::<LogEntry>(&uri_str)
            .await?;
        Ok(stream)
    }
}

fn create_application_build_form(
    request: &CreateApplicationBuildRequest,
    image_contexts: &[ApplicationBuildContext],
) -> Result<Form, SdkError> {
    let mut contexts_by_part_name: HashMap<&str, &ApplicationBuildContext> = HashMap::new();
    for context in image_contexts {
        if contexts_by_part_name
            .insert(context.context_tar_part_name.as_str(), context)
            .is_some()
        {
            return Err(ImagesError::InvalidBuildRequest(format!(
                "duplicate image context part name '{}'",
                context.context_tar_part_name
            ))
            .into());
        }
    }

    let app_version_json = serde_json::to_vec(request)?;
    let mut form = Form::new().part(
        "app_version",
        Part::bytes(app_version_json)
            .file_name("app_version")
            .mime_str("application/json")?,
    );

    for image in &request.images {
        let context = contexts_by_part_name
            .get(image.context_tar_part_name.as_str())
            .ok_or_else(|| {
                ImagesError::InvalidBuildRequest(format!(
                    "missing image context for part '{}'",
                    image.context_tar_part_name
                ))
            })?;

        form = form.part(
            image.context_tar_part_name.clone(),
            Part::bytes(context.context_tar_gz.clone())
                .file_name(format!("{}.tar.gz", image.context_tar_part_name)),
        );
    }

    for context in image_contexts {
        if !request
            .images
            .iter()
            .any(|image| image.context_tar_part_name == context.context_tar_part_name)
        {
            return Err(ImagesError::InvalidBuildRequest(format!(
                "unexpected image context for part '{}'",
                context.context_tar_part_name
            ))
            .into());
        }
    }

    Ok(form)
}

type ImageBuildLogStream = Pin<Box<dyn Stream<Item = Result<LogEntry, SdkError>> + Send>>;
