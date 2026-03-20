pub mod models;

use crate::{client::Client, error::SdkError};
use models::*;
use reqwest::Method;

/// A client for managing cron schedules for Tensorlake applications.
#[derive(Clone)]
pub struct CronClient {
    client: Client,
}

impl CronClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Create a new cron schedule for an application.
    pub async fn create(
        &self,
        namespace: &str,
        application: &str,
        cron_expression: &str,
        input_base64: Option<String>,
    ) -> Result<CreateCronScheduleResponse, SdkError> {
        let path = format!(
            "/v1/namespaces/{}/applications/{}/cron-schedules",
            namespace, application
        );
        let body = CreateCronScheduleRequest {
            cron_expression: cron_expression.to_string(),
            input_base64,
        };
        let req = self
            .client
            .build_post_json_request(Method::POST, &path, &body)?;
        let resp = self.client.execute(req).await?;
        let bytes = resp.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_reader(bytes.as_ref());
        Ok(serde_path_to_error::deserialize(jd)?)
    }

    /// List all cron schedules for an application.
    pub async fn list(
        &self,
        namespace: &str,
        application: &str,
    ) -> Result<ListCronSchedulesResponse, SdkError> {
        let path = format!(
            "/v1/namespaces/{}/applications/{}/cron-schedules",
            namespace, application
        );
        let req = self.client.build_get_json_request(&path, None)?;
        let resp = self.client.execute(req).await?;
        let bytes = resp.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_reader(bytes.as_ref());
        Ok(serde_path_to_error::deserialize(jd)?)
    }

    /// Delete a cron schedule by ID.
    pub async fn delete(
        &self,
        namespace: &str,
        application: &str,
        schedule_id: &str,
    ) -> Result<(), SdkError> {
        let path = format!(
            "/v1/namespaces/{}/applications/{}/cron-schedules/{}",
            namespace, application, schedule_id
        );
        let req = self
            .client
            .request(Method::DELETE, &path)
            .build()
            .map_err(SdkError::from)?;
        self.client.execute(req).await?;
        Ok(())
    }
}
