use reqwest::{Method, StatusCode};

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod models;

use models::{CreateSandboxTemplateRequest, SandboxTemplate};

#[derive(Clone)]
pub struct SandboxTemplatesClient {
    client: Client,
    organization_id: String,
    project_id: String,
}

impl SandboxTemplatesClient {
    pub fn new(
        client: Client,
        organization_id: impl Into<String>,
        project_id: impl Into<String>,
    ) -> Self {
        Self {
            client,
            organization_id: organization_id.into(),
            project_id: project_id.into(),
        }
    }

    fn endpoint(&self) -> String {
        format!(
            "/platform/v1/organizations/{}/projects/{}/sandbox-templates",
            self.organization_id, self.project_id
        )
    }

    pub async fn create(
        &self,
        request: &CreateSandboxTemplateRequest,
    ) -> Result<Traced<SandboxTemplate>, SdkError> {
        let req = self
            .client
            .build_post_json_request(Method::POST, &self.endpoint(), request)?;
        self.client.execute_json(req).await
    }

    /// Look up a sandbox template by its registered name.
    ///
    /// Returns `Ok(None)` when no template with that name is found. The
    /// `name` argument is percent-encoded into the URL path so image-style
    /// references containing `/` and `:` (e.g. `tensorlake/python:3.12-slim`)
    /// round-trip correctly.
    pub async fn find_by_name(
        &self,
        name: &str,
    ) -> Result<Option<Traced<SandboxTemplate>>, SdkError> {
        let encoded = urlencoding::encode(name);
        let path = format!("{}/by-name/{}", self.endpoint(), encoded);
        let req = self.client.build_get_json_request(&path, None)?;
        match self.client.execute_json::<SandboxTemplate>(req).await {
            Ok(traced) => Ok(Some(traced)),
            Err(SdkError::ServerError { status, .. }) if status == StatusCode::NOT_FOUND => {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }
}
