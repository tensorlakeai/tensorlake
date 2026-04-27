use reqwest::Method;

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
}
