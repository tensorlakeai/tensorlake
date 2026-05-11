use reqwest::Method;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod models;

use models::{
    CompleteSandboxTemplateBuildRequest, CreateSandboxTemplateRequest,
    PrepareSandboxTemplateBuildRequest, SandboxTemplate, SandboxTemplateBuildPrepared,
};

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

    fn builds_endpoint(&self) -> String {
        format!(
            "/platform/v1/organizations/{}/projects/{}/sandbox-template-builds",
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

    pub async fn prepare_build(
        &self,
        request: &PrepareSandboxTemplateBuildRequest,
    ) -> Result<Traced<SandboxTemplateBuildPrepared>, SdkError> {
        let req =
            self.client
                .build_post_json_request(Method::POST, &self.builds_endpoint(), request)?;
        self.client.execute_json(req).await
    }

    pub async fn complete_build(
        &self,
        build_id: &str,
        request: &CompleteSandboxTemplateBuildRequest,
    ) -> Result<Traced<SandboxTemplate>, SdkError> {
        let uri = format!("{}/{build_id}/complete", self.builds_endpoint());
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        self.client.execute_json(req).await
    }
}
