use reqwest::{Method, StatusCode};
use serde::Deserialize;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod models;

use models::{CreateSandboxTemplateRequest, SandboxTemplate};

/// One page of the paginated sandbox-templates list response.
#[derive(Debug, Default, Deserialize)]
struct SandboxTemplatesPage {
    #[serde(default)]
    items: Vec<SandboxTemplate>,
    #[serde(default)]
    pagination: Option<Pagination>,
}

#[derive(Debug, Default, Deserialize)]
struct Pagination {
    #[serde(default)]
    next: Option<String>,
}

/// Reduce a `pagination.next` link to a base-URL-relative request path.
///
/// The server may hand back either an absolute URL or an absolute path; the
/// SDK client always prepends its base URL, so absolute URLs must be reduced
/// to their path+query first.
fn next_request_path(next: &str) -> String {
    if let Some(idx) = next.find("://") {
        let after = &next[idx + 3..];
        match after.find('/') {
            Some(slash) => after[slash..].to_string(),
            None => "/".to_string(),
        }
    } else if next.starts_with('/') {
        next.to_string()
    } else {
        format!("/{next}")
    }
}

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

    /// List all registered sandbox templates, following pagination to the end.
    ///
    /// Pages through `?pageSize=100` results, accumulating every entry. The
    /// returned `Traced` carries the trace id of the final page request.
    pub async fn list(&self) -> Result<Traced<Vec<SandboxTemplate>>, SdkError> {
        let mut path = format!("{}?pageSize=100", self.endpoint());
        let mut items: Vec<SandboxTemplate> = Vec::new();
        loop {
            let req = self.client.build_get_json_request(&path, None)?;
            let traced = self
                .client
                .execute_json::<SandboxTemplatesPage>(req)
                .await?;
            let trace_id = traced.trace_id.clone();
            let page = traced.into_inner();
            items.extend(page.items);
            match page.pagination.and_then(|p| p.next) {
                Some(next) if !next.is_empty() => path = next_request_path(&next),
                _ => return Ok(Traced::new(trace_id, items)),
            }
        }
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

    /// Delete a sandbox template by registered name.
    ///
    /// The `name` argument is percent-encoded into the URL path so image-style
    /// references containing `/` and `:` (e.g. `tensorlake/python:3.12-slim`)
    /// round-trip correctly.
    pub async fn delete(&self, name: &str) -> Result<Traced<()>, SdkError> {
        let encoded = urlencoding::encode(name);
        let path = format!("{}/{}", self.endpoint(), encoded);
        let req = self.client.request(Method::DELETE, &path).build()?;
        self.client
            .execute_traced(req)
            .await
            .map(|traced| traced.map(|_| ()))
    }
}

#[cfg(test)]
mod tests {
    use super::next_request_path;

    #[test]
    fn next_request_path_keeps_absolute_path_and_query() {
        let next = "/platform/v1/organizations/org/projects/proj/sandbox-templates?pageSize=100&next=abc";
        assert_eq!(next_request_path(next), next);
    }

    #[test]
    fn next_request_path_reduces_absolute_url_to_path_and_query() {
        let next =
            "https://api.tensorlake.ai/platform/v1/organizations/org/projects/proj/sandbox-templates?next=abc";
        assert_eq!(
            next_request_path(next),
            "/platform/v1/organizations/org/projects/proj/sandbox-templates?next=abc"
        );
    }

    #[test]
    fn next_request_path_prefixes_bare_relative_links() {
        assert_eq!(next_request_path("sandbox-templates?next=abc"), "/sandbox-templates?next=abc");
    }
}
