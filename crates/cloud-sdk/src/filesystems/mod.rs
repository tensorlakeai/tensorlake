use reqwest::Method;
use serde::Deserialize;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod models;

use models::{CreateFileSystemRequest, FileSystem};

/// One page of the paginated file-systems list response.
#[derive(Debug, Default, Deserialize)]
struct FileSystemsPage {
    #[serde(default)]
    items: Vec<FileSystem>,
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

/// Client for the project-scoped ZeroFS file-system registry.
///
/// File systems are created and listed against the Platform API under
/// `/platform/v1/organizations/{org}/projects/{project}/file-systems`. Once
/// registered, a file system can be mounted into a sandbox at boot (via
/// [`CreateSandboxRequest::file_systems`](crate::sandboxes::models::CreateSandboxRequest))
/// or attached to a running sandbox (via
/// [`SandboxesClient::attach_file_system`](crate::sandboxes::SandboxesClient::attach_file_system)).
#[derive(Clone)]
pub struct FileSystemsClient {
    client: Client,
    organization_id: String,
    project_id: String,
}

impl FileSystemsClient {
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
            "/platform/v1/organizations/{}/projects/{}/file-systems",
            self.organization_id, self.project_id
        )
    }

    /// Register a new file system with the project.
    pub async fn create(
        &self,
        request: &CreateFileSystemRequest,
    ) -> Result<Traced<FileSystem>, SdkError> {
        let req = self
            .client
            .build_post_json_request(Method::POST, &self.endpoint(), request)?;
        self.client.execute_json(req).await
    }

    /// List all registered file systems, following pagination to the end.
    ///
    /// Pages through `?pageSize=100` results, accumulating every entry. The
    /// returned `Traced` carries the trace id of the final page request.
    pub async fn list(&self) -> Result<Traced<Vec<FileSystem>>, SdkError> {
        let mut path = format!("{}?pageSize=100", self.endpoint());
        let mut items: Vec<FileSystem> = Vec::new();
        loop {
            let req = self.client.build_get_json_request(&path, None)?;
            let traced = self.client.execute_json::<FileSystemsPage>(req).await?;
            let trace_id = traced.trace_id.clone();
            let page = traced.into_inner();
            items.extend(page.items);
            match page.pagination.and_then(|p| p.next) {
                Some(next) if !next.is_empty() => path = next_request_path(&next),
                _ => return Ok(Traced::new(trace_id, items)),
            }
        }
    }

    /// Delete a registered file system by its id (e.g. `file_system_...`).
    pub async fn delete(&self, file_system_id: &str) -> Result<Traced<()>, SdkError> {
        let encoded = urlencoding::encode(file_system_id);
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
        let next =
            "/platform/v1/organizations/org/projects/proj/file-systems?pageSize=100&next=abc";
        assert_eq!(next_request_path(next), next);
    }

    #[test]
    fn next_request_path_reduces_absolute_url_to_path_and_query() {
        let next = "https://api.tensorlake.ai/platform/v1/organizations/org/projects/proj/file-systems?next=abc";
        assert_eq!(
            next_request_path(next),
            "/platform/v1/organizations/org/projects/proj/file-systems?next=abc"
        );
    }

    #[test]
    fn next_request_path_prefixes_bare_relative_links() {
        assert_eq!(
            next_request_path("file-systems?next=abc"),
            "/file-systems?next=abc"
        );
    }
}
