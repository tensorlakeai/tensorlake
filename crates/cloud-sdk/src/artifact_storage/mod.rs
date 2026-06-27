use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod models;

use models::{
    CreateRepoRequest, GitCredential, ListBranchesResponse, ListOperationsResponse,
    ListRefsResponse, ListReposResponse,
};

#[derive(Clone)]
pub struct ArtifactStorageClient {
    api_client: Client,
    git_client: reqwest::Client,
    git_base_url: String,
}

impl ArtifactStorageClient {
    pub fn new(api_client: Client, git_base_url: impl Into<String>) -> Result<Self, SdkError> {
        Ok(Self {
            api_client,
            git_client: reqwest::Client::builder()
                .user_agent(concat!("tensorlake-rust-sdk/", env!("CARGO_PKG_VERSION")))
                .build()?,
            git_base_url: trim_base_url(git_base_url.into()),
        })
    }

    pub fn git_base_url(&self) -> &str {
        &self.git_base_url
    }

    pub fn git_repo_url(&self, project_id: &str, repo: &str) -> String {
        format!(
            "{}/{}/{}",
            self.git_base_url,
            encode_path_segment(project_id),
            encode_path_segment(repo)
        )
    }

    pub async fn mint_token(&self, project_id: &str) -> Result<Traced<GitCredential>, SdkError> {
        // Ingress authenticates the bearer token and forwards the authorized project id to Artifact
        // Storage. Callers should build the SDK with `ClientBuilder::scope(...)` when using PATs.
        let _ = project_id;
        let path = "/artifact-storage/v1/token";
        let req =
            self.api_client
                .build_post_json_request(Method::POST, path, &serde_json::json!({}))?;
        self.api_client.execute_json(req).await
    }

    pub async fn create_repo(
        &self,
        project_id: &str,
        repo: &str,
        default_branch: Option<&str>,
    ) -> Result<Traced<()>, SdkError> {
        let credential = self.mint_token(project_id).await?;
        self.create_repo_with_credential(
            project_id,
            repo,
            default_branch,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn create_repo_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        default_branch: Option<&str>,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<()>, SdkError> {
        let request = CreateRepoRequest {
            default_branch: default_branch.unwrap_or("main").to_string(),
        };
        let (request_builder, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            None,
            git_username,
            git_token,
        )?;
        let response = request_builder.json(&request).send().await?;
        decode_empty(response, trace_id).await
    }

    pub async fn fork_repo(
        &self,
        project_id: &str,
        repo: &str,
        base_repo: &str,
    ) -> Result<Traced<()>, SdkError> {
        let credential = self.mint_token(project_id).await?;
        self.fork_repo_with_credential(
            project_id,
            repo,
            base_repo,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn fork_repo_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        base_repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<()>, SdkError> {
        let suffix = format!("fork/{}", encode_path_segment(base_repo));
        let (request, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_empty(response, trace_id).await
    }

    pub async fn delete_repo(&self, project_id: &str, repo: &str) -> Result<Traced<()>, SdkError> {
        let credential = self.mint_token(project_id).await?;
        self.delete_repo_with_credential(
            project_id,
            repo,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn delete_repo_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<()>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::DELETE,
            project_id,
            repo,
            None,
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_empty(response, trace_id).await
    }

    pub async fn archive_repo(&self, project_id: &str, repo: &str) -> Result<Traced<()>, SdkError> {
        self.set_repo_status(project_id, repo, "readonly").await
    }

    pub async fn restore_repo(&self, project_id: &str, repo: &str) -> Result<Traced<()>, SdkError> {
        self.set_repo_status(project_id, repo, "active").await
    }

    pub async fn set_repo_status(
        &self,
        project_id: &str,
        repo: &str,
        status: &str,
    ) -> Result<Traced<()>, SdkError> {
        let credential = self.mint_token(project_id).await?;
        self.set_repo_status_with_credential(
            project_id,
            repo,
            status,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn set_repo_status_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        status: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<()>, SdkError> {
        let suffix = format!("_status?status={}", urlencoding::encode(status));
        let (request, trace_id) = self.git_request(
            Method::PUT,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_empty(response, trace_id).await
    }

    pub async fn list_repos(
        &self,
        project_id: &str,
    ) -> Result<Traced<ListReposResponse>, SdkError> {
        let credential = self.mint_token(project_id).await?;
        self.list_repos_with_credential(project_id, &credential.git_username, &credential.token)
            .await
    }

    pub async fn list_repos_with_credential(
        &self,
        project_id: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<ListReposResponse>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            "_repos",
            None,
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    pub async fn list_refs(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<Traced<ListRefsResponse>, SdkError> {
        let credential = self.mint_token(project_id).await?;
        self.list_refs_with_credential(
            project_id,
            repo,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn list_refs_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<ListRefsResponse>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("_refs"),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    pub async fn list_branches(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<Traced<ListBranchesResponse>, SdkError> {
        let credential = self.mint_token(project_id).await?;
        self.list_branches_with_credential(
            project_id,
            repo,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn list_branches_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<ListBranchesResponse>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("_branches"),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    pub async fn list_operations_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<ListOperationsResponse>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("_admin/operations"),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    fn git_request(
        &self,
        method: Method,
        project_id: &str,
        repo: &str,
        suffix: Option<&str>,
        git_username: &str,
        git_token: &str,
    ) -> Result<(reqwest::RequestBuilder, String), SdkError> {
        let path = match suffix {
            Some(suffix) => format!(
                "{}/{}/{}/{}",
                self.git_base_url,
                encode_path_segment(project_id),
                encode_path_segment(repo),
                suffix
            ),
            None => format!(
                "{}/{}/{}",
                self.git_base_url,
                encode_path_segment(project_id),
                encode_path_segment(repo)
            ),
        };
        let (traceparent, trace_id) = traceparent();
        Ok((
            self.git_client
                .request(method, path)
                .basic_auth(git_username, Some(git_token))
                .header("traceparent", traceparent),
            trace_id,
        ))
    }
}

pub fn resolve_artifact_storage_url(api_url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(api_url) {
        let host = parsed.host_str().unwrap_or("");
        if host == "localhost" || host == "127.0.0.1" {
            return api_url.to_string();
        }
        if let Some(rest) = host.strip_prefix("api.") {
            return format!("{}://git.{}", parsed.scheme(), rest);
        }
    }
    "https://git.tensorlake.ai".to_string()
}

fn trim_base_url(url: String) -> String {
    url.trim_end_matches('/').to_string()
}

fn encode_path_segment(segment: &str) -> String {
    urlencoding::encode(segment).into_owned()
}

fn traceparent() -> (String, String) {
    let trace_id = hex::encode(rand::random::<[u8; 16]>());
    let span_id = hex::encode(rand::random::<[u8; 8]>());
    (format!("00-{trace_id}-{span_id}-01"), trace_id)
}

async fn decode_empty(
    response: reqwest::Response,
    trace_id: String,
) -> Result<Traced<()>, SdkError> {
    handle_response(response).await?;
    Ok(Traced::new(trace_id, ()))
}

async fn decode_json<T: DeserializeOwned>(
    response: reqwest::Response,
    trace_id: String,
) -> Result<Traced<T>, SdkError> {
    let response = handle_response(response).await?;
    let bytes = response.bytes().await?;
    let jd = &mut serde_json::Deserializer::from_slice(bytes.as_ref());
    let value = serde_path_to_error::deserialize(jd)?;
    Ok(Traced::new(trace_id, value))
}

async fn handle_response(response: reqwest::Response) -> Result<reqwest::Response, SdkError> {
    let status = response.status();
    match status {
        StatusCode::UNAUTHORIZED => Err(SdkError::Authentication(body_message(response).await)),
        StatusCode::FORBIDDEN => Err(SdkError::Authorization(body_message(response).await)),
        status if !status.is_success() => Err(SdkError::ServerError {
            status,
            message: body_message(response).await,
        }),
        _ => Ok(response),
    }
}

async fn body_message(response: reqwest::Response) -> String {
    response.text().await.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{encode_path_segment, resolve_artifact_storage_url};

    #[test]
    fn resolves_git_url_from_api_url() {
        assert_eq!(
            resolve_artifact_storage_url("https://api.tensorlake.ai"),
            "https://git.tensorlake.ai"
        );
        assert_eq!(
            resolve_artifact_storage_url("https://api.tensorlake.dev"),
            "https://git.tensorlake.dev"
        );
        assert_eq!(
            resolve_artifact_storage_url("http://localhost:3000"),
            "http://localhost:3000"
        );
    }

    #[test]
    fn encodes_path_segments() {
        assert_eq!(encode_path_segment("project_123"), "project_123");
        assert_eq!(encode_path_segment("repo/name"), "repo%2Fname");
    }
}
