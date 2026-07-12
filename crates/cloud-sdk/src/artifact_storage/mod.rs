use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod ingest;
pub mod merge;
pub mod models;
pub mod workspaces;

use models::{
    CreateRepoRequest, GitCredential, ListBranchesResponse, ListOperationsResponse,
    ListRefsResponse, ListReposResponse, MintGitTokenRequest, RepoInfo,
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
        self.mint_token_for_repo(project_id, None).await
    }

    pub async fn mint_token_for_repo(
        &self,
        project_id: &str,
        repo: Option<&str>,
    ) -> Result<Traced<GitCredential>, SdkError> {
        // Ingress authenticates the bearer token and forwards the authorized project id to Artifact
        // Storage. Callers should build the SDK with `ClientBuilder::scope(...)` when using PATs.
        let _ = project_id;
        let path = "/artifact-storage/v1/token";
        let body = MintGitTokenRequest {
            repo: repo.map(str::to_string),
        };
        let req = self
            .api_client
            .build_post_json_request(Method::POST, path, &body)?;
        self.api_client.execute_json(req).await
    }

    pub fn git_credential_from_env() -> Option<GitCredential> {
        std::env::var("TENSORLAKE_GIT_TOKEN")
            .ok()
            .map(|token| GitCredential {
                token,
                token_type: "bearer".to_string(),
                expires_at: String::new(),
                git_username: std::env::var("TENSORLAKE_GIT_USERNAME")
                    .unwrap_or_else(|_| "t".to_string()),
                repo_pattern: "*".to_string(),
                scopes: Vec::new(),
            })
    }

    /// Resolve the Git credential used by repository helpers.
    ///
    /// `TENSORLAKE_GIT_TOKEN` is honored first for local artifact-storage development; otherwise
    /// the SDK mints a short-lived token scoped to `repo`.
    pub async fn git_credential_for_repo(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<GitCredential, SdkError> {
        if let Some(credential) = Self::git_credential_from_env() {
            return Ok(credential);
        }
        Ok(self
            .mint_token_for_repo(project_id, Some(repo))
            .await?
            .into_inner())
    }

    pub async fn git_credential_for_project(
        &self,
        project_id: &str,
    ) -> Result<GitCredential, SdkError> {
        if let Some(credential) = Self::git_credential_from_env() {
            return Ok(credential);
        }
        Ok(self.mint_token(project_id).await?.into_inner())
    }

    pub async fn create_repo(
        &self,
        project_id: &str,
        repo: &str,
        default_branch: Option<&str>,
    ) -> Result<Traced<()>, SdkError> {
        self.create_repo_of_kind(project_id, repo, default_branch, None)
            .await
    }

    /// Create a repo of an explicit kind ("repository" | "filesystem"). `None` omits the field
    /// entirely, which pre-kind servers require.
    pub async fn create_repo_of_kind(
        &self,
        project_id: &str,
        repo: &str,
        default_branch: Option<&str>,
        kind: Option<&str>,
    ) -> Result<Traced<()>, SdkError> {
        let credential = self.git_credential_for_project(project_id).await?;
        self.create_repo_with_credential(
            project_id,
            repo,
            default_branch,
            kind,
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
        kind: Option<&str>,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<()>, SdkError> {
        let request = CreateRepoRequest {
            default_branch: default_branch.unwrap_or("main").to_string(),
            kind: kind.map(str::to_string),
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
        let credential = self.git_credential_for_project(project_id).await?;
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
        // Structural repo management needs the `repo:write` scope, which repo-scoped mints
        // deliberately omit — mint project-wide, like `create_repo`/`fork_repo`.
        let credential = self.git_credential_for_project(project_id).await?;
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
        // Archive/restore is structural (`repo:write`), which repo-scoped mints omit.
        let credential = self.git_credential_for_project(project_id).await?;
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
        let suffix = format!("status?status={}", urlencoding::encode(status));
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
        self.list_repos_of_kind(project_id, None).await
    }

    /// List repos restricted to one kind ("repository" | "filesystem"). `None` lists all kinds
    /// and sends no filter, which pre-kind servers require.
    pub async fn list_repos_of_kind(
        &self,
        project_id: &str,
        kind: Option<&str>,
    ) -> Result<Traced<ListReposResponse>, SdkError> {
        let credential = self.git_credential_for_project(project_id).await?;
        self.list_repos_with_credential(
            project_id,
            kind,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn list_repos_with_credential(
        &self,
        project_id: &str,
        kind: Option<&str>,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<ListReposResponse>, SdkError> {
        let mut url = format!(
            "{}/project/{}/repos",
            self.git_base_url,
            encode_path_segment(project_id)
        );
        if let Some(kind) = kind {
            url.push_str(&format!("?kind={}", urlencoding::encode(kind)));
        }
        let (request, trace_id) = self.git_request_url(Method::GET, url, git_username, git_token);
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    pub async fn list_refs(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<Traced<ListRefsResponse>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
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
            Some("refs"),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    pub async fn repo_info(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<Traced<RepoInfo>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        // Independent reads — issue them concurrently.
        let (branches, refs) = tokio::try_join!(
            self.list_branches_with_credential(
                project_id,
                repo,
                &credential.git_username,
                &credential.token,
            ),
            self.list_refs_with_credential(
                project_id,
                repo,
                &credential.git_username,
                &credential.token,
            )
        )?;
        let trace_id = refs.trace_id.clone();
        let branches = branches.into_inner();
        let refs = refs.into_inner();
        Ok(Traced::new(
            trace_id,
            RepoInfo {
                repo: repo.to_string(),
                url: self.git_repo_url(project_id, repo),
                branches: branches.branches,
                refs: refs.refs,
            },
        ))
    }

    pub async fn list_branches(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<Traced<ListBranchesResponse>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
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
            Some("branches"),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    pub async fn delete_branch(
        &self,
        project_id: &str,
        repo: &str,
        branch: &str,
    ) -> Result<Traced<()>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        self.delete_branch_with_credential(
            project_id,
            repo,
            branch,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn delete_branch_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        branch: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<()>, SdkError> {
        let suffix = format!("branches/{}", encode_path_segment(branch));
        let (request, trace_id) = self.git_request(
            Method::DELETE,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_empty(response, trace_id).await
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
            Some("admin/operations"),
            git_username,
            git_token,
        )?;
        let response = request.send().await?;
        decode_json(response, trace_id).await
    }

    pub async fn list_operations(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<Traced<ListOperationsResponse>, SdkError> {
        // The operation log is `project:admin`-gated, which repo-scoped mints omit.
        let credential = self.git_credential_for_project(project_id).await?;
        self.list_operations_with_credential(
            project_id,
            repo,
            &credential.git_username,
            &credential.token,
        )
        .await
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
        let base = format!(
            "{}/project/{}/repos/{}",
            self.git_base_url,
            encode_path_segment(project_id),
            encode_path_segment(repo)
        );
        let path = match suffix {
            Some(suffix) => format!("{base}/{suffix}"),
            None => base,
        };
        Ok(self.git_request_url(method, path, git_username, git_token))
    }

    /// A project-scope request (`/project/{project}/{suffix}`) with a git credential —
    /// the URL shape for endpoints that span repos, like the workspace fleet.
    fn project_git_request(
        &self,
        method: Method,
        project_id: &str,
        suffix: &str,
        git_username: &str,
        git_token: &str,
    ) -> (reqwest::RequestBuilder, String) {
        let url = format!(
            "{}/project/{}/{}",
            self.git_base_url,
            encode_path_segment(project_id),
            suffix
        );
        self.git_request_url(method, url, git_username, git_token)
    }

    fn git_request_url(
        &self,
        method: Method,
        url: String,
        git_username: &str,
        git_token: &str,
    ) -> (reqwest::RequestBuilder, String) {
        let (traceparent, trace_id) = traceparent();
        (
            self.git_client
                .request(method, url)
                .basic_auth(git_username, Some(git_token))
                .header("traceparent", traceparent),
            trace_id,
        )
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
    use super::{ArtifactStorageClient, encode_path_segment, resolve_artifact_storage_url};
    use crate::ClientBuilder;

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

    #[test]
    fn git_repo_url_uses_git_root_project_repo_shape() {
        let api_client = ClientBuilder::new("https://api.tensorlake.ai")
            .bearer_token("token")
            .build()
            .unwrap();
        let client = ArtifactStorageClient::new(api_client, "https://git.tensorlake.ai/").unwrap();

        assert_eq!(
            client.git_repo_url("project_123", "myrepo"),
            "https://git.tensorlake.ai/project_123/myrepo"
        );
    }
}
