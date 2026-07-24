use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    ListRefsResponse, ListReposResponse, MintGitTokenRequest, NativeDirectBlobRequest,
    NativeDirectBlobTargetsRequest, NativeDirectBlobTargetsResponse, NativeDirectFilePathWrite,
    NativeDirectFileWrite, NativeDirectMutation, NativeDirectPathTransfer,
    NativeDirectPublishRequest, NativeDirectPublishResponse, NativeDirectUploadLeaseResponse,
    NativeDirectUploadReceipt, NativeFilesystemFileRead, NativeFilesystemSnapshot,
    NativeFilesystemSnapshotPage, NativeFilesystemSnapshotRetentionResponse, NativeHeadResponse,
    REPO_KIND_FILESYSTEM, RepoInfo, RepoMetaInfo,
};

#[derive(Clone)]
pub struct ArtifactStorageClient {
    api_client: Client,
    git_client: reqwest::Client,
    git_base_url: String,
    repo_credentials: Arc<tokio::sync::Mutex<HashMap<(String, String), GitCredential>>>,
}

impl ArtifactStorageClient {
    pub fn new(api_client: Client, git_base_url: impl Into<String>) -> Result<Self, SdkError> {
        Ok(Self {
            api_client,
            git_client: reqwest::Client::builder()
                .user_agent(concat!("tensorlake-rust-sdk/", env!("CARGO_PKG_VERSION")))
                .build()?,
            git_base_url: trim_base_url(git_base_url.into()),
            repo_credentials: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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
        let key = (project_id.to_string(), repo.to_string());
        if let Some(credential) = self.repo_credentials.lock().await.get(&key).cloned()
            && git_credential_is_fresh(&credential)
        {
            return Ok(credential);
        }
        let credential = self
            .mint_token_for_repo(project_id, Some(repo))
            .await?
            .into_inner();
        if git_credential_is_fresh(&credential) {
            self.repo_credentials
                .lock()
                .await
                .insert(key, credential.clone());
        }
        Ok(credential)
    }

    pub async fn git_credential_for_project(
        &self,
        project_id: &str,
    ) -> Result<GitCredential, SdkError> {
        if let Some(credential) = Self::git_credential_from_env() {
            return Ok(credential);
        }
        let key = (project_id.to_string(), "*".to_string());
        if let Some(credential) = self.repo_credentials.lock().await.get(&key).cloned()
            && git_credential_is_fresh(&credential)
        {
            return Ok(credential);
        }
        let credential = self.mint_token(project_id).await?.into_inner();
        if git_credential_is_fresh(&credential) {
            self.repo_credentials
                .lock()
                .await
                .insert(key, credential.clone());
        }
        Ok(credential)
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

    /// Create a metadata-only filesystem fork at the live head or a selected native snapshot.
    pub async fn fork_filesystem(
        &self,
        project_id: &str,
        filesystem: &str,
        base_filesystem: &str,
        snapshot: Option<&str>,
    ) -> Result<Traced<()>, SdkError> {
        let credential = self.git_credential_for_project(project_id).await?;
        let mut suffix = format!("fork/{}", encode_path_segment(base_filesystem));
        if let Some(snapshot) = snapshot {
            let snapshot = native_snapshot_id(snapshot)?.ok_or_else(|| {
                SdkError::ClientError(
                    "filesystem fork snapshot must be a 64-character snapshot id".to_string(),
                )
            })?;
            suffix.push_str("?snapshot=");
            suffix.push_str(&encode_path_segment(snapshot));
        }
        let (request, trace_id) = self.git_request(
            Method::POST,
            project_id,
            filesystem,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        decode_empty(send_idempotent(request).await?, trace_id).await
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
        let mut repos = Vec::new();
        let mut after = None::<String>;
        let mut seen_after = HashSet::new();
        loop {
            let query = repo_list_query(kind, after.as_deref());
            let url = format!(
                "{}/project/{}/repos?{}",
                self.git_base_url,
                encode_path_segment(project_id),
                query,
            );
            let (request, trace_id) =
                self.git_request_url(Method::GET, url, git_username, git_token);
            let page: Traced<ListReposResponse> =
                decode_json(request.send().await?, trace_id).await?;
            let trace_id = page.trace_id.clone();
            let page = page.into_inner();
            repos.extend(page.repos);
            let Some(next) = page.next_after else {
                return Ok(Traced::new(
                    trace_id,
                    ListReposResponse {
                        project: project_id.to_string(),
                        repos,
                        next_after: None,
                    },
                ));
            };
            after = Some(advance_pagination_cursor(
                &mut seen_after,
                next,
                "repository listing",
            )?);
        }
    }

    /// Authoritative point-read of one repo's meta (kind, default branch, status). 404 =>
    /// SdkError::ServerError with status 404.
    pub async fn repo_meta_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<RepoMetaInfo>, SdkError> {
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("meta"),
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
        let mut operations = Vec::new();
        let mut after = None::<String>;
        let mut seen_after = HashSet::new();
        loop {
            let suffix = after.as_ref().map_or_else(
                || "admin/operations?limit=1000".to_string(),
                |after| {
                    format!(
                        "admin/operations?limit=1000&after={}",
                        urlencoding::encode(after)
                    )
                },
            );
            let (request, trace_id) = self.git_request(
                Method::GET,
                project_id,
                repo,
                Some(&suffix),
                git_username,
                git_token,
            )?;
            let page: Traced<ListOperationsResponse> =
                decode_json(request.send().await?, trace_id).await?;
            let trace_id = page.trace_id.clone();
            let page = page.into_inner();
            operations.extend(page.operations);
            let Some(next) = page.next_after else {
                return Ok(Traced::new(
                    trace_id,
                    ListOperationsResponse {
                        repo: format!("{project_id}/{repo}"),
                        operations,
                        next_after: None,
                    },
                ));
            };
            after = Some(advance_pagination_cursor(
                &mut seen_after,
                next,
                "operation listing",
            )?);
        }
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

    pub async fn native_filesystem_head(
        &self,
        project_id: &str,
        filesystem: &str,
    ) -> Result<Traced<NativeHeadResponse>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            filesystem,
            Some("fs/head"),
            &credential.git_username,
            &credential.token,
        )?;
        decode_json(send_idempotent(request).await?, trace_id).await
    }

    pub async fn read_native_filesystem_file(
        &self,
        project_id: &str,
        filesystem: &str,
        path: &str,
        version: &str,
    ) -> Result<Traced<Vec<u8>>, SdkError> {
        self.read_native_filesystem_file_with_metadata(project_id, filesystem, path, version, None)
            .await
            .map(|read| read.map(|read| read.data))
    }

    pub async fn read_native_filesystem_file_with_metadata(
        &self,
        project_id: &str,
        filesystem: &str,
        path: &str,
        version: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Traced<NativeFilesystemFileRead>, SdkError> {
        let snapshot = native_snapshot_id(version)?;
        let path = encode_native_path(path)?;
        let suffix = match snapshot {
            Some(snapshot) => format!("fs/files/{path}?snapshot={snapshot}"),
            None => format!("fs/files/{path}"),
        };
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            filesystem,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        let request = match range {
            Some((offset, length)) => {
                if length == 0 {
                    return Err(SdkError::ClientError(
                        "filesystem read range length must be positive".to_string(),
                    ));
                }
                let end = offset.checked_add(length - 1).ok_or_else(|| {
                    SdkError::ClientError("filesystem read range overflow".to_string())
                })?;
                request.header(reqwest::header::RANGE, format!("bytes={offset}-{end}"))
            }
            None => request,
        };
        let response = handle_response(send_idempotent(request).await?).await?;
        let content_id = response
            .headers()
            .get("x-tensorlake-content-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                SdkError::ClientError(
                    "filesystem read response omitted content identity".to_string(),
                )
            })?
            .to_string();
        let full_size = match response.headers().get(reqwest::header::CONTENT_RANGE) {
            Some(value) => value
                .to_str()
                .ok()
                .and_then(|value| value.rsplit_once('/'))
                .and_then(|(_, total)| total.parse::<u64>().ok()),
            None => response.content_length(),
        }
        .ok_or_else(|| {
            SdkError::ClientError("filesystem read response omitted total size".to_string())
        })?;
        let data = response.bytes().await?.to_vec();
        Ok(Traced::new(
            trace_id,
            NativeFilesystemFileRead {
                data,
                content_id,
                full_size,
            },
        ))
    }

    pub async fn list_native_filesystem_entries_page(
        &self,
        project_id: &str,
        filesystem: &str,
        path: &str,
        version: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Traced<workspaces::TreePage>, SdkError> {
        if !path.is_empty() {
            validate_native_direct_path(path)?;
        }
        let suffix = native_entries_suffix(path, native_snapshot_id(version)?, after, limit);
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        let (request, trace_id) = self.git_request(
            Method::GET,
            project_id,
            filesystem,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        decode_json(send_idempotent(request).await?, trace_id).await
    }

    /// Permanently retain the current filesystem head with a user-visible message. This publishes
    /// no content or directory metadata: it changes only the snapshot's retention class.
    pub async fn retain_current_native_filesystem_snapshot(
        &self,
        project_id: &str,
        filesystem: &str,
        message: String,
        request_id: String,
    ) -> Result<Traced<NativeFilesystemSnapshotRetentionResponse>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        let (request, trace_id) = self.git_request(
            Method::POST,
            project_id,
            filesystem,
            Some("fs/snapshots"),
            &credential.git_username,
            &credential.token,
        )?;
        let body = serde_json::json!({ "message": message, "request_id": request_id });
        decode_json(send_idempotent(request.json(&body)).await?, trace_id).await
    }

    /// List every explicitly retained native snapshot in newest-first history order.
    pub async fn list_native_filesystem_snapshots(
        &self,
        project_id: &str,
        filesystem: &str,
    ) -> Result<Traced<Vec<NativeFilesystemSnapshot>>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        let mut snapshots = Vec::new();
        let mut after: Option<String> = None;
        let mut seen = HashSet::new();
        let trace_id = loop {
            let suffix = match after.as_deref() {
                Some(after) => format!(
                    "fs/snapshots?limit=1000&after={}",
                    encode_path_segment(after)
                ),
                None => "fs/snapshots?limit=1000".to_string(),
            };
            let (request, request_trace_id) = self.git_request(
                Method::GET,
                project_id,
                filesystem,
                Some(&suffix),
                &credential.git_username,
                &credential.token,
            )?;
            let page = decode_json::<NativeFilesystemSnapshotPage>(
                send_idempotent(request).await?,
                request_trace_id,
            )
            .await?;
            snapshots.extend(
                page.snapshots
                    .iter()
                    .filter(|snapshot| snapshot.snapshot_class == "permanent_snapshot")
                    .cloned(),
            );
            match page.next_after.clone() {
                Some(next) if !next.is_empty() && seen.insert(next.clone()) => after = Some(next),
                Some(_) => {
                    return Err(SdkError::ClientError(
                        "native snapshot history returned a repeated or empty cursor".to_string(),
                    ));
                }
                None => break page.trace_id.clone(),
            }
        };
        Ok(Traced::new(trace_id, snapshots))
    }

    /// Drop one permanent snapshot retention root. Content still reachable from a head, fork, or
    /// another snapshot remains durable.
    pub async fn delete_native_filesystem_snapshot(
        &self,
        project_id: &str,
        filesystem: &str,
        snapshot: &str,
    ) -> Result<Traced<()>, SdkError> {
        let snapshot = native_snapshot_id(snapshot)?.ok_or_else(|| {
            SdkError::ClientError(
                "filesystem snapshot must be a 64-character snapshot id".to_string(),
            )
        })?;
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        let snapshot_suffix = format!("fs/snapshots/{}", encode_path_segment(snapshot));
        let (snapshot_request, _) = self.git_request(
            Method::GET,
            project_id,
            filesystem,
            Some(&snapshot_suffix),
            &credential.git_username,
            &credential.token,
        )?;
        let current = decode_json::<NativeFilesystemSnapshot>(
            send_idempotent(snapshot_request).await?,
            String::new(),
        )
        .await?
        .into_inner();
        let suffix = format!(
            "{snapshot_suffix}?expected_permanence_epoch={}",
            current.permanence_epoch
        );
        let (request, trace_id) = self.git_request(
            Method::DELETE,
            project_id,
            filesystem,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        decode_empty(send_idempotent(request).await?, trace_id).await
    }

    /// Publish complete files through the native SDK path: SHA-256 locally, upload missing bytes
    /// directly to checksum-bound object-store targets, then atomically publish logical mutations.
    /// Payload bytes never traverse Artifact Storage.
    pub async fn publish_filesystem_files(
        &self,
        project_id: &str,
        filesystem: &str,
        files: Vec<NativeDirectFileWrite>,
        deletes: Vec<String>,
        moves: Vec<NativeDirectPathTransfer>,
        copies: Vec<NativeDirectPathTransfer>,
        message: String,
        operation_id: String,
    ) -> Result<Traced<NativeDirectPublishResponse>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        self.push_native_files_direct_with_credential(
            project_id,
            filesystem,
            files,
            Vec::new(),
            deletes,
            moves,
            copies,
            message,
            operation_id,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    /// Publish complete files from local paths without retaining their payloads in SDK memory.
    /// Each source is hashed and streamed in bounded direct-to-object-store parts.
    pub async fn publish_filesystem_paths(
        &self,
        project_id: &str,
        filesystem: &str,
        files: Vec<NativeDirectFilePathWrite>,
        message: String,
        operation_id: String,
    ) -> Result<Traced<NativeDirectPublishResponse>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, filesystem).await?;
        self.push_native_files_direct_with_credential(
            project_id,
            filesystem,
            Vec::new(),
            files,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            message,
            operation_id,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    /// Credential-explicit form used by callers that already cache or manage a repository token.
    #[allow(clippy::too_many_arguments)]
    pub async fn push_native_files_direct_with_credential(
        &self,
        project_id: &str,
        filesystem: &str,
        files: Vec<NativeDirectFileWrite>,
        path_files: Vec<NativeDirectFilePathWrite>,
        deletes: Vec<String>,
        moves: Vec<NativeDirectPathTransfer>,
        copies: Vec<NativeDirectPathTransfer>,
        message: String,
        operation_id: String,
        git_username: &str,
        git_token: &str,
    ) -> Result<Traced<NativeDirectPublishResponse>, SdkError> {
        use futures::StreamExt;
        if files.is_empty()
            && path_files.is_empty()
            && deletes.is_empty()
            && moves.is_empty()
            && copies.is_empty()
        {
            return Err(SdkError::ClientError(
                "nothing to publish: no writes, deletions, moves, or copies given".to_string(),
            ));
        }
        for file in &files {
            validate_native_direct_path(&file.path)?;
        }
        for file in &path_files {
            validate_native_direct_path(&file.path)?;
        }
        for path in &deletes {
            validate_native_direct_path(path)?;
        }
        for transfer in moves.iter().chain(&copies) {
            validate_native_direct_path(&transfer.from)?;
            validate_native_direct_path(&transfer.to)?;
        }
        const DIRECT_PART_BYTES: usize = 64 * 1024 * 1024;
        let prepare = tokio::task::spawn_blocking(move || {
            prepare_native_direct_publication(
                files,
                path_files,
                deletes,
                moves,
                copies,
                DIRECT_PART_BYTES,
            )
        });

        // Head and lease are independent control requests. Issuing them together keeps the
        // fixed-cost portion of a small publication to three sequential data-plane round trips:
        // (head+lease), targets, then publish after the parallel direct PUTs. SHA-256 preparation
        // runs concurrently on the blocking pool, so multi-GiB inputs add no serial setup phase.
        let head_request = send_idempotent(
            self.git_request(
                Method::GET,
                project_id,
                filesystem,
                Some("fs/head"),
                git_username,
                git_token,
            )?
            .0,
        );
        let lease_request = send_idempotent(
            self.git_request(
                Method::POST,
                project_id,
                filesystem,
                Some("fs/upload-leases"),
                git_username,
                git_token,
            )?
            .0
            .json(&serde_json::json!({})),
        );
        let control = async {
            let (head_response, lease_response) = tokio::try_join!(head_request, lease_request)?;
            let head = decode_json::<NativeHeadResponse>(head_response, String::new())
                .await?
                .into_inner();
            let lease =
                decode_json::<NativeDirectUploadLeaseResponse>(lease_response, String::new())
                    .await?
                    .into_inner();
            Ok::<_, SdkError>((head, lease))
        };
        let prepared = async {
            prepare.await.map_err(|error| {
                SdkError::ClientError(format!(
                    "native direct publication preparation failed: {error}"
                ))
            })?
        };
        let ((head, lease), (mutations, unique_blobs)) = tokio::try_join!(control, prepared)?;
        if head.snapshot_id.is_none()
            && mutations
                .iter()
                .all(|mutation| matches!(mutation, NativeDirectMutation::Delete { .. }))
        {
            return Err(SdkError::ClientError(
                "cannot delete from an empty filesystem".to_string(),
            ));
        }
        if lease.transport != "checksum_presigned_put" || lease.checksum_algorithm != "sha256" {
            return Err(SdkError::ClientError(format!(
                "server returned unsupported direct upload transport {:?} with checksum {:?}",
                lease.transport, lease.checksum_algorithm
            )));
        }
        if lease.max_targets_per_request == 0 || lease.max_parts_per_file == 0 {
            return Err(SdkError::ClientError(
                "server returned a zero direct-upload batching limit".to_string(),
            ));
        }
        if lease.max_blob_bytes < DIRECT_PART_BYTES as u64 {
            return Err(SdkError::ClientError(format!(
                "server direct-upload part limit {} is smaller than the SDK's {} byte part size",
                lease.max_blob_bytes, DIRECT_PART_BYTES
            )));
        }
        if let Some(part_count) = mutations.iter().find_map(|mutation| match mutation {
            NativeDirectMutation::Put { blobs, .. } if blobs.len() > lease.max_parts_per_file => {
                Some(blobs.len())
            }
            _ => None,
        }) {
            return Err(SdkError::ClientError(format!(
                "native file has {part_count} parts, exceeding the server's {} part limit",
                lease.max_parts_per_file
            )));
        }
        if let Some((blob_id, data)) = unique_blobs
            .iter()
            .find(|(_, data)| data.logical_len() > lease.max_blob_bytes)
        {
            return Err(SdkError::ClientError(format!(
                "native blob {blob_id} has {} bytes, exceeding the server's {} byte direct-upload limit",
                data.logical_len(),
                lease.max_blob_bytes
            )));
        }

        let blob_requests = unique_blobs
            .iter()
            .map(|(blob_id, data)| NativeDirectBlobRequest {
                blob_id: blob_id.clone(),
                logical_len: data.logical_len(),
            })
            .collect::<Vec<_>>();
        let mut targets = NativeDirectBlobTargetsResponse {
            targets: Vec::with_capacity(blob_requests.len()),
        };
        let mut target_requests = Vec::new();
        for batch in blob_requests.chunks(lease.max_targets_per_request) {
            let targets_path = format!(
                "fs/upload-leases/{}/targets",
                encode_path_segment(&lease.lease_id)
            );
            let (request, trace_id) = self.git_request(
                Method::POST,
                project_id,
                filesystem,
                Some(&targets_path),
                git_username,
                git_token,
            )?;
            let body = NativeDirectBlobTargetsRequest {
                blobs: batch.to_vec(),
            };
            target_requests.push(async move {
                decode_json::<NativeDirectBlobTargetsResponse>(
                    send_idempotent(request.json(&body)).await?,
                    trace_id,
                )
                .await
                .map(Traced::into_inner)
            });
        }
        let target_results = futures::stream::iter(target_requests)
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
        for result in target_results {
            targets.targets.extend(result?.targets);
        }
        validate_native_direct_targets(&blob_requests, &targets.targets)?;

        let mut receipts = Vec::new();
        let mut uploads = Vec::new();
        for target in targets.targets {
            if target.already_present {
                continue;
            }
            let data = unique_blobs.get(&target.blob_id).cloned().ok_or_else(|| {
                SdkError::ClientError(format!(
                    "server returned an unknown native blob target {}",
                    target.blob_id
                ))
            })?;
            let url = target.url.ok_or_else(|| {
                SdkError::ClientError("direct upload target omitted URL".to_string())
            })?;
            let checksum = target.checksum_sha256.ok_or_else(|| {
                SdkError::ClientError("direct upload target omitted SHA-256 header".to_string())
            })?;
            receipts.push(NativeDirectUploadReceipt {
                blob_id: target.blob_id,
                logical_len: target.logical_len,
            });
            let client = self.git_client.clone();
            uploads.push(async move {
                let response = upload_native_direct_source(&client, &url, &checksum, &data).await?;
                if response.status().is_success() {
                    Ok::<(), SdkError>(())
                } else {
                    Err(SdkError::ServerError {
                        status: response.status(),
                        message: body_message(response).await,
                    })
                }
            });
        }
        let results = futures::stream::iter(uploads)
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result?;
        }

        let publish = NativeDirectPublishRequest {
            operation_id,
            message,
            expected_version_id: head.snapshot_id,
            lease_id: lease.lease_id,
            uploads: receipts,
            mutations,
            retain_as_snapshot: false,
        };
        let (publish_request, trace_id) = self.git_request(
            Method::POST,
            project_id,
            filesystem,
            Some("fs/publish"),
            git_username,
            git_token,
        )?;
        decode_json(
            send_idempotent(publish_request.json(&publish)).await?,
            trace_id,
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

type NativeDirectPrepared = (
    Vec<NativeDirectMutation>,
    std::collections::BTreeMap<String, NativeDirectUploadSource>,
);

#[derive(Clone, Debug)]
enum NativeDirectUploadSource {
    Bytes(bytes::Bytes),
    FileRange {
        path: std::sync::Arc<std::path::PathBuf>,
        offset: u64,
        logical_len: u64,
    },
}

impl NativeDirectUploadSource {
    fn logical_len(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::FileRange { logical_len, .. } => *logical_len,
        }
    }
}

fn prepare_native_direct_publication(
    files: Vec<NativeDirectFileWrite>,
    path_files: Vec<NativeDirectFilePathWrite>,
    deletes: Vec<String>,
    moves: Vec<NativeDirectPathTransfer>,
    copies: Vec<NativeDirectPathTransfer>,
    part_bytes: usize,
) -> Result<NativeDirectPrepared, SdkError> {
    use rayon::prelude::*;
    use sha2::{Digest, Sha256};

    if part_bytes == 0 {
        return Err(SdkError::ClientError(
            "native direct-upload part size must be non-zero".to_string(),
        ));
    }
    let mut mutations = Vec::with_capacity(
        files.len() + path_files.len() + deletes.len() + moves.len() + copies.len(),
    );
    let mut unique_blobs = std::collections::BTreeMap::<String, NativeDirectUploadSource>::new();
    for file in files {
        let content = bytes::Bytes::from(file.data);
        let parts = if content.is_empty() {
            vec![(hex::encode(Sha256::digest([])), bytes::Bytes::new())]
        } else {
            (0..content.len())
                .step_by(part_bytes)
                .map(|start| content.slice(start..(start + part_bytes).min(content.len())))
                .collect::<Vec<_>>()
                .into_par_iter()
                .map(|part| (hex::encode(Sha256::digest(&part)), part))
                .collect()
        };
        let mut blobs = Vec::with_capacity(parts.len());
        for (blob_id, part) in parts {
            blobs.push(NativeDirectBlobRequest {
                blob_id: blob_id.clone(),
                logical_len: part.len() as u64,
            });
            if !part.is_empty() {
                unique_blobs
                    .entry(blob_id)
                    .or_insert(NativeDirectUploadSource::Bytes(part));
            }
        }
        mutations.push(NativeDirectMutation::Put {
            path: file.path,
            blobs,
        });
    }
    for file in path_files {
        use std::io::Read;

        let source_path = std::sync::Arc::new(file.source_path);
        let mut source = std::fs::File::open(source_path.as_ref()).map_err(|error| {
            SdkError::ClientError(format!(
                "opening native publication source {}: {error}",
                source_path.display()
            ))
        })?;
        let logical_len = source
            .metadata()
            .map_err(|error| {
                SdkError::ClientError(format!(
                    "reading native publication source metadata {}: {error}",
                    source_path.display()
                ))
            })?
            .len();
        let mut blobs = Vec::new();
        if logical_len == 0 {
            blobs.push(NativeDirectBlobRequest {
                blob_id: hex::encode(Sha256::digest([])),
                logical_len: 0,
            });
        } else {
            let mut offset = 0u64;
            let mut buffer = vec![0u8; part_bytes];
            while offset < logical_len {
                let part_len = usize::try_from(
                    (logical_len - offset).min(u64::try_from(part_bytes).unwrap_or(u64::MAX)),
                )
                .map_err(|_| {
                    SdkError::ClientError("native publication part length overflow".to_string())
                })?;
                source
                    .read_exact(&mut buffer[..part_len])
                    .map_err(|error| {
                        SdkError::ClientError(format!(
                            "reading native publication source {}: {error}",
                            source_path.display()
                        ))
                    })?;
                let blob_id = hex::encode(Sha256::digest(&buffer[..part_len]));
                blobs.push(NativeDirectBlobRequest {
                    blob_id: blob_id.clone(),
                    logical_len: part_len as u64,
                });
                unique_blobs.entry(blob_id).or_insert_with(|| {
                    NativeDirectUploadSource::FileRange {
                        path: source_path.clone(),
                        offset,
                        logical_len: part_len as u64,
                    }
                });
                offset += part_len as u64;
            }
        }
        mutations.push(NativeDirectMutation::Put {
            path: file.path,
            blobs,
        });
    }
    mutations.extend(
        deletes
            .into_iter()
            .map(|path| NativeDirectMutation::Delete { path }),
    );
    mutations.extend(
        moves
            .into_iter()
            .map(|transfer| NativeDirectMutation::Move {
                from: transfer.from,
                to: transfer.to,
            }),
    );
    mutations.extend(
        copies
            .into_iter()
            .map(|transfer| NativeDirectMutation::Copy {
                from: transfer.from,
                to: transfer.to,
            }),
    );
    if unique_blobs.len() > 4_096 {
        return Err(SdkError::ClientError(format!(
            "native publication contains {} unique non-empty parts; the maximum is 4096",
            unique_blobs.len()
        )));
    }
    Ok((mutations, unique_blobs))
}

fn validate_native_direct_path(path: &str) -> Result<(), SdkError> {
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || path.contains('\0')
        || path.split('/').any(|component| {
            component.is_empty() || component == "." || component == ".." || component.contains(':')
        })
    {
        return Err(SdkError::ClientError(format!(
            "invalid native filesystem path {path:?}"
        )));
    }
    Ok(())
}

fn validate_native_direct_targets(
    requested: &[NativeDirectBlobRequest],
    targets: &[models::NativeDirectBlobTarget],
) -> Result<(), SdkError> {
    use base64::Engine;

    let expected = requested
        .iter()
        .map(|blob| (blob.blob_id.as_str(), blob.logical_len))
        .collect::<std::collections::HashMap<_, _>>();
    let mut seen = std::collections::HashSet::with_capacity(targets.len());
    for target in targets {
        let Some(expected_len) = expected.get(target.blob_id.as_str()) else {
            return Err(SdkError::ClientError(format!(
                "server returned an unrequested native blob target {}",
                target.blob_id
            )));
        };
        if !seen.insert(target.blob_id.as_str()) {
            return Err(SdkError::ClientError(format!(
                "server returned native blob target {} more than once",
                target.blob_id
            )));
        }
        if target.logical_len != *expected_len {
            return Err(SdkError::ClientError(format!(
                "server returned native blob target {} with length {}, expected {}",
                target.blob_id, target.logical_len, expected_len
            )));
        }
        if !target.already_present {
            let checksum = target.checksum_sha256.as_deref().ok_or_else(|| {
                SdkError::ClientError("direct upload target omitted SHA-256 header".to_string())
            })?;
            let digest = hex::decode(&target.blob_id).map_err(|error| {
                SdkError::ClientError(format!(
                    "server returned invalid native blob identity {}: {error}",
                    target.blob_id
                ))
            })?;
            let expected_checksum = base64::engine::general_purpose::STANDARD.encode(digest);
            if checksum != expected_checksum {
                return Err(SdkError::ClientError(format!(
                    "server returned a checksum that does not match native blob target {}",
                    target.blob_id
                )));
            }
            if target.url.is_none() {
                return Err(SdkError::ClientError(
                    "direct upload target omitted URL".to_string(),
                ));
            }
        }
    }
    if seen.len() != expected.len() {
        let missing = expected
            .keys()
            .filter(|blob_id| !seen.contains(**blob_id))
            .copied()
            .collect::<Vec<_>>();
        return Err(SdkError::ClientError(format!(
            "server omitted native blob target(s): {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

fn encode_native_path(path: &str) -> Result<String, SdkError> {
    validate_native_direct_path(path)?;
    Ok(path
        .split('/')
        .map(encode_path_segment)
        .collect::<Vec<_>>()
        .join("/"))
}

fn native_snapshot_id(version: &str) -> Result<Option<&str>, SdkError> {
    match version {
        "" | "main" | "refs/heads/main" => Ok(None),
        snapshot
            if snapshot.len() == 64 && snapshot.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            Ok(Some(snapshot))
        }
        _ => Err(SdkError::ClientError(format!(
            "native filesystems support the current head or a 64-character snapshot id, not {version:?}"
        ))),
    }
}

fn native_entries_suffix(
    path: &str,
    snapshot: Option<&str>,
    after: Option<&str>,
    limit: usize,
) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(snapshot) = snapshot {
        query.append_pair("snapshot", snapshot);
    }
    query.append_pair("path", path);
    if let Some(after) = after {
        query.append_pair("after", after);
    }
    query.append_pair("limit", &limit.clamp(1, 10_000).to_string());
    format!("fs/entries?{}", query.finish())
}

fn repo_list_query(kind: Option<&str>, after: Option<&str>) -> String {
    // `form_urlencoded::Serializer` contains a non-Send callback reference. Keep it in this
    // synchronous helper so napi/tonic callers can carry the async request future across workers.
    let mut params = url::form_urlencoded::Serializer::new(String::new());
    if let Some(kind) = kind {
        params.append_pair("kind", kind);
        // Every filesystem page carries a synchronous cache-generation fence. Project inventory
        // routes have no repository affinity, so consecutive requests may land on different
        // server pods; fencing only page one could splice a fresh first page to a stale later page
        // and omit a just-created filesystem after the first cursor.
        if kind == REPO_KIND_FILESYSTEM {
            params.append_pair("fresh", "true");
        }
    }
    if let Some(after) = after {
        params.append_pair("after", after);
    }
    params.append_pair("limit", "1000");
    params.finish()
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

fn git_credential_is_fresh(credential: &GitCredential) -> bool {
    chrono::DateTime::parse_from_rfc3339(&credential.expires_at)
        .map(|expires_at| {
            expires_at.timestamp_millis()
                > chrono::Utc::now().timestamp_millis().saturating_add(30_000)
        })
        .unwrap_or(false)
}

/// A paged collection must always make forward progress. Treat an empty or repeated cursor as a
/// malformed server response rather than spinning forever in an SDK call.
fn advance_pagination_cursor(
    seen: &mut HashSet<String>,
    next: String,
    surface: &str,
) -> Result<String, SdkError> {
    if next.is_empty() || !seen.insert(next.clone()) {
        return Err(SdkError::ClientError(format!(
            "{surface} returned an empty or repeated pagination cursor"
        )));
    }
    Ok(next)
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

async fn native_direct_request_body(
    source: &NativeDirectUploadSource,
) -> Result<reqwest::Body, SdkError> {
    match source {
        NativeDirectUploadSource::Bytes(bytes) => Ok(reqwest::Body::from(bytes.clone())),
        NativeDirectUploadSource::FileRange {
            path,
            offset,
            logical_len,
        } => {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};

            let mut file = tokio::fs::File::open(path.as_ref()).await?;
            file.seek(std::io::SeekFrom::Start(*offset)).await?;
            // ReaderStream's small default allocation creates excessive body frames for 64 MiB
            // parts. A 1 MiB transport buffer keeps eight concurrent uploads memory-bounded while
            // reaching object-store line rate with far less scheduler and HTTP framing overhead.
            let stream =
                tokio_util::io::ReaderStream::with_capacity(file.take(*logical_len), 1024 * 1024);
            Ok(reqwest::Body::wrap_stream(stream))
        }
    }
}

/// Upload a replayable source. File-backed parts reopen and seek the source for each transport
/// attempt, avoiding both whole-file memory and reqwest's non-cloneable streaming-body limitation.
async fn upload_native_direct_source(
    client: &reqwest::Client,
    url: &str,
    checksum: &str,
    source: &NativeDirectUploadSource,
) -> Result<reqwest::Response, SdkError> {
    const ATTEMPTS: usize = 3;
    for attempt in 0..ATTEMPTS {
        let body = native_direct_request_body(source).await?;
        match client
            .put(url)
            .header(
                reqwest::header::CONTENT_LENGTH,
                source.logical_len().to_string(),
            )
            .header("x-amz-checksum-sha256", checksum)
            .body(body)
            .send()
            .await
        {
            Ok(response)
                if attempt + 1 < ATTEMPTS
                    && (response.status().is_server_error()
                        || response.status() == StatusCode::TOO_MANY_REQUESTS) => {}
            Ok(response) => return Ok(response),
            Err(_) if attempt + 1 < ATTEMPTS => {}
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(std::time::Duration::from_millis(25 << attempt)).await;
    }
    unreachable!("the final direct upload attempt always returns")
}

/// Send an idempotent direct-publication step with a small transport retry budget. Every caller
/// either uses a read, a lease/target allocation that reattaches by server key, an immutable PUT,
/// or the operation-id-fenced publish endpoint.
async fn send_idempotent(request: reqwest::RequestBuilder) -> Result<reqwest::Response, SdkError> {
    const ATTEMPTS: usize = 3;
    for attempt in 0..ATTEMPTS {
        let current = request.try_clone().ok_or_else(|| {
            SdkError::ClientError("direct upload request body cannot be retried".to_string())
        })?;
        match current.send().await {
            Ok(response)
                if attempt + 1 < ATTEMPTS
                    && (response.status().is_server_error()
                        || response.status() == StatusCode::TOO_MANY_REQUESTS) => {}
            Ok(response) => return Ok(response),
            Err(error) if attempt + 1 < ATTEMPTS => {
                let _ = error;
            }
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(std::time::Duration::from_millis(25 << attempt)).await;
    }
    unreachable!("the final direct request attempt always returns")
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
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use base64::Engine;
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{
        ArtifactStorageClient, advance_pagination_cursor, encode_path_segment,
        git_credential_is_fresh, prepare_native_direct_publication, repo_list_query,
        resolve_artifact_storage_url, validate_native_direct_targets,
    };
    use crate::ClientBuilder;
    use crate::artifact_storage::models::{
        GitCredential, NativeDirectBlobRequest, NativeDirectBlobTarget, NativeDirectFilePathWrite,
        NativeDirectFileWrite, NativeDirectMutation, REPO_KIND_FILESYSTEM,
    };

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
    fn direct_publication_splits_memory_and_file_sources_into_bounded_parts() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("large.bin");
        std::fs::write(&source_path, b"abcdefgh").unwrap();
        let (mutations, unique) = prepare_native_direct_publication(
            vec![NativeDirectFileWrite {
                path: "memory.bin".into(),
                data: b"abcdefgh".to_vec(),
            }],
            vec![NativeDirectFilePathWrite {
                path: "disk.bin".into(),
                source_path,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            3,
        )
        .unwrap();

        assert_eq!(mutations.len(), 2);
        for mutation in mutations {
            let NativeDirectMutation::Put { blobs, .. } = mutation else {
                panic!("expected put mutation");
            };
            assert_eq!(
                blobs
                    .iter()
                    .map(|blob| blob.logical_len)
                    .collect::<Vec<_>>(),
                vec![3, 3, 2]
            );
        }
        // Identical memory/file content deduplicates to one source per part identity.
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn direct_upload_targets_must_exactly_match_the_request() {
        use base64::Engine;

        let blob_id = hex::encode(Sha256::digest(b"payload"));
        let checksum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(b"payload"));
        let request = NativeDirectBlobRequest {
            blob_id: blob_id.clone(),
            logical_len: 7,
        };
        let target = NativeDirectBlobTarget {
            blob_id: blob_id.clone(),
            logical_len: 7,
            already_present: false,
            url: Some("https://objects.example/upload".to_string()),
            checksum_sha256: Some(checksum),
        };
        assert!(
            validate_native_direct_targets(
                std::slice::from_ref(&request),
                std::slice::from_ref(&target)
            )
            .is_ok()
        );

        let mut wrong_len = target.clone();
        wrong_len.logical_len += 1;
        assert!(
            validate_native_direct_targets(std::slice::from_ref(&request), &[wrong_len]).is_err()
        );
        assert!(
            validate_native_direct_targets(
                std::slice::from_ref(&request),
                &[target.clone(), target.clone()]
            )
            .is_err()
        );
        assert!(validate_native_direct_targets(std::slice::from_ref(&request), &[]).is_err());

        let mut wrong_checksum = target;
        wrong_checksum.checksum_sha256 = Some("not-the-content-digest".to_string());
        assert!(
            validate_native_direct_targets(std::slice::from_ref(&request), &[wrong_checksum])
                .is_err()
        );
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

    #[test]
    fn pagination_cursor_must_make_progress() {
        let mut seen = HashSet::new();
        assert_eq!(
            advance_pagination_cursor(&mut seen, "page-2".to_string(), "test").unwrap(),
            "page-2"
        );
        assert!(advance_pagination_cursor(&mut seen, "page-2".to_string(), "test").is_err());
        assert!(advance_pagination_cursor(&mut seen, String::new(), "test").is_err());
    }

    #[test]
    fn repository_credential_cache_keeps_an_expiry_safety_margin() {
        let credential = |expires_at: String| GitCredential {
            token: "token".to_string(),
            token_type: "bearer".to_string(),
            expires_at,
            git_username: "user".to_string(),
            repo_pattern: "filesystem".to_string(),
            scopes: vec!["fs:write".to_string()],
        };
        assert!(git_credential_is_fresh(&credential(
            (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339()
        )));
        assert!(!git_credential_is_fresh(&credential(
            (chrono::Utc::now() + chrono::Duration::seconds(10)).to_rfc3339()
        )));
        assert!(!git_credential_is_fresh(&credential(String::new())));
    }

    #[tokio::test]
    async fn project_credential_reuses_the_shared_expiry_checked_cache() {
        let api_client = ClientBuilder::new("http://127.0.0.1:1").build().unwrap();
        let client = ArtifactStorageClient::new(api_client, "http://127.0.0.1:1").unwrap();
        let cached = GitCredential {
            token: "cached-project-token".to_string(),
            token_type: "bearer".to_string(),
            expires_at: (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
            git_username: "project-user".to_string(),
            repo_pattern: "*".to_string(),
            scopes: vec!["project:admin".to_string()],
        };
        client
            .repo_credentials
            .lock()
            .await
            .insert(("project".to_string(), "*".to_string()), cached.clone());

        assert_eq!(
            client.git_credential_for_project("project").await.unwrap(),
            cached,
            "a cached project credential must avoid another control-plane mint"
        );
    }

    #[test]
    fn filesystem_inventory_fences_every_page() {
        assert_eq!(
            repo_list_query(Some(REPO_KIND_FILESYSTEM), None),
            "kind=filesystem&fresh=true&limit=1000"
        );
        assert_eq!(
            repo_list_query(Some(REPO_KIND_FILESYSTEM), Some("project/repo")),
            "kind=filesystem&fresh=true&after=project%2Frepo&limit=1000"
        );
        assert_eq!(
            repo_list_query(Some("repository"), None),
            "kind=repository&limit=1000"
        );
    }

    #[tokio::test]
    async fn direct_native_push_uploads_payload_only_to_the_presigned_target() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let payload = b"payload goes straight to object storage".to_vec();
        let blob_id = hex::encode(Sha256::digest(&payload));
        let checksum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&payload));
        let requests = Arc::new(Mutex::new(Vec::<(String, String, Vec<u8>)>::new()));
        let captured = requests.clone();
        let server_base = base.clone();
        let server_blob_id = blob_id.clone();
        let server_payload = payload.clone();
        let server = tokio::spawn(async move {
            for _ in 0..13 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut chunk = [0u8; 4096];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(read, 0, "request ended before its headers");
                    bytes.extend_from_slice(&chunk[..read]);
                    if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break offset + 4;
                    }
                };
                let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                let content_len = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                let range_header = headers.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("range")
                        .then(|| value.trim().to_string())
                });
                while bytes.len() < header_end + content_len {
                    let mut chunk = [0u8; 4096];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(read, 0, "request ended before its body");
                    bytes.extend_from_slice(&chunk[..read]);
                }
                let request_line = headers.lines().next().unwrap();
                let mut request_parts = request_line.split_whitespace();
                let method = request_parts.next().unwrap().to_string();
                let path = request_parts.next().unwrap().to_string();
                let body = bytes[header_end..header_end + content_len].to_vec();
                captured
                    .lock()
                    .unwrap()
                    .push((method.clone(), path.clone(), body));

                let response_body = match (method.as_str(), path.as_str()) {
                    ("POST", "/artifact-storage/v1/token") => {
                        serde_json::to_vec(&serde_json::json!({
                            "token": "repo-token",
                            "tokenType": "bearer",
                            "expiresAt": "2099-01-01T00:00:00Z",
                            "gitUsername": "repo-user",
                            "repoPattern": "filesystem",
                            "scopes": ["fs:read", "fs:write"],
                        }))
                        .unwrap()
                    }
                    ("GET", "/project/project/repos/filesystem/fs/head") => {
                        serde_json::to_vec(
                            &serde_json::json!({"snapshot_id": null, "generation": 0}),
                        )
                        .unwrap()
                    }
                    ("POST", "/project/project/repos/filesystem/fs/upload-leases") => {
                        serde_json::to_vec(&serde_json::json!({
                            "lease_id": "lease-1",
                            "expires_at_ms": 9_999_999_999_999_u64,
                            "max_blob_bytes": 268_435_456_u64,
                            "max_targets_per_request": 256,
                            "max_parts_per_file": 4096,
                            "transport": "checksum_presigned_put",
                            "checksum_algorithm": "sha256",
                        }))
                        .unwrap()
                    }
                    (
                        "POST",
                        "/project/project/repos/filesystem/fs/upload-leases/lease-1/targets",
                    ) => serde_json::to_vec(&serde_json::json!({
                        "targets": [{
                            "blob_id": server_blob_id,
                            "logical_len": server_payload.len(),
                            "already_present": false,
                            "url": format!("{server_base}/object-store/stage-1"),
                            "checksum_sha256": checksum,
                        }],
                    }))
                    .unwrap(),
                    ("PUT", "/object-store/stage-1") => b"{}".to_vec(),
                    ("POST", "/project/project/repos/filesystem/fs/publish") => {
                        serde_json::to_vec(&serde_json::json!({
                            "version_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "previous_version_id": null,
                        }))
                        .unwrap()
                    }
                    ("POST", "/project/project/repos/filesystem/fs/snapshots") => {
                        serde_json::to_vec(&serde_json::json!({
                            "snapshot_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "snapshot_class": "permanent_snapshot",
                            "message": "retained by SDK",
                        }))
                        .unwrap()
                    }
                    ("GET", "/project/project/repos/filesystem/fs/snapshots?limit=1000") => {
                        serde_json::to_vec(&serde_json::json!({
                            "snapshots": [{
                                "snapshot_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "created_at_ms": 1234,
                                "message": "retained by SDK",
                                "snapshot_class": "permanent_snapshot",
                            }],
                            "next_after": null,
                        }))
                        .unwrap()
                    }
                    (
                        "GET",
                        "/project/project/repos/filesystem/fs/snapshots/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    ) => serde_json::to_vec(&serde_json::json!({
                        "snapshot_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "created_at_ms": 1234,
                        "message": "retained by SDK",
                        "snapshot_class": "permanent_snapshot",
                        "permanence_epoch": 7,
                    }))
                    .unwrap(),
                    (
                        "DELETE",
                        "/project/project/repos/filesystem/fs/snapshots/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?expected_permanence_epoch=7",
                    ) => Vec::new(),
                    ("GET", "/project/project/repos/filesystem/fs/files/data/probe.bin") => {
                        if range_header.as_deref() == Some("bytes=8-14") {
                            server_payload[8..15].to_vec()
                        } else {
                            server_payload.clone()
                        }
                    }
                    ("GET", path) if path.starts_with(
                        "/project/project/repos/filesystem/fs/entries?path=data&limit=1000",
                    ) => serde_json::to_vec(&serde_json::json!({
                        "entries": [{
                            "name": "probe.bin",
                            "oid": server_blob_id,
                            "mode": 33188,
                            "size": server_payload.len(),
                        }],
                        "truncated": false,
                        "next_after": null,
                    }))
                    .unwrap(),
                    other => panic!("unexpected request: {other:?}"),
                };
                let native_file_headers = if path
                    == "/project/project/repos/filesystem/fs/files/data/probe.bin"
                {
                    let content_range = range_header
                        .as_ref()
                        .map(|_| format!("Content-Range: bytes 8-14/{}\r\n", server_payload.len()))
                        .unwrap_or_default();
                    format!("x-tensorlake-content-id: {server_blob_id}\r\n{content_range}")
                } else {
                    String::new()
                };
                let response_status = if range_header.is_some() {
                    "206 Partial Content"
                } else {
                    "200 OK"
                };
                let response = format!(
                    "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
                    response_body.len(),
                    native_file_headers,
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(&response_body).await.unwrap();
            }
        });

        let api_client = ClientBuilder::new(&base).build().unwrap();
        let client = ArtifactStorageClient::new(api_client, &base).unwrap();
        let result = client
            .publish_filesystem_files(
                "project",
                "filesystem",
                vec![
                    NativeDirectFileWrite {
                        path: "data/probe.bin".to_string(),
                        data: payload.clone(),
                    },
                    NativeDirectFileWrite {
                        path: "data/empty.txt".to_string(),
                        data: Vec::new(),
                    },
                ],
                vec!["old.txt".to_string()],
                Vec::new(),
                Vec::new(),
                "SDK publication".to_string(),
                "operation-1".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            result.version_id,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            client
                .read_native_filesystem_file("project", "filesystem", "data/probe.bin", "main")
                .await
                .unwrap()
                .into_inner(),
            payload
        );
        let ranged = client
            .read_native_filesystem_file_with_metadata(
                "project",
                "filesystem",
                "data/probe.bin",
                "main",
                Some((8, 7)),
            )
            .await
            .unwrap();
        assert_eq!(ranged.data, b"goes st");
        assert_eq!(ranged.content_id, blob_id);
        assert_eq!(ranged.full_size, payload.len() as u64);
        let entries = client
            .list_native_filesystem_entries_page(
                "project",
                "filesystem",
                "data",
                "main",
                None,
                1000,
            )
            .await
            .unwrap();
        assert_eq!(entries.entries.len(), 1);
        assert_eq!(entries.entries[0].name, "probe.bin");
        let retained = client
            .retain_current_native_filesystem_snapshot(
                "project",
                "filesystem",
                "retained by SDK".to_string(),
                "retain-1".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(retained.snapshot_class, "permanent_snapshot");
        let snapshots = client
            .list_native_filesystem_snapshots("project", "filesystem")
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 1);
        client
            .delete_native_filesystem_snapshot(
                "project",
                "filesystem",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .await
            .unwrap();
        server.await.unwrap();

        let requests = requests.lock().unwrap();
        let upload = requests
            .iter()
            .find(|(method, path, _)| method == "PUT" && path == "/object-store/stage-1")
            .unwrap();
        assert_eq!(upload.2, payload);
        let targets = requests
            .iter()
            .find(|(_, path, _)| path.ends_with("/targets"))
            .unwrap();
        let targets: serde_json::Value = serde_json::from_slice(&targets.2).unwrap();
        assert_eq!(targets["blobs"].as_array().unwrap().len(), 1);
        assert_eq!(targets["blobs"][0]["blob_id"], blob_id);
        let publish = requests
            .iter()
            .find(|(_, path, _)| path.ends_with("/publish"))
            .unwrap();
        assert!(
            !String::from_utf8_lossy(&publish.2).contains("payload goes straight"),
            "control-plane publish body must not contain file payload bytes"
        );
        let publish: serde_json::Value = serde_json::from_slice(&publish.2).unwrap();
        assert_eq!(publish["operation_id"], "operation-1");
        assert_eq!(publish["message"], "SDK publication");
        assert_eq!(publish["retain_as_snapshot"], false);
        assert_eq!(publish["uploads"].as_array().unwrap().len(), 1);
        assert_eq!(publish["mutations"].as_array().unwrap().len(), 3);
        let retain = requests
            .iter()
            .find(|(method, path, _)| method == "POST" && path.ends_with("/fs/snapshots"))
            .unwrap();
        let retain: serde_json::Value = serde_json::from_slice(&retain.2).unwrap();
        assert_eq!(retain["request_id"], "retain-1");
        assert!(
            requests.iter().any(|(method, path, _)| {
                method == "DELETE" && path.ends_with("expected_permanence_epoch=7")
            }),
            "snapshot deletion must fence the server-observed permanence epoch"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(_, path, _)| path == "/artifact-storage/v1/token")
                .count(),
            1,
            "the repository credential should be reused across write and reads"
        );
    }
}
