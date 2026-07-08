//! napi-rs bindings for Tensorlake Artifact Storage Git repositories.

use std::path::PathBuf;

use napi_derive::napi;
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::ingest::PushOptions;
use tensorlake::artifact_storage::merge::MergeRequest;
use tensorlake::{ClientBuilder, error::SdkError};

use crate::sandbox::{TracedJson, duration_from_seconds, into_napi_error, usage_error, with_retry};

#[napi]
pub struct NativeRepositoryClient {
    client: ArtifactStorageClient,
    project_id: Option<String>,
}

#[napi]
impl NativeRepositoryClient {
    #[napi(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_url: String,
        api_key: Option<String>,
        organization_id: Option<String>,
        project_id: Option<String>,
        user_agent: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> napi::Result<Self> {
        let mut builder = ClientBuilder::new(&api_url);
        if let Some(token) = api_key.as_deref() {
            builder = builder.bearer_token(token);
        }
        if let (Some(org_id), Some(project_id)) =
            (organization_id.as_deref(), project_id.as_deref())
        {
            builder = builder.scope(org_id, project_id);
        }
        if let Some(ua) = user_agent.as_deref() {
            builder = builder.user_agent(ua);
        }
        if let Some(seconds) = request_timeout_sec {
            builder = builder.timeout(duration_from_seconds("request_timeout_sec", seconds)?);
        }

        let api_client = builder.build().map_err(into_napi_error)?;
        let client = ArtifactStorageClient::new(
            api_client,
            tensorlake::resolve_artifact_storage_url(&api_url),
        )
        .map_err(into_napi_error)?;
        Ok(Self { client, project_id })
    }

    fn project_id(&self) -> napi::Result<&str> {
        self.project_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| usage_error("Repository operations require projectId".to_string()))
    }

    #[napi]
    pub fn git_repo_url(&self, repo: String) -> napi::Result<String> {
        Ok(self.client.git_repo_url(self.project_id()?, &repo))
    }

    #[napi]
    pub async fn create_repo(
        &self,
        repo: String,
        default_branch: Option<String>,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            let default_branch = default_branch.clone();
            async move {
                let traced = client
                    .create_repo(&project_id, &repo, default_branch.as_deref())
                    .await?;
                let url = client.git_repo_url(&project_id, &repo);
                let json = serde_json::to_string(&serde_json::json!({
                    "repo": repo,
                    "url": url,
                }))?;
                Ok(TracedJson {
                    trace_id: traced.trace_id,
                    json,
                })
            }
        })
        .await
    }

    #[napi]
    pub async fn list_repos(&self) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            async move {
                let traced = client.list_repos(&project_id).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn delete_repo(&self, repo: String) -> napi::Result<String> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                client
                    .delete_repo(&project_id, &repo)
                    .await
                    .map(|t| t.trace_id)
            }
        })
        .await
    }

    #[napi]
    pub async fn fork_repo(&self, repo: String, base_repo: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            let base_repo = base_repo.clone();
            async move {
                let traced = client.fork_repo(&project_id, &repo, &base_repo).await?;
                let url = client.git_repo_url(&project_id, &repo);
                let json = serde_json::to_string(&serde_json::json!({
                    "repo": repo,
                    "url": url,
                    "base_repo": base_repo,
                }))?;
                Ok(TracedJson {
                    trace_id: traced.trace_id,
                    json,
                })
            }
        })
        .await
    }

    #[napi]
    pub async fn archive_repo(&self, repo: String) -> napi::Result<String> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                client
                    .archive_repo(&project_id, &repo)
                    .await
                    .map(|t| t.trace_id)
            }
        })
        .await
    }

    #[napi]
    pub async fn restore_repo(&self, repo: String) -> napi::Result<String> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                client
                    .restore_repo(&project_id, &repo)
                    .await
                    .map(|t| t.trace_id)
            }
        })
        .await
    }

    #[napi]
    pub async fn repo_info(&self, repo: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                let traced = client.repo_info(&project_id, &repo).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn list_branches(&self, repo: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                let traced = client.list_branches(&project_id, &repo).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn list_refs(&self, repo: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                let traced = client.list_refs(&project_id, &repo).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn delete_branch(&self, repo: String, branch: String) -> napi::Result<String> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            let branch = branch.clone();
            async move {
                client
                    .delete_branch(&project_id, &repo, &branch)
                    .await
                    .map(|t| t.trace_id)
            }
        })
        .await
    }

    #[napi]
    pub async fn list_operations(&self, repo: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                let traced = client.list_operations(&project_id, &repo).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn git_credential(&self, repo: Option<String>) -> napi::Result<String> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            async move {
                let credential = match repo.as_deref() {
                    Some(repo) => client.git_credential_for_repo(&project_id, repo).await?,
                    None => client.git_credential_for_project(&project_id).await?,
                };
                serde_json::to_string(&credential).map_err(SdkError::from)
            }
        })
        .await
    }

    #[napi]
    pub async fn commit_status(&self, repo: String, job_id: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            let job_id = job_id.clone();
            async move {
                let credential = client.git_credential_for_repo(&project_id, &repo).await?;
                let traced = client
                    .commit_job_status(
                        &project_id,
                        &repo,
                        &credential.git_username,
                        &credential.token,
                        &job_id,
                    )
                    .await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn push_worktree(
        &self,
        repo: String,
        root: String,
        branch: String,
        message: String,
        expect_oid: Option<String>,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            let root = root.clone();
            let branch = branch.clone();
            let message = message.clone();
            let expect_oid = expect_oid.clone();
            async move {
                let credential = client.git_credential_for_repo(&project_id, &repo).await?;
                let opts = PushOptions {
                    branch,
                    message,
                    expect_oid,
                    ..Default::default()
                };
                let traced = client
                    .push_worktree(
                        &project_id,
                        &repo,
                        &credential.git_username,
                        &credential.token,
                        PathBuf::from(root),
                        opts,
                    )
                    .await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn merge_repo(
        &self,
        repo: String,
        ours: String,
        theirs: String,
        preflight: bool,
        deep: bool,
        materialize: bool,
        message: Option<String>,
        base: Option<String>,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            let ours = ours.clone();
            let theirs = theirs.clone();
            let message = message.clone();
            let base = base.clone();
            async move {
                let request = MergeRequest {
                    ours,
                    theirs,
                    base,
                    deep,
                    mode: (!preflight).then(|| "commit".to_string()),
                    policy: materialize.then(|| "materialize".to_string()),
                    message,
                    ..Default::default()
                };
                let traced = client.merge_repo(&project_id, &repo, &request).await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    #[napi]
    pub async fn commit_conflicts(&self, repo: String, commit: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let repo = repo.clone();
            let commit = commit.clone();
            async move {
                let traced = client
                    .get_commit_conflicts(&project_id, &repo, &commit)
                    .await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }
}
