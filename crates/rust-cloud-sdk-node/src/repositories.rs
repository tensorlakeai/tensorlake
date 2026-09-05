//! napi-rs bindings for Tensorlake Artifact Storage Git repositories.

use std::path::PathBuf;

use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::ingest::PushOptions;
use tensorlake::artifact_storage::merge::MergeRequest;
use tensorlake::artifact_storage::models::REPO_KIND_FILESYSTEM;
use tensorlake::artifact_storage::models::{
    NativeDirectFilePathWrite, NativeDirectFileWrite, NativeDirectPathTransfer,
};
use tensorlake::{
    ClientBuilder,
    error::{SdkError, TransportFailure},
};

use crate::sandbox::{
    TracedBytes, TracedJson, duration_from_seconds, into_napi_error, usage_error, with_retry,
};

/// One file write in a filesystem push.
#[napi(object)]
pub struct FilesystemFileWrite {
    /// Path inside the filesystem (forward-slash separated).
    pub path: String,
    pub content: Buffer,
}

/// One metadata-only path transfer in a filesystem publication.
#[napi(object)]
pub struct FilesystemPathTransfer {
    pub from: String,
    pub to: String,
}

/// One local file streamed into a filesystem publication.
#[napi(object)]
pub struct FilesystemFilePathWrite {
    pub path: String,
    pub local_path: String,
}

/// Whether a failed request may nonetheless have been processed server-side.
/// True for timeouts and gateway 5xx (the request was sent; the response was
/// lost or the gateway gave up), false for connect failures (the request was
/// never transmitted). Gates the 409/404-on-retry forgiveness in the
/// filesystem create/delete bindings.
fn request_may_have_executed(err: &SdkError) -> bool {
    if err.transport_failure() == Some(TransportFailure::Timeout) {
        return true;
    }
    match err {
        SdkError::ServerError { status, .. } => status.is_server_error(),
        _ => false,
    }
}

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

    async fn project_id(&self) -> napi::Result<String> {
        if let Some(project_id) = self.project_id.as_deref().filter(|value| !value.is_empty()) {
            return Ok(project_id.to_string());
        }
        self.client
            .resolve_authorized_project_id()
            .await
            .map_err(into_napi_error)
    }

    #[napi]
    pub async fn git_repo_url(&self, repo: String) -> napi::Result<String> {
        Ok(self.client.git_repo_url(&self.project_id().await?, &repo))
    }

    #[napi]
    pub async fn create_repo(
        &self,
        repo: String,
        default_branch: Option<String>,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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
        let project_id = self.project_id().await?;
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

    /// Create a filesystem (an artifact-storage repo of kind "filesystem").
    ///
    /// Returns JSON `{"trace_id", "default_branch"}` — the effective default
    /// branch differs from "main" only when a lost-response retry adopted a
    /// pre-existing filesystem.
    #[napi]
    pub async fn create_filesystem(&self, name: String) -> napi::Result<String> {
        let project_id = self.project_id().await?;
        let maybe_executed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            let maybe_executed = maybe_executed.clone();
            async move {
                // Minted before the forgiveness-tracked call: a mint failure
                // says nothing about whether a create reached the server, so
                // it must never arm the 409 forgiveness below.
                let credential = client.git_credential_for_project(&project_id).await?;
                match client
                    .create_repo_with_credential(
                        &project_id,
                        &name,
                        Some("main"),
                        Some(REPO_KIND_FILESYSTEM),
                        &credential.git_username,
                        &credential.token,
                    )
                    .await
                {
                    Ok(traced) => Ok(serde_json::to_string(&serde_json::json!({
                        "trace_id": traced.trace_id,
                        "default_branch": "main",
                    }))?),
                    // Forgive the conflict only when an earlier attempt may
                    // have reached the server (timeout / gateway 5xx after
                    // send): the 409 then means that attempt created it.
                    // After connect failures the request was never
                    // transmitted, so a 409 can only mean the repo
                    // pre-existed — surface it.
                    Err(SdkError::ServerError { status, .. })
                        if maybe_executed.load(std::sync::atomic::Ordering::SeqCst)
                            && status.as_u16() == 409 =>
                    {
                        // Even then, only accept the conflict as ours if the
                        // existing repo really is a filesystem; a same-named
                        // plain repository must stay an error, or later
                        // writes would land in the wrong repo.
                        let meta = client
                            .repo_meta_with_credential(
                                &project_id,
                                &name,
                                &credential.git_username,
                                &credential.token,
                            )
                            .await?;
                        if meta.is_filesystem() {
                            // Report the adopted filesystem's real default
                            // branch so the SDK handle never assumes "main".
                            Ok(serde_json::to_string(&serde_json::json!({
                                "trace_id": "",
                                "default_branch": meta.default_branch,
                            }))?)
                        } else {
                            Err(SdkError::ClientError(format!(
                                "a non-filesystem repo named {name} already exists"
                            )))
                        }
                    }
                    Err(e) => {
                        if request_may_have_executed(&e) {
                            maybe_executed.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                        Err(e)
                    }
                }
            }
        })
        .await
    }

    /// Create a metadata-only filesystem fork at a live head or retained snapshot.
    #[napi]
    pub async fn fork_filesystem(
        &self,
        name: String,
        base: String,
        snapshot: Option<String>,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id().await?;
        let report = self
            .client
            .fork_filesystem(&project_id, &name, &base, snapshot.as_deref())
            .await
            .map_err(into_napi_error)?;
        let json = serde_json::to_string(&serde_json::json!({
            "name": name,
            "base": base,
            "snapshot": snapshot,
            "default_branch": "main",
        }))
        .map_err(|error| into_napi_error(error.into()))?;
        Ok(TracedJson {
            trace_id: report.trace_id,
            json,
        })
    }

    /// List every filesystem in the project (all pages, cache-fenced).
    #[napi]
    pub async fn list_filesystems(&self) -> napi::Result<TracedJson> {
        let project_id = self.project_id().await?;
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            async move {
                let traced = client
                    .list_repos_of_kind(&project_id, Some(REPO_KIND_FILESYSTEM))
                    .await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    /// Point-read one filesystem's identity (name, status, kind, default branch).
    #[napi]
    pub async fn filesystem_meta(&self, name: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id().await?;
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            async move {
                // A project-scoped credential: repo-scoped mints can fail for a
                // repo that does not exist, which would mask the 404 callers
                // need to distinguish "no such filesystem".
                let credential = client.git_credential_for_project(&project_id).await?;
                let traced = client
                    .repo_meta_with_credential(
                        &project_id,
                        &name,
                        &credential.git_username,
                        &credential.token,
                    )
                    .await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    /// Delete a filesystem. Returns the trace id.
    #[napi]
    pub async fn delete_filesystem(&self, name: String) -> napi::Result<String> {
        let project_id = self.project_id().await?;
        let maybe_executed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            let maybe_executed = maybe_executed.clone();
            async move {
                // Minted before the forgiveness-tracked call: a mint failure
                // says nothing about whether a delete reached the server, so
                // it must never arm the 404 forgiveness below.
                let credential = client.git_credential_for_project(&project_id).await?;
                match client
                    .delete_repo_with_credential(
                        &project_id,
                        &name,
                        &credential.git_username,
                        &credential.token,
                    )
                    .await
                {
                    Ok(traced) => Ok(traced.trace_id),
                    // Forgive the 404 only when an earlier attempt may have
                    // reached the server (timeout / gateway 5xx after send):
                    // the repo is then gone because that attempt deleted it.
                    // After connect failures the request was never
                    // transmitted, so the 404 means the filesystem never
                    // existed — surface FilesystemNotFoundError.
                    Err(SdkError::ServerError { status, .. })
                        if maybe_executed.load(std::sync::atomic::Ordering::SeqCst)
                            && status.as_u16() == 404 =>
                    {
                        Ok(String::new())
                    }
                    Err(e) => {
                        if request_may_have_executed(&e) {
                            maybe_executed.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                        Err(e)
                    }
                }
            }
        })
        .await
    }

    /// One native head + movement generation for a filesystem.
    #[napi]
    pub async fn filesystem_ref_status(
        &self,
        name: String,
        refspec: String,
    ) -> napi::Result<TracedJson> {
        if !matches!(refspec.as_str(), "" | "main" | "refs/heads/main") {
            return Err(usage_error(
                "native filesystem status must target the main head".to_string(),
            ));
        }
        let project_id = self.project_id().await?;
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            async move {
                let status = client.native_filesystem_head(&project_id, &name).await?;
                let json = serde_json::to_string(&serde_json::json!({
                    "resolved_commit": status.snapshot_id,
                    "generation": status.generation,
                }))?;
                Ok(TracedJson {
                    trace_id: status.trace_id.clone(),
                    json,
                })
            }
        })
        .await
    }

    /// Pin the current native head as a permanent snapshot without copying content.
    #[napi]
    pub async fn retain_filesystem_snapshot(
        &self,
        name: String,
        message: String,
        request_id: String,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id().await?;
        let report = self
            .client
            .retain_current_native_filesystem_snapshot(&project_id, &name, message, request_id)
            .await
            .map_err(into_napi_error)?;
        let trace_id = report.trace_id.clone();
        let json =
            serde_json::to_string(&*report).map_err(|error| into_napi_error(error.into()))?;
        Ok(TracedJson { trace_id, json })
    }

    #[napi]
    pub async fn list_filesystem_snapshots(&self, name: String) -> napi::Result<TracedJson> {
        let project_id = self.project_id().await?;
        let report = self
            .client
            .list_native_filesystem_snapshots(&project_id, &name)
            .await
            .map_err(into_napi_error)?;
        let trace_id = report.trace_id.clone();
        let json = serde_json::to_string(&serde_json::json!({ "snapshots": &*report }))
            .map_err(|error| into_napi_error(error.into()))?;
        Ok(TracedJson { trace_id, json })
    }

    #[napi]
    pub async fn delete_filesystem_snapshot(
        &self,
        name: String,
        snapshot: String,
    ) -> napi::Result<String> {
        let project_id = self.project_id().await?;
        self.client
            .delete_native_filesystem_snapshot(&project_id, &name, &snapshot)
            .await
            .map(|report| report.trace_id)
            .map_err(into_napi_error)
    }

    /// Raw native file bytes at the current head or one snapshot.
    #[napi]
    pub async fn read_filesystem_file(
        &self,
        name: String,
        path: String,
        version: String,
        offset: Option<f64>,
        length: Option<f64>,
    ) -> napi::Result<TracedBytes> {
        let range = match (offset, length) {
            (None, None) => None,
            (Some(offset), Some(length))
                if offset.is_finite()
                    && length.is_finite()
                    && offset >= 0.0
                    && length > 0.0
                    && offset.fract() == 0.0
                    && length.fract() == 0.0
                    && offset <= u64::MAX as f64
                    && length <= u64::MAX as f64 =>
            {
                Some((offset as u64, length as u64))
            }
            _ => {
                return Err(usage_error(
                    "filesystem range requires a non-negative integer offset and positive integer length"
                        .to_string(),
                ));
            }
        };
        let project_id = self.project_id().await?;
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            let path = path.clone();
            let version = version.clone();
            async move {
                let read = client
                    .read_native_filesystem_file_with_metadata(
                        &project_id,
                        &name,
                        &path,
                        &version,
                        range,
                    )
                    .await?;
                let trace_id = read.trace_id.clone();
                let read = read.into_inner();
                Ok(TracedBytes {
                    trace_id,
                    data: Buffer::from(read.data),
                    content_id: Some(read.content_id),
                    full_size: Some(read.full_size as f64),
                })
            }
        })
        .await
    }

    /// One native directory's full listing at the current head or one snapshot.
    #[napi]
    pub async fn list_filesystem_tree(
        &self,
        name: String,
        dir_path: String,
        version: String,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id().await?;
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            let dir_path = dir_path.clone();
            let version = version.clone();
            async move {
                let mut entries = Vec::new();
                let mut after = None;
                let mut seen = std::collections::HashSet::new();
                loop {
                    let page = client
                        .list_native_filesystem_entries_page(
                            &project_id,
                            &name,
                            &dir_path,
                            &version,
                            after.as_deref(),
                            1000,
                        )
                        .await?;
                    let trace_id = page.trace_id.clone();
                    let page = page.into_inner();
                    entries.extend(page.entries);
                    if !page.truncated {
                        let json =
                            serde_json::to_string(&serde_json::json!({ "entries": entries }))?;
                        return Ok(TracedJson { trace_id, json });
                    }
                    match page.next_after {
                        Some(next) if !next.is_empty() && seen.insert(next.clone()) => {
                            after = Some(next);
                        }
                        _ => {
                            return Err(SdkError::ClientError(
                                "directory listing truncated without a fresh pagination cursor"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        })
        .await
    }

    /// Write `files` and delete `deletes` in one native snapshot publication.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn push_filesystem_files(
        &self,
        name: String,
        files: Vec<FilesystemFileWrite>,
        deletes: Vec<String>,
        moves: Vec<FilesystemPathTransfer>,
        copies: Vec<FilesystemPathTransfer>,
        message: String,
        branch: String,
        idempotency_key: Option<String>,
    ) -> napi::Result<TracedJson> {
        if !matches!(branch.as_str(), "" | "main" | "refs/heads/main") {
            return Err(usage_error(
                "native filesystem writes must target the main head".to_string(),
            ));
        }
        let project_id = self.project_id().await?;
        let client = self.client.clone();
        let result: Result<TracedJson, SdkError> = async move {
            let operation_id =
                idempotency_key.unwrap_or_else(|| format!("sdk-{}", uuid::Uuid::new_v4()));
            let writes = files
                .into_iter()
                .map(|file| NativeDirectFileWrite {
                    path: file.path,
                    data: file.content.to_vec(),
                })
                .collect();
            let moves = moves
                .into_iter()
                .map(|transfer| NativeDirectPathTransfer {
                    from: transfer.from,
                    to: transfer.to,
                })
                .collect();
            let copies = copies
                .into_iter()
                .map(|transfer| NativeDirectPathTransfer {
                    from: transfer.from,
                    to: transfer.to,
                })
                .collect();
            let report = client
                .publish_filesystem_files(
                    &project_id,
                    &name,
                    writes,
                    deletes,
                    moves,
                    copies,
                    message,
                    operation_id,
                )
                .await?;
            let trace_id = report.trace_id.clone();
            let json = serde_json::to_string(&serde_json::json!({
                "version_id": report.version_id,
                "previous_version_id": report.previous_version_id,
            }))?;
            Ok(TracedJson { trace_id, json })
        }
        .await;
        result.map_err(into_napi_error)
    }

    /// Stream local files through bounded client-to-object-store parts.
    #[napi]
    pub async fn push_filesystem_paths(
        &self,
        name: String,
        files: Vec<FilesystemFilePathWrite>,
        message: String,
        branch: String,
        idempotency_key: Option<String>,
    ) -> napi::Result<TracedJson> {
        if !matches!(branch.as_str(), "" | "main" | "refs/heads/main") {
            return Err(usage_error(
                "native filesystem writes must target the main head".to_string(),
            ));
        }
        let project_id = self.project_id().await?;
        let operation_id =
            idempotency_key.unwrap_or_else(|| format!("sdk-{}", uuid::Uuid::new_v4()));
        let writes = files
            .into_iter()
            .map(|file| NativeDirectFilePathWrite {
                path: file.path,
                source_path: PathBuf::from(file.local_path),
            })
            .collect();
        let report = self
            .client
            .publish_filesystem_paths(&project_id, &name, writes, message, operation_id)
            .await
            .map_err(into_napi_error)?;
        let trace_id = report.trace_id.clone();
        let json = serde_json::to_string(&serde_json::json!({
            "version_id": report.version_id,
            "previous_version_id": report.previous_version_id,
        }))
        .map_err(|error| into_napi_error(error.into()))?;
        Ok(TracedJson { trace_id, json })
    }
}
