//! napi-rs bindings for Tensorlake Artifact Storage Git repositories.

use std::path::PathBuf;

use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::ingest::{PushFile, PushOptions, PushSource};
use tensorlake::artifact_storage::merge::MergeRequest;
use tensorlake::artifact_storage::models::REPO_KIND_FILESYSTEM;
use tensorlake::{ClientBuilder, error::SdkError};

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

/// Whether a failed request may nonetheless have been processed server-side.
/// True for timeouts and gateway 5xx (the request was sent; the response was
/// lost or the gateway gave up), false for connect failures (the request was
/// never transmitted). Gates the 409/404-on-retry forgiveness in the
/// filesystem create/delete bindings.
fn request_may_have_executed(err: &SdkError) -> bool {
    match err {
        SdkError::ServerError { status, .. } => status.is_server_error(),
        SdkError::Http(e) => e.is_timeout(),
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

    /// Create a filesystem (an artifact-storage repo of kind "filesystem").
    ///
    /// Returns JSON `{"trace_id", "default_branch"}` — the effective default
    /// branch differs from "main" only when a lost-response retry adopted a
    /// pre-existing filesystem.
    #[napi]
    pub async fn create_filesystem(&self, name: String) -> napi::Result<String> {
        let project_id = self.project_id()?.to_string();
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

    /// List every filesystem in the project (all pages, cache-fenced).
    #[napi]
    pub async fn list_filesystems(&self) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
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
        let project_id = self.project_id()?.to_string();
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
        let project_id = self.project_id()?.to_string();
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

    /// One ref's head + movement generation for a filesystem branch.
    #[napi]
    pub async fn filesystem_ref_status(
        &self,
        name: String,
        refspec: String,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            let refspec = refspec.clone();
            async move {
                let credential = client.git_credential_for_repo(&project_id, &name).await?;
                let traced = client
                    .ref_status(
                        &project_id,
                        &name,
                        &credential.git_username,
                        &credential.token,
                        &refspec,
                    )
                    .await?;
                let trace_id = traced.trace_id.clone();
                let json = serde_json::to_string(&*traced)?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    /// Raw file bytes at `version` (branch, ref, or commit).
    #[napi]
    pub async fn read_filesystem_file(
        &self,
        name: String,
        path: String,
        version: String,
    ) -> napi::Result<TracedBytes> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            let path = path.clone();
            let version = version.clone();
            async move {
                let credential = client.git_credential_for_repo(&project_id, &name).await?;
                let traced = client
                    .get_file_bytes(
                        &project_id,
                        &name,
                        &credential.git_username,
                        &credential.token,
                        &version,
                        &path,
                    )
                    .await?;
                let trace_id = traced.trace_id.clone();
                let data = Buffer::from(traced.into_inner());
                Ok(TracedBytes { trace_id, data })
            }
        })
        .await
    }

    /// One directory's full listing at `version` (all pages), as
    /// `{"entries": [...]}`.
    #[napi]
    pub async fn list_filesystem_tree(
        &self,
        name: String,
        dir_path: String,
        version: String,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        with_retry(self.client.clone(), 5, move |client| {
            let project_id = project_id.clone();
            let name = name.clone();
            let dir_path = dir_path.clone();
            let version = version.clone();
            async move {
                let credential = client.git_credential_for_repo(&project_id, &name).await?;
                let mut entries = Vec::new();
                let mut after: Option<String> = None;
                let mut seen = std::collections::HashSet::new();
                let mut trace_id;
                loop {
                    let traced = client
                        .list_tree_page(
                            &project_id,
                            &name,
                            &credential.git_username,
                            &credential.token,
                            &version,
                            &dir_path,
                            after.as_deref(),
                            1000,
                        )
                        .await?;
                    trace_id = traced.trace_id.clone();
                    let page = traced.into_inner();
                    entries.extend(page.entries);
                    if !page.truncated {
                        break;
                    }
                    // A truncated page must carry a fresh cursor; anything else
                    // would silently drop entries or loop forever.
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
                let json = serde_json::to_string(&serde_json::json!({ "entries": entries }))?;
                Ok(TracedJson { trace_id, json })
            }
        })
        .await
    }

    /// Write `files` and delete `deletes` in one atomic commit on `branch`.
    ///
    /// `idempotency_key` must be stable for one logical write: it is what
    /// makes a retried submit reattach to the same durable commit job.
    #[napi]
    pub async fn push_filesystem_files(
        &self,
        name: String,
        files: Vec<FilesystemFileWrite>,
        deletes: Vec<String>,
        message: String,
        branch: String,
        idempotency_key: Option<String>,
    ) -> napi::Result<TracedJson> {
        let project_id = self.project_id()?.to_string();
        // Single-shot on purpose: `push_files` already retries every step
        // internally (idempotent chunk uploads, commit reattachment through
        // the caller's stable idempotency key), and an outer whole-push retry
        // would force a full deep clone of the payload per attempt.
        let client = self.client.clone();
        let result: Result<TracedJson, SdkError> = async move {
            let credential = client.git_credential_for_repo(&project_id, &name).await?;
            let push_files: Vec<PushFile> = files
                .into_iter()
                .map(|file| PushFile {
                    repo_path: file.path,
                    source: PushSource::Bytes(file.content.to_vec()),
                    mode: None,
                    delete: false,
                })
                .chain(deletes.into_iter().map(|repo_path| PushFile {
                    repo_path,
                    source: PushSource::Bytes(Vec::new()),
                    mode: None,
                    delete: true,
                }))
                .collect();
            let opts = PushOptions {
                branch,
                message,
                idempotency_key,
                ..Default::default()
            };
            let traced = client
                .push_files(
                    &project_id,
                    &name,
                    &credential.git_username,
                    &credential.token,
                    push_files,
                    opts,
                )
                .await?;
            let trace_id = traced.trace_id.clone();
            let json = serde_json::to_string(&*traced)?;
            Ok(TracedJson { trace_id, json })
        }
        .await;
        result.map_err(into_napi_error)
    }
}
