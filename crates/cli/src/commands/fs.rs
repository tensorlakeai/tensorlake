//! `tl fs` — versioned filesystem workspaces on artifact storage.
//!
//! Product model (artifact_storage issue #24): a *file system* is an artifact-storage repo; a
//! *mount* is a workspace — a private leased ref created from a base commit, materialized into a
//! local directory. Work happens on plain local files; `snapshot` seals local changes into a
//! commit on the workspace ref (transferring only chunks the server lacks); `promote`
//! CAS-advances a real branch to the snapshot (squash by default). The activity lease is re-armed
//! by every snapshot and by the heartbeat each `tl fs` command sends, and an unpinned workspace
//! whose lease lapses is reaped server-side.

use std::path::Path;
use std::sync::Arc;

use comfy_table::Cell;
use console::style;
use futures::StreamExt;
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::ingest::{PushEvent, PushFile, PushOptions, PushSource};
use tensorlake::artifact_storage::models::GitCredential;
use tensorlake::artifact_storage::workspaces::{
    CreateWorkspaceRequest, PromoteWorkspaceRequest, TreeEntry, WorkspaceInfo,
};

use crate::auth::context::CliContext;
use crate::commands::git::{artifact_storage_client, project_id};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub mod local;

use local::{Change, Manifest, WorkspaceState};

const MATERIALIZE_CONCURRENCY: usize = 16;

struct FsSession {
    client: ArtifactStorageClient,
    project_id: String,
    credential: GitCredential,
}

impl FsSession {
    async fn open(ctx: &CliContext, repo: Option<&str>) -> Result<FsSession> {
        let client = artifact_storage_client(ctx)?;
        let project_id = project_id(ctx)?;
        // Self-hosted / dev affordance: a pre-provisioned git credential skips platform token
        // minting entirely (e.g. against a local artifact-storage server in open-auth mode).
        if let Ok(token) = std::env::var("TENSORLAKE_GIT_TOKEN") {
            let username =
                std::env::var("TENSORLAKE_GIT_USERNAME").unwrap_or_else(|_| "t".to_string());
            return Ok(FsSession {
                client,
                project_id,
                credential: GitCredential {
                    token,
                    token_type: "bearer".to_string(),
                    expires_at: String::new(),
                    git_username: username,
                    repo_pattern: "*".to_string(),
                    scopes: Vec::new(),
                },
            });
        }
        // Minted tokens are short-lived but not per-command: cache them under the CLI's global
        // config dir (same convention as the PAT) so each `tl fs` invocation doesn't pay a
        // platform mint round trip.
        let scope = repo.unwrap_or("*");
        if let Some((username, token, expires_at)) =
            crate::config::files::load_git_credential(&ctx.api_url, &project_id, scope)
        {
            return Ok(FsSession {
                client,
                project_id,
                credential: GitCredential {
                    token,
                    token_type: "bearer".to_string(),
                    expires_at,
                    git_username: username,
                    repo_pattern: scope.to_string(),
                    scopes: Vec::new(),
                },
            });
        }
        let credential = client
            .mint_token_for_repo(&project_id, repo)
            .await?
            .into_inner();
        if let Err(e) = crate::config::files::save_git_credential(
            &ctx.api_url,
            &project_id,
            scope,
            &credential.git_username,
            &credential.token,
            &credential.expires_at,
        ) {
            eprintln!("warning: could not cache git credential: {e}");
        }
        Ok(FsSession {
            client,
            project_id,
            credential,
        })
    }

    fn creds(&self) -> (&str, &str) {
        (&self.credential.git_username, &self.credential.token)
    }
}

// ---------------------------------------------------------------------------------------------
// Registry: a file system is an artifact-storage repo.
// ---------------------------------------------------------------------------------------------

pub async fn create(ctx: &CliContext, name: &str, output_json: bool) -> Result<()> {
    let session = FsSession::open(ctx, None).await?;
    let (user, token) = session.creds();
    session
        .client
        .create_repo_with_credential(&session.project_id, name, None, user, token)
        .await?;
    // Seed an initial empty commit so the file system is immediately mountable: a workspace
    // needs a base commit to exist.
    session
        .client
        .push_files(
            &session.project_id,
            name,
            user,
            token,
            Vec::new(),
            PushOptions {
                message: "Initialize file system".to_string(),
                ..Default::default()
            },
        )
        .await?;
    if output_json {
        println!("{}", serde_json::json!({ "name": name }));
    } else {
        println!("Created file system '{name}'.");
        println!("Mount it with: tl fs mount {name} ./work");
    }
    Ok(())
}

pub async fn list(ctx: &CliContext, output_json: bool) -> Result<()> {
    let session = FsSession::open(ctx, None).await?;
    let (user, token) = session.creds();
    let repos = session
        .client
        .list_repos_with_credential(&session.project_id, user, token)
        .await?
        .into_inner();
    if output_json {
        println!("{}", serde_json::to_string_pretty(&repos)?);
        return Ok(());
    }
    if repos.repos.is_empty() {
        println!("No file systems found.");
        return Ok(());
    }
    let mut table = new_table(&["Name", "Default branch", "Status"]);
    for repo in &repos.repos {
        table.add_row(vec![
            Cell::new(&repo.name),
            Cell::new(&repo.default_branch),
            Cell::new(&repo.status),
        ]);
    }
    println!("{table}");
    Ok(())
}

pub async fn remove(ctx: &CliContext, name: &str) -> Result<()> {
    let session = FsSession::open(ctx, Some(name)).await?;
    let (user, token) = session.creds();
    session
        .client
        .delete_repo_with_credential(&session.project_id, name, user, token)
        .await?;
    println!("Deleted file system '{name}'.");
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Mount: create a workspace and materialize it.
// ---------------------------------------------------------------------------------------------

/// `tl fs mount <file-system>[:<ref-or-commit>] <path>`
pub async fn mount(
    ctx: &CliContext,
    target: &str,
    path: &Path,
    lease_seconds: Option<u64>,
) -> Result<()> {
    let (repo, base) = match target.split_once(':') {
        Some((repo, base)) => (repo, Some(base.to_string())),
        None => (target, None),
    };
    if path.exists()
        && path
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(true)
    {
        return Err(CliError::usage(format!(
            "{} already exists and is not empty",
            path.display()
        )));
    }
    let session = FsSession::open(ctx, Some(repo)).await?;
    let (user, token) = session.creds();

    // The workspace pins the base against GC and validates it server-side before any transfer.
    let ws = session
        .client
        .create_workspace(
            &session.project_id,
            repo,
            user,
            token,
            &CreateWorkspaceRequest {
                base: base.clone(),
                lease_seconds,
            },
        )
        .await?
        .into_inner();

    std::fs::create_dir_all(path)?;
    let manifest = if base.is_none() {
        // Default-branch mount: fast-clone (pack download, local decode) then drop `.git`.
        materialize_fastclone(&session, repo, path, &ws).await?
    } else {
        // Pinned to an explicit ref/commit: differential tree-walk materialize.
        materialize_tree_walk(&session, repo, &ws.base, path, &Manifest::new()).await?
    };

    let state = WorkspaceState {
        project_id: session.project_id.clone(),
        repo: repo.to_string(),
        workspace_id: ws.id.clone(),
        ref_name: ws.ref_name.clone(),
        base_commit: ws.base.clone(),
    };
    local::save_state(path, &state)?;
    local::save_manifest(path, &manifest)?;
    println!(
        "Mounted {}:{} at {} (workspace {}, {} files, {})",
        repo,
        ws.base_ref.as_deref().unwrap_or(&ws.base[..12]),
        path.display(),
        short_id(&ws.id),
        manifest.len(),
        lease_display(&ws),
    );
    println!("Work locally, then: tl fs snapshot {}", path.display());
    Ok(())
}

async fn materialize_fastclone(
    session: &FsSession,
    repo: &str,
    path: &Path,
    ws: &WorkspaceInfo,
) -> Result<Manifest> {
    use crate::commands::git::fastclone::{self, BasicAuth, FastCloneOptions};
    let url = session.client.git_repo_url(&session.project_id, repo);
    let progress = fastclone::new_spinner("fetching");
    fastclone::fast_clone(FastCloneOptions {
        repo_url: url,
        dest: path.to_path_buf(),
        cache_dir: None,
        cache_max_bytes: None,
        credential: Some(BasicAuth {
            username: session.credential.git_username.clone(),
            password: Some(session.credential.token.clone()),
        }),
        checkout: false,
        progress,
    })
    .await?;
    // Check out the exact commit the workspace was created at: the clone fetched the branch
    // head, which can already have moved past the workspace base under concurrent pushes.
    let co = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["checkout", "-q", &ws.base])
        .status()?;
    if !co.success() {
        return Err(CliError::usage("materialize checkout failed"));
    }
    let manifest = local::manifest_from_git_checkout(path)?;
    std::fs::remove_dir_all(path.join(".git"))?;
    Ok(manifest)
}

/// Materialize `version` into `path` differentially against `have`: walk the remote tree
/// (paged, concurrent), fetch entries whose oid differs, delete tracked paths that vanished.
/// Returns the new manifest.
async fn materialize_tree_walk(
    session: &FsSession,
    repo: &str,
    version: &str,
    path: &Path,
    have: &Manifest,
) -> Result<Manifest> {
    let entries = walk_remote_tree(session, repo, version).await?;
    let (user, token) = session.creds();

    let mut manifest = Manifest::new();
    let mut to_fetch: Vec<(String, u32, String)> = Vec::new();
    for (file_path, entry) in &entries {
        match have.get(file_path) {
            Some(existing) if existing.oid == entry.oid && existing.mode == entry.mode => {
                manifest.insert(file_path.clone(), existing.clone());
            }
            _ => to_fetch.push((file_path.clone(), entry.mode, entry.oid.clone())),
        }
    }

    type Fetched = (String, u32, String, Vec<u8>);
    let fetched: Vec<Result<Fetched>> =
        futures::stream::iter(to_fetch.into_iter().map(|(file_path, mode, oid)| {
            let client = session.client.clone();
            let (project, repo, user, token, version) = (
                session.project_id.clone(),
                repo.to_string(),
                user.to_string(),
                token.to_string(),
                version.to_string(),
            );
            async move {
                let bytes = client
                    .get_file_bytes(&project, &repo, &user, &token, &version, &file_path)
                    .await?
                    .into_inner();
                Ok((file_path, mode, oid, bytes))
            }
        }))
        .buffer_unordered(MATERIALIZE_CONCURRENCY)
        .collect()
        .await;
    for item in fetched {
        let (file_path, mode, oid, bytes) = item?;
        let entry = local::write_entry(path, &file_path, mode, &oid, &bytes)?;
        manifest.insert(file_path, entry);
    }

    // Tracked paths that no longer exist at `version` go away locally too.
    for stale in have.keys().filter(|p| !entries.contains_key(*p)) {
        let _ = std::fs::remove_file(path.join(stale));
    }
    Ok(manifest)
}

/// Full recursive listing of `version`: repo path -> entry. Directories are traversed
/// concurrently; each directory is paged through `next_after`.
async fn walk_remote_tree(
    session: &FsSession,
    repo: &str,
    version: &str,
) -> Result<std::collections::BTreeMap<String, TreeEntry>> {
    let (user, token) = session.creds();
    let mut out = std::collections::BTreeMap::new();
    let mut pending: Vec<String> = vec![String::new()];
    while !pending.is_empty() {
        let batch: Vec<String> = std::mem::take(&mut pending);
        let pages: Vec<Result<(String, Vec<TreeEntry>)>> =
            futures::stream::iter(batch.into_iter().map(|dir| {
                let client = session.client.clone();
                let (project, repo, user, token, version) = (
                    session.project_id.clone(),
                    repo.to_string(),
                    user.to_string(),
                    token.to_string(),
                    version.to_string(),
                );
                async move {
                    let mut entries = Vec::new();
                    let mut after: Option<String> = None;
                    loop {
                        let page = client
                            .list_tree_page(
                                &project,
                                &repo,
                                &user,
                                &token,
                                &version,
                                &dir,
                                after.as_deref(),
                                2000,
                            )
                            .await?
                            .into_inner();
                        entries.extend(page.entries);
                        if !page.truncated {
                            break;
                        }
                        after = page.next_after;
                    }
                    Ok((dir, entries))
                }
            }))
            .buffer_unordered(MATERIALIZE_CONCURRENCY)
            .collect()
            .await;
        for page in pages {
            let (dir, entries) = page?;
            for entry in entries {
                let full = if dir.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{dir}/{}", entry.name)
                };
                if entry.mode == 0o40000 {
                    pending.push(full);
                } else {
                    out.insert(full, entry);
                }
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Snapshot / promote / status / restore / diff / unmount / pin / workspaces
// ---------------------------------------------------------------------------------------------

pub async fn snapshot(ctx: &CliContext, path: &Path, message: Option<&str>) -> Result<()> {
    let state = local::load_state(path)?;
    let mut manifest = local::load_manifest(path)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;

    let changes = local::scan_dirty(path, &manifest)?;
    if changes.is_empty() {
        println!("Nothing to snapshot: workspace is clean.");
        return Ok(());
    }

    let mut files = Vec::with_capacity(changes.len());
    for change in &changes {
        match change {
            Change::Upsert {
                path: p, abs, mode, ..
            } => {
                // A symlink's blob content is its target path; reading through `abs` would
                // upload the target file's bytes instead.
                let source = if *mode == 0o120000 {
                    PushSource::Bytes(
                        std::fs::read_link(abs)?
                            .to_string_lossy()
                            .into_owned()
                            .into_bytes(),
                    )
                } else {
                    PushSource::Path(abs.clone())
                };
                files.push(PushFile {
                    repo_path: p.clone(),
                    source,
                    mode: Some(*mode),
                    delete: false,
                });
            }
            Change::Delete { path: p } => files.push(PushFile {
                repo_path: p.clone(),
                source: PushSource::Bytes(Vec::new()),
                mode: None,
                delete: true,
            }),
        }
    }
    let (user, token) = session.creds();
    let progress: Arc<dyn Fn(PushEvent) + Send + Sync> = Arc::new(|ev| {
        if let PushEvent::Negotiated { missing, total } = ev {
            eprintln!("uploading {missing} of {total} chunks (rest already stored)");
        }
    });
    let report = session
        .client
        .push_files(
            &session.project_id,
            &state.repo,
            user,
            token,
            files,
            PushOptions {
                message: message.unwrap_or("tl fs snapshot").to_string(),
                workspace_snapshot: Some(state.workspace_id.clone()),
                progress: Some(progress),
                ..Default::default()
            },
        )
        .await?
        .into_inner();

    // Fold the observed changes into the manifest with the freshness we hashed at. New files
    // skipped the pre-hash read; their oids come from the push's single chunk pass.
    let pushed_oids: std::collections::HashMap<&str, &str> = report
        .file_blob_oids
        .iter()
        .map(|(p, o)| (p.as_str(), o.as_str()))
        .collect();
    for change in changes {
        match change {
            Change::Upsert {
                path: p,
                mode,
                oid,
                size,
                mtime_ms,
                ..
            } => {
                let oid = match oid {
                    Some(oid) => oid,
                    None => pushed_oids
                        .get(p.as_str())
                        .map(|o| o.to_string())
                        .unwrap_or_default(),
                };
                manifest.insert(
                    p,
                    local::ManifestEntry {
                        oid,
                        mode,
                        size,
                        mtime_ms,
                    },
                );
            }
            Change::Delete { path: p } => {
                manifest.remove(&p);
            }
        }
    }
    local::save_manifest(path, &manifest)?;
    let mut state = state;
    state.base_commit = report.commit.clone();
    local::save_state(path, &state)?;
    println!(
        "Snapshot {} ({} file(s), {} of {} chunks uploaded)",
        report.commit, report.files, report.chunks_uploaded, report.chunks_total,
    );
    Ok(())
}

pub async fn promote(
    ctx: &CliContext,
    path: &Path,
    branch: &str,
    full_history: bool,
    message: Option<&str>,
) -> Result<()> {
    let state = local::load_state(path)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;
    let dirty = local::scan_dirty(path, &local::load_manifest(path)?)?;
    if !dirty.is_empty() {
        eprintln!(
            "{} {} local change(s) not in any snapshot; promoting the last snapshot only. Run `tl fs snapshot` first to include them.",
            style("note:").yellow(),
            dirty.len()
        );
    }
    let (user, token) = session.creds();
    let request = PromoteWorkspaceRequest {
        branch: branch.to_string(),
        expect_oid: None,
        full_history,
        message: message.map(str::to_string),
    };
    // A squash promote reads the snapshot's commit-index row, which materializes asynchronously
    // after the snapshot publishes; a promote issued right behind a snapshot can land in that
    // window. The server signals it with 425 Too Early — poll it out.
    let resp = {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match session
                .client
                .workspace_promote(
                    &session.project_id,
                    &state.repo,
                    user,
                    token,
                    &state.workspace_id,
                    &request,
                )
                .await
            {
                Ok(resp) => break resp.into_inner(),
                Err(tensorlake::error::SdkError::ServerError { status, message })
                    if status.as_u16() == 425 && std::time::Instant::now() < deadline =>
                {
                    let _ = message;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    };
    println!(
        "Promoted workspace {} -> {} at {}{}",
        short_id(&state.workspace_id),
        resp.ref_name,
        resp.commit,
        if resp.squashed {
            " (squashed)"
        } else {
            " (full history)"
        },
    );
    Ok(())
}

pub async fn status(ctx: &CliContext, path: &Path, output_json: bool) -> Result<()> {
    let state = local::load_state(path)?;
    let manifest = local::load_manifest(path)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    let (user, token) = session.creds();
    let ws = session
        .client
        .get_workspace(
            &session.project_id,
            &state.repo,
            user,
            token,
            &state.workspace_id,
        )
        .await?
        .into_inner();
    // Reading status is activity too: keep the lease alive for whoever is looking.
    let _ = heartbeat(&session, &state).await;
    let changes = local::scan_dirty(path, &manifest)?;

    if output_json {
        let dirty: Vec<_> = changes.iter().map(|c| c.path().to_string()).collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "workspace": ws,
                "base_commit": state.base_commit,
                "tracked_files": manifest.len(),
                "dirty": dirty,
            }))?
        );
        return Ok(());
    }
    println!("{} {}", style("file system:").dim(), state.repo);
    println!(
        "{} {} ({})",
        style("workspace:").dim(),
        short_id(&ws.id),
        lease_display(&ws)
    );
    println!("{} {}", style("snapshot:").dim(), state.base_commit);
    println!("{} {} file(s)", style("tracked:").dim(), manifest.len());
    if changes.is_empty() {
        println!("{} clean", style("local:").dim());
    } else {
        println!("{} {} change(s):", style("local:").dim(), changes.len());
        for change in changes.iter().take(20) {
            let tag = match change {
                Change::Upsert { .. } => style("M").yellow(),
                Change::Delete { .. } => style("D").red(),
            };
            println!("  {tag} {}", change.path());
        }
        if changes.len() > 20 {
            println!("  … and {} more", changes.len() - 20);
        }
    }
    Ok(())
}

pub async fn restore(ctx: &CliContext, path: &Path, version: &str) -> Result<()> {
    let state = local::load_state(path)?;
    let manifest = local::load_manifest(path)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;
    let new_manifest =
        materialize_tree_walk(&session, &state.repo, version, path, &manifest).await?;
    let restored = new_manifest.len();
    local::save_manifest(path, &new_manifest)?;
    let mut state = state;
    state.base_commit = version.to_string();
    local::save_state(path, &state)?;
    println!(
        "Restored {} to {} ({restored} file(s) tracked).",
        path.display(),
        &version[..version.len().min(12)]
    );
    Ok(())
}

/// `tl fs diff <path>` — local changes vs the last snapshot; `tl fs diff <path> <a> <b>` —
/// server-side tree diff between two commits/refs.
pub async fn diff(ctx: &CliContext, path: &Path, a: Option<&str>, b: Option<&str>) -> Result<()> {
    let state = local::load_state(path)?;
    match (a, b) {
        (None, None) => {
            let manifest = local::load_manifest(path)?;
            for change in local::scan_dirty(path, &manifest)? {
                let tag = match &change {
                    Change::Upsert { path: p, .. } if manifest.contains_key(p) => "M",
                    Change::Upsert { .. } => "A",
                    Change::Delete { .. } => "D",
                };
                println!("{tag} {}", change.path());
            }
            Ok(())
        }
        (Some(a), Some(b)) => {
            let session = FsSession::open(ctx, Some(&state.repo)).await?;
            let left = walk_remote_tree(&session, &state.repo, a).await?;
            let right = walk_remote_tree(&session, &state.repo, b).await?;
            for (p, entry) in &right {
                match left.get(p) {
                    None => println!("A {p}"),
                    Some(prev) if prev.oid != entry.oid || prev.mode != entry.mode => {
                        println!("M {p}")
                    }
                    Some(_) => {}
                }
            }
            for p in left.keys().filter(|p| !right.contains_key(*p)) {
                println!("D {p}");
            }
            Ok(())
        }
        _ => Err(CliError::usage(
            "diff takes no versions (local vs snapshot) or two (snapshot vs snapshot)",
        )),
    }
}

pub async fn pin(ctx: &CliContext, path: &Path) -> Result<()> {
    let state = local::load_state(path)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    let (user, token) = session.creds();
    session
        .client
        .workspace_pin(
            &session.project_id,
            &state.repo,
            user,
            token,
            &state.workspace_id,
        )
        .await?;
    println!(
        "Pinned workspace {}: it will not expire until deleted.",
        short_id(&state.workspace_id)
    );
    Ok(())
}

/// Unmount: end the workspace (unless `--keep-workspace`) and drop local `.tlfs` state. The
/// working files stay on disk.
pub async fn unmount(ctx: &CliContext, path: &Path, keep_workspace: bool) -> Result<()> {
    let state = local::load_state(path)?;
    if !keep_workspace {
        let session = FsSession::open(ctx, Some(&state.repo)).await?;
        let (user, token) = session.creds();
        session
            .client
            .delete_workspace(
                &session.project_id,
                &state.repo,
                user,
                token,
                &state.workspace_id,
            )
            .await?;
    }
    std::fs::remove_dir_all(local::state_dir(path))?;
    println!(
        "Unmounted {} ({}).",
        path.display(),
        if keep_workspace {
            "workspace kept; its lease keeps ticking".to_string()
        } else {
            format!("workspace {} deleted", short_id(&state.workspace_id))
        }
    );
    Ok(())
}

pub async fn workspaces(ctx: &CliContext, file_system: &str, output_json: bool) -> Result<()> {
    let session = FsSession::open(ctx, Some(file_system)).await?;
    let (user, token) = session.creds();
    let list = session
        .client
        .list_workspaces(&session.project_id, file_system, user, token)
        .await?
        .into_inner();
    if output_json {
        println!("{}", serde_json::to_string_pretty(&list)?);
        return Ok(());
    }
    if list.is_empty() {
        println!("No workspaces.");
        return Ok(());
    }
    let mut table = new_table(&["Workspace", "Base", "Head", "Lease"]);
    for ws in &list {
        table.add_row(vec![
            Cell::new(short_id(&ws.id)),
            Cell::new(&ws.base[..12]),
            Cell::new(&ws.head[..12]),
            Cell::new(lease_display(ws)),
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn heartbeat(session: &FsSession, state: &WorkspaceState) -> Result<()> {
    let (user, token) = session.creds();
    session
        .client
        .workspace_heartbeat(
            &session.project_id,
            &state.repo,
            user,
            token,
            &state.workspace_id,
        )
        .await?;
    Ok(())
}

fn short_id(id: &str) -> &str {
    &id[..id.len().min(12)]
}

fn lease_display(ws: &WorkspaceInfo) -> String {
    match ws.lease_due_ms {
        None => "pinned".to_string(),
        Some(due) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            if due <= now {
                "lease expired".to_string()
            } else {
                let mins = (due - now) / 60_000;
                format!("lease expires in {}h{:02}m", mins / 60, mins % 60)
            }
        }
    }
}
