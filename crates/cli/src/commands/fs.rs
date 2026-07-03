//! `tl fs` — versioned filesystem workspaces on artifact storage, mounted over FUSE.
//!
//! Product model (artifact_storage issue #24): a *file system* is an artifact-storage repo; a
//! *mount* is a workspace (private leased ref) served by a FUSE daemon — reads stream lazily
//! from the server through the vendored `gsvc-mount` core's immutable caches, writes land in a
//! local overlay. **The overlay is the dirty set**: `snapshot` enumerates it (nothing is
//! scanned), seals it into a commit on the workspace ref, and the mount's lower layer follows
//! the ref to the new snapshot; `promote` CAS-advances a real branch (squash by default);
//! `restore` refills the overlay from any snapshot. FUSE is the only mount path — Linux builds
//! carry it unconditionally, macOS requires macFUSE and the `macfuse` build feature.

use std::path::{Path, PathBuf};
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

pub mod daemon;
#[cfg(target_os = "linux")]
pub mod fusefs;
pub mod local;
// The overlay's write surface is driven by the FUSE glue; without it (macOS build sans macFUSE)
// the methods are intentionally uncalled.
#[cfg(unix)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub mod overlay;

use daemon::MountState;

const MATERIALIZE_CONCURRENCY: usize = 16;

pub(crate) struct FsSession {
    pub(crate) client: ArtifactStorageClient,
    pub(crate) project_id: String,
    credential: GitCredential,
}

impl FsSession {
    pub(crate) async fn open(ctx: &CliContext, repo: Option<&str>) -> Result<FsSession> {
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

    pub(crate) fn creds(&self) -> (&str, &str) {
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
// Mount registry: mountpoint -> state dir, in the CLI config dir.
// ---------------------------------------------------------------------------------------------

fn mounts_registry_path() -> PathBuf {
    crate::config::files::config_dir().join("mounts.toml")
}

fn canonical_mountpoint(path: &Path) -> Result<String> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    // The mountpoint itself may be a live FUSE fs; canonicalize the parent instead.
    let parent = abs.parent().unwrap_or(&abs);
    let name = abs.file_name().map(|n| n.to_string_lossy().into_owned());
    let parent = parent
        .canonicalize()
        .unwrap_or_else(|_| parent.to_path_buf());
    Ok(match name {
        Some(name) => parent.join(name).to_string_lossy().into_owned(),
        None => parent.to_string_lossy().into_owned(),
    })
}

fn registry_load() -> toml::map::Map<String, toml::Value> {
    std::fs::read_to_string(mounts_registry_path())
        .ok()
        .and_then(|raw| raw.parse::<toml::Value>().ok())
        .and_then(|v| v.as_table().cloned())
        .unwrap_or_default()
}

fn registry_save(table: &toml::map::Map<String, toml::Value>) -> Result<()> {
    std::fs::create_dir_all(crate::config::files::config_dir())?;
    std::fs::write(
        mounts_registry_path(),
        toml::to_string_pretty(&toml::Value::Table(table.clone()))?,
    )?;
    Ok(())
}

fn registry_add(mountpoint: &str, state_dir: &Path) -> Result<()> {
    let mut table = registry_load();
    table.insert(
        mountpoint.to_string(),
        toml::Value::String(state_dir.to_string_lossy().into_owned()),
    );
    registry_save(&table)
}

fn registry_remove(mountpoint: &str) -> Result<()> {
    let mut table = registry_load();
    table.remove(mountpoint);
    registry_save(&table)
}

fn state_dir_for(path: &Path) -> Result<(String, PathBuf)> {
    let mountpoint = canonical_mountpoint(path)?;
    let table = registry_load();
    let Some(state_dir) = table.get(&mountpoint).and_then(|v| v.as_str()) else {
        return Err(CliError::usage(format!(
            "{mountpoint} is not a tl fs mount; run `tl fs mount` first"
        )));
    };
    Ok((mountpoint, PathBuf::from(state_dir)))
}

// ---------------------------------------------------------------------------------------------
// Mount / unmount
// ---------------------------------------------------------------------------------------------

/// `tl fs mount <file-system>[:<ref-or-commit>] <path>` — create the workspace and start its
/// FUSE daemon. Reads stream lazily; nothing is copied to disk up front.
pub async fn mount(
    ctx: &CliContext,
    target: &str,
    path: &Path,
    lease_seconds: Option<u64>,
    foreground: bool,
) -> Result<()> {
    let (repo, base) = match target.split_once(':') {
        Some((repo, base)) => (repo, Some(base.to_string())),
        None => (target, None),
    };
    std::fs::create_dir_all(path)?;
    if path
        .read_dir()
        .map(|mut d| d.next().is_some())
        .unwrap_or(true)
    {
        return Err(CliError::usage(format!(
            "{} is not an empty directory",
            path.display()
        )));
    }
    let session = FsSession::open(ctx, Some(repo)).await?;
    let (user, token) = session.creds();
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

    let mountpoint = canonical_mountpoint(path)?;
    let state_dir = daemon::state_dir_root().join(&ws.id);
    daemon::save_mount_state(
        &state_dir,
        &MountState {
            project_id: session.project_id.clone(),
            repo: repo.to_string(),
            workspace_id: ws.id.clone(),
            ref_name: ws.ref_name.clone(),
            mountpoint: PathBuf::from(&mountpoint),
        },
    )?;
    registry_add(&mountpoint, &state_dir)?;

    if foreground {
        return daemon::run(ctx, &state_dir).await;
    }

    // Detach the daemon and wait for its control socket to answer.
    let exe = std::env::current_exe()?;
    std::process::Command::new(exe)
        .args(["fs", "daemon", "--state-dir"])
        .arg(&state_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        match daemon::control(&state_dir, "ping").await {
            Ok(resp) => {
                println!(
                    "Mounted {}:{} at {} (workspace {}, {})",
                    repo,
                    ws.base_ref.as_deref().unwrap_or(&ws.base[..12]),
                    mountpoint,
                    short_id(&ws.id),
                    lease_display(&ws),
                );
                println!(
                    "Lower commit {}. Work in the mount, then: tl fs snapshot {}",
                    resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                    path.display()
                );
                return Ok(());
            }
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            Err(e) => {
                // The workspace is unusable without its daemon; don't leave it leaking until
                // lease expiry.
                registry_remove(&mountpoint)?;
                let _ = session
                    .client
                    .delete_workspace(&session.project_id, repo, user, token, &ws.id)
                    .await;
                let _ = std::fs::remove_dir_all(&state_dir);
                return Err(CliError::usage(format!(
                    "mount daemon did not come up: {e}. Linux builds need /dev/fuse; macOS needs the \
                     TensorLake FSKit extension enabled."
                )));
            }
        }
    }
}

/// Unmount: stop the daemon (unmounts the kernel fs), delete the workspace (unless
/// `--keep-workspace`), and forget the mount. Overlay state dies with the workspace.
pub async fn unmount(ctx: &CliContext, path: &Path, keep_workspace: bool) -> Result<()> {
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    let _ = daemon::control(&state_dir, "shutdown").await;
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
        std::fs::remove_dir_all(&state_dir)?;
    }
    registry_remove(&mountpoint)?;
    println!(
        "Unmounted {mountpoint} ({}).",
        if keep_workspace {
            "workspace kept; its lease keeps ticking".to_string()
        } else {
            format!("workspace {} deleted", short_id(&state.workspace_id))
        }
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Snapshot: the overlay is the dirty set.
// ---------------------------------------------------------------------------------------------

/// Walk the overlay state dir: `(upserts, deletes)` as repo paths. Ignored names (built-ins +
/// the mount's `.tlignore`) are workspace-local and never enumerate.
fn enumerate_overlay(
    state_dir: &Path,
    mount_root: &Path,
) -> Result<(Vec<(String, PathBuf, u32)>, Vec<String>)> {
    let ignored = local::ignored_names(mount_root);
    let mut upserts = Vec::new();
    let mut deletes = Vec::new();
    let upper = state_dir.join("upper");
    let wh = state_dir.join("wh");

    fn walk(
        root: &Path,
        dir: &Path,
        ignored: &[String],
        out: &mut dyn FnMut(String, PathBuf, &std::fs::Metadata),
    ) -> Result<()> {
        let Ok(read) = std::fs::read_dir(dir) else {
            return Ok(());
        };
        for entry in read.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if ignored.contains(&name) {
                continue;
            }
            let abs = entry.path();
            let meta = std::fs::symlink_metadata(&abs)?;
            let rel = abs
                .strip_prefix(root)
                .expect("under root")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if meta.file_type().is_symlink() || meta.is_file() {
                out(rel, abs, &meta);
            } else if meta.is_dir() {
                walk(root, &abs, ignored, out)?;
            }
        }
        Ok(())
    }

    walk(&upper, &upper, &ignored, &mut |rel, abs, meta| {
        use std::os::unix::fs::PermissionsExt;
        let mode = if meta.file_type().is_symlink() {
            0o120000
        } else if meta.permissions().mode() & 0o111 != 0 {
            0o100755
        } else {
            0o100644
        };
        upserts.push((rel, abs, mode));
    })?;
    walk(&wh, &wh, &ignored, &mut |rel, _abs, _meta| {
        deletes.push(rel);
    })?;
    // A whiteout under a path that upper re-created is already shadowed; don't double-send.
    let upserted: std::collections::HashSet<&str> =
        upserts.iter().map(|(p, _, _)| p.as_str()).collect();
    deletes.retain(|p| !upserted.contains(p.as_str()));
    upserts.sort_by(|a, b| a.0.cmp(&b.0));
    deletes.sort();
    Ok((upserts, deletes))
}

pub async fn snapshot(ctx: &CliContext, path: &Path, message: Option<&str>) -> Result<()> {
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;

    let (upserts, deletes) = enumerate_overlay(&state_dir, Path::new(&mountpoint))?;
    if upserts.is_empty() && deletes.is_empty() {
        println!("Nothing to snapshot: workspace is clean.");
        return Ok(());
    }
    let mut files = Vec::with_capacity(upserts.len() + deletes.len());
    for (rel, abs, mode) in &upserts {
        // A symlink's blob content is its target path; reading through `abs` would upload the
        // target file's bytes instead.
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
            repo_path: rel.clone(),
            source,
            mode: Some(*mode),
            delete: false,
        });
    }
    for rel in &deletes {
        files.push(PushFile {
            repo_path: rel.clone(),
            source: PushSource::Bytes(Vec::new()),
            mode: None,
            delete: true,
        });
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

    // Swap the mount's lower layer to the new snapshot, then drop the overlay: the content the
    // upper layer held is now served (identically) by the lower commit.
    daemon::control(&state_dir, "refresh").await?;
    daemon::control(&state_dir, "clear_upper").await?;
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
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;
    let (upserts, deletes) = enumerate_overlay(&state_dir, Path::new(&mountpoint))?;
    if !upserts.is_empty() || !deletes.is_empty() {
        eprintln!(
            "{} {} local change(s) not in any snapshot; promoting the last snapshot only. Run `tl fs snapshot` first to include them.",
            style("note:").yellow(),
            upserts.len() + deletes.len()
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
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
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
    let _ = heartbeat(&session, &state).await;
    let daemon_commit = daemon::control(&state_dir, "ping")
        .await
        .ok()
        .and_then(|r| r.get("commit").and_then(|c| c.as_str().map(str::to_string)));
    let (upserts, deletes) = enumerate_overlay(&state_dir, Path::new(&mountpoint))?;

    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "workspace": ws,
                "mounted": daemon_commit.is_some(),
                "lower_commit": daemon_commit,
                "dirty": upserts.iter().map(|(p, _, _)| p.clone())
                    .chain(deletes.iter().cloned()).collect::<Vec<_>>(),
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
    match &daemon_commit {
        Some(commit) => println!("{} mounted at {commit}", style("daemon:").dim()),
        None => println!(
            "{} not running (remount with tl fs mount)",
            style("daemon:").dim()
        ),
    }
    let dirty = upserts.len() + deletes.len();
    if dirty == 0 {
        println!("{} clean", style("local:").dim());
    } else {
        println!("{} {} change(s):", style("local:").dim(), dirty);
        for (p, _, _) in upserts.iter().take(20) {
            println!("  {} {p}", style("M").yellow());
        }
        for p in deletes.iter().take(20) {
            println!("  {} {p}", style("D").red());
        }
        if dirty > 40 {
            println!("  … and more");
        }
    }
    Ok(())
}

/// Restore: refill the overlay so the merged view equals `version`. The mount's lower layer is
/// untouched (history preserved); the next snapshot seals the restored state.
pub async fn restore(ctx: &CliContext, path: &Path, version: &str) -> Result<()> {
    let (_, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;

    let lower = daemon::control(&state_dir, "ping")
        .await?
        .get("commit")
        .and_then(|c| c.as_str().map(str::to_string))
        .ok_or_else(|| CliError::usage("daemon did not report a commit"))?;
    daemon::control(&state_dir, "clear_upper").await?;

    let target = walk_remote_tree(&session, &state.repo, version).await?;
    let current = walk_remote_tree(&session, &state.repo, &lower).await?;
    let upper = state_dir.join("upper");
    let wh = state_dir.join("wh");

    let (user, token) = session.creds();
    let mut to_fetch: Vec<(String, u32)> = Vec::new();
    for (file_path, entry) in &target {
        match current.get(file_path) {
            Some(cur) if cur.oid == entry.oid && cur.mode == entry.mode => {}
            _ => to_fetch.push((file_path.clone(), entry.mode)),
        }
    }
    let fetched: Vec<Result<(String, u32, Vec<u8>)>> =
        futures::stream::iter(to_fetch.into_iter().map(|(file_path, mode)| {
            let client = session.client.clone();
            let (project, repo, user, token, version) = (
                session.project_id.clone(),
                state.repo.clone(),
                user.to_string(),
                token.to_string(),
                version.to_string(),
            );
            async move {
                let bytes = client
                    .get_file_bytes(&project, &repo, &user, &token, &version, &file_path)
                    .await?
                    .into_inner();
                Ok((file_path, mode, bytes))
            }
        }))
        .buffer_unordered(MATERIALIZE_CONCURRENCY)
        .collect()
        .await;
    let mut restored = 0usize;
    for item in fetched {
        let (file_path, mode, bytes) = item?;
        local::write_entry(&upper, &file_path, mode, &bytes)?;
        restored += 1;
    }
    let mut removed = 0usize;
    for file_path in current.keys().filter(|p| !target.contains_key(*p)) {
        let marker = wh.join(file_path);
        if let Some(parent) = marker.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&marker, b"")?;
        removed += 1;
    }
    println!(
        "Restored {} to {} ({restored} file(s) refreshed, {removed} removed).",
        path.display(),
        &version[..version.len().min(12)]
    );
    Ok(())
}

/// `tl fs diff <path>` — overlay changes vs the last snapshot; `tl fs diff <path> <a> <b>` —
/// server-side tree diff between two commits/refs.
pub async fn diff(ctx: &CliContext, path: &Path, a: Option<&str>, b: Option<&str>) -> Result<()> {
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    match (a, b) {
        (None, None) => {
            let (upserts, deletes) = enumerate_overlay(&state_dir, Path::new(&mountpoint))?;
            for (p, _, _) in &upserts {
                println!("M {p}");
            }
            for p in &deletes {
                println!("D {p}");
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
    let (_, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
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

async fn heartbeat(session: &FsSession, state: &MountState) -> Result<()> {
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
