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
    CreateWorkspaceRequest, PromoteWorkspaceRequest, TreeEntry,
};

use crate::auth::context::CliContext;
use crate::commands::git::{artifact_storage_client, project_id};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub mod daemon;
#[cfg(target_os = "linux")]
pub mod fusefs;
pub mod local;
#[cfg(unix)]
pub mod overlay;
// How the kernel reaches the overlay on macOS: the FSKit extension proxies this wire protocol
// over localhost TCP (Linux talks to the overlay in-process through the FUSE glue). macOS-only —
// on Linux the whole module would be dead code.
#[cfg(target_os = "macos")]
pub mod vfsserver;

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
    // Deserialize as a Table: `toml::Value::from_str` stopped accepting top-level documents in
    // toml 0.9, which silently yielded an empty registry (and add-then-save clobbered entries).
    std::fs::read_to_string(mounts_registry_path())
        .ok()
        .and_then(|raw| toml::from_str::<toml::map::Map<String, toml::Value>>(&raw).ok())
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

/// How a mount treats the branch it was created from. Every mode is still a private,
/// single-writer workspace — modes vary only what the view follows and what happens to writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountMode {
    /// Private workspace; publish with explicit `promote`.
    Workspace,
    /// Read session following the branch: reads see new commits, writes answer EROFS.
    SharedRo,
    /// Private workspace whose every snapshot auto-publishes to the branch (server-ordered).
    SharedRw,
}

/// `tl fs mount <file-system>[:<ref-or-commit>] <path>` — create a workspace (or attach to an
/// existing one with `--workspace <id>`, resuming at its last snapshot) and start its FUSE
/// daemon. Reads stream lazily; nothing is copied to disk up front.
pub async fn mount(
    ctx: &CliContext,
    target: &str,
    path: &Path,
    workspace: Option<&str>,
    mode: MountMode,
    foreground: bool,
) -> Result<()> {
    // Bail before creating a workspace or spawning the daemon-wait loop.
    if cfg!(not(unix)) {
        return Err(CliError::usage(
            "tl fs mount is supported on Linux (FUSE) and macOS (FSKit) only.",
        ));
    }
    let (repo, base) = match target.split_once(':') {
        Some((repo, base)) => (repo, Some(base.to_string())),
        None => (target, None),
    };
    if workspace.is_some() && base.is_some() {
        return Err(CliError::usage(
            "--workspace attaches at the workspace's own head; drop the :<ref-or-commit> suffix",
        ));
    }
    if mode == MountMode::SharedRw && base.is_none() {
        return Err(CliError::usage(
            "--shared-rw needs the branch to publish to: tl fs mount <file-system>:<branch> ...",
        ));
    }
    if mode == MountMode::SharedRo
        && base
            .as_deref()
            .is_some_and(|b| b.len() == 40 && b.bytes().all(|c| c.is_ascii_hexdigit()))
    {
        return Err(CliError::usage(
            "--shared-ro follows a branch; a fixed commit never advances (mount it without \
             --shared-ro instead)",
        ));
    }
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
    // Attach = reconnect: the workspace ref (and everything snapshotted onto it) survived
    // whatever happened to the previous mount — sandbox crash, timeout, unmount.
    let (ws, attached) = match workspace {
        Some(id) => {
            // Accept the short id every listing displays: resolve a <40-hex prefix against the
            // live workspace list, requiring uniqueness.
            let full_id = if id.len() < 40 {
                let all = session
                    .client
                    .list_workspaces(&session.project_id, repo, user, token)
                    .await?
                    .into_inner();
                let matches: Vec<String> = all
                    .iter()
                    .map(|w| w.id.clone())
                    .filter(|wid| wid.starts_with(id))
                    .collect();
                match matches.as_slice() {
                    [one] => one.clone(),
                    [] => {
                        return Err(CliError::usage(format!(
                            "no workspace matches {id:?} (see: tl fs workspace ls {repo})"
                        )));
                    }
                    many => {
                        return Err(CliError::usage(format!(
                            "workspace id {id:?} is ambiguous ({} matches); use more characters",
                            many.len()
                        )));
                    }
                }
            } else {
                id.to_string()
            };
            (
                session
                    .client
                    .get_workspace(&session.project_id, repo, user, token, &full_id)
                    .await?
                    .into_inner(),
                true,
            )
        }
        None => (
            session
                .client
                .create_workspace(
                    &session.project_id,
                    repo,
                    user,
                    token,
                    &CreateWorkspaceRequest {
                        base: base.clone(),
                        shared_target: (mode == MountMode::SharedRw)
                            .then(|| base.clone().expect("guarded above")),
                    },
                )
                .await?
                .into_inner(),
            false,
        ),
    };

    // Shared modes follow the branch itself (default mode follows the workspace ref): shared-ro
    // as a pure read session, shared-rw so every writer's view converges on the reconciled
    // branch rather than staying pinned to its own snapshots. An explicit `:branch` names it;
    // for shared-ro without one, the workspace's resolved base ref (the repo HEAD) is the branch.
    let follow_ref = match mode {
        MountMode::SharedRo => {
            let branch_ref = match &base {
                Some(b) => format!("refs/heads/{b}"),
                None => {
                    let base_ref = ws.base_ref.clone().unwrap_or_default();
                    if !base_ref.starts_with("refs/heads/") {
                        return Err(CliError::usage(
                            "--shared-ro follows a branch, and the repo HEAD did not resolve to \
                             one; name it explicitly: tl fs mount <file-system>:<branch> --shared-ro",
                        ));
                    }
                    base_ref
                }
            };
            Some(branch_ref)
        }
        MountMode::SharedRw => Some(format!(
            "refs/heads/{}",
            base.clone()
                .expect("shared-rw requires <file-system>:<branch>")
        )),
        _ => None,
    };

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
            follow_ref,
            read_only: Some(mode == MountMode::SharedRo),
        },
    )?;
    registry_add(&mountpoint, &state_dir)?;

    if foreground {
        #[cfg(target_os = "macos")]
        vfsserver::TRACE_OPS.store(true, std::sync::atomic::Ordering::Relaxed);
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
                if attached {
                    println!(
                        "Attached workspace {} at {} (resumed at its last snapshot)",
                        short_id(&ws.id),
                        mountpoint,
                    );
                } else {
                    println!(
                        "Mounted {}:{} at {} (workspace {}{})",
                        repo,
                        ws.base_ref.as_deref().unwrap_or(&ws.base[..12]),
                        mountpoint,
                        short_id(&ws.id),
                        match mode {
                            MountMode::Workspace => "",
                            MountMode::SharedRo => ", read-only, follows the branch",
                            MountMode::SharedRw => ", snapshots auto-publish to the branch",
                        },
                    );
                }
                match mode {
                    MountMode::SharedRo => println!(
                        "Reading commit {}; new commits on the branch appear as it advances.",
                        resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                    ),
                    _ => println!(
                        "Lower commit {}. Work in the mount, then: tl fs snapshot {}",
                        resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                        path.display()
                    ),
                }
                return Ok(());
            }
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            Err(e) => {
                registry_remove(&mountpoint)?;
                // A workspace we just created is useless without its daemon; an attached one
                // predates this mount and is not ours to destroy.
                if !attached {
                    let _ = session
                        .client
                        .delete_workspace(&session.project_id, repo, user, token, &ws.id)
                        .await;
                }
                let _ = std::fs::remove_dir_all(&state_dir);
                return Err(CliError::usage(format!(
                    "mount daemon did not come up: {e}. Linux builds need /dev/fuse; macOS needs the \
                     TensorLake FSKit extension enabled."
                )));
            }
        }
    }
}

/// Unmount: stop the daemon (unmounts the kernel fs) and forget the mount. The workspace — and
/// every snapshot on it — stays on the server until `tl fs workspace rm` (or `--delete` here);
/// unsnapshotted overlay changes are local and die with the mount's state directory.
pub async fn unmount(ctx: &CliContext, path: &Path, delete: bool) -> Result<()> {
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    let pid = daemon::daemon_pid(&state_dir);
    let _ = daemon::control(&state_dir, "shutdown").await;
    // Wait for the daemon to actually exit before tearing down its state dir: the shutdown op
    // races the process exit, and deleting upper/control state under a live daemon is how
    // daemons leak (and how a reattach ends up sharing a state dir with a zombie).
    if let Some(pid) = pid {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while unsafe { libc::kill(pid, 0) } == 0 {
            if std::time::Instant::now() >= deadline {
                // Still alive: escalate once, then proceed — better a killed daemon than a
                // shared state dir.
                unsafe { libc::kill(pid, libc::SIGKILL) };
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    if delete {
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
    std::fs::remove_dir_all(&state_dir)?;
    registry_remove(&mountpoint)?;
    if delete {
        println!(
            "Unmounted {mountpoint} (workspace {} deleted).",
            short_id(&state.workspace_id)
        );
    } else {
        println!(
            "Unmounted {mountpoint}. Workspace {} kept — reattach with: tl fs mount {} \
             --workspace {} <path>",
            short_id(&state.workspace_id),
            state.repo,
            state.workspace_id,
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Snapshot: the overlay is the dirty set.
// ---------------------------------------------------------------------------------------------

/// Walk the overlay state dir: `(upserts, deletes)` as repo paths. Ignored names (built-ins +
/// the mount's `.tlignore`) are workspace-local and never enumerate.
/// Overlay upserts as `(repo path, upper file, git mode)`.
type OverlayUpserts = Vec<(String, PathBuf, u32)>;

fn enumerate_overlay(state_dir: &Path, mount_root: &Path) -> Result<(OverlayUpserts, Vec<String>)> {
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
            if ignored.contains(&name) || local::is_metadata_turd(&name) {
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
        #[cfg(unix)]
        let exec = {
            use std::os::unix::fs::PermissionsExt;
            meta.permissions().mode() & 0o111 != 0
        };
        // Windows has no exec bit (and no mounts — this only runs for local state inspection).
        #[cfg(not(unix))]
        let exec = false;
        let mode = if meta.file_type().is_symlink() {
            0o120000
        } else if exec {
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
    if state.read_only() {
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; there is nothing to {}",
            state.follow_ref.as_deref().unwrap_or("the branch"),
            "snapshot",
        )));
    }
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
    // Content is byte-identical across the swap, but the previously-dirty paths' attributes
    // changed backing (upper mtimes -> lower commit time); refresh the kernel's view.
    let sealed: Vec<String> = upserts
        .iter()
        .map(|(p, _, _)| p.clone())
        .chain(deletes.iter().cloned())
        .collect();
    revalidate_paths(Path::new(&mountpoint), &sealed);
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
    if state.read_only() {
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; there is nothing to promote",
            state.follow_ref.as_deref().unwrap_or("the branch"),
        )));
    }
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
        "{} {} (created {} ago)",
        style("workspace:").dim(),
        short_id(&ws.id),
        age_display(ws.created_at_secs)
    );
    if let Some(followed) = &state.follow_ref {
        println!("{} read-only, follows {followed}", style("mode:").dim());
    } else if let Some(target) = &ws.shared_target {
        println!(
            "{} shared-rw, snapshots auto-publish to {target}",
            style("mode:").dim()
        );
    }
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
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.read_only() {
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; there is nothing to restore",
            state.follow_ref.as_deref().unwrap_or("the branch"),
        )));
    }
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;

    let lower = daemon::control(&state_dir, "ping")
        .await?
        .get("commit")
        .and_then(|c| c.as_str().map(str::to_string))
        .ok_or_else(|| CliError::usage("daemon did not report a commit"))?;
    // Read everything from the server BEFORE touching local state, so a failed restore leaves
    // the workspace exactly as it was. A commit's index materializes asynchronously after its
    // snapshot publishes; a restore issued right behind one can land in that window — the
    // server signals 425 Too Early, poll it out (same contract as promote).
    let target = walk_remote_tree_ready(&session, &state.repo, version).await?;
    let current = walk_remote_tree_ready(&session, &state.repo, &lower).await?;
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
                let deadline = std::time::Instant::now() + TOO_EARLY_DEADLINE;
                let bytes = loop {
                    match client
                        .get_file_bytes(&project, &repo, &user, &token, &version, &file_path)
                        .await
                    {
                        Ok(resp) => break resp.into_inner(),
                        Err(tensorlake::error::SdkError::ServerError { status, .. })
                            if status.as_u16() == 425 && std::time::Instant::now() < deadline =>
                        {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        Err(e) => return Err(e.into()),
                    }
                };
                Ok((file_path, mode, bytes))
            }
        }))
        .buffer_unordered(MATERIALIZE_CONCURRENCY)
        .collect()
        .await;
    let fetched: Vec<(String, u32, Vec<u8>)> = fetched.into_iter().collect::<Result<Vec<_>>>()?;

    // Point of no return: everything needed is local, now swap the overlay.
    // The overlay's dirty set is about to be dropped — those paths' kernel views flip to the
    // target too (even when the target equals the lower and the tree diff below is empty).
    let (pre_upserts, pre_deletes) = enumerate_overlay(&state_dir, Path::new(&mountpoint))?;
    daemon::control(&state_dir, "clear_upper").await?;
    let mut changed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut expect: std::collections::BTreeMap<String, PathExpect> =
        std::collections::BTreeMap::new();
    let mut restored = 0usize;
    for (file_path, mode, bytes) in fetched {
        local::write_entry(&upper, &file_path, mode, &bytes)?;
        let e = if mode == 0o120000 {
            PathExpect::Present
        } else {
            PathExpect::FileSize(bytes.len() as u64)
        };
        expect.insert(file_path.clone(), e);
        changed.insert(file_path);
        restored += 1;
    }
    let mut removed = 0usize;
    for file_path in current.keys().filter(|p| !target.contains_key(*p)) {
        write_whiteout(&wh, file_path)?;
        expect.insert(file_path.clone(), PathExpect::Absent);
        changed.insert(file_path.clone());
        removed += 1;
    }
    // Directories only present in `current` vanish too; a dir-level marker supersedes any
    // child markers (same convention as OverlayFs::set_whiteout). Shallowest first, so one
    // marker covers a vanished subtree.
    let implied_dirs = |tree: &std::collections::BTreeMap<String, TreeEntry>| {
        let mut dirs = std::collections::BTreeSet::new();
        for file_path in tree.keys() {
            let mut dir = file_path.as_str();
            while let Some((parent, _)) = dir.rsplit_once('/') {
                dirs.insert(parent.to_string());
                dir = parent;
            }
        }
        dirs
    };
    let target_dirs = implied_dirs(&target);
    let mut whited_dirs: Vec<String> = Vec::new();
    for dir in implied_dirs(&current)
        .into_iter()
        .filter(|d| !target_dirs.contains(d))
    {
        if whited_dirs
            .iter()
            .any(|w| dir.starts_with(w.as_str()) && dir.as_bytes().get(w.len()) == Some(&b'/'))
        {
            continue;
        }
        write_whiteout(&wh, &dir)?;
        expect.insert(dir.clone(), PathExpect::Absent);
        changed.insert(dir.clone());
        whited_dirs.push(dir);
    }
    // Paths the dropped overlay used to answer now flip to the target view as well.
    for p in pre_upserts
        .iter()
        .map(|(p, _, _)| p.clone())
        .chain(pre_deletes.iter().cloned())
    {
        if changed.contains(&p) {
            continue;
        }
        let e = match target.get(&p) {
            Some(entry) if entry.mode == 0o120000 => PathExpect::Present,
            Some(entry) => entry
                .size
                .map(PathExpect::FileSize)
                .unwrap_or(PathExpect::Present),
            None if target_dirs.contains(&p) => PathExpect::Present,
            None => PathExpect::Absent,
        };
        expect.insert(p.clone(), e);
        changed.insert(p);
    }
    converge_kernel_view(Path::new(&mountpoint), &changed, &expect);
    println!(
        "Restored {} to {} ({restored} file(s) refreshed, {removed} removed).",
        path.display(),
        &version[..version.len().min(12)]
    );
    Ok(())
}

/// What the kernel's view of a path must look like once a restore has settled.
enum PathExpect {
    /// A regular file with exactly this size.
    FileSize(u64),
    /// Present (symlinks and directories, or files whose size isn't cheaply known).
    Present,
    /// No longer visible.
    Absent,
}

/// Nudge the kernel to revalidate paths whose content changed behind its back (restore and
/// snapshot mutate the overlay out-of-band). A stat through the mountpoint makes the kernel
/// re-fetch attributes; the changed mtime/size then revalidates that file.
/// Best-effort: a failed stat (e.g. the path was just deleted) is itself the fresh answer.
fn revalidate_paths(mountpoint: &Path, changed: &[String]) {
    // Parent directories first (dedup'd): their listings changed too.
    let mut dirs = std::collections::BTreeSet::new();
    dirs.insert(String::new());
    for p in changed {
        let mut dir = p.as_str();
        while let Some((parent, _)) = dir.rsplit_once('/') {
            dirs.insert(parent.to_string());
            dir = parent;
        }
    }
    for dir in &dirs {
        let _ = std::fs::symlink_metadata(mountpoint.join(dir));
    }
    for p in changed {
        let _ = std::fs::symlink_metadata(mountpoint.join(p));
    }
}

/// Open a path without following a final symlink and return its fstat size, or `None` when it
/// does not exist. `open(2)` is the coherence workhorse here: the kernel revalidates a path's
/// item on open (close-to-open, like NFS), cutting through stale positive AND negative name
/// cache entries that plain `stat(2)` keeps serving until their TTL (~30s measured) —
/// `purge` additionally drops cached data pages via `msync(MS_INVALIDATE)` on a shared read
/// mapping, the only userspace lever that does so: attribute changes alone make the kernel
/// adopt a new size but NOT refetch cached pages (a file that grew behind the kernel keeps a
/// zero-filled tail forever otherwise — measured on macOS 26.5 FSKit/lifs).
#[cfg(unix)]
fn open_truth(path: &Path, purge: bool) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    #[cfg(target_os = "macos")]
    let flags = libc::O_RDONLY | libc::O_SYMLINK;
    #[cfg(not(target_os = "macos"))]
    let flags = libc::O_RDONLY | libc::O_PATH | libc::O_NOFOLLOW;
    unsafe {
        let fd = libc::open(c.as_ptr(), flags);
        if fd < 0 {
            return None;
        }
        let mut st: libc::stat = std::mem::zeroed();
        if libc::fstat(fd, &mut st) != 0 {
            libc::close(fd);
            return None;
        }
        let len = st.st_size as usize;
        if purge && st.st_mode & libc::S_IFMT == libc::S_IFREG && len > 0 {
            let addr = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if addr != libc::MAP_FAILED {
                libc::msync(addr, len, libc::MS_INVALIDATE);
                libc::munmap(addr, len);
            }
        }
        libc::close(fd);
        Some(st.st_size as u64)
    }
}

/// Break a stale negative name-cache entry for a path that exists daemon-side. The kernel can
/// pin ENOENT for a name (directories especially) past any lookup we drive; a create attempt
/// is the one operation it cannot answer from that cache — the overlay's exclusivity check
/// answers EEXIST, teaching the kernel the name is real. Safe by construction: this is only
/// called for paths the overlay is already known to serve, so nothing is ever created.
#[cfg(unix)]
fn probe_negative_dentry(path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return;
    };
    unsafe {
        let fd = libc::open(
            c.as_ptr(),
            libc::O_RDONLY | libc::O_CREAT | libc::O_EXCL,
            0o644,
        );
        if fd >= 0 {
            // The expectation machinery only probes paths the daemon serves; reaching here
            // means the view raced badly — undo and let the caller's re-check decide.
            libc::close(fd);
            libc::unlink(c.as_ptr());
            return;
        }
        if libc::mkdir(c.as_ptr(), 0o755) == 0 {
            libc::rmdir(c.as_ptr());
        }
    }
}

/// Mounts don't exist off unix, so there is no kernel view to converge — these are compile
/// stubs so the shared restore/snapshot plumbing stays portable (the mount-family commands
/// themselves fail with "unsupported" long before reaching here).
#[cfg(not(unix))]
fn open_truth(path: &Path, _purge: bool) -> Option<u64> {
    std::fs::symlink_metadata(path).ok().map(|m| m.len())
}

#[cfg(not(unix))]
fn probe_negative_dentry(_path: &Path) {}

/// Nudge and wait (bounded) until the kernel's view through the mountpoint matches `expect`.
/// The kernel applies out-of-band changes asynchronously and never refetches cached pages on
/// its own; each round opens every changed path (open revalidates), purges cached pages of
/// expected files, and re-checks. Returns once settled or after ~5s. After this, open/read
/// and directory listings are coherent; a bare `stat(2)` of a path that was never re-opened
/// can still serve cached attributes until the kernel's TTL.
fn converge_kernel_view(
    mountpoint: &Path,
    changed: &std::collections::BTreeSet<String>,
    expect: &std::collections::BTreeMap<String, PathExpect>,
) {
    let changed: Vec<String> = changed.iter().cloned().collect();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        revalidate_paths(mountpoint, &changed);
        let mut settled = true;
        for (p, e) in expect {
            let full = mountpoint.join(p);
            match e {
                PathExpect::Absent => {
                    if open_truth(&full, false).is_some() {
                        settled = false;
                    }
                }
                PathExpect::Present => {
                    if open_truth(&full, false).is_none() {
                        probe_negative_dentry(&full);
                        if open_truth(&full, false).is_none() {
                            settled = false;
                        }
                    }
                }
                PathExpect::FileSize(size) => {
                    if open_truth(&full, true).is_none() {
                        probe_negative_dentry(&full);
                    }
                    match open_truth(&full, true) {
                        Some(len) if len == *size => {}
                        _ => settled = false,
                    }
                }
            }
        }
        if settled || std::time::Instant::now() > deadline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// Write a whiteout marker file, superseding any container of child markers at the same path
/// (mirrors OverlayFs::set_whiteout).
fn write_whiteout(wh: &Path, rel: &str) -> Result<()> {
    let marker = wh.join(rel);
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if marker.is_dir() {
        std::fs::remove_dir_all(&marker)?;
    }
    std::fs::write(&marker, b"")?;
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

/// `tl fs workspace ls <file-system>` — every live workspace, with enough context to pick one
/// to reattach to (`tl fs mount <fs> --workspace <id> <path>`).
pub async fn workspace_ls(ctx: &CliContext, file_system: &str, output_json: bool) -> Result<()> {
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
    let mut table = new_table(&["Workspace", "Base", "Head", "Snapshots", "Mode", "Age"]);
    for ws in &list {
        table.add_row(vec![
            Cell::new(&ws.id),
            Cell::new(ws.base_ref.as_deref().unwrap_or(&ws.base[..12])),
            Cell::new(&ws.head[..12]),
            Cell::new(if ws.head == ws.base { "-" } else { "yes" }),
            Cell::new(match &ws.shared_target {
                Some(target) => format!("shared-rw -> {target}"),
                None => "workspace".to_string(),
            }),
            Cell::new(age_display(ws.created_at_secs)),
        ]);
    }
    println!("{table}");
    println!(
        "Reattach: tl fs mount {file_system} --workspace <id> <path> — delete: tl fs workspace \
         rm {file_system} <id>"
    );
    Ok(())
}

/// `tl fs workspace rm <file-system> <workspace-id>` — the one way a workspace dies. Its
/// snapshots become unreachable (promoted work is unaffected).
pub async fn workspace_rm(ctx: &CliContext, file_system: &str, workspace_id: &str) -> Result<()> {
    let session = FsSession::open(ctx, Some(file_system)).await?;
    let (user, token) = session.creds();
    session
        .client
        .delete_workspace(&session.project_id, file_system, user, token, workspace_id)
        .await?;
    println!("Deleted workspace {}.", short_id(workspace_id));
    Ok(())
}

/// Full recursive listing of `version`: repo path -> entry. Directories are traversed
/// concurrently; each directory is paged through `next_after`.
/// How long read paths poll out 425 Too Early while a just-published commit's index
/// materializes (same contract as promote's poll loop).
const TOO_EARLY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// `walk_remote_tree`, polling out 425 Too Early: a commit's index materializes
/// asynchronously after its snapshot publishes, and reads issued right behind one land in
/// that window.
async fn walk_remote_tree_ready(
    session: &FsSession,
    repo: &str,
    version: &str,
) -> Result<std::collections::BTreeMap<String, TreeEntry>> {
    let deadline = std::time::Instant::now() + TOO_EARLY_DEADLINE;
    loop {
        match walk_remote_tree(session, repo, version).await {
            Err(CliError::Sdk(tensorlake::error::SdkError::ServerError { status, .. }))
                if status.as_u16() == 425 && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            other => return other,
        }
    }
}

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

fn age_display(created_at_secs: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mins = now.saturating_sub(created_at_secs) / 60;
    match mins {
        0..=59 => format!("{mins}m"),
        60..=1439 => format!("{}h{:02}m", mins / 60, mins % 60),
        _ => format!("{}d", mins / 1440),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn registry_document_parses_as_table() {
        // toml 0.9 rejects top-level documents through Value::from_str; the registry must
        // deserialize as a Table or every lookup sees an empty registry.
        let raw = "\"/Users/u/work\" = \"/Users/u/.local/share/tensorlake/mounts/abc\"\n";
        let table: toml::map::Map<String, toml::Value> = toml::from_str(raw).unwrap();
        assert_eq!(
            table.get("/Users/u/work").and_then(|v| v.as_str()),
            Some("/Users/u/.local/share/tensorlake/mounts/abc")
        );
    }
}
