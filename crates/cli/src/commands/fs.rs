//! `tl fs` — versioned filesystem workspaces on artifact storage, mounted over FUSE.
//!
//! Product model (artifact_storage issue #24): the *workspace* is the unit `tl fs` manages —
//! `mount` creates or attaches one, `ls` lists them, `rm` deletes them. A *file system* is the
//! artifact-storage repo backing them (managed with `tl git`); a mounted workspace (private
//! leased ref) is served by a FUSE daemon — reads stream lazily from the server through the
//! vendored `gsvc-mount` core's immutable caches, writes land in a local overlay.
//! The daemon's dirty index is the snapshot source of truth: its sealer resolves that index
//! into incremental snapshot commits on the workspace ref (auto-commit ticks it; `tl fs
//! snapshot` runs one cycle through the `seal` control op), and the mount's lower layer follows
//! the ref to the new snapshot. The overlay is **kept** after sealing, so a raw upper walk is
//! only an approximate fallback when the daemon cannot report the exact next-snapshot set.
//! `snapshot --clear` is the explicit, destructive opt-in that drops the upper (required before
//! `sync`). `promote`
//! CAS-advances a real branch (squash by default); `restore` refills the overlay from any
//! snapshot. FUSE is the only mount path — Linux builds
//! carry it unconditionally, macOS requires macFUSE and the `macfuse` build feature.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use comfy_table::Cell;
use console::style;
use futures::StreamExt;
use ignore::Match;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::ingest::{PushFile, PushOptions, PushSource};
use tensorlake::artifact_storage::models::GitCredential;
use tensorlake::artifact_storage::workspaces::{
    CreateWorkspaceRequest, PromoteOutcome, PromoteWorkspaceRequest, SyncWorkspaceRequest,
    TreeEntry, WorkspaceInfo,
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
// Plain-directory workspace snapshots: `tl fs init` binds a directory to a workspace with no
// mount at all; snapshot/status dispatch here when the path is a binding rather than a mount.
pub mod plaindir;
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
// Workspaces: the unit `tl fs` manages. File systems (artifact-storage repos) are managed with
// `tl git`; here they are only the containers workspaces live in.
// ---------------------------------------------------------------------------------------------

/// Write policy for `tl fs mount`, from `--mode`. `Auto` means writable — except when attaching
/// a workspace that is already mounted live somewhere else, which defaults to read-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WritePolicy {
    Auto,
    Ro,
    Rw,
}

async fn file_system_names(session: &FsSession) -> Result<Vec<String>> {
    let (user, token) = session.creds();
    Ok(session
        .client
        .list_repos_with_credential(&session.project_id, user, token)
        .await?
        .into_inner()
        .repos
        .into_iter()
        .map(|r| r.name)
        .collect())
}

/// Every workspace across the given file systems, newest first: `(file system, workspace)`.
async fn all_workspaces(
    session: &FsSession,
    fs_names: &[String],
) -> Result<Vec<(String, WorkspaceInfo)>> {
    let (user, token) = session.creds();
    let lists: Vec<Result<(String, Vec<WorkspaceInfo>)>> =
        futures::stream::iter(fs_names.iter().cloned().map(|repo| {
            let client = session.client.clone();
            let (project, user, token) = (
                session.project_id.clone(),
                user.to_string(),
                token.to_string(),
            );
            async move {
                let list = client
                    .list_workspaces(&project, &repo, &user, &token)
                    .await?
                    .into_inner();
                Ok((repo, list))
            }
        }))
        .buffer_unordered(8)
        .collect()
        .await;
    let mut rows = Vec::new();
    for list in lists {
        let (repo, workspaces) = list?;
        rows.extend(workspaces.into_iter().map(|ws| (repo.clone(), ws)));
    }
    rows.sort_by_key(|(_, ws)| std::cmp::Reverse(ws.created_at_secs));
    Ok(rows)
}

/// Resolve a workspace id (or unique prefix) to its file system + info, scanning every file
/// system in the project.
async fn resolve_workspace(
    session: &FsSession,
    fs_names: &[String],
    id: &str,
) -> Result<Option<(String, WorkspaceInfo)>> {
    let mut matches: Vec<(String, WorkspaceInfo)> = all_workspaces(session, fs_names)
        .await?
        .into_iter()
        .filter(|(_, ws)| ws.id.starts_with(id))
        .collect();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(matches.remove(0))),
        n => Err(CliError::usage(format!(
            "workspace id {id:?} is ambiguous ({n} matches); use more characters"
        ))),
    }
}

/// `tl fs ls [file-system]` — every live workspace (across all file systems by default), with
/// where each one is currently mounted on this machine.
pub async fn ls(ctx: &CliContext, file_system: Option<&str>, output_json: bool) -> Result<()> {
    let session = FsSession::open(ctx, file_system).await?;
    let fs_names = match file_system {
        Some(fs) => vec![fs.to_string()],
        None => file_system_names(&session).await?,
    };
    let rows = all_workspaces(&session, &fs_names).await?;
    let mounts = live_mounts();
    let bound = plaindir::bound_workspaces();
    // A workspace's local attachment: the directory plus what kind of attachment it is.
    // Plain-directory bindings are attachments too; without this they would be invisible
    // everywhere except path-addressed commands.
    let attachment = |id: &str| -> Option<(String, &'static str)> {
        mounts
            .iter()
            .find(|(_, s)| s.workspace_id == id)
            .map(|(m, _)| (m.clone(), "mount"))
            .or_else(|| {
                bound
                    .iter()
                    .find(|(ws, _)| ws == id)
                    .map(|(_, root)| (root.clone(), "binding"))
            })
    };
    if output_json {
        let out: Vec<serde_json::Value> = rows
            .iter()
            .map(|(fs, ws)| {
                // `mounted_at` is the plain path (machine-consumable); `kind` says whether it
                // is a kernel mount or a plain-directory binding — decorating the path itself
                // broke every consumer that fed it back to another command.
                let attached = attachment(&ws.id);
                serde_json::json!({
                    "file_system": fs,
                    "workspace": ws,
                    "mounted_at": attached.as_ref().map(|(path, _)| path.clone()),
                    "kind": attached.as_ref().map(|(_, kind)| *kind),
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("No workspaces. Create and mount one with: tl fs mount <file-system> <path>");
        return Ok(());
    }
    let mut table = new_table(&[
        "Workspace",
        "File system",
        "Base",
        "Snapshots",
        "Mode",
        "Mounted",
        "Age",
    ]);
    for (fs, ws) in &rows {
        table.add_row(vec![
            Cell::new(&ws.id),
            Cell::new(fs),
            Cell::new(ws.base_ref.as_deref().unwrap_or(&ws.base[..12])),
            Cell::new(if ws.head == ws.base { "-" } else { "yes" }),
            Cell::new(match &ws.shared_target {
                Some(target) => format!("shared-rw -> {target}"),
                None => "workspace".to_string(),
            }),
            // Human output keeps the annotation; the JSON path/kind split serves machines.
            Cell::new(match attachment(&ws.id) {
                Some((path, "binding")) => format!("{path} (bound)"),
                Some((path, _)) => path,
                None => "-".to_string(),
            }),
            Cell::new(age_display(ws.created_at_secs)),
        ]);
    }
    println!("{table}");
    println!("Mount: tl fs mount <workspace> <path> — delete: tl fs rm <workspace>");
    Ok(())
}

/// `tl fs rm <workspace-id>` — the one way a workspace dies. Its snapshots become unreachable
/// (promoted work is unaffected).
pub async fn rm(ctx: &CliContext, workspace_id: &str) -> Result<()> {
    let session = FsSession::open(ctx, None).await?;
    let fs_names = file_system_names(&session).await?;
    let Some((file_system, ws)) = resolve_workspace(&session, &fs_names, workspace_id).await?
    else {
        return Err(CliError::usage(format!(
            "no workspace matches {workspace_id:?} (see: tl fs ls)"
        )));
    };
    if let Some(mountpoint) = live_mount_of(&ws.id) {
        return Err(CliError::usage(format!(
            "workspace {} is mounted at {mountpoint}; unmount and delete in one step: tl fs \
             unmount {mountpoint} --delete",
            short_id(&ws.id)
        )));
    }
    // A plain-directory binding references the workspace exactly like a live mount does —
    // deleting it out from under the binding would strand the local index (and every future
    // snapshot) against a dead workspace. Fail-closed lookup: a corrupt binding.json aborts
    // the delete instead of being skipped as if unbound.
    if let Some(bound_at) = plaindir::binding_using_workspace(&ws.id)? {
        return Err(CliError::usage(format!(
            "workspace {} is bound to {bound_at}; unbind first (tl fs unbind {bound_at}), \
             then delete",
            short_id(&ws.id)
        )));
    }
    let (user, token) = session.creds();
    session
        .client
        .delete_workspace(&session.project_id, &file_system, user, token, &ws.id)
        .await?;
    println!(
        "Deleted workspace {} (file system {file_system}).",
        short_id(&ws.id)
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// `tl fs setup` — install/verify the macOS FSKit extension mounts need on end-user machines.
// ---------------------------------------------------------------------------------------------

/// `tl fs setup [--from <path-or-url>] [--check]`. Linux mounts talk to /dev/fuse directly and
/// need nothing; macOS mounts go through the TLFS FSKit extension, which ships as a notarized
/// app bundle attached to the CLI release. This installs it and walks the one manual step Apple
/// keeps for the user (the System Settings toggle).
pub async fn setup(from: Option<&str>, check_only: bool) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        setup_macos(from, check_only).await
    }
    #[cfg(target_os = "linux")]
    {
        // Nothing to install on Linux (mounts go straight to /dev/fuse); `setup` is purely a
        // diagnosis command there, identical to `setup --check`.
        let _ = (from, check_only);
        diagnose_linux();
        Ok(())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (from, check_only);
        Err(CliError::usage(
            "tl fs mounts are supported on Linux (FUSE) and macOS (FSKit) only.",
        ))
    }
}

#[cfg(target_os = "macos")]
const FSKIT_APP_PATH: &str = "/Applications/TLFS.app";
#[cfg(target_os = "macos")]
const FSKIT_MODULE_ID: &str = "ai.tensorlake.tlfs.fsmodule";
/// FSKit floor. The TLFS extension is built against the macOS 26 SDK and its Info.plist sets
/// LSMinimumSystemVersion 26.0 (see platform/macos/tlfs/); LaunchServices refuses to register a
/// bundle whose minimum exceeds the running OS, so on anything older the extension silently
/// never registers. Name the floor instead of leaving that as a dead-end registration loop.
#[cfg(target_os = "macos")]
const MACOS_MIN_MAJOR: u32 = 26;
#[cfg(target_os = "macos")]
const MACOS_MIN_NAME: &str = "macOS 26 (Tahoe)";

/// The running macOS product version (`26.1`, …), via sw_vers. `None` if it can't be read.
#[cfg(target_os = "macos")]
fn macos_product_version() -> Option<String> {
    let out = std::process::Command::new("sw_vers")
        .arg("-productVersion")
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// `Ok(version_string)` when the OS meets the FSKit floor (or its version can't be parsed — we
/// don't block on uncertainty; the mount would surface the real error). `Err(guidance)` when it
/// is provably too old.
#[cfg(target_os = "macos")]
fn macos_version_supported() -> std::result::Result<String, String> {
    let version = macos_product_version().unwrap_or_default();
    match version
        .split('.')
        .next()
        .and_then(|m| m.parse::<u32>().ok())
    {
        Some(major) if major < MACOS_MIN_MAJOR => Err(format!(
            "tl fs on macOS needs {MACOS_MIN_NAME} or later on Apple Silicon; this machine is \
             macOS {version}. The TLFS file-system extension uses FSKit APIs introduced in \
             {MACOS_MIN_NAME}, so mounts cannot work on this OS."
        )),
        _ => Ok(version),
    }
}

/// Official darwin release builds carry the notarized TLFS.app.zip inside the binary (see
/// crates/cli/build.rs), so setup needs no network and cannot skew versions. Source builds
/// don't embed it and fall back to the release download.
#[cfg(all(target_os = "macos", tlfs_app_embedded))]
const EMBEDDED_APP_ZIP: Option<&[u8]> = Some(include_bytes!(env!("TLFS_APP_ZIP")));
#[cfg(all(target_os = "macos", not(tlfs_app_embedded)))]
const EMBEDDED_APP_ZIP: Option<&[u8]> = None;

/// The release asset built by `platform/macos/tlfs/build.sh --release --notarize` and attached
/// to the same GitHub release as this CLI version, so extension and daemon stay in wire-protocol
/// lockstep (there is no version negotiation beyond the HELLO check).
#[cfg(target_os = "macos")]
fn default_app_url() -> String {
    format!(
        "https://github.com/tensorlakeai/tensorlake/releases/download/cli-v{v}/TLFS-{v}.app.zip",
        v = env!("CARGO_PKG_VERSION"),
    )
}

/// pluginkit's status for the module: `Some('+')` registered and elected, `Some('-')` registered
/// but disabled/ignored, `None` unknown to pluginkit.
#[cfg(target_os = "macos")]
fn appex_registration() -> Option<char> {
    let out = std::process::Command::new("pluginkit")
        .args(["-m", "-i", FSKIT_MODULE_ID])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().chars().next()
}

/// fskit_agent's per-user allowlist — the third enablement gate, and the one the System
/// Settings "File System Extensions" toggle actually writes. A plain array of bundle ids.
/// `None` when the home directory is unresolvable (never fabricate a relative path: a plist
/// written under `./Library/...` is one fskit_agent will never read).
#[cfg(target_os = "macos")]
fn fskit_enabled_modules_path() -> Option<PathBuf> {
    Some(
        dirs::home_dir()?
            .join("Library/Group Containers/group.com.apple.fskit.settings/enabledModules.plist"),
    )
}

/// The allowlist's contents. `Some(vec![])` for a missing file (a fresh machine — safe to
/// create); `None` when the file exists but cannot be read or parsed as a string array.
/// Callers must NEVER rewrite it in the `None` state: the file is shared with every other
/// FSKit extension on the machine, and clobbering it from a bad read would disable them all.
#[cfg(target_os = "macos")]
fn fskit_enabled_modules() -> Option<Vec<String>> {
    let path = fskit_enabled_modules_path()?;
    if !path.exists() {
        return Some(Vec::new());
    }
    let out = std::process::Command::new("plutil")
        .args(["-convert", "json", "-o", "-"])
        .arg(&path)
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    serde_json::from_slice(&out.stdout).ok()
}

/// The gates between an installed app bundle and a serving mount. pluginkit's `+` is
/// necessary but NOT sufficient — measured: an elected module still fails `mount -F` with
/// "Module … is disabled!" until its id appears in the allowlist. Every caller judges
/// readiness through this one snapshot so the criteria cannot drift apart.
#[cfg(target_os = "macos")]
struct FskitGates {
    /// pluginkit registration/election state (`'+'` = elected, `'-'` = ignored,
    /// `None` = unregistered).
    registration: Option<char>,
    /// The allowlist, when it read cleanly.
    modules: Option<Vec<String>>,
}

#[cfg(target_os = "macos")]
impl FskitGates {
    fn read() -> FskitGates {
        FskitGates {
            registration: appex_registration(),
            modules: fskit_enabled_modules(),
        }
    }

    /// `Some(bool)`: the allowlist read cleanly and does/doesn't contain the module.
    /// `None`: unreadable — only System Settings can manage it safely.
    fn allowlisted(&self) -> Option<bool> {
        self.modules
            .as_ref()
            .map(|ids| ids.iter().any(|id| id == FSKIT_MODULE_ID))
    }

    /// Both gates verifiably open: mount(8) will be served.
    fn ready(&self) -> bool {
        self.registration == Some('+') && self.allowlisted() == Some(true)
    }
}

/// Rewrite the allowlist: plutil converts our JSON from stdin straight onto the plist path —
/// no temp file, no cleanup.
#[cfg(target_os = "macos")]
fn write_enabled_modules(ids: &[String]) -> bool {
    let Some(path) = fskit_enabled_modules_path() else {
        return false;
    };
    let write = || -> std::io::Result<bool> {
        use std::io::Write as _;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut child = std::process::Command::new("plutil")
            .args(["-convert", "xml1", "-", "-o"])
            .arg(&path)
            .stdin(std::process::Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .expect("stdin piped above")
            .write_all(&serde_json::to_vec(ids)?)?;
        Ok(child.wait()?.success())
    };
    write().unwrap_or(false)
}

/// The pid of this user's running fskit_agent, if any. Scoped to our uid: every logged-in
/// user gets an agent, and another user's is neither signalable nor the one serving our mounts.
#[cfg(target_os = "macos")]
fn fskit_agent_pid() -> Option<libc::pid_t> {
    let uid = unsafe { libc::getuid() }.to_string();
    let out = std::process::Command::new("pgrep")
        .args(["-x", "-U", &uid, "fskit_agent"])
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()?
        .trim()
        .parse()
        .ok()
}

/// True once the pid is gone, polling up to `timeout`. kill(pid, 0) probes without signaling;
/// a non-zero return here means ESRCH (the pid was ours, so never EPERM).
#[cfg(target_os = "macos")]
async fn wait_pid_exit(pid: libc::pid_t, timeout: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if unsafe { libc::kill(pid, 0) } != 0 {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Whether fskit_agent is serving any live volume. Guessing by fstype cannot answer this —
/// FSKit's whole point is third-party modules with arbitrary type names — but livefsd's
/// per-boot record lists every FSKit-served mount regardless of vendor. An entry whose
/// mountpoint is no longer attached is a stale leftover and doesn't count. Unreadable state
/// reads as busy: never SIGKILL on uncertainty.
#[cfg(target_os = "macos")]
fn fskit_agent_busy() -> bool {
    let Some(mounts) = livefs_mounted_on() else {
        return true;
    };
    mounts.iter().any(|path| daemon::mounted_at(path))
}

/// Get the live fskit_agent to drop its stale allowlist snapshot: it re-reads the file only at
/// launch, so an agent started before our write keeps failing mounts with "Module … is
/// disabled!" no matter what the on-disk gates say. SIGTERM is the polite ask, but measured on
/// macOS 26.5 the agent ignores SIGTERM outright (and SIP refuses `launchctl kickstart -k`);
/// SIGKILL is the only signal that lands. That is safe exactly when the agent serves no FSKit
/// volume — launchd relaunches it on demand, and a fresh launch reads a fresh allowlist — so a
/// possibly-serving agent is left alone and the caller falls back to the manual guidance.
/// True when no stale agent remains.
#[cfg(target_os = "macos")]
async fn restart_fskit_agent() -> bool {
    let Some(pid) = fskit_agent_pid() else {
        // Not running: the next mount launches it fresh, which is exactly what we want.
        return true;
    };
    unsafe { libc::kill(pid, libc::SIGTERM) };
    if wait_pid_exit(pid, std::time::Duration::from_secs(2)).await {
        return true;
    }
    // Sampled immediately before the kill (the SIGTERM grace above is the racy window a
    // volume could attach in). A microscopic window remains — inherent to kill-by-pid.
    if fskit_agent_busy() {
        return false;
    }
    unsafe { libc::kill(pid, libc::SIGKILL) };
    wait_pid_exit(pid, std::time::Duration::from_secs(2)).await
}

/// Ask the live mount stack — not the files — whether fskit_agent will serve the module. A
/// mount against a loopback URL nothing listens on fails either way, and the failure text is
/// the verdict: "Module … is disabled!" is the agent's stale/disabled answer, a connection
/// error means the module was invoked, i.e. every gate is open end to end. The files alone
/// cannot answer this (measured on macOS 26.5: setup wrote the allowlist, every file read back
/// correct, and mounts still failed until the agent restarted). `None` when the probe can't
/// run or the error is unrecognized — then the on-disk gates remain the best evidence.
#[cfg(target_os = "macos")]
async fn probe_module_served() -> Option<bool> {
    let dir = std::env::temp_dir().join(format!("tlfs-probe-{}", std::process::id()));
    std::fs::create_dir_all(&dir).ok()?;
    // Port 1 (tcpmux): nothing listens there, so the module fails before any protocol
    // traffic — and without a real tlfs server behind the URL the mount cannot succeed, so
    // the probe can never leave a volume behind.
    // Both recognizable verdicts arrive in well under a second (the agent answers its
    // disabled verdict from memory; port 1 answers with an instant RST) — the timeout only
    // bounds a wedged fskitd, where the verdict is None anyway. kill_on_drop reaps the child
    // on timeout so probes never accumulate; the dir is removed only after the child is done.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new("/sbin/mount")
            .args(["-F", "-t", "tlfs", "tlfs://127.0.0.1:1/probe"])
            .arg(&dir)
            // The verdict is matched on message text; keep the tool side unlocalized.
            .env("LC_ALL", "C")
            .kill_on_drop(true)
            .output(),
    )
    .await;
    let verdict = (|| {
        let out = result.ok()?.ok()?;
        if out.status.success() {
            return Some(true);
        }
        let err = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        if err.contains(daemon::MODULE_DISABLED_MARKER) {
            Some(false)
        } else if err.contains("Connection refused") {
            Some(true)
        } else {
            None
        }
    })();
    let _ = std::fs::remove_dir(&dir);
    verdict
}

/// fskitd's (LiveFS) per-boot record of live FSKit mounts. Root-owned but world-readable.
#[cfg(target_os = "macos")]
const LIVEFS_SETTINGS: &str = "/Library/Application Support/livefsd/settings.plist";

/// fskitd records every live FSKit mount in [`LIVEFS_SETTINGS`]. A volume that vanishes
/// behind its back — fskit_agent killed while the volume was attached, a crashed extension
/// host — leaks its record, and every later mount at the same path dies at the "final mount
/// step" with "a file with the same name already exists" (measured on macOS 26.5; fskitd
/// logs "Failed to store the mount point in settings file!", NSCocoaErrorDomain 516). The
/// index of the stale record for `mountpoint`, so the error can print the exact `plutil
/// -remove mounts.<i>` remedy; `None` when the file is absent/unreadable or holds no record
/// for the path.
#[cfg(target_os = "macos")]
fn livefs_mounted_on() -> Option<Vec<String>> {
    let out = std::process::Command::new("plutil")
        .args(["-convert", "json", "-o", "-", LIVEFS_SETTINGS])
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    let settings: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    Some(
        settings
            .get("mounts")?
            .as_array()?
            .iter()
            .filter_map(|m| m.get("mountedOn").and_then(|p| p.as_str()))
            .map(str::to_string)
            .collect(),
    )
}

#[cfg(target_os = "macos")]
fn livefs_stale_record_index(mountpoint: &str) -> Option<usize> {
    livefs_mounted_on()?
        .iter()
        .position(|mounted_on| mounted_on == mountpoint)
}

/// Best-effort CLI substitute for the System Settings toggle, which is flaky on some machines
/// (measured: the pane failed to even show the FSKit entry after an OS upgrade). Elect the
/// plugin if needed, append the id to the allowlist (never rewriting one that didn't parse),
/// then prove readiness against the live agent: fskit_agent snapshots the allowlist at launch,
/// so an agent older than our write still refuses the module — restart it (see
/// restart_fskit_agent) and probe again. Success means a probe mount reached the module, not
/// that our writes landed. Settings remains the fallback.
///
/// No early return on already-open disk gates: that is precisely the state a stale agent
/// leaves behind, and re-running `tl fs setup` after a failed mount must repair it.
///
/// Deliberately invoked only from `tl fs setup` and the fresh-install bootstrap: this mutates
/// the same state the Settings toggle owns, so it runs on explicit user intent, never as a
/// side effect of a routine mount.
#[cfg(target_os = "macos")]
async fn enable_fskit_module() -> bool {
    let gates = FskitGates::read();
    if gates.registration != Some('+') {
        let _ = std::process::Command::new("pluginkit")
            .args(["-e", "use", "-i", FSKIT_MODULE_ID])
            .status();
    }
    if let Some(mut ids) = gates.modules
        && !ids.iter().any(|id| id == FSKIT_MODULE_ID)
    {
        ids.push(FSKIT_MODULE_ID.to_string());
        // A failed write means the allowlist gate can never open; don't wait out the poll.
        if !write_enabled_modules(&ids) {
            return false;
        }
    }
    // The election is asynchronous; poll the on-disk gates briefly instead of trusting our
    // own writes.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !FskitGates::read().ready() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    // Files right ≠ served: ask the live agent, restart it if it answers from a pre-write
    // snapshot, and ask again.
    match probe_module_served().await {
        Some(true) => true,
        None => {
            // No evidence either way (probe couldn't run, or unrecognized error text — e.g.
            // a future macOS rewording). The on-disk gates are open, so proceed, but say so
            // instead of silently claiming a verified end-to-end success.
            eprintln!(
                "{} could not verify the live agent (probe inconclusive); the on-disk gates \
                 are open — if mounts still fail, re-run `tl fs setup`",
                style("note:").yellow()
            );
            true
        }
        Some(false) => {
            if !restart_fskit_agent().await {
                return false;
            }
            probe_module_served().await != Some(false)
        }
    }
}

#[cfg(target_os = "macos")]
fn print_enable_instructions() {
    println!();
    println!(
        "{}",
        style("One manual step remains — enable the extension in System Settings:").bold()
    );
    println!("  System Settings -> General -> Login Items & Extensions -> File System");
    println!("  Extensions -> enable {}", style("TLFS").bold());
    println!();
    println!("Then mount with: tl fs mount <file-system> <path>");
    // Best-effort deep link into the extensions pane; the printed path is the contract.
    let _ = std::process::Command::new("open")
        .arg("x-apple.systempreferences:com.apple.ExtensionsPreferences")
        .status();
}

/// The macOS diagnosis: OS floor, install, and the two enablement gates — plus a probe of the
/// live agent when the gates read open, since fskit_agent answers mounts from the allowlist
/// snapshot it took at launch — then one ✓/✗ verdict with the single next action. Printed by
/// `--check`, and automatically whenever `setup` or a mount ends in a not-ready state — the
/// report is never hidden behind a flag.
#[cfg(target_os = "macos")]
async fn report_macos() {
    let installed = Path::new(FSKIT_APP_PATH).exists();
    let os = macos_version_supported();
    // OS floor first: when this fails nothing downstream can work, and it explains the
    // otherwise-baffling "extension never registers" state on older macOS.
    match &os {
        Ok(version) => println!(
            "{} macOS {} (meets the {MACOS_MIN_NAME} floor)",
            style("os:").dim(),
            if version.is_empty() {
                "(unknown)"
            } else {
                version
            },
        ),
        Err(msg) => println!("{} {}", style("os:").red().bold(), msg),
    }
    let gates = FskitGates::read();
    println!(
        "{} {}",
        style("app:").dim(),
        if installed {
            format!("installed at {FSKIT_APP_PATH}")
        } else {
            "not installed".to_string()
        }
    );
    println!(
        "{} {}",
        style("extension:").dim(),
        match gates.registration {
            Some('+') => "registered and elected".to_string(),
            Some('-') => "registered but not elected".to_string(),
            Some(other) => format!("registered (pluginkit state {other:?})"),
            None => "not registered".to_string(),
        }
    );
    // The gate mount(8) actually cares about: pluginkit election alone still fails with
    // "Module … is disabled!" until the id is in fskit_agent's allowlist.
    println!(
        "{} {}",
        style("fskit allowlist:").dim(),
        match gates.allowlisted() {
            Some(true) => "enabled",
            Some(false) => "NOT enabled (mount -F will report the module disabled)",
            None =>
                "unreadable — manage the toggle in System Settings (the CLI never \
                     rewrites an allowlist it cannot parse)",
        }
    );
    // A single verdict + the one next action, so the user never has to interpret the gates.
    println!();
    if os.is_err() {
        println!(
            "{} this macOS is too old for tl fs; nothing to do here.",
            style("✗").red().bold()
        );
    } else if !installed {
        println!(
            "{} not installed. Run `tl fs setup` to install and enable it.",
            style("✗").red().bold()
        );
    } else if gates.ready() {
        // Gates open on disk — but the serving agent may still hold a pre-enablement
        // snapshot (the gap behind "setup said enabled, mount said disabled").
        match probe_module_served().await {
            Some(false) => println!(
                "{} enabled on disk, but the running fskit_agent predates the enablement \
                 and still refuses the module. Run `tl fs setup` to restart it (or reboot).",
                style("✗").yellow().bold()
            ),
            Some(true) => println!(
                "{} ready — mount with: tl fs mount <file-system> <path>",
                style("✓").green().bold()
            ),
            None => println!(
                "{} gates are open on disk, but the live agent could not be probed \
                 (inconclusive). Mounts should work; if they fail, re-run `tl fs setup`.",
                style("~").yellow().bold()
            ),
        }
    } else if gates.allowlisted() == Some(false) || gates.registration != Some('+') {
        println!(
            "{} installed but disabled. Run `tl fs setup` to enable it (or turn on TLFS \
             under System Settings -> General -> Login Items & Extensions -> File System \
             Extensions).",
            style("✗").yellow().bold()
        );
    } else {
        println!(
            "{} the fskit allowlist is unreadable; enable TLFS under System Settings -> \
             General -> Login Items & Extensions -> File System Extensions.",
            style("✗").yellow().bold()
        );
    }
}

#[cfg(target_os = "macos")]
async fn setup_macos(from: Option<&str>, check_only: bool) -> Result<()> {
    let installed = Path::new(FSKIT_APP_PATH).exists();
    let os = macos_version_supported();
    if check_only {
        report_macos().await;
        return Ok(());
    }

    // Refuse to install on an OS that can never run the extension — otherwise the bundle lands
    // in /Applications but never registers, and the user chases a phantom. Show the full report
    // so the version line is right there with the error.
    if os.is_err() {
        report_macos().await;
        return Err(CliError::usage(
            "this macOS is too old for the TensorLake file-system extension (see the report \
             above)",
        ));
    }

    // Stage the app bundle. Priority: an explicit --from override, then the copy embedded in
    // this binary (official release builds), then the release asset matching this CLI version.
    let staging = std::env::temp_dir().join(format!("tlfs-setup-{}", std::process::id()));
    std::fs::create_dir_all(&staging)?;
    if from.is_none()
        && let Some(zip) = EMBEDDED_APP_ZIP
    {
        println!("Installing the TLFS app embedded in this CLI build.");
        let archive = staging.join("TLFS.app.zip");
        std::fs::write(&archive, zip)?;
        let app_src = unzip_app(&archive, &staging)?;
        return install_app(&app_src, installed, &staging).await;
    }
    let source = from.map(str::to_string).unwrap_or_else(default_app_url);
    let app_src: PathBuf = if source.starts_with("http://") || source.starts_with("https://") {
        println!("Downloading {source}");
        let response = reqwest::get(&source).await.map_err(anyhow::Error::from)?;
        if !response.status().is_success() {
            return Err(CliError::usage(format!(
                "download failed ({}): {source}\nIs the TLFS app published for this CLI \
                 version? Pass --from <path-or-url> to install a specific build.",
                response.status()
            )));
        }
        let archive = staging.join("TLFS.app.zip");
        std::fs::write(
            &archive,
            response.bytes().await.map_err(anyhow::Error::from)?,
        )?;
        unzip_app(&archive, &staging)?
    } else if source.ends_with(".zip") {
        unzip_app(Path::new(&source), &staging)?
    } else {
        PathBuf::from(&source)
    };
    install_app(&app_src, installed, &staging).await
}

/// Install a staged TLFS.app into /Applications, register its extension, and walk the user
/// through the System Settings toggle.
#[cfg(target_os = "macos")]
async fn install_app(app_src: &Path, already_installed: bool, staging: &Path) -> Result<()> {
    if !app_src
        .join("Contents/Extensions/TLFSModule.appex")
        .exists()
    {
        return Err(CliError::usage(format!(
            "{} does not look like a TLFS app bundle (no Contents/Extensions/TLFSModule.appex)",
            app_src.display()
        )));
    }

    // Install into /Applications with ditto (preserves signatures, xattrs, and the notarization
    // staple — a plain copy can strip what Gatekeeper checks).
    if already_installed {
        std::fs::remove_dir_all(FSKIT_APP_PATH).map_err(|e| {
            CliError::usage(format!(
                "could not replace {FSKIT_APP_PATH}: {e}. Unmount any tl fs mounts and retry \
                 (or remove it manually)."
            ))
        })?;
    }
    let status = std::process::Command::new("ditto")
        .arg(app_src)
        .arg(FSKIT_APP_PATH)
        .status()?;
    if !status.success() {
        return Err(CliError::usage(format!(
            "installing to {FSKIT_APP_PATH} failed; retry with write access to /Applications"
        )));
    }
    println!("Installed {FSKIT_APP_PATH}");

    // Launching the (headless) host app once is what makes LaunchServices register the embedded
    // extension on a fresh machine — no lsregister/pluginkit surgery on user installs.
    let _ = std::process::Command::new("open")
        .args(["-g", "-j", FSKIT_APP_PATH])
        .status();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let registered = loop {
        match appex_registration() {
            Some(_) => break true,
            None if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            None => break false,
        }
    };
    let _ = std::fs::remove_dir_all(staging);
    // Registered with LaunchServices; now flip the remaining gates (pluginkit election +
    // fskit_agent's allowlist) automatically — the System Settings toggle is flaky on some
    // machines, so it is the fallback rather than the happy path. On any not-ready outcome
    // (didn't register, election/allowlist didn't take), print the full diagnosis inline —
    // never make the user re-run with --check to find out what's wrong.
    if registered && enable_fskit_module().await {
        println!("Extension registered and enabled.");
        println!("Mount with: tl fs mount <file-system> <path>");
    } else {
        println!();
        report_macos().await;
        print_enable_instructions();
    }
    Ok(())
}

/// Mount's pre-flight: make sure the FSKit extension is ready before any workspace is
/// created — and only LOOK. Repair (election, allowlist writes, agent nudges) lives in
/// `tl fs setup`, which the user invokes deliberately: a routine mount must never silently
/// re-enable an extension someone turned off in System Settings. The one exception is a
/// missing install, where mount bootstraps a fresh machine by running setup once. Only mount
/// needs this — every other command talks to the server or to an existing mount's daemon.
#[cfg(target_os = "macos")]
async fn ensure_fskit_ready() -> Result<()> {
    // OS floor first — on older macOS the extension can never register, so every gate below
    // would read "not installed" and send the user in circles.
    if let Err(msg) = macos_version_supported() {
        return Err(CliError::usage(msg));
    }
    let gates = FskitGates::read();
    if gates.ready() {
        return Ok(());
    }
    if gates.registration.is_none() {
        eprintln!(
            "{} the TensorLake file-system extension is not installed; running `tl fs setup` \
             first",
            style("note:").yellow()
        );
        // setup() prints its own full diagnosis on any not-ready outcome.
        setup(None, false).await?;
        if FskitGates::read().ready() {
            return Ok(());
        }
    } else {
        // Installed but disabled — don't repair from a routine mount (that would override the
        // user's Settings toggle); just show the full diagnosis so the fix is obvious.
        report_macos().await;
    }
    Err(CliError::usage(
        "the TensorLake file-system extension is disabled; run `tl fs setup` to enable it \
         (or flip it in System Settings), then re-run the mount",
    ))
}

/// Who a new mount belongs to: the human who asked for it. Under `sudo tl fs mount` that is
/// the invoking user (SUDO_UID/SUDO_GID), not root — the daemon presents every file as theirs
/// and mounts with allow_other so the volume is actually usable by them.
fn mount_owner() -> (u32, u32) {
    #[cfg(unix)]
    {
        let sudo_id = |key: &str| std::env::var(key).ok().and_then(|v| v.parse::<u32>().ok());
        if unsafe { libc::geteuid() } == 0
            && let (Some(uid), Some(gid)) = (sudo_id("SUDO_UID"), sudo_id("SUDO_GID"))
        {
            return (uid, gid);
        }
        unsafe { (libc::getuid(), libc::getgid()) }
    }
    #[cfg(not(unix))]
    {
        (0, 0)
    }
}

/// Mount's pre-flight on Linux. Unprivileged FUSE needs /dev/fuse to be openable plus the
/// setuid fusermount3 helper (fuse3 package) — mount(2) itself needs CAP_SYS_ADMIN regardless
/// of device permissions, and not every environment grants either. `sudo tl fs mount` is the
/// universal fallback: root mounts directly, and the volume is presented to (and owned by)
/// the invoking user, not root. Checking up front turns "mount daemon did not come up" into
/// the exact missing piece.
#[cfg(target_os = "linux")]
fn helper_on_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|path| std::env::split_paths(&path).any(|dir| dir.join(name).is_file()))
}

#[cfg(target_os = "linux")]
fn ensure_fuse_ready() -> Result<()> {
    if unsafe { libc::geteuid() } == 0 {
        let (uid, _) = mount_owner();
        if uid != 0 {
            eprintln!(
                "{} mounting via sudo: the volume is presented to uid {uid} ({}). Mount state \
                 lives in root's home — run the other commands (snapshot, promote, unmount) \
                 with sudo too.",
                style("note:").yellow(),
                std::env::var("SUDO_USER").unwrap_or_else(|_| "the invoking user".to_string()),
            );
        }
        return Ok(());
    }
    let dev_openable = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .is_ok();
    let helper = helper_on_path("fusermount3") || helper_on_path("fusermount");
    if !dev_openable || !helper {
        // Print the full diagnosis inline (the same report as `tl fs setup`) so the exact
        // missing pieces and their fixes are right here — never hidden behind a separate flag.
        println!();
        diagnose_linux();
        return Err(CliError::usage(
            "the mount prerequisites are not set up (see the diagnosis above)",
        ));
    }
    if !Path::new("/etc/mtab").exists() {
        eprintln!(
            "{} /etc/mtab is missing; fusermount3 will refuse to unmount later. Fix:\n  \
             sudo ln -s /proc/self/mounts /etc/mtab",
            style("warning:").yellow()
        );
    }
    Ok(())
}

/// `tl fs setup` / `--check` on Linux: report each thing an unprivileged FUSE mount needs, then
/// a single verdict + the exact fix. Running as root short-circuits — mount(2) is direct and
/// needs none of the userspace plumbing.
#[cfg(target_os = "linux")]
fn diagnose_linux() {
    if unsafe { libc::geteuid() } == 0 {
        println!(
            "{} running as root: mounts use mount(2) directly — no /dev/fuse permission, \
             fusermount3, or fuse3 package required.",
            style("✓").green().bold()
        );
        return;
    }

    let dev_fuse = Path::new("/dev/fuse");
    let dev_exists = dev_fuse.exists();
    let dev_openable = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dev_fuse)
        .is_ok();
    println!(
        "{} {}",
        style("/dev/fuse:").dim(),
        if !dev_exists {
            "missing (kernel has no FUSE support?)".to_string()
        } else if dev_openable {
            "openable read/write".to_string()
        } else {
            "present but not readable/writable (needs mode 666, or use sudo)".to_string()
        }
    );

    let helper = helper_on_path("fusermount3") || helper_on_path("fusermount");
    println!(
        "{} {}",
        style("fusermount3:").dim(),
        if helper {
            "found (fuse3 installed)"
        } else {
            "not found (install the fuse3 package, or use sudo)"
        }
    );

    let mtab = Path::new("/etc/mtab").exists();
    println!(
        "{} {}",
        style("/etc/mtab:").dim(),
        if mtab {
            "present"
        } else {
            "missing (unprivileged unmount will fail without it)"
        }
    );

    println!();
    if dev_openable && helper && mtab {
        println!(
            "{} ready — mount with: tl fs mount <file-system> <path>",
            style("✓").green().bold()
        );
    } else {
        println!(
            "{} unprivileged FUSE is not fully set up. Fastest path — run with sudo (works \
             everywhere; the mount is presented to your user):",
            style("✗").yellow().bold()
        );
        println!("  sudo tl fs mount <file-system> <path>");
        println!("Or enable unprivileged FUSE once (needs root):");
        if !helper {
            println!("  sudo apt-get install fuse3   # provides the setuid fusermount3 helper");
        }
        if dev_exists && !dev_openable {
            println!("  sudo chmod 666 /dev/fuse");
        }
        if !mtab {
            println!("  sudo ln -s /proc/self/mounts /etc/mtab");
        }
    }
}

/// Unpack a TLFS app archive with ditto (keeps signatures/staple intact) and return the .app.
#[cfg(target_os = "macos")]
fn unzip_app(archive: &Path, staging: &Path) -> Result<PathBuf> {
    let dest = staging.join("unpacked");
    std::fs::create_dir_all(&dest)?;
    let status = std::process::Command::new("ditto")
        .arg("-x")
        .arg("-k")
        .arg(archive)
        .arg(&dest)
        .status()?;
    if !status.success() {
        return Err(CliError::usage(format!(
            "could not unpack {}",
            archive.display()
        )));
    }
    std::fs::read_dir(&dest)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|ext| ext == "app"))
        .ok_or_else(|| CliError::usage(format!("no .app found inside {}", archive.display())))
}

/// The two workspace-create failures `tl fs mount` can recover from, read off the server's
/// answer. Mount tries the create outright (its one required round trip) instead of
/// pre-flighting with list-repos and ref-status calls; these are the answers that pick the
/// recovery path. Anything else propagates as-is.
enum CreateRecovery {
    /// 404: no repo by that name — a bare target may be a workspace id to attach.
    RepoMissing,
    /// 400 on base resolution — an unborn default branch is seedable; other branches are not.
    BaseUnresolved,
}

fn create_recovery(e: &tensorlake::error::SdkError) -> Option<CreateRecovery> {
    let tensorlake::error::SdkError::ServerError { status, message } = e else {
        return None;
    };
    match status.as_u16() {
        // Require the server's actual wording ("repo <id> not found"), not just any 404 body:
        // a misrouted base URL answered by a generic proxy ("404 page not found") must surface
        // raw, not masquerade as a missing file system. Pinned server-side by the e2e test
        // `workspace_create_errors_keep_the_cli_recovery_contract`.
        404 if message.contains("repo") && message.contains("not found") => {
            Some(CreateRecovery::RepoMissing)
        }
        400 if message.contains("does not resolve to a commit")
            || message.contains("has no commits") =>
        {
            Some(CreateRecovery::BaseUnresolved)
        }
        _ => None,
    }
}

/// A workspace needs a base commit, but a file system fresh out of `tl git create` has an
/// unborn default branch. Seed it with an empty initial commit so the first mount just works.
/// No existence pre-check: the caller just learned from the failed create that the branch has
/// no commits, and a raced concurrent seed is benign anyway — the commit endpoint defaults its
/// base to the branch tip, so the push lands a harmless empty commit and the retried create
/// still succeeds.
async fn ensure_seeded(session: &FsSession, default_branch: &str, repo: &str) -> Result<()> {
    let (user, token) = session.creds();
    session
        .client
        .push_files(
            &session.project_id,
            repo,
            user,
            token,
            Vec::new(),
            PushOptions {
                branch: default_branch.to_string(),
                message: "Initialize file system".to_string(),
                ..Default::default()
            },
        )
        .await?;
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
        return Err(not_a_mount_error(format!(
            "{mountpoint} is not a tl fs mount; run `tl fs mount` first"
        )));
    };
    Ok((mountpoint, PathBuf::from(state_dir)))
}

/// Build a "not a mount"-shaped usage error, appending the binding-registry corruption note
/// when the lenient binding dispatch has silently degraded this session — the path may
/// really be a plain-directory binding the corrupt registry can no longer name, and a
/// `--json`/CI consumer only sees the error, never the stderr warning.
fn not_a_mount_error(message: String) -> CliError {
    match plaindir::registry_corruption_note() {
        Some(note) => CliError::usage(format!("{message}\n{note}")),
        None => CliError::usage(message),
    }
}

/// Whether `path` is a registered mountpoint or plain-directory binding root. Used to
/// disambiguate optional positional args (`tl fs diff <a> <b>` vs `tl fs diff <path> <a>`) —
/// a binding here resolves as the command's path, whose dispatch then answers with the
/// binding-appropriate behavior (or a clear v1 "not supported").
pub fn is_registered_mount(path: &Path) -> bool {
    state_dir_for(path).is_ok() || plaindir::binding_for_lenient(path).is_some()
}

/// The registered mountpoint or bound directory containing the current directory (the
/// deepest one, for nesting). This is what path-addressed commands operate on when no path
/// argument is given.
pub fn mount_containing_cwd() -> Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let cwd = cwd.canonicalize().unwrap_or(cwd);
    let mut roots: Vec<PathBuf> = registry_load().keys().map(PathBuf::from).collect();
    roots.extend(
        plaindir::binding_roots_lenient()
            .into_iter()
            .map(PathBuf::from),
    );
    roots
        .into_iter()
        .filter(|root| {
            // Registry keys keep the leaf component un-canonicalized (it may be a live FUSE
            // fs); compare against both spellings so a symlinked leaf still matches the
            // canonicalized CWD.
            cwd.starts_with(root)
                || root
                    .canonicalize()
                    .is_ok_and(|canonical| cwd.starts_with(canonical))
        })
        .max_by_key(|root| root.components().count())
        .ok_or_else(|| {
            not_a_mount_error(format!(
                "{} is not inside a tl fs mount or bound directory; pass the directory \
                 explicitly",
                cwd.display()
            ))
        })
}

/// Resolve the optional mounted-directory argument of path-addressed commands: an explicit
/// path wins; otherwise default to the mount containing the current directory.
pub fn resolve_mount_path(path: Option<PathBuf>) -> Result<PathBuf> {
    match path {
        Some(path) => Ok(path),
        None => mount_containing_cwd(),
    }
}

/// Whether a positional argument is unmistakably a filesystem path rather than a snapshot,
/// ref, or branch name: absolute, explicitly relative (`./`, `../`), or naming an existing
/// directory. Branch names like `feature/x` contain separators too, so a bare separator is
/// not enough. Used to keep the explicit "not a tl fs mount" error for typo'd or stale mount
/// paths instead of silently reinterpreting them.
fn is_path_shaped(value: &Path) -> bool {
    use std::path::Component;
    value.is_absolute()
        || matches!(
            value.components().next(),
            Some(Component::CurDir | Component::ParentDir)
        )
        || value.is_dir()
}

/// `tl fs diff` positionals are ambiguous once the mount path is optional: one leading arg can
/// be either the mounted directory or the older snapshot. Treat it as the mount when it's a
/// registered mountpoint; reject it when it's path-shaped but unregistered (a typo or stale
/// mount, not a snapshot ref); otherwise shift everything right and infer the mount from the
/// current directory.
pub fn resolve_diff_args(
    path: Option<PathBuf>,
    a: Option<String>,
    b: Option<String>,
) -> Result<(PathBuf, Option<String>, Option<String>)> {
    match path {
        Some(path) if is_registered_mount(&path) => Ok((path, a, b)),
        Some(not_a_mount) => {
            if b.is_some() || is_path_shaped(&not_a_mount) {
                // Three args, or a path-shaped first arg that isn't registered: surface the
                // real problem instead of treating the path as a snapshot ref.
                return Err(not_a_mount_error(format!(
                    "{} is not a tl fs mount; run `tl fs mount` first",
                    not_a_mount.display()
                )));
            }
            Ok((
                mount_containing_cwd()?,
                Some(not_a_mount.to_string_lossy().into_owned()),
                a,
            ))
        }
        None => Ok((mount_containing_cwd()?, a, b)),
    }
}

/// Guard for `tl fs promote <branch>` / `tl fs restore <version>` with the mount path omitted:
/// when the sole positional is itself a mounted directory (or an explicit path), the user
/// almost certainly forgot the branch/version — without this, promote would publish the CWD
/// mount onto a branch literally named after the directory.
pub fn reject_mount_like_positional(value: &str, what: &str, usage: &str) -> Result<()> {
    let as_path = Path::new(value);
    if is_registered_mount(as_path) || is_path_shaped(as_path) {
        return Err(CliError::usage(format!(
            "{value} looks like a mounted directory, not a {what}; usage: {usage}"
        )));
    }
    Ok(())
}

/// Path-addressed commands (snapshot/promote/status/restore/diff/unmount) know their scope
/// from the mount they operate on; seed the auth context from the mount state so they work
/// from any working directory. Without this, running `tl fs snapshot` from a CWD with no
/// `.tensorlake/config.toml` up-tree dropped into the interactive init flow — which, run from
/// inside the mount, wrote its config INTO the workspace and the snapshot sealed it.
pub fn hydrate_scope_from_mount(ctx: &mut CliContext, path: &Path) {
    if ctx.effective_project_id().is_some() {
        return;
    }
    // Plain-directory bindings carry the same scope record as mounts (binding.json).
    if let Some((_, binding_state)) = plaindir::binding_for_lenient(path)
        && let Ok(binding) = plaindir::load_binding(&binding_state)
    {
        ctx.project_id = Some(binding.project_id);
        if ctx.organization_id.is_none() {
            ctx.organization_id = binding.organization_id;
        }
        return;
    }
    let Ok((_, state_dir)) = state_dir_for(path) else {
        return;
    };
    let Ok(state) = daemon::load_mount_state(&state_dir) else {
        return;
    };
    ctx.project_id = Some(state.project_id);
    if ctx.organization_id.is_none() {
        ctx.organization_id = state.organization_id;
    }
}

#[cfg(unix)]
fn daemon_alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

// Mounts don't exist off unix; compile stub, like the kernel-view helpers below.
#[cfg(not(unix))]
fn daemon_alive(_pid: i32) -> bool {
    false
}

/// Local mounts whose daemon is still running: `(mountpoint, state)`.
fn live_mounts() -> Vec<(String, MountState)> {
    registry_load()
        .iter()
        .filter_map(|(mountpoint, state_dir)| {
            let state_dir = PathBuf::from(state_dir.as_str()?);
            let state = daemon::load_mount_state(&state_dir).ok()?;
            let alive = daemon::daemon_pid(&state_dir).is_some_and(daemon_alive);
            alive.then(|| (mountpoint.clone(), state))
        })
        .collect()
}

/// Where a workspace is live-mounted on this machine, if anywhere.
fn live_mount_of(workspace_id: &str) -> Option<String> {
    live_mounts()
        .into_iter()
        .find(|(_, state)| state.workspace_id == workspace_id)
        .map(|(mountpoint, _)| mountpoint)
}

/// State dir for a new mount of `workspace_id`. The workspace's canonical dir is reused when
/// free (that's what lets a plain re-mount resume its local cache); when another registered
/// mount of the same workspace holds it, pick a fresh suffixed dir — concurrent second mounts
/// (read-only views especially) must never share overlay state with the writer.
fn alloc_state_dir(workspace_id: &str) -> PathBuf {
    let registered: std::collections::HashSet<PathBuf> = registry_load()
        .values()
        .filter_map(|v| v.as_str().map(PathBuf::from))
        .collect();
    let root = daemon::state_dir_root();
    let mut n = 1u32;
    loop {
        let candidate = if n == 1 {
            root.join(workspace_id)
        } else {
            root.join(format!("{workspace_id}.{n}"))
        };
        if !registered.contains(&candidate) {
            return candidate;
        }
        n += 1;
    }
}

// ---------------------------------------------------------------------------------------------
// Mount / unmount
// ---------------------------------------------------------------------------------------------

/// `tl fs mount <target> <path>` — how workspaces are born and revived.
/// `<file-system>[:<ref-or-commit>]` creates a new workspace on that file system;
/// `<workspace-id>` (or a unique prefix; see `tl fs ls`) mounts an existing one, resuming at
/// its last snapshot. Reads stream lazily; nothing is copied to disk up front.
#[allow(clippy::too_many_arguments)]
pub async fn mount(
    ctx: &CliContext,
    target: &str,
    path: &Path,
    mode: WritePolicy,
    shared_rw: bool,
    auto_commit_interval_secs: Option<u64>,
    foreground: bool,
    trace_ops: bool,
    log_level: &str,
) -> Result<()> {
    #[cfg(not(target_os = "macos"))]
    let _ = trace_ops;
    // Bail before creating a workspace or spawning the daemon-wait loop.
    if cfg!(not(unix)) {
        return Err(CliError::usage(
            "tl fs mount is supported on Linux (FUSE) and macOS (FSKit) only.",
        ));
    }
    // The CLI's own phase timings (`phase=… "mount timing"`, debug level) surface on stderr
    // through the same subscriber the daemon uses for daemon.log — pass `--log-level debug`
    // to see them; the default "info" keeps mount's stderr clean for scripts.
    daemon::init_logging(log_level)?;
    let started = std::time::Instant::now();
    #[cfg(target_os = "macos")]
    ensure_fskit_ready().await?;
    #[cfg(target_os = "linux")]
    ensure_fuse_ready()?;
    let (name, base) = match target.split_once(':') {
        Some((name, base)) => (name, Some(base.to_string())),
        None => (target, None),
    };
    if shared_rw && mode == WritePolicy::Ro {
        return Err(CliError::usage(
            "--shared-rw publishes every snapshot; it cannot be combined with --mode ro",
        ));
    }
    if auto_commit_interval_secs.is_some() && mode == WritePolicy::Ro {
        return Err(CliError::usage(
            "--auto-commit-interval-secs seals local writes; a read-only mount has none",
        ));
    }
    if shared_rw && base.is_none() {
        return Err(CliError::usage(
            "--shared-rw needs the branch to publish to: tl fs mount <file-system>:<branch> ...",
        ));
    }
    // A volume whose daemon died stays attached (on macOS the FSKit extension proxies to the
    // daemon over TCP, so the kernel serves the mountpoint as ECONNREFUSED forever) and turns
    // every operation on the path into a confusing error — mkdir below would say "File exists".
    // Name the actual problem and the command that clears it.
    #[cfg(target_os = "macos")]
    {
        let mountpoint = canonical_mountpoint(path)?;
        if daemon::still_mounted(Path::new(&mountpoint)) {
            let live = state_dir_for(path)
                .ok()
                .and_then(|(_, state_dir)| daemon::daemon_pid(&state_dir))
                .is_some_and(daemon_alive);
            return Err(CliError::usage(if live {
                format!(
                    "{mountpoint} is already mounted; unmount it first: tl fs unmount \
                     {mountpoint}"
                )
            } else {
                format!(
                    "{mountpoint} still has a previous mount attached with no daemon behind \
                     it (a killed mount leaves the volume in place). Detach it with: tl fs \
                     unmount {mountpoint}"
                )
            }));
        }
        if let Some(index) = livefs_stale_record_index(&mountpoint) {
            // A record backed by a volume that is still attached (any filesystem type) is a
            // LIVE record, not a stale one — removing it would corrupt fskitd's view of a
            // healthy mount. The only correct guidance there is "this path is taken".
            if daemon::mounted_at(&mountpoint) {
                return Err(CliError::usage(format!(
                    "{mountpoint} already hosts a mounted volume; unmount it or pick a \
                     different path"
                )));
            }
            // The remedy self-verifies at execution time: livefsd's mounts array shifts as
            // volumes attach/detach, so a frozen index could point at a live record by the
            // time the user pastes the command — the guard re-checks the entry still names
            // this path before removing anything.
            return Err(CliError::usage(format!(
                "macOS still has a record of a dead mount at {mountpoint} (a volume that \
                 vanished without a proper unmount), and fskitd refuses to mount there again \
                 (\"a file with the same name already exists\"). Clear it with:\n  sudo sh \
                 -c '[ \"$(plutil -extract mounts.{index}.mountedOn raw \
                 \"{LIVEFS_SETTINGS}\")\" = \"{mountpoint}\" ] && plutil -remove \
                 mounts.{index} \"{LIVEFS_SETTINGS}\" || echo \"records shifted; re-run tl \
                 fs mount for a fresh command\"'\n  sudo launchctl kickstart -k \
                 system/com.apple.filesystems.fskitd\nor reboot, or mount at a different \
                 path."
            )));
        }
    }
    // A mountpoint must not overlap a plain-directory binding in either direction: the
    // binding's scanner would walk the kernel volume, and the mount would shadow the bound
    // files. Checked before any server-side workspace is created.
    plaindir::assert_no_binding_overlap(&canonical_mountpoint(path)?)?;
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
    let session = FsSession::open(ctx, None).await?;
    let (user, token) = session.creds();
    tracing::debug!(
        phase = "session",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "mount timing"
    );
    let workspace_started = std::time::Instant::now();

    // `<file-system>[:<base>]` creates a workspace; a bare target that names no file system is
    // resolved as a workspace id (unique prefix) and attached. Attach = reconnect: the
    // workspace ref (and everything snapshotted onto it) survived whatever happened to the
    // previous mount — sandbox crash, timeout, unmount.
    //
    // The create is attempted outright — the common path's one required round trip — and the
    // server's answer picks the slow path when one applies: 404 means `name` is no file
    // system (perhaps a workspace id: attach), an unresolvable base may be an unborn default
    // branch (seed and retry). The old pre-flight (list repos, ref-status the base) re-derived
    // what the create response already says, at two extra round trips per mount.
    enum Resolved {
        Created(WorkspaceInfo),
        Attached(String, WorkspaceInfo),
    }
    let create_req = CreateWorkspaceRequest {
        base: base.clone(),
        shared_target: shared_rw.then(|| base.clone().expect("guarded above")),
        ..Default::default()
    };
    // One create call site for both the first attempt and the post-seed retry, so the two can
    // never drift apart.
    let try_create = || {
        session
            .client
            .create_workspace(&session.project_id, name, user, token, &create_req)
    };
    let resolved = match try_create().await {
        Ok(ws) => Resolved::Created(ws.into_inner()),
        Err(e) => match create_recovery(&e) {
            // No file system by this name and no branch was named: try it as a workspace id.
            Some(CreateRecovery::RepoMissing) if base.is_none() => {
                let fs_names = file_system_names(&session).await?;
                match resolve_workspace(&session, &fs_names, name).await? {
                    Some((repo, ws)) => Resolved::Attached(repo, ws),
                    None => {
                        return Err(CliError::usage(format!(
                            "no file system or workspace matches {name:?}. See `tl fs ls`, or \
                             create the file system first: tl git create {name}"
                        )));
                    }
                }
            }
            Some(CreateRecovery::RepoMissing) => {
                return Err(CliError::usage(format!(
                    "no file system named {name:?}; create it first: tl git create {name}"
                )));
            }
            // Seed an unborn default branch whether it is implied OR named (either spelling):
            // a fresh `tl git create` repo has no commits, and `tl fs mount repo:main` used to
            // fail with `base "main" does not resolve to a commit` while plain
            // `tl fs mount repo` worked. Writable mounts only — a read-only view must never
            // write to the server (and with read-scoped credentials the seed push would fail
            // opaquely); ro keeps the clear server error. Other branch names stay strict —
            // seeding cannot conjure them.
            Some(CreateRecovery::BaseUnresolved) => {
                let file_systems = session
                    .client
                    .list_repos_with_credential(&session.project_id, user, token)
                    .await?
                    .into_inner();
                let Some(fs) = file_systems.repos.iter().find(|r| r.name == name) else {
                    return Err(e.into());
                };
                let default_branch = fs.default_branch.clone();
                let names_default = base.as_deref().is_none_or(|b| {
                    b == default_branch || b.strip_prefix("refs/heads/") == Some(&default_branch)
                });
                if !names_default || mode == WritePolicy::Ro {
                    return Err(e.into());
                }
                ensure_seeded(&session, &default_branch, name).await?;
                Resolved::Created(try_create().await?.into_inner())
            }
            None => return Err(e.into()),
        },
    };
    tracing::debug!(
        phase = "workspace",
        attached = matches!(resolved, Resolved::Attached(..)),
        elapsed_ms = workspace_started.elapsed().as_millis() as u64,
        "mount timing"
    );

    // `start_oid` hands the daemon the commit this response resolved the view to, letting the
    // mount core overlap its serve probe with ref resolution (one startup round trip instead
    // of two chained). The exception is a writable attach of a shared-rw workspace: its view
    // follows the target branch — a ref this response says nothing about — so the daemon
    // resolves that one serially.
    let (repo, ws, attached, read_only, follow_ref, start_oid) = match resolved {
        Resolved::Attached(repo, ws) => {
            if shared_rw {
                return Err(CliError::usage(
                    "--shared-rw is chosen when creating a workspace on a branch: tl fs mount \
                     <file-system>:<branch> --shared-rw <path>",
                ));
            }
            // Single-writer by default: a workspace attached elsewhere — live mount OR
            // plain-directory binding (a binding is always a writer) — attaches read-only
            // unless the user explicitly takes writes with --mode rw.
            // Advisory only (write-policy default): unreadable binding state degrades to
            // "not attached" here — the destructive path (`tl fs rm`) stays fail-closed.
            let attached_at = live_mount_of(&ws.id).or_else(|| {
                plaindir::binding_using_workspace(&ws.id)
                    .ok()
                    .flatten()
                    .map(|root| format!("{root} (plain-directory binding)"))
            });
            let read_only = match mode {
                WritePolicy::Rw => false,
                WritePolicy::Ro => true,
                WritePolicy::Auto => attached_at.is_some(),
            };
            match (&attached_at, mode) {
                (Some(at), WritePolicy::Auto) => eprintln!(
                    "{} workspace is already attached at {at}; mounting read-only (pass \
                     --mode rw to mount it writable anyway)",
                    style("note:").yellow(),
                ),
                (Some(at), WritePolicy::Rw) => eprintln!(
                    "{} workspace is also writable at {at}; two writers race snapshots",
                    style("warning:").yellow(),
                ),
                _ => {}
            }
            // A read-only view follows the workspace ref, so it sees each snapshot as the
            // writer seals one; a writable attach of a shared-rw workspace keeps following the
            // branch its snapshots publish to — and only that branch case gets no start hint,
            // since this response resolved the workspace ref (`head`), not the branch.
            let (follow_ref, start_oid) = if read_only {
                (Some(ws.ref_name.clone()), Some(ws.head.clone()))
            } else {
                match &ws.shared_target {
                    Some(target) => (Some(format!("refs/heads/{target}")), None),
                    None => (None, Some(ws.head.clone())),
                }
            };
            (repo, ws, true, read_only, follow_ref, start_oid)
        }
        Resolved::Created(ws) => {
            let read_only = mode == WritePolicy::Ro;
            // What the view follows. Writable workspaces follow their own ref; shared-rw
            // follows the branch it publishes to, so every writer's view converges on the
            // reconciled branch rather than staying pinned to its own snapshots. A read-only
            // view follows the named branch (or the repo HEAD's branch) so new commits appear —
            // except a fixed commit base, which is a pinned view that never advances.
            let follow_ref = if shared_rw {
                Some(format!(
                    "refs/heads/{}",
                    base.clone()
                        .expect("shared-rw requires <file-system>:<branch>")
                ))
            } else if read_only {
                match &base {
                    Some(b) if b.len() == 40 && b.bytes().all(|c| c.is_ascii_hexdigit()) => {
                        Some(ws.ref_name.clone())
                    }
                    Some(b) => Some(format!("refs/heads/{b}")),
                    None => {
                        let base_ref = ws.base_ref.clone().unwrap_or_default();
                        if !base_ref.starts_with("refs/heads/") {
                            // The read session never got a branch to follow; don't leak the
                            // workspace that was just created for it.
                            let _ = session
                                .client
                                .delete_workspace(&session.project_id, name, user, token, &ws.id)
                                .await;
                            return Err(CliError::usage(
                                "--mode ro follows a branch, and the repo HEAD did not resolve \
                                 to one; name it explicitly: tl fs mount <file-system>:<branch> \
                                 --mode ro <path>",
                            ));
                        }
                        Some(base_ref)
                    }
                }
            } else {
                None
            };
            // Everything a fresh workspace can follow was resolved by the create response
            // itself: the workspace ref sits at `head` (== base), and a followed branch is the
            // one `base` was just resolved from (a snapshot racing in between is caught by the
            // daemon's first follow poll).
            let start_oid = Some(ws.head.clone());
            (
                name.to_string(),
                ws,
                false,
                read_only,
                follow_ref,
                start_oid,
            )
        }
    };

    // An attach can resolve read-only implicitly (the workspace is live-mounted elsewhere);
    // nothing server-side was created on that path, so erroring here leaks nothing.
    if auto_commit_interval_secs.is_some() && read_only {
        return Err(CliError::usage(
            "--auto-commit-interval-secs needs a writable mount; this workspace is already \
             mounted elsewhere and attached read-only (pass --mode rw to take writes)",
        ));
    }

    let mountpoint = canonical_mountpoint(path)?;
    let state_dir = alloc_state_dir(&ws.id);
    let (owner_uid, owner_gid) = mount_owner();
    daemon::save_mount_state(
        &state_dir,
        &MountState {
            project_id: session.project_id.clone(),
            organization_id: ctx.effective_organization_id(),
            owner_uid: Some(owner_uid),
            owner_gid: Some(owner_gid),
            repo: repo.clone(),
            workspace_id: ws.id.clone(),
            ref_name: ws.ref_name.clone(),
            mountpoint: PathBuf::from(&mountpoint),
            follow_ref,
            read_only: Some(read_only),
            auto_commit_interval_secs,
            start_oid,
        },
    )?;
    registry_add(&mountpoint, &state_dir)?;

    if foreground {
        #[cfg(target_os = "macos")]
        vfsserver::TRACE_OPS.store(trace_ops, std::sync::atomic::Ordering::Relaxed);
        return daemon::run(ctx, &state_dir, log_level).await;
    }

    // Detach the daemon and wait for its control socket to answer. Its stderr — where the
    // tracing subscriber writes — lands in the state dir's daemon.log (`tl fs status` prints
    // the path), so a daemon that dies on startup (no /dev/fuse access, missing fusermount3,
    // FSKit extension disabled) explains itself instead of just never answering.
    let exe = std::env::current_exe()?;
    let daemon_log = state_dir.join("daemon.log");
    std::process::Command::new(exe)
        .args(["fs", "daemon", "--state-dir"])
        .arg(&state_dir)
        .args(["--log-level", log_level])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&daemon_log)?)
        .spawn()?;
    let daemon_started = std::time::Instant::now();
    let deadline = daemon_started + std::time::Duration::from_secs(20);
    // Ramp the readiness poll: a healthy daemon (cached credential, caller-resolved commit)
    // answers within tens of milliseconds, so a flat 250ms grid would dominate its startup;
    // one that needs real work still gets probed only ~4 times a second.
    let mut backoff = std::time::Duration::from_millis(15);
    loop {
        match daemon::control(&state_dir, "ping").await {
            Ok(resp) => {
                tracing::debug!(
                    phase = "daemon",
                    elapsed_ms = daemon_started.elapsed().as_millis() as u64,
                    total_ms = started.elapsed().as_millis() as u64,
                    "mount timing"
                );
                if attached {
                    println!(
                        "Mounted workspace {} ({}) at {}{}",
                        short_id(&ws.id),
                        repo,
                        mountpoint,
                        if read_only {
                            ", read-only, follows its snapshots"
                        } else {
                            ", resumed at its last snapshot"
                        },
                    );
                } else {
                    println!(
                        "Mounted {}:{} at {} (workspace {}{})",
                        repo,
                        ws.base_ref.as_deref().unwrap_or(&ws.base[..12]),
                        mountpoint,
                        short_id(&ws.id),
                        if shared_rw {
                            ", snapshots auto-publish to the branch"
                        } else if read_only {
                            ", read-only, follows the branch"
                        } else {
                            ""
                        },
                    );
                }
                if read_only {
                    println!(
                        "Reading commit {}; new commits appear as the followed ref advances.",
                        resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                    );
                } else {
                    println!(
                        "Lower commit {}. Work in the mount, then: tl fs snapshot {}",
                        resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                        path.display()
                    );
                }
                if let Some(secs) = auto_commit_interval_secs {
                    println!(
                        "Auto-commit: local changes seal into a snapshot every {secs}s (async)."
                    );
                }
                return Ok(());
            }
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_millis(250));
            }
            Err(e) => {
                registry_remove(&mountpoint)?;
                // A workspace we just created is useless without its daemon; an attached one
                // predates this mount and is not ours to destroy.
                if !attached {
                    let _ = session
                        .client
                        .delete_workspace(&session.project_id, &repo, user, token, &ws.id)
                        .await;
                }
                // The daemon's own last words are the diagnosis; read them before the state
                // dir (and the log with it) goes away.
                let last_words = std::fs::read_to_string(&daemon_log)
                    .ok()
                    .map(|log| {
                        let mut tail: Vec<&str> =
                            log.lines().filter(|l| !l.trim().is_empty()).collect();
                        tail = tail.split_off(tail.len().saturating_sub(5));
                        tail.join("\n  ")
                    })
                    .filter(|tail| !tail.is_empty())
                    .map(|tail| format!(" Daemon log:\n  {tail}\n"))
                    .unwrap_or_default();
                let _ = std::fs::remove_dir_all(&state_dir);
                #[cfg(target_os = "macos")]
                let os_hint = "macOS mounts need the TensorLake file-system extension; run \
                               `tl fs setup` to diagnose and repair it.";
                #[cfg(target_os = "linux")]
                let os_hint = "Linux mounts need /dev/fuse accessible (mode 666) and the \
                               fuse3 package (fusermount3 + /etc/mtab); run `tl fs setup` to \
                               diagnose.";
                #[cfg(not(any(target_os = "macos", target_os = "linux")))]
                let os_hint = "tl fs mounts are supported on Linux (FUSE) and macOS (FSKit) \
                               only.";
                return Err(CliError::usage(format!(
                    "mount daemon did not come up: {e}.{last_words}\n{os_hint}"
                )));
            }
        }
    }
}

/// Unmount: stop the daemon (unmounts the kernel fs) and forget the mount. The workspace — and
/// every snapshot on it — stays on the server until `tl fs rm` (or `--delete` here);
/// unsnapshotted overlay changes are local and die with the mount's state directory.
/// The pid of a live `tl fs daemon` serving `mountpoint`, if one is visible. Guards leftover
/// detach against yanking a healthy volume whose registry record is out of reach — a sudo run
/// sees root's empty registry, and a corrupted registry file reads as empty. Positive matches
/// only: a daemon whose state dir we cannot read (another user's, without sudo) doesn't block.
fn live_daemon_for(mountpoint: &str) -> Option<i32> {
    let out = std::process::Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines() {
        let Some(state_dir) = line.split("fs daemon --state-dir ").nth(1) else {
            continue;
        };
        let Ok(state) = daemon::load_mount_state(Path::new(state_dir.trim())) else {
            continue;
        };
        if state.mountpoint.to_string_lossy() == mountpoint
            && let Some(pid) = line
                .trim_start()
                .split_whitespace()
                .next()
                .and_then(|pid| pid.parse::<i32>().ok())
            && daemon_alive(pid)
        {
            return Some(pid);
        }
    }
    None
}

/// Detach a tlfs volume left attached with no daemon behind it (macOS FSKit keeps serving
/// ECONNREFUSED after its daemon dies; a killed FUSE daemon leaves an ENOTCONN mount on
/// Linux). No-op when nothing tlfs is attached; refuses when a live daemon is actually
/// serving the path or the volume is busy.
async fn detach_leftover(mountpoint: &str) -> Result<()> {
    if !daemon::still_mounted(Path::new(mountpoint)) {
        return Ok(());
    }
    if let Some(pid) = live_daemon_for(mountpoint) {
        return Err(CliError::usage(format!(
            "a live mount daemon (pid {pid}) is serving {mountpoint}; its record is not in \
             this user's registry — run `tl fs unmount {mountpoint}` as the user who mounted \
             it"
        )));
    }
    if !daemon::unmount(Path::new(mountpoint)).await {
        return Err(CliError::usage(format!(
            "could not detach the volume at {mountpoint} (its daemon is already gone): it \
             is busy. Close whatever is using it (shells cd'd inside, editors holding \
             files), then re-run: tl fs unmount {mountpoint}"
        )));
    }
    Ok(())
}

pub async fn unmount(
    ctx: &CliContext,
    path: &Path,
    delete: bool,
    discard_local: bool,
) -> Result<()> {
    if let Some((root, _)) = plaindir::binding_for_lenient(path) {
        return Err(CliError::usage(format!(
            "{root} is a plain-directory binding, not a mount; detach it with: tl fs unbind \
             {root}"
        )));
    }
    let (mountpoint, state_dir) = match state_dir_for(path) {
        Ok(found) => found,
        Err(e) => {
            // Nothing registered — but the kernel may still hold an orphaned volume here (a
            // killed daemon leaves the volume attached, and older CLIs then forgot the local
            // state without detaching it). Detaching that is exactly this command's job.
            let mountpoint = canonical_mountpoint(path)?;
            if !daemon::still_mounted(Path::new(&mountpoint)) {
                return Err(e);
            }
            detach_leftover(&mountpoint).await?;
            println!(
                "Detached the orphaned volume at {mountpoint} (no local mount state remained)."
            );
            if delete {
                return Err(CliError::usage(
                    "no local record of its workspace remains; find it with `tl fs ls` and \
                     delete it with `tl fs rm <workspace-id>`",
                ));
            }
            return Ok(());
        }
    };
    let state = daemon::load_mount_state(&state_dir)?;
    // Unmount deletes the state dir — the overlay with it. Anything in the upper/wh trees is
    // local-only state (unsealed writes, whiteouts, and ignored files that never enter a
    // snapshot); destroying it needs the explicit flag. Checked before the shutdown so a
    // refusal leaves the mount fully intact.
    if !discard_local && overlay_has_local_state(&state_dir)? {
        return Err(CliError::usage(format!(
            "the mount at {mountpoint} has local overlay state that unmounting would destroy. \
             Seal it first, then re-run with the flag:\n  tl fs snapshot {mountpoint}\n  \
             tl fs unmount --discard-local {mountpoint}\nNote: --discard-local also drops \
             ignored files under the mount — they are never part of a snapshot."
        )));
    }
    let pid = daemon::daemon_pid(&state_dir);
    // The daemon replies to `shutdown` only once the kernel released the volume, so this call
    // covers the slow phase (FSKit teardown on macOS takes seconds); spin so it doesn't look
    // hung. A busy volume answers ok:false — surface it and leave the mount fully intact.
    let bar = indicatif::ProgressBar::new_spinner();
    bar.enable_steady_tick(std::time::Duration::from_millis(120));
    bar.set_message(format!(
        "unmounting {mountpoint} (waiting for the kernel to release the volume)..."
    ));
    if let Err(e) = daemon::control(&state_dir, "shutdown").await {
        // A dead daemon does NOT mean a detached volume: on macOS the FSKit extension proxies
        // to the daemon over TCP, so the kernel keeps the volume attached (serving
        // ECONNREFUSED) after the daemon dies — and a killed FUSE daemon similarly leaves an
        // ENOTCONN mount on Linux. Detach any leftover before forgetting the local state, or
        // the mountpoint stays poisoned with no command left that clears it.
        // A busy volume means it is still live and serving. There is a third shape (measured):
        // the daemon unmounts, replies, and exits so fast that the reply read loses the race
        // and errors — poll briefly, and if the daemon is gone and the kernel released the
        // volume, that IS success.
        let message = e.to_string();
        if message.contains("mount daemon is not running") {
            if let Err(e) = detach_leftover(&mountpoint).await {
                bar.finish_and_clear();
                return Err(e);
            }
        } else {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            let settled = loop {
                let daemon_gone = daemon::daemon_pid(&state_dir).is_none_or(|p| !daemon_alive(p));
                if daemon_gone && !daemon::still_mounted(Path::new(&mountpoint)) {
                    break true;
                }
                if std::time::Instant::now() >= deadline {
                    break false;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            };
            if !settled {
                bar.finish_and_clear();
                return Err(CliError::usage(format!(
                    "could not unmount {mountpoint}: {message}\nThe volume stays mounted and \
                     usable. Close whatever is using it (shells cd'd inside, editors holding \
                     files), then re-run: tl fs unmount {mountpoint}"
                )));
            }
        }
    }
    // Wait for the daemon to actually exit before tearing down its state dir: the shutdown op
    // races the process exit, and deleting upper/control state under a live daemon is how
    // daemons leak (and how a reattach ends up sharing a state dir with a zombie). The kernel
    // already let go by the time shutdown answered, so this is quick.
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
    bar.finish_and_clear();
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
            "Unmounted {mountpoint}. Workspace {} kept — mount it again with: tl fs mount {} \
             <path>",
            short_id(&state.workspace_id),
            short_id(&state.workspace_id),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Snapshot: the daemon sealer's dirty index is the dirty set.
// ---------------------------------------------------------------------------------------------

/// Walk the overlay state dir: `(upserts, deletes)` as repo paths. Ignored names (built-ins +
/// the mount's `.tlignore`) are workspace-local and never enumerate.
/// Overlay upserts as `(repo path, upper file, git mode)`.
type OverlayUpserts = Vec<(String, PathBuf, u32)>;

struct SnapshotIgnore {
    mount_root: PathBuf,
    ignored_names: Vec<String>,
    gitignores: HashMap<PathBuf, Gitignore>,
}

impl SnapshotIgnore {
    fn new(mount_root: &Path) -> Self {
        Self {
            mount_root: mount_root.to_path_buf(),
            ignored_names: local::ignored_names(mount_root),
            gitignores: HashMap::new(),
        }
    }

    fn matcher_for(&mut self, rel_dir: &Path) -> Result<&Gitignore> {
        if !self.gitignores.contains_key(rel_dir) {
            let abs_dir = self.mount_root.join(rel_dir);
            let mut builder = GitignoreBuilder::new(&abs_dir);
            let gitignore = abs_dir.join(".gitignore");
            if gitignore.is_file()
                && let Some(err) = builder.add(&gitignore)
            {
                return Err(CliError::usage(format!(
                    "failed to read {}: {err}",
                    gitignore.display()
                )));
            }
            let matcher = builder.build().map_err(|err| {
                CliError::usage(format!("failed to parse {}: {err}", gitignore.display()))
            })?;
            self.gitignores.insert(rel_dir.to_path_buf(), matcher);
        }
        Ok(self.gitignores.get(rel_dir).expect("matcher inserted"))
    }

    fn is_ignored(&mut self, rel: &str, is_dir: bool) -> Result<bool> {
        let rel_path = Path::new(rel);
        for component in rel_path.components() {
            let Component::Normal(name) = component else {
                continue;
            };
            let name = name.to_string_lossy();
            if self.ignored_names.iter().any(|ignored| ignored == &*name)
                || local::is_metadata_turd(&name)
            {
                return Ok(true);
            }
        }

        let abs = self.mount_root.join(rel_path);
        let mut ignored = false;
        for dir in gitignore_dirs_for(rel_path) {
            match self
                .matcher_for(&dir)?
                .matched_path_or_any_parents(&abs, is_dir)
            {
                Match::Ignore(_) => ignored = true,
                Match::Whitelist(_) => ignored = false,
                Match::None => {}
            }
        }
        Ok(ignored)
    }
}

fn gitignore_dirs_for(rel: &Path) -> Vec<PathBuf> {
    let mut dirs = vec![PathBuf::new()];
    let Some(parent) = rel.parent() else {
        return dirs;
    };

    let mut current = PathBuf::new();
    for component in parent.components() {
        if let Component::Normal(name) = component {
            current.push(name);
            dirs.push(current.clone());
        }
    }
    dirs
}

fn enumerate_overlay(state_dir: &Path, mount_root: &Path) -> Result<(OverlayUpserts, Vec<String>)> {
    let mut ignored = SnapshotIgnore::new(mount_root);
    let mut upserts = Vec::new();
    let mut deletes = Vec::new();
    let upper = state_dir.join("upper");
    let wh = state_dir.join("wh");

    fn walk(
        root: &Path,
        dir: &Path,
        ignored: &mut SnapshotIgnore,
        out: &mut dyn FnMut(String, PathBuf, &std::fs::Metadata),
    ) -> Result<()> {
        let Ok(read) = std::fs::read_dir(dir) else {
            return Ok(());
        };
        for entry in read.flatten() {
            let abs = entry.path();
            let meta = std::fs::symlink_metadata(&abs)?;
            let rel = abs
                .strip_prefix(root)
                .expect("under root")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if ignored.is_ignored(&rel, meta.is_dir())? {
                continue;
            }
            if meta.file_type().is_symlink() || meta.is_file() {
                out(rel, abs, &meta);
            } else if meta.is_dir() {
                walk(root, &abs, ignored, out)?;
            }
        }
        Ok(())
    }

    walk(&upper, &upper, &mut ignored, &mut |rel, abs, meta| {
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
    walk(&wh, &wh, &mut ignored, &mut |rel, _abs, _meta| {
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

/// Whether the overlay holds ANY local state — deliberately broader than what enumerates as
/// dirty: ignored files skip enumeration (and every snapshot) but die with the upper all the
/// same, so the destructive commands (`restore`, `unmount`) gate on the raw upper/wh trees
/// plus pending renames, not on the dirty walk.
///
/// Fails CLOSED: this guards data destruction, so an overlay tree that cannot be read is an
/// error, not "no state" — a permissions hiccup must never wave a destructive command
/// through. Only a missing tree (never-written overlay side) is honestly empty. The explicit
/// `--discard-local` flag bypasses the check entirely (callers short-circuit before calling).
fn overlay_has_local_state(state_dir: &Path) -> Result<bool> {
    fn unreadable(path: &Path, err: &std::io::Error) -> CliError {
        CliError::usage(format!(
            "cannot verify local overlay state: {} is unreadable ({err}); fix permissions \
             or pass --discard-local to drop the overlay without checking",
            path.display()
        ))
    }
    fn any_entry(dir: &Path) -> Result<bool> {
        let read = match std::fs::read_dir(dir) {
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(unreadable(dir, &e)),
        };
        for entry in read {
            let entry = entry.map_err(|e| unreadable(dir, &e))?;
            let meta = match entry.path().symlink_metadata() {
                Ok(meta) => meta,
                // Deleted between readdir and stat: honestly not state anymore.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(unreadable(&entry.path(), &e)),
            };
            if meta.is_dir() && !meta.file_type().is_symlink() {
                if any_entry(&entry.path())? {
                    return Ok(true);
                }
            } else {
                return Ok(true);
            }
        }
        Ok(false)
    }
    Ok(any_entry(&state_dir.join("upper"))?
        || any_entry(&state_dir.join("wh"))?
        || !pending_renames(state_dir).is_empty())
}

/// Pending committed-directory renames recorded by the mount daemon (`redirects.json` in the
/// state dir, destination -> true-lower source), sorted by destination. Empty when the file
/// is absent or unreadable — the daemon owns the authoritative copy.
fn pending_renames(state_dir: &Path) -> Vec<(String, String)> {
    let Ok(raw) = std::fs::read(state_dir.join("redirects.json")) else {
        return Vec::new();
    };
    let Ok(map) = serde_json::from_slice::<HashMap<String, String>>(&raw) else {
        return Vec::new();
    };
    let mut entries: Vec<(String, String)> = map.into_iter().collect();
    entries.sort();
    entries
}

/// Where a local-change view came from. Only the daemon can report the exact set the next
/// snapshot would seal; walking the retained upper is deliberately labelled approximate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DirtySource {
    Daemon,
    OverlayDaemonDown,
}

impl DirtySource {
    fn approximate(self) -> bool {
        self != Self::Daemon
    }

    fn json_name(self) -> &'static str {
        match self {
            Self::Daemon => "daemon",
            Self::OverlayDaemonDown => "overlay_fallback_daemon_down",
        }
    }

    fn warning(self) -> Option<&'static str> {
        match self {
            Self::Daemon => None,
            Self::OverlayDaemonDown => {
                Some("daemon not running; showing approximate local changes")
            }
        }
    }
}

#[derive(Debug)]
struct LocalChanges {
    dirty: Vec<daemon::DirtyPath>,
    lower_commit: Option<String>,
    daemon_running: bool,
    source: DirtySource,
}

/// Parse and semantically validate the daemon's exact next-snapshot view. Serde makes every
/// top-level field required; the checks below also prevent malformed rename rows (or paths) from
/// being rendered as ordinary changes. A rename source delete is redundant with the R row and is
/// suppressed even if a transition-version daemon includes both.
fn parse_dirty_reply(
    response: serde_json::Value,
) -> std::result::Result<(Vec<daemon::DirtyPath>, String), String> {
    fn valid_repo_path(path: &str) -> bool {
        !path.is_empty()
            && !path.contains('\0')
            && path
                .split('/')
                .all(|component| !component.is_empty() && component != "." && component != "..")
    }

    let reply: daemon::DirtyReply =
        serde_json::from_value(response).map_err(|e| format!("invalid reply shape: {e}"))?;
    if reply.lower_commit.trim().is_empty() {
        return Err("lower_commit is empty".to_string());
    }
    for entry in &reply.dirty {
        if !valid_repo_path(&entry.path) {
            return Err(format!("invalid dirty path {:?}", entry.path));
        }
        match (&entry.kind, &entry.from) {
            (daemon::DirtyPathKind::Renamed, Some(from))
                if valid_repo_path(from) && from != &entry.path => {}
            (daemon::DirtyPathKind::Renamed, _) => {
                return Err(format!(
                    "rename destination {:?} has no valid, distinct source",
                    entry.path
                ));
            }
            (daemon::DirtyPathKind::Modified | daemon::DirtyPathKind::Deleted, None) => {}
            (daemon::DirtyPathKind::Modified | daemon::DirtyPathKind::Deleted, Some(_)) => {
                return Err(format!(
                    "non-rename dirty path {:?} unexpectedly has a source",
                    entry.path
                ));
            }
        }
    }

    let rename_sources: std::collections::HashSet<String> = reply
        .dirty
        .iter()
        .filter_map(|entry| {
            (entry.kind == daemon::DirtyPathKind::Renamed)
                .then_some(entry.from.as_deref())
                .flatten()
        })
        .map(str::to_string)
        .collect();
    let dirty = reply
        .dirty
        .into_iter()
        .filter(|entry| {
            entry.kind != daemon::DirtyPathKind::Deleted
                || !rename_sources.contains(entry.path.as_str())
        })
        .collect();
    Ok((dirty, reply.lower_commit))
}

fn approximate_overlay_changes(
    state_dir: &Path,
    mount_root: &Path,
) -> Result<Vec<daemon::DirtyPath>> {
    let (upserts, deletes) = enumerate_overlay(state_dir, mount_root)?;
    let renames = pending_renames(state_dir); // (destination, source)
    let rename_sources: std::collections::HashSet<String> =
        renames.iter().map(|(_, from)| from.clone()).collect();
    let mut dirty = Vec::with_capacity(upserts.len() + deletes.len() + renames.len());
    for (path, from) in renames {
        dirty.push(daemon::DirtyPath {
            path,
            kind: daemon::DirtyPathKind::Renamed,
            from: Some(from),
        });
    }
    for (path, _, _) in upserts {
        dirty.push(daemon::DirtyPath {
            path,
            kind: daemon::DirtyPathKind::Modified,
            from: None,
        });
    }
    for path in deletes {
        if !rename_sources.contains(path.as_str()) {
            dirty.push(daemon::DirtyPath {
                path,
                kind: daemon::DirtyPathKind::Deleted,
                from: None,
            });
        }
    }
    Ok(dirty)
}

/// Ask the running sealer for its exact dirty set. Only an unreachable daemon falls back to the
/// raw overlay walk, and the caller must surface that approximation. A live daemon that is busy,
/// old, or malformed fails closed: raw upper state can miss dirty-index-only delete events, so it
/// is not a safe substitute while the authoritative in-memory index still exists.
async fn query_local_changes(state_dir: &Path, mount_root: &Path) -> Result<LocalChanges> {
    fn is_connect_failure(error: &CliError) -> bool {
        // `daemon::control` uses this prefix only when UnixStream::connect fails. Operation
        // errors use `daemon <op> failed`, so a live/busy daemon cannot enter the raw fallback.
        matches!(
            error,
            CliError::Usage(message) if message.starts_with("mount daemon is not running (")
        )
    }

    fn daemon_down_changes(state_dir: &Path, mount_root: &Path) -> Result<LocalChanges> {
        Ok(LocalChanges {
            dirty: approximate_overlay_changes(state_dir, mount_root)?,
            lower_commit: None,
            daemon_running: false,
            source: DirtySource::OverlayDaemonDown,
        })
    }

    enum DirtyFailure {
        Outdated,
        Unavailable,
        Malformed,
    }

    let failure = match daemon::control(state_dir, "dirty").await {
        Ok(response) => match parse_dirty_reply(response) {
            Ok((dirty, lower_commit)) => {
                return Ok(LocalChanges {
                    dirty,
                    lower_commit: Some(lower_commit),
                    daemon_running: true,
                    source: DirtySource::Daemon,
                });
            }
            Err(_) => DirtyFailure::Malformed,
        },
        Err(error) if is_connect_failure(&error) => {
            return daemon_down_changes(state_dir, mount_root);
        }
        Err(error)
            if error.to_string().contains("unknown op")
                || error.to_string().contains("unknown_op") =>
        {
            DirtyFailure::Outdated
        }
        Err(_) => DirtyFailure::Unavailable,
    };

    match daemon::control(state_dir, "ping").await {
        Err(error) if is_connect_failure(&error) => daemon_down_changes(state_dir, mount_root),
        Ok(_) => {
            let message = match failure {
                DirtyFailure::Outdated => {
                    "the mount daemon predates exact local status; remount this workspace with \
                     the current tl version, then retry"
                        .to_string()
                }
                DirtyFailure::Malformed => {
                    "the mount daemon returned invalid local status; remount to upgrade it, then \
                     retry"
                        .to_string()
                }
                DirtyFailure::Unavailable => {
                    "the mount daemon is busy or could not provide exact local status; retry \
                     after the current snapshot or restore finishes"
                        .to_string()
                }
            };
            Err(CliError::usage(message))
        }
        Err(_) => Err(CliError::usage(
            "could not verify exact local status with the mount daemon; retry or inspect the \
             daemon log",
        )),
    }
}

/// An enumerated dirty set as push files, used by the daemon's sealer (`tl fs snapshot`
/// seals through the daemon, so nothing pushes from the CLI process).
fn overlay_push_files(upserts: &OverlayUpserts, deletes: &[String]) -> Result<Vec<PushFile>> {
    let mut files = Vec::with_capacity(upserts.len() + deletes.len());
    for (rel, abs, mode) in upserts {
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
    for rel in deletes {
        files.push(PushFile {
            repo_path: rel.clone(),
            source: PushSource::Bytes(Vec::new()),
            mode: None,
            delete: true,
        });
    }
    Ok(files)
}

pub async fn snapshot(
    ctx: &CliContext,
    path: &Path,
    message: Option<&str>,
    clear: bool,
) -> Result<()> {
    // A plain-directory binding snapshots by scanning the directory against its stat index;
    // there is no overlay, so the mount-only --clear flag has nothing to drop.
    if let Some((root, binding_state)) = plaindir::binding_for_lenient(path) {
        if clear {
            return Err(CliError::usage(
                "--clear drops a mount's local overlay; a plain-directory binding has no \
                 overlay to clear",
            ));
        }
        return plaindir::snapshot(ctx, &root, &binding_state, message).await;
    }
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

    let started = std::time::Instant::now();
    let bar = indicatif::ProgressBar::new_spinner();
    bar.enable_steady_tick(std::time::Duration::from_millis(120));
    bar.set_message("sealing workspace changes...");
    let (outcome, cleared) = seal_via_daemon(&state_dir, &mountpoint, message, clear, &bar).await?;
    let total = started.elapsed();
    bar.finish_and_clear();
    let Some(sealed) = outcome else {
        println!("{}", clean_snapshot_message(cleared));
        return Ok(());
    };
    // Small files skip chunk negotiation (token-only commits), so uploads can exceed the
    // negotiated chunk count — clamp so the summary never reads "3 of 0 chunks".
    println!(
        "Snapshot {} ({} file(s), {} of {} chunks uploaded in {})",
        sealed.commit,
        sealed.files,
        sealed.chunks_uploaded,
        sealed.chunks_total.max(sealed.chunks_uploaded),
        fmt_dur(total),
    );
    if let Some(push_ms) = sealed.push_ms {
        println!(
            "  push {} (sealed by the mount daemon)",
            fmt_dur(std::time::Duration::from_millis(push_ms)),
        );
    }
    Ok(())
}

/// One daemon-sealed snapshot, parsed out of the `seal` control reply.
struct DaemonSeal {
    commit: String,
    files: u64,
    chunks_uploaded: u64,
    chunks_total: u64,
    push_ms: Option<u64>,
}

/// What a clean (nothing-to-seal) snapshot prints. Never claims a clean workspace when a
/// requested clear actually dropped retained files — ignored files and previously sealed
/// content live in the upper without ever enumerating as dirty.
fn clean_snapshot_message(cleared: Option<usize>) -> String {
    match cleared {
        Some(n) if n > 0 => format!(
            "Nothing new to snapshot; cleared {n} locally retained file(s) (including \
             ignored files) from the overlay."
        ),
        _ => "Nothing to snapshot: workspace is clean.".to_string(),
    }
}

/// Seal through the mount daemon's sealer — the SAME machinery (and state) as auto-commit,
/// which is what makes manual snapshots correct: the shared dirty watermark means an
/// auto-commit mount never re-publishes manually sealed paths (and vice versa), only paths
/// touched since the last seal are pushed instead of the whole ever-dirty upper, and deletes
/// racing a seal go through the sealer's resurrection tombstone guard. Returns
/// `(None, cleared)` when nothing was dirty; `cleared` reports how many retained files a
/// requested clear dropped.
///
/// The daemon advances the lower to the sealed commit before replying, so the mount serves
/// the new snapshot when this returns; the reply also drains the banked probe backlog, which
/// macOS converges here (Linux rode the FUSE notifier inside the daemon). The `seal` op is
/// line-streaming: progress events narrate onto `bar` until the final reply line arrives.
///
/// `clear` rides the seal request itself — the daemon drops the whole overlay inside the
/// same sealer cycle (the explicit, destructive opt-in: it also deletes ignored files and
/// any writes racing the seal; it is what empties the local dirty set so `tl fs sync` can
/// run) and replies with exactly the paths the drop removed, which is the kernel
/// revalidation set here. The clear runs even after a clean seal: earlier kept-overlay seals
/// leave a populated upper that `sync` refuses to run over.
async fn seal_via_daemon(
    state_dir: &Path,
    mountpoint: &str,
    message: Option<&str>,
    clear: bool,
    bar: &indicatif::ProgressBar,
) -> Result<(Option<DaemonSeal>, Option<usize>)> {
    let request = daemon::SealRequest {
        message: message.map(str::to_string),
        clear,
    };
    let resp = daemon::control_streaming(
        state_dir,
        "seal",
        serde_json::to_value(&request)?,
        |event| bar.set_message(event.to_string()),
    )
    .await?;
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        let error = resp
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        // Structured detection first; the prose match is a documented legacy fallback for
        // daemons that predate the `code` field (they phrase it as `unknown op "seal"`).
        let unknown_op = resp.get("code").and_then(|c| c.as_str()) == Some("unknown_op")
            || error.contains("unknown op");
        if unknown_op {
            return Err(CliError::usage(format!(
                "the running mount daemon predates seal-through-daemon; remount to \
                 upgrade it (tl fs unmount {mountpoint} && tl fs mount), then retry: {error}"
            )));
        }
        return Err(CliError::usage(format!("daemon seal failed: {error}")));
    }
    // The reply is a shared serde struct, parsed strictly: a missing or mistyped field is a
    // protocol error to report, never a value to default (no "Snapshot ?" summaries).
    let reply: daemon::SealReply = serde_json::from_value(resp.clone()).map_err(|e| {
        CliError::usage(format!("the mount daemon sent a malformed seal reply: {e}"))
    })?;
    // The seal's post-push refresh may also have adopted foreign ref movement (a concurrent
    // writer advanced the workspace ref past our snapshot commit), and the drained probe list
    // can carry a backlog from background polls. Converge those the same way sync does —
    // macOS only; the FUSE notifier already handled them on Linux. Our own sealed paths are
    // upper-shadowed and filter out of the probe list.
    if cfg!(target_os = "macos") {
        let (expect, _complete, _new_daemon) = parse_refresh_probes(&resp);
        if !expect.is_empty() {
            let changed: std::collections::BTreeSet<String> = expect.keys().cloned().collect();
            converge_kernel_view(Path::new(mountpoint), &changed, &expect);
        }
    }
    let cleared = if clear {
        let cleared = reply.cleared.ok_or_else(|| {
            CliError::usage(
                "the mount daemon did not report which paths its clear dropped; \
                 remount to upgrade it (tl fs unmount && tl fs mount), then retry",
            )
        })?;
        // Content is byte-identical across the swap for sealed paths, but every cleared
        // path's attributes changed backing (upper mtimes -> lower serve time) — and the
        // clear also dropped never-sealed state (ignored files); refresh the kernel's view
        // of exactly what the daemon says it removed.
        revalidate_paths(Path::new(mountpoint), &cleared);
        Some(cleared.len())
    } else {
        None
    };
    if reply.clean {
        return Ok((None, cleared));
    }
    let sealed_field = |name: &str, v: Option<u64>| {
        v.ok_or_else(|| {
            CliError::usage(format!(
                "the mount daemon's seal reply is missing {name:?}; \
                 remount to upgrade it (tl fs unmount && tl fs mount), then retry"
            ))
        })
    };
    Ok((
        Some(DaemonSeal {
            commit: reply.commit,
            files: sealed_field("files", reply.files)?,
            chunks_uploaded: sealed_field("chunks_uploaded", reply.chunks_uploaded)?,
            chunks_total: sealed_field("chunks_total", reply.chunks_total)?,
            push_ms: reply.push_ms,
        }),
        cleared,
    ))
}

/// Render a phase duration compactly: sub-second phases as whole milliseconds (`42ms`), longer
/// ones as fractional seconds (`1.83s`). `{:.2}s` alone would flatten every fast phase to `0.00s`.
fn fmt_dur(d: std::time::Duration) -> String {
    if d.as_secs() == 0 {
        format!("{}ms", d.as_millis())
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

pub async fn promote(
    ctx: &CliContext,
    path: &Path,
    branch: &str,
    full_history: bool,
    merge: bool,
    message: Option<&str>,
) -> Result<()> {
    if plaindir::binding_for_lenient(path).is_some() {
        return Err(CliError::usage(
            "promote is not supported for plain-directory bindings in v1; snapshots land on \
             the workspace ref — publish them from a future release (or mount the workspace)",
        ));
    }
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
    let local = query_local_changes(&state_dir, Path::new(&mountpoint)).await?;
    if let Some(warning) = local.source.warning() {
        eprintln!("{} {warning}", style("note:").yellow());
    }
    if !local.dirty.is_empty() {
        if local.source.approximate() {
            eprintln!(
                "{} approximate overlay scan found {} local change(s) that may not be in a \
                 snapshot; promoting the last snapshot only. Run `tl fs snapshot` first to be \
                 sure they are included.",
                style("note:").yellow(),
                local.dirty.len(),
            );
        } else {
            eprintln!(
                "{} {} unsealed local change(s); promoting the last snapshot only. Run `tl fs \
                 snapshot` first to include them.",
                style("note:").yellow(),
                local.dirty.len(),
            );
        }
    }
    let (user, token) = session.creds();
    let request = PromoteWorkspaceRequest {
        branch: branch.to_string(),
        expect_oid: None,
        full_history,
        mode: merge.then(|| "merge".to_string()),
        message: message.map(str::to_string),
        ..Default::default()
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
                Ok(resp) => match resp.into_inner() {
                    PromoteOutcome::Promoted(resp) => break resp,
                    PromoteOutcome::Conflicted(report) => {
                        eprintln!(
                            "{} promote to {branch} conflicts on {} path(s); nothing was published:",
                            style("error:").red(),
                            report.conflicts.len(),
                        );
                        for c in &report.conflicts {
                            eprintln!("  {:<14} {}", style(&c.kind).yellow(), c.path);
                        }
                        return Err(CliError::usage(format!(
                            "pull {branch} into the workspace, resolve, and promote again:\n  tl fs sync {}\n  # fix the conflict markers, then\n  tl fs snapshot {} && tl fs promote {} {branch} --merge",
                            path.display(),
                            path.display(),
                            path.display(),
                        )));
                    }
                },
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
        if resp.fast_forwarded {
            " (fast-forward)"
        } else if resp.merged {
            " (merge)"
        } else if resp.squashed {
            " (squashed)"
        } else {
            " (full history)"
        },
    );
    Ok(())
}

/// Sync: pull the target branch into a behind workspace — one server-side rebase-style merge
/// commit on the target head; the mount's lower layer then advances to it. Under the default
/// materialize policy conflicts land as diff3 markers in the workspace files; resolve them and
/// snapshot. Local overlay changes would shadow synced content (markers included), so a dirty
/// mount must seal-and-clear (`tl fs snapshot --clear`) first — a plain snapshot keeps the
/// overlay, which still shadows.
pub async fn sync(
    ctx: &CliContext,
    path: &Path,
    target: Option<&str>,
    fail_on_conflict: bool,
    message: Option<&str>,
) -> Result<()> {
    if plaindir::binding_for_lenient(path).is_some() {
        return Err(CliError::usage(
            "sync is not supported for plain-directory bindings in v1 (there is no mount to \
             materialize pulled content into); v1 bindings are single-writer capture only",
        ));
    }
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.read_only() {
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; it syncs automatically",
            state.follow_ref.as_deref().unwrap_or("the branch"),
        )));
    }
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;
    let (upserts, deletes) = enumerate_overlay(&state_dir, Path::new(&mountpoint))?;
    let renames = pending_renames(&state_dir);
    if !upserts.is_empty() || !deletes.is_empty() || !renames.is_empty() {
        return Err(CliError::usage(format!(
            "{} local change(s) would shadow synced content. Seal and drop them first: `tl fs snapshot --clear {}` (pause writers first — the clear also drops ignored files and any writes made while the snapshot uploads).",
            upserts.len() + deletes.len() + renames.len(),
            path.display(),
        )));
    }
    let (user, token) = session.creds();
    let request = SyncWorkspaceRequest {
        target: target.map(str::to_string),
        policy: fail_on_conflict.then(|| "fail".to_string()),
        message: message.map(str::to_string),
        ..Default::default()
    };
    // Same 425 contract as promote: a sync issued right behind a snapshot can catch the
    // commit index still materializing.
    let resp = {
        let deadline = std::time::Instant::now() + TOO_EARLY_DEADLINE;
        loop {
            match session
                .client
                .workspace_sync(
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
                Err(tensorlake::error::SdkError::ServerError { status, .. })
                    if status.as_u16() == 425 && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    };
    if resp.up_to_date {
        println!(
            "Already up to date with {}.",
            &resp.target_head[..resp.target_head.len().min(12)]
        );
        return Ok(());
    }
    if !resp.clean && fail_on_conflict {
        eprintln!(
            "{} sync conflicts on {} path(s); the workspace is unchanged:",
            style("error:").red(),
            resp.conflicts.len(),
        );
        for c in &resp.conflicts {
            eprintln!("  {:<14} {}", style(&c.kind).yellow(), c.path);
        }
        return Err(CliError::usage(
            "rerun without --fail-on-conflict to materialize the conflicts as diff3 markers",
        ));
    }
    // The workspace ref moved server-side; swap the mount's lower layer now instead of
    // waiting out the follow poll, then make sure the kernel drops stale views of every
    // path the pull changed. This matters most for names that newly appeared: a lookup
    // answered ENOENT before the sync can live on as a kernel-cached negative dentry that
    // readdir traffic never revalidates (`ls` shows the file, `cat` says ENOENT), and on
    // macOS there is no daemon-side notify channel to drop it — probing from out here is
    // the only lever. Conflict paths get the same treatment (their content changed to
    // marker text behind the kernel's back).
    let refresh = daemon::control(&state_dir, "refresh").await?;
    let (mut expect, complete, new_daemon) = parse_refresh_probes(&refresh);
    // On Linux the daemon already pushed exact FUSE invalidations for these paths while
    // serving the refresh; probing would redo that work through the mount. macOS (FSKit)
    // has no notify channel — probing from out here is the only lever there.
    if !cfg!(target_os = "macos") {
        expect.clear();
    }
    for c in &resp.conflicts {
        expect.insert(c.path.clone(), PathExpect::Present);
    }
    if !expect.is_empty() {
        let changed: std::collections::BTreeSet<String> = expect.keys().cloned().collect();
        converge_kernel_view(Path::new(&mountpoint), &changed, &expect);
    }
    if cfg!(target_os = "macos") {
        if !new_daemon {
            eprintln!(
                "{} the mount daemon predates this CLI; newly pulled files can transiently \
                 answer ENOENT — remount to fix",
                style("warning:").yellow(),
            );
        } else if !complete {
            eprintln!(
                "{} a refresh could not enumerate newly added paths; files pulled by this \
                 sync can transiently answer ENOENT (kernel cache, ~30s)",
                style("warning:").yellow(),
            );
        }
    }
    println!(
        "Synced with {} ({} path(s) pulled){}.",
        &resp.target_head[..resp.target_head.len().min(12)],
        resp.changed_paths,
        if resp.fast_forwarded {
            "; workspace fast-forwarded"
        } else {
            ""
        },
    );
    if !resp.conflicts.is_empty() {
        println!(
            "{} {} conflict(s) materialized as diff3 markers:",
            style("note:").yellow(),
            resp.conflicts.len(),
        );
        for c in &resp.conflicts {
            println!("  {:<14} {}", style(&c.kind).yellow(), c.path);
        }
        println!(
            "Resolve the markers, then `tl fs snapshot {}`.",
            path.display()
        );
    }
    Ok(())
}

pub async fn status(ctx: &CliContext, path: &Path, output_json: bool) -> Result<()> {
    if let Some((root, binding_state)) = plaindir::binding_for_lenient(path) {
        return plaindir::status(ctx, &root, &binding_state, output_json).await;
    }
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
    let local = query_local_changes(&state_dir, Path::new(&mountpoint)).await?;

    if output_json {
        let pending_renames = local
            .dirty
            .iter()
            .filter(|entry| entry.kind == daemon::DirtyPathKind::Renamed)
            .map(|entry| {
                serde_json::json!({
                    "from": entry.from.as_deref().expect("validated rename source"),
                    "to": entry.path,
                })
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "workspace": ws,
                "mounted": local.daemon_running,
                "lower_commit": local.lower_commit,
                "log": state_dir.join("daemon.log"),
                // Preserve the legacy JSON split: M/D paths live in `dirty`; renames live in
                // `pending_renames` and are not duplicated here.
                "dirty": local.dirty.iter()
                    .filter(|entry| entry.kind != daemon::DirtyPathKind::Renamed)
                    .map(|entry| entry.path.clone())
                    .collect::<Vec<_>>(),
                "pending_renames": pending_renames,
                "dirty_approximate": local.source.approximate(),
                "dirty_source": local.source.json_name(),
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
    if let Some(secs) = state.auto_commit_interval_secs {
        println!(
            "{} local changes seal into a snapshot every {secs}s (local status shows only \
             changes pending for the next snapshot)",
            style("auto-commit:").dim()
        );
    }
    match (&local.lower_commit, local.daemon_running) {
        (Some(commit), true) => println!("{} mounted at {commit}", style("daemon:").dim()),
        (None, true) => println!(
            "{} running (lower commit unavailable)",
            style("daemon:").dim()
        ),
        (_, false) => println!(
            "{} not running (remount with tl fs mount)",
            style("daemon:").dim()
        ),
    }
    println!(
        "{} {}",
        style("log:").dim(),
        state_dir.join("daemon.log").display()
    );
    if let Some(warning) = local.source.warning() {
        println!("{} {warning}", style("note:").yellow());
    }
    let dirty = local.dirty.len();
    if dirty == 0 {
        if local.source.approximate() {
            println!("{} no changes found (approximate)", style("local:").dim());
        } else {
            println!("{} clean", style("local:").dim());
        }
    } else {
        println!("{} {} change(s):", style("local:").dim(), dirty);
        for entry in local
            .dirty
            .iter()
            .filter(|entry| entry.kind == daemon::DirtyPathKind::Renamed)
            .take(20)
        {
            println!(
                "  {} {} -> {}",
                style("R").cyan(),
                entry.from.as_deref().expect("validated rename source"),
                entry.path,
            );
        }
        for entry in local
            .dirty
            .iter()
            .filter(|entry| entry.kind == daemon::DirtyPathKind::Modified)
            .take(20)
        {
            println!("  {} {}", style("M").yellow(), entry.path);
        }
        for entry in local
            .dirty
            .iter()
            .filter(|entry| entry.kind == daemon::DirtyPathKind::Deleted)
            .take(20)
        {
            println!("  {} {}", style("D").red(), entry.path);
        }
        if dirty > 60 {
            println!("  … and more");
        }
    }
    Ok(())
}

/// Restore: refill the overlay so the merged view equals `version`. The mount's lower layer is
/// untouched (history preserved); the next snapshot seals the restored state.
pub async fn restore(
    ctx: &CliContext,
    path: &Path,
    version: &str,
    discard_local: bool,
) -> Result<()> {
    if plaindir::binding_for_lenient(path).is_some() {
        return Err(CliError::usage(
            "restore is not supported for plain-directory bindings in v1 (it would overwrite \
             local files the index has not sealed); check out the snapshot elsewhere instead",
        ));
    }
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.read_only() {
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; there is nothing to restore",
            state.follow_ref.as_deref().unwrap_or("the branch"),
        )));
    }
    // Restore's point of no return drops the ENTIRE overlay — unsealed writes, whiteouts, and
    // ignored files that never enter any snapshot. Destroying it needs the explicit flag.
    if !discard_local && overlay_has_local_state(&state_dir)? {
        return Err(CliError::usage(format!(
            "the workspace has local overlay state that restoring would destroy. Seal it \
             first, then re-run with the flag:\n  tl fs snapshot {path}\n  tl fs restore \
             --discard-local {path} {version}\nNote: --discard-local also drops ignored files \
             under the mount — they are never part of a snapshot.",
            path = path.display(),
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
    // The upper was refilled behind the daemon's back; rebuild its dirty index so an
    // auto-commit mount seals the restored state (clear_upper above reset the index).
    // Tolerated failure: a still-running daemon from an older tl binary doesn't know the op,
    // and the restore has already materially completed — failing here would skip the
    // kernel-view convergence below and report a false failure.
    if let Err(e) = daemon::control(&state_dir, "reindex").await {
        eprintln!(
            "{} the mount daemon predates auto-commit ({e}); if this mount uses \
             --auto-commit-interval-secs, remount so the restored state seals",
            style("warning:").yellow()
        );
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
    // The first round nudges every changed path; later rounds re-probe only what has not
    // settled, and the deadline is consulted per path so one huge round cannot blow far
    // through the budget (a branch-jump sync can carry tens of thousands of paths).
    let mut unsettled: Vec<String> = changed.iter().cloned().collect();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    'rounds: loop {
        revalidate_paths(mountpoint, &unsettled);
        let mut still = Vec::new();
        for p in unsettled {
            if std::time::Instant::now() > deadline {
                break 'rounds;
            }
            let full = mountpoint.join(&p);
            let settled = match expect.get(&p) {
                // A changed path with no expectation only needed the nudge above.
                None => true,
                Some(PathExpect::Absent) => open_truth(&full, false).is_none(),
                Some(PathExpect::Present) => {
                    open_truth(&full, false).is_some() || {
                        probe_negative_dentry(&full);
                        open_truth(&full, false).is_some()
                    }
                }
                Some(PathExpect::FileSize(size)) => {
                    if open_truth(&full, true).is_none() {
                        probe_negative_dentry(&full);
                    }
                    matches!(open_truth(&full, true), Some(len) if len == *size)
                }
            };
            if !settled {
                still.push(p);
            }
        }
        if still.is_empty() || std::time::Instant::now() > deadline {
            break;
        }
        unsettled = still;
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// Parse a daemon `refresh` reply's drained probe expectations into converge inputs. Returns
/// `(expectation map, complete, new_daemon)`: `complete` is false when some refresh since the
/// last drain could not enumerate first-appearance names (stat-walk fallback), and
/// `new_daemon` is false when the reply carries no `changed` key at all — a still-running
/// daemon from an older tl binary, which cannot report probe lists.
fn parse_refresh_probes(
    reply: &serde_json::Value,
) -> (std::collections::BTreeMap<String, PathExpect>, bool, bool) {
    let new_daemon = reply.get("changed").is_some();
    let items: Vec<overlay::KernelExpectation> =
        serde_json::from_value(reply.get("changed").cloned().unwrap_or_default())
            .unwrap_or_default();
    let mut expect = std::collections::BTreeMap::new();
    for e in items {
        let want = match (e.present, e.size) {
            (false, _) => PathExpect::Absent,
            // A size means content changed behind the kernel: the prober must purge cached
            // pages, not just confirm existence.
            (true, Some(size)) => PathExpect::FileSize(size),
            (true, None) => PathExpect::Present,
        };
        expect.insert(e.path, want);
    }
    let complete = reply
        .get("complete")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    (expect, complete, new_daemon)
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
    if plaindir::binding_for_lenient(path).is_some() {
        return Err(CliError::usage(
            "diff is not supported for plain-directory bindings in v1; `tl fs status` lists \
             the changed paths",
        ));
    }
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
    use super::*;

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

    #[test]
    fn snapshot_enumeration_honors_gitignore_for_upserts_and_deletes() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let upper = state.path().join("upper");
        let wh = state.path().join("wh");
        std::fs::create_dir_all(&upper).unwrap();
        std::fs::create_dir_all(&wh).unwrap();

        std::fs::write(mount.path().join(".gitignore"), "*.tmp\nignored/\n").unwrap();
        std::fs::write(upper.join("keep.txt"), "keep").unwrap();
        std::fs::write(upper.join("drop.tmp"), "ignored").unwrap();
        std::fs::create_dir_all(upper.join("ignored")).unwrap();
        std::fs::write(upper.join("ignored/file.txt"), "ignored").unwrap();
        std::fs::write(wh.join("drop.tmp"), "").unwrap();

        let (upserts, deletes) = enumerate_overlay(state.path(), mount.path()).unwrap();

        let upsert_paths: Vec<_> = upserts.iter().map(|(path, _, _)| path.as_str()).collect();
        assert_eq!(upsert_paths, vec!["keep.txt"]);
        assert!(deletes.is_empty());
    }

    /// A fake daemon control endpoint: records the op sequence, returns the supplied `dirty`
    /// reply, and makes `seal` reply like a real sealer that minted a commit — so the snapshot
    /// and local-status control flows can be exercised without a mount. `seal` honors framing:
    /// each string in `events` is written as an `{"event": ...}` progress line before the
    /// final reply (pass none for the plain single-line reply older tests exercise), and a
    /// `clear:true` request gets a `cleared` list back.
    #[cfg(unix)]
    fn clean_dirty_reply() -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "dirty": [],
            "lower_commit": "cafe0000",
            "watermark": 7,
        })
    }

    #[cfg(unix)]
    fn fake_daemon_with_events(
        state_dir: &Path,
        events: Vec<String>,
        dirty_reply: serde_json::Value,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<String>>> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let ops = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = std::os::unix::net::UnixListener::bind(daemon::control_socket(state_dir))
            .expect("bind control socket");
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::UnixListener::from_std(listener).unwrap();
        let recorded = ops.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let recorded = recorded.clone();
                let events = events.clone();
                let dirty_reply = dirty_reply.clone();
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.is_err() {
                        return;
                    }
                    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                    let op = v["op"].as_str().unwrap_or_default().to_string();
                    recorded.lock().unwrap().push(op.clone());
                    let mut stream = reader.into_inner();
                    let resp = if op == "seal" {
                        for event in &events {
                            let line = serde_json::json!({ "event": event });
                            let _ = stream.write_all(format!("{line}\n").as_bytes()).await;
                        }
                        let mut resp = serde_json::json!({
                            "ok": true,
                            "clean": false,
                            "commit": "cafe0000",
                            "files": 1,
                            "chunks_uploaded": 1,
                            "chunks_total": 1,
                            "sealed": ["keep.txt"],
                            "push_ms": 5,
                        });
                        if v["clear"].as_bool() == Some(true) {
                            resp["cleared"] =
                                serde_json::json!(["keep.txt", "target/build.o", "raced.txt"]);
                        }
                        resp
                    } else if op == "dirty" {
                        dirty_reply
                    } else {
                        serde_json::json!({ "ok": true })
                    };
                    let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
                });
            }
        });
        ops
    }

    #[cfg(unix)]
    fn fake_daemon(state_dir: &Path) -> std::sync::Arc<std::sync::Mutex<Vec<String>>> {
        fake_daemon_with_events(state_dir, Vec::new(), clean_dirty_reply())
    }

    /// Retained upper files are not dirty after a seal. The exact daemon reply must therefore
    /// win over the raw overlay walk, and the fast path must not pay a second ping round-trip.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_changes_use_daemon_view_instead_of_retained_upper() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(state.path().join("upper")).unwrap();
        std::fs::write(state.path().join("upper/already-sealed.txt"), "sealed").unwrap();
        let ops = fake_daemon(state.path());

        let changes = query_local_changes(state.path(), mount.path())
            .await
            .unwrap();

        assert_eq!(changes.source, DirtySource::Daemon);
        assert!(!changes.source.approximate());
        assert_eq!(changes.lower_commit.as_deref(), Some("cafe0000"));
        assert!(changes.dirty.is_empty(), "retained upper is not dirty");
        assert_eq!(*ops.lock().unwrap(), vec!["dirty"], "no ping on success");
    }

    /// A live old daemon still owns dirty-index-only events that a raw upper walk cannot see.
    /// Fail closed and require a remount instead of presenting that walk as approximate truth.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_changes_do_not_raw_fallback_for_live_old_daemon() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(state.path().join("upper")).unwrap();
        std::fs::write(state.path().join("upper/retained.txt"), "sealed").unwrap();
        let ops = fake_daemon_with_events(
            state.path(),
            Vec::new(),
            serde_json::json!({
                "ok": false,
                "code": "unknown_op",
                "error": "unknown op \"dirty\"",
            }),
        );

        let error = query_local_changes(state.path(), mount.path())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("predates exact local status"));
        assert_eq!(*ops.lock().unwrap(), vec!["dirty", "ping"]);
    }

    /// With no daemon, compatibility falls back to the overlay and says so explicitly. A
    /// pending rename is rendered source -> destination, without a duplicate source delete.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_changes_label_daemon_down_fallback_and_dedupe_rename_delete() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(state.path().join("upper")).unwrap();
        std::fs::create_dir_all(state.path().join("wh/old-dir")).unwrap();
        std::fs::write(state.path().join("upper/changed.txt"), "changed").unwrap();
        std::fs::write(state.path().join("wh/old-dir/file.txt"), "").unwrap();
        std::fs::write(
            state.path().join("redirects.json"),
            serde_json::to_vec(&serde_json::json!({ "new-dir": "old-dir/file.txt" })).unwrap(),
        )
        .unwrap();

        let changes = query_local_changes(state.path(), mount.path())
            .await
            .unwrap();

        assert_eq!(changes.source, DirtySource::OverlayDaemonDown);
        assert!(changes.source.approximate());
        assert_eq!(
            changes.source.warning(),
            Some("daemon not running; showing approximate local changes")
        );
        assert!(!changes.daemon_running);
        assert_eq!(changes.dirty.len(), 2);
        assert_eq!(changes.dirty[0].kind, daemon::DirtyPathKind::Renamed);
        assert_eq!(changes.dirty[0].from.as_deref(), Some("old-dir/file.txt"));
        assert_eq!(changes.dirty[0].path, "new-dir");
        assert_eq!(changes.dirty[1].kind, daemon::DirtyPathKind::Modified);
        assert_eq!(changes.dirty[1].path, "changed.txt");
    }

    #[test]
    fn dirty_reply_requires_semantically_valid_renames() {
        let malformed = serde_json::json!({
            "ok": true,
            "dirty": [{ "path": "new-dir", "kind": "R" }],
            "lower_commit": "cafe0000",
            "watermark": 7,
        });
        assert!(parse_dirty_reply(malformed).is_err());

        let transitional = serde_json::json!({
            "ok": true,
            "dirty": [
                { "path": "new-dir", "kind": "R", "from": "old-dir" },
                { "path": "old-dir", "kind": "D" }
            ],
            "lower_commit": "cafe0000",
            "watermark": 7,
        });
        let (dirty, commit) = parse_dirty_reply(transitional).unwrap();
        assert_eq!(commit, "cafe0000");
        assert_eq!(dirty.len(), 1, "rename source delete is redundant");
        assert_eq!(dirty[0].kind, daemon::DirtyPathKind::Renamed);
    }

    /// Regression for the snapshot data-loss bug: sealing must KEEP the overlay, and the seal
    /// itself now runs inside the daemon (one `seal` control op) — the CLI enumerates and
    /// clears nothing, so ignored paths and writes racing the push window can't be destroyed
    /// (the old flow cleared the whole upper after its own push: a Rust project lost `target/`
    /// on every snapshot, and mid-push writes were deleted without entering any snapshot).
    #[cfg(unix)]
    #[tokio::test]
    async fn snapshot_seal_keeps_overlay_by_default() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let ops = fake_daemon(state.path());

        let upper = state.path().join("upper");
        std::fs::create_dir_all(upper.join("target")).unwrap();
        std::fs::create_dir_all(state.path().join("wh")).unwrap();
        std::fs::write(mount.path().join(".gitignore"), "target/\n").unwrap();
        std::fs::write(upper.join("keep.txt"), "sealed").unwrap();
        std::fs::write(upper.join("target/build.o"), "ignored, never pushed").unwrap();
        // A write landing while the daemon seals, i.e. racing the push window.
        std::fs::write(upper.join("raced.txt"), "written mid-push").unwrap();

        let bar = indicatif::ProgressBar::hidden();
        let (sealed, cleared) = seal_via_daemon(
            state.path(),
            mount.path().to_str().unwrap(),
            Some("msg"),
            false,
            &bar,
        )
        .await
        .unwrap();
        let sealed = sealed.expect("daemon sealed a commit");

        assert_eq!(
            *ops.lock().unwrap(),
            vec!["seal"],
            "one seal op, no clear by default"
        );
        assert_eq!(sealed.commit, "cafe0000");
        assert_eq!(cleared, None, "no clear was requested");
        assert!(upper.join("keep.txt").exists(), "sealed file kept locally");
        assert!(
            upper.join("target/build.o").exists(),
            "ignored path survives the seal"
        );
        assert!(upper.join("raced.txt").exists(), "raced write survives");
    }

    /// `--clear` keeps the old seal-and-clear behavior as an explicit opt-in (it is what
    /// empties the dirty set so `sync` can run) — but the clear now rides the seal request
    /// itself (ONE control op, the daemon clears under its sealer lock), and the caller's
    /// revalidation set is exactly the daemon-reported `cleared` list, which is broader than
    /// the seal delta (it includes ignored/retained files that never enumerate as dirty).
    #[cfg(unix)]
    #[tokio::test]
    async fn snapshot_clear_rides_the_seal_op_and_reports_dropped_paths() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let ops = fake_daemon(state.path());

        let bar = indicatif::ProgressBar::hidden();
        let (sealed, cleared) = seal_via_daemon(
            state.path(),
            mount.path().to_str().unwrap(),
            None,
            true,
            &bar,
        )
        .await
        .unwrap();

        assert_eq!(
            *ops.lock().unwrap(),
            vec!["seal"],
            "the clear must not be a separate control round-trip"
        );
        assert!(sealed.is_some());
        // The fake daemon cleared 3 paths (one sealed, two never-sealed) — the revalidation
        // set came from `cleared`, not the seal delta.
        assert_eq!(cleared, Some(3));
    }

    /// The `seal` op is line-streaming: `{"event": ...}` progress lines narrate onto the
    /// spinner until the final reply line lands.
    #[cfg(unix)]
    #[tokio::test]
    async fn seal_event_lines_update_progress_and_reply_parses() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let _ops = fake_daemon_with_events(
            state.path(),
            vec![
                "hashing 1/2 files (1 KiB)...".to_string(),
                "uploaded 3 chunks (2 KiB)...".to_string(),
            ],
            clean_dirty_reply(),
        );

        let bar = indicatif::ProgressBar::hidden();
        let (sealed, _cleared) = seal_via_daemon(
            state.path(),
            mount.path().to_str().unwrap(),
            Some("msg"),
            false,
            &bar,
        )
        .await
        .unwrap();

        assert_eq!(sealed.expect("sealed").commit, "cafe0000");
        assert_eq!(
            bar.message(),
            "uploaded 3 chunks (2 KiB)...",
            "the spinner followed the streamed event lines"
        );
    }

    /// A clean seal that cleared retained files must say so — never "workspace is clean".
    #[test]
    fn clean_snapshot_message_is_honest_about_cleared_files() {
        assert_eq!(
            clean_snapshot_message(None),
            "Nothing to snapshot: workspace is clean."
        );
        assert_eq!(
            clean_snapshot_message(Some(0)),
            "Nothing to snapshot: workspace is clean."
        );
        assert_eq!(
            clean_snapshot_message(Some(4)),
            "Nothing new to snapshot; cleared 4 locally retained file(s) (including \
             ignored files) from the overlay."
        );
    }

    /// The destructive commands gate on the RAW overlay trees (broader than the dirty walk:
    /// ignored files never enumerate but die with the upper all the same).
    #[test]
    fn overlay_local_state_gate_sees_ignored_files_and_whiteouts() {
        let state = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(state.path().join("upper")).unwrap();
        std::fs::create_dir_all(state.path().join("wh")).unwrap();
        assert!(
            !overlay_has_local_state(state.path()).unwrap(),
            "empty overlay"
        );

        std::fs::create_dir_all(state.path().join("upper/target")).unwrap();
        assert!(
            !overlay_has_local_state(state.path()).unwrap(),
            "bare directories alone are not local data"
        );
        std::fs::write(state.path().join("upper/target/build.o"), "x").unwrap();
        assert!(
            overlay_has_local_state(state.path()).unwrap(),
            "an (ignored-looking) upper file is local state"
        );
        std::fs::remove_file(state.path().join("upper/target/build.o")).unwrap();

        std::fs::write(state.path().join("wh/gone.txt"), "").unwrap();
        assert!(
            overlay_has_local_state(state.path()).unwrap(),
            "a whiteout is local state"
        );
    }

    /// Missing overlay trees are honestly empty, but an UNREADABLE tree must fail closed:
    /// the guard protects data destruction, so "couldn't look" is never "nothing there".
    #[cfg(unix)]
    #[test]
    fn overlay_local_state_gate_fails_closed_on_unreadable_dirs() {
        use std::os::unix::fs::PermissionsExt;
        let state = tempfile::tempdir().unwrap();
        // Neither tree exists yet: that IS a clean overlay (a mount that never wrote).
        assert!(!overlay_has_local_state(state.path()).unwrap());

        let upper = state.path().join("upper");
        std::fs::create_dir_all(upper.join("dir")).unwrap();
        std::fs::write(upper.join("dir/file.txt"), "x").unwrap();
        std::fs::set_permissions(&upper, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Root sees through 0o000 directories; the guard cannot be exercised there.
        if std::fs::read_dir(&upper).is_err() {
            let err = overlay_has_local_state(state.path()).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("cannot verify local overlay state"),
                "unexpected error: {msg}"
            );
            assert!(
                msg.contains("--discard-local"),
                "names the escape hatch: {msg}"
            );
        }
        // Restore so the tempdir can be cleaned up.
        std::fs::set_permissions(&upper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}
