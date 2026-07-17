//! `tl fs` — filesystems on artifact storage: create, mount, save, restore.
//!
//! Product model (the fs/git split): the *filesystem* is the unit `tl fs` manages — `create`
//! makes one (a `kind=filesystem` storage namespace), `ls` lists them, `rm` deletes them,
//! `mount` attaches one, and `push` uploads a folder into one. Native saves use metadata trees
//! plus aggregate blob segments and are deliberately not Git-addressable.
//!
//! The mount daemon's journal is the authority for local changes. Its background preparer reads,
//! hashes, compresses, and uploads changed content before a save is requested; autosave or
//! `snapshot` then publishes the prepared root. If preparation has not caught up, `snapshot`
//! reports a pending watermark without blocking the mount. Published upper files remain as a
//! local byte cache and are reported by `status` as retained. `snapshot --clear` trims the
//! published generation's retained cache without touching later writes or ignored/local-only
//! files. `restore` moves the filesystem back to a retained save without transferring file bytes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Component, Path, PathBuf};

use comfy_table::Cell;
use console::style;
use futures::StreamExt;
use ignore::Match;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::ingest::{PushFile, PushOptions, PushSource};
use tensorlake::artifact_storage::merge::MergeRequest;
use tensorlake::artifact_storage::models::GitCredential;
use tensorlake::artifact_storage::native_fs::{
    NativeChangeSet, NativeLocalUpsert, NativePreparedSnapshotCandidate, NativePushEvent,
    NativePushOptions, NativePushProgress, NativePushReport, NativeSnapshotInfo,
    NativeWorkspaceInfo,
};
use tensorlake::artifact_storage::workspaces::{
    CreateWorkspaceRequest, GitMountSource, GitSmartlogPage, GitWorkspaceLogPage, PromoteOutcome,
    PromoteWorkspaceRequest, RebaseWorkspaceRequest, SyncWorkspaceRequest, TreeEntry,
    WorkspaceFleetItem, WorkspaceFleetQuery, WorkspaceInfo,
};

use crate::auth::context::CliContext;
use crate::commands::git::{artifact_storage_client, project_id};
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub mod daemon;
#[cfg(target_os = "linux")]
pub mod fusefs;
#[cfg(unix)]
mod generation_capture;
pub mod local;
pub mod local_state;
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
pub(crate) const LOCAL_STATE_WRITER_LOCK: &str = "snapshot-state.writer.lock";
pub(crate) const LOCAL_STATE_FORMAT_MARKER: &str = "snapshot-state.format-v5";
const LOCAL_STATE_REPAIR_MARKER: &str = "snapshot-state.repairing.json";

/// Serialize the writable native mount daemon with offline journal repair.
///
/// redb itself protects an open database, but repair must also replace a missing/corrupt database
/// and retire stale generation captures. A dedicated kernel-owned flock covers that larger
/// artifact set and disappears automatically when either process crashes.
#[cfg(unix)]
pub(crate) fn try_local_state_writer_lock(state_dir: &Path) -> Result<Option<std::fs::File>> {
    plaindir::flock_exclusive(&state_dir.join(LOCAL_STATE_WRITER_LOCK), false)
}

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

/// The authenticated subject inside a minted git credential — an unverified JWT-payload decode,
/// used only to filter a listing client-side (the server enforces real principal checks). `None`
/// for opaque tokens: dev/self-hosted `TENSORLAKE_GIT_TOKEN` values, which usually mean an
/// open-mode server where workspaces are unbound. Known gap: a *static-token* server binds
/// workspaces to token fingerprints, so against one that also predates the server-side
/// `principal=self` filter, an opaque token skips filtering entirely — deploy order (server
/// before CLI) is what closes that window.
fn token_subject(token: &str) -> Option<String> {
    use base64::Engine;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    Some(claims.get("sub")?.as_str()?.to_string())
}

/// The mount-facing workspace record, rebuilt from a fleet row. Lease fields come off the wire
/// (`lease_due_ms` absent = pinned); servers that predate them omit both, which decodes to the
/// durable-workspace constants.
fn fleet_item_to_info(item: &WorkspaceFleetItem) -> WorkspaceInfo {
    WorkspaceInfo {
        id: item.id.clone(),
        ref_name: if item.storage == "native" {
            "fs/head".to_string()
        } else {
            format!("refs/workspaces/{}", item.id)
        },
        principal: item
            .created_by
            .as_ref()
            .map(|by| by.name.clone())
            .unwrap_or_default(),
        base: item.base.clone(),
        base_ref: item.base_ref.clone(),
        head: item.head.clone(),
        created_at_secs: item.created_at_secs,
        lease_secs: item.lease_secs,
        lease_due_ms: item.lease_due_ms,
        pinned: item.lease_due_ms.is_none(),
        shared_target: item.shared_target.clone(),
    }
}

fn native_workspace_to_mount_info(workspace: NativeWorkspaceInfo) -> WorkspaceInfo {
    let base = workspace.base_snapshot_id.unwrap_or_default();
    let head = workspace
        .latest_snapshot_id
        .clone()
        .unwrap_or_else(|| base.clone());
    WorkspaceInfo {
        id: workspace.workspace_id,
        ref_name: "fs/head".to_string(),
        principal: workspace.principal,
        base,
        base_ref: Some("fs/head".to_string()),
        head,
        created_at_secs: workspace.created_at_ms / 1000,
        lease_secs: 0,
        lease_due_ms: workspace.expires_at_ms,
        pinned: workspace.expires_at_ms.is_none(),
        // The existing mount state machine interprets Some as publish-on-save. The native mount
        // client ignores the Git-shaped spelling and follows `/fs/head` directly.
        shared_target: (!workspace.read_only).then(|| "native-head".to_string()),
    }
}

fn filesystem_session_json(session: &WorkspaceInfo) -> serde_json::Value {
    serde_json::json!({
        "session_id": session.id,
        "principal": session.principal,
        "base_save_id": (!session.base.is_empty()).then_some(&session.base),
        "latest_save_id": (!session.head.is_empty()).then_some(&session.head),
        "created_at_secs": session.created_at_secs,
        "expires_at_ms": session.lease_due_ms,
        "read_only": session.shared_target.is_none(),
    })
}

/// Every workspace visible to this caller across the whole project — one paginated fleet
/// request instead of one listing per file system — newest first, as `(file system, item)`.
///
/// The server narrows to the caller's own + unbound workspaces (`principal=self`); the same
/// filter is applied here too, because servers that predate the parameter ignore it and would
/// leak other principals' rows into the listing.
async fn fleet_workspaces(
    session: &FsSession,
    file_system: Option<&str>,
    id_query: Option<&str>,
) -> Result<Vec<(String, WorkspaceFleetItem)>> {
    let (user, token) = session.creds();
    let own_sub = token_subject(token);
    let repo_prefix = format!("{}/", session.project_id);
    let items = session
        .client
        .workspace_fleet_all(
            &session.project_id,
            user,
            token,
            &WorkspaceFleetQuery {
                repo: file_system,
                q: id_query,
                principal_self: true,
                after: None,
                limit: Some(200),
            },
        )
        .await?
        .into_inner();
    let mut rows: Vec<(String, WorkspaceFleetItem)> = items
        .into_iter()
        .filter(|item| match (&item.created_by, &own_sub) {
            // Drop only what is provably someone else's: a bound workspace whose principal
            // differs from our decoded subject. Unbound rows and undecodable tokens pass.
            (Some(by), Some(sub)) => by.name == *sub,
            _ => true,
        })
        .map(|item| {
            // The fleet reports full repo ids (`{project}/{repo}`); everything here speaks
            // bare file-system names.
            let fs = item
                .repo
                .strip_prefix(&repo_prefix)
                .unwrap_or(&item.repo)
                .to_string();
            (fs, item)
        })
        .collect();
    rows.sort_by_key(|(_, item)| std::cmp::Reverse(item.created_at_secs));
    Ok(rows)
}

/// Resolve a workspace id (or unique prefix) to its file system + record, project-wide. The
/// fleet index finds the id (`q` narrows server-side); the authoritative record then comes from
/// the per-repo, principal-checked API — resolution never widens what the caller may touch.
async fn resolve_workspace(
    session: &FsSession,
    id: &str,
) -> Result<Option<(String, WorkspaceInfo)>> {
    let mut matches: Vec<(String, String)> = fleet_workspaces(session, None, Some(id))
        .await?
        .into_iter()
        .filter(|(_, item)| item.storage != "native")
        // The fleet's `q` is a substring match; a prefix is what resolves here.
        .filter(|(_, item)| item.id.starts_with(id))
        .map(|(fs, item)| (fs, item.id))
        .collect();
    match matches.len() {
        0 => Ok(None),
        1 => {
            let (fs, full_id) = matches.remove(0);
            let (user, token) = session.creds();
            match session
                .client
                .get_workspace(&session.project_id, &fs, user, token, &full_id)
                .await
            {
                Ok(ws) => Ok(Some((fs, ws.into_inner()))),
                // Deleted between the fleet hit and this fetch: resolve as absent (the caller
                // prints the friendly "no workspace matches"), not as a raw server error.
                Err(tensorlake::error::SdkError::ServerError { status, .. })
                    if status == reqwest::StatusCode::NOT_FOUND =>
                {
                    Ok(None)
                }
                Err(e) => Err(e.into()),
            }
        }
        n => Err(CliError::usage(format!(
            "workspace id {id:?} is ambiguous ({n} matches); use more characters"
        ))),
    }
}

/// One `tl fs ls` row: the workspace record plus project-fleet liveness enrichments. A named
/// filesystem is rendered directly from its repo-scoped native workspace page.
struct LsRow {
    fs: String,
    ws: WorkspaceInfo,
    status: Option<String>,
    snapshot_count: Option<u64>,
    mounted_on: Option<String>,
}

/// `tl fs ls [file-system]` — every live workspace (across all file systems by default), with
/// where each one is currently mounted on this machine.
pub async fn ls(ctx: &CliContext, file_system: Option<&str>, output_json: bool) -> Result<()> {
    // A named filesystem works with a repo-scoped token; the all-filesystems form uses the bounded
    // project fleet below.
    if let Some(fs) = file_system {
        let session = FsSession::open(ctx, Some(fs)).await?;
        let (user, token) = session.creds();
        let mut workspaces = session
            .client
            .list_native_workspaces_with_credential(&session.project_id, fs, user, token)
            .await?;
        workspaces.sort_by_key(|workspace| std::cmp::Reverse(workspace.created_at_ms));
        return print_native_workspaces(fs, workspaces, output_json);
    }
    // Project-wide session first: the fleet listing requires `project:read`, which repo-scoped
    // credentials deliberately lack — a named file system narrows server-side via `?repo=`.
    let session = FsSession::open(ctx, None).await?;
    let rows: Vec<LsRow> = fleet_workspaces(&session, None, None)
        .await?
        .into_iter()
        .filter(|(_, item)| item.storage == "native")
        .map(|(fs, item)| LsRow {
            fs,
            ws: fleet_item_to_info(&item),
            status: Some(item.status),
            snapshot_count: Some(item.snapshot_count),
            mounted_on: item.mounted_on,
        })
        .collect();
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
            .map(|row| {
                // `mounted_at` is the plain path (machine-consumable); `kind` says whether it
                // is a kernel mount or a plain-directory binding — decorating the path itself
                // broke every consumer that fed it back to another command.
                let attached = attachment(&row.ws.id);
                serde_json::json!({
                    "file_system": row.fs,
                    "session": filesystem_session_json(&row.ws),
                    "mounted_at": attached.as_ref().map(|(path, _)| path.clone()),
                    "kind": attached.as_ref().map(|(_, kind)| *kind),
                    // Fleet liveness enrichments; null on the per-repo fallback path.
                    "status": row.status,
                    "save_count": row.snapshot_count,
                    "mounted_on": row.mounted_on,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("No sessions. Mount the filesystem to start one: tl fs mount <filesystem> <path>");
        return Ok(());
    }
    let mut table = new_table(&[
        "Session",
        "Filesystem",
        "Base",
        "Saves",
        "Mode",
        "Mounted",
        "Age",
    ]);
    for row in &rows {
        let ws = &row.ws;
        table.add_row(vec![
            Cell::new(&ws.id),
            Cell::new(&row.fs),
            Cell::new(ws.base_ref.as_deref().unwrap_or(short_id(&ws.base))),
            Cell::new(if ws.head == ws.base { "-" } else { "yes" }),
            Cell::new(match &ws.shared_target {
                Some(_) => "publishing".to_string(),
                None => "private".to_string(),
            }),
            // Human output keeps the annotation; the JSON path/kind split serves machines.
            // A mount session on another machine (fleet liveness) shows as its host.
            Cell::new(match attachment(&ws.id) {
                Some((path, "binding")) => format!("{path} (bound)"),
                Some((path, _)) => path,
                None => match &row.mounted_on {
                    Some(host) => format!("{host} (remote)"),
                    None => "-".to_string(),
                },
            }),
            Cell::new(age_display(ws.created_at_secs)),
        ]);
    }
    println!("{table}");
    println!("Remounting the filesystem resumes its newest detached session on this machine.");
    Ok(())
}

fn print_native_workspaces(
    fs: &str,
    workspaces: Vec<NativeWorkspaceInfo>,
    output_json: bool,
) -> Result<()> {
    let mounts = live_mounts();
    let mounted_at = |id: &str| {
        mounts
            .iter()
            .find(|(_, state)| state.workspace_id == id)
            .map(|(path, _)| path.clone())
    };
    if output_json {
        let rows: Vec<_> = workspaces
            .iter()
            .map(|workspace| {
                let session = native_workspace_to_mount_info(workspace.clone());
                serde_json::json!({
                    "file_system": fs,
                    "session": filesystem_session_json(&session),
                    "mounted_at": mounted_at(&workspace.workspace_id),
                    "kind": mounted_at(&workspace.workspace_id).map(|_| "mount"),
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if workspaces.is_empty() {
        println!("No sessions. Mount the filesystem to start one: tl fs mount {fs} <path>");
        return Ok(());
    }
    let mut table = new_table(&[
        "Session",
        "Filesystem",
        "Base",
        "Saves",
        "Mode",
        "Mounted",
        "Age",
    ]);
    for workspace in workspaces {
        let base = workspace.base_snapshot_id.as_deref().unwrap_or("-");
        let latest = workspace.latest_snapshot_id.as_deref().unwrap_or(base);
        table.add_row(vec![
            Cell::new(&workspace.workspace_id),
            Cell::new(fs),
            Cell::new(short_id(base)),
            Cell::new(if latest == base { "-" } else { "yes" }),
            Cell::new(if workspace.read_only {
                "read-only"
            } else {
                "publishing"
            }),
            Cell::new(mounted_at(&workspace.workspace_id).unwrap_or_else(|| "-".to_string())),
            Cell::new(age_display(workspace.created_at_ms / 1000)),
        ]);
    }
    println!("{table}");
    println!("Remounting the filesystem resumes its newest detached session on this machine.");
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The filesystem product surface: create / ls / rm / mount / push / history. A filesystem is a
// `kind=filesystem` repo; the vocabulary here is drives and saves — never branches, commits, or
// workspaces (sessions at most). `tl git mount` is the branch-vocabulary sibling on the same
// engine.
// ---------------------------------------------------------------------------------------------

/// How often an fs-surface mount autosaves when the user says nothing. A drive doesn't lose
/// your work; `tl fs snapshot -m` remains the named save point.
const FS_AUTOSAVE_DEFAULT_SECS: u64 = 30;

/// `tl fs create <name>` — a new empty filesystem. Born with a genesis "empty" save
/// server-side, so it mounts immediately.
pub async fn create_filesystem(ctx: &CliContext, name: &str, output_json: bool) -> Result<()> {
    // FsSession caches the minted credential across invocations; the raw client would mint a
    // fresh platform token on every call.
    let session = FsSession::open(ctx, None).await?;
    let (user, token) = session.creds();
    session
        .client
        .create_repo_with_credential(
            &session.project_id,
            name,
            None,
            Some(tensorlake::artifact_storage::models::REPO_KIND_FILESYSTEM),
            user,
            token,
        )
        .await?;
    // A native filesystem has no Git genesis commit. Publish the canonical empty directory so a
    // just-created filesystem has a mountable/time-travelable head immediately.
    let empty = tempfile::tempdir()?;
    if let Err(error) = session
        .client
        .push_native_directory_with_credential(
            &session.project_id,
            name,
            empty.path(),
            user,
            token,
            NativePushOptions {
                message: "Initial empty filesystem".to_string(),
                ..Default::default()
            },
        )
        .await
    {
        // Creation is one product operation. Do not leave a half-created, headless filesystem
        // when initial snapshot verification or publication fails.
        let _ = session
            .client
            .delete_repo_with_credential(&session.project_id, name, user, token)
            .await;
        return Err(error.into());
    }
    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "filesystem": name }))?
        );
        return Ok(());
    }
    println!("Created filesystem {name}.");
    println!("  mount it:        tl fs mount {name} <path>");
    println!("  or push a folder: tl fs push <dir> {name}");
    Ok(())
}

/// `tl fs ls` — the filesystems of this project, with where each is attached on this machine.
pub async fn ls_filesystems(ctx: &CliContext, output_json: bool) -> Result<()> {
    let session = FsSession::open(ctx, None).await?;
    let (user, token) = session.creds();
    let listing = session
        .client
        .list_repos_with_credential(
            &session.project_id,
            Some(tensorlake::artifact_storage::models::REPO_KIND_FILESYSTEM),
            user,
            token,
        )
        .await?
        .into_inner();
    // Local attachments: kernel mounts and plain-directory bindings, keyed by backing repo.
    let mounts = live_mounts();
    let bound = plaindir::bound_binding_repos();
    let tracked = load_tracked_directories()?;
    let attached = |fs: &str| -> Vec<String> {
        let mut at: Vec<String> = mounts
            .iter()
            .filter(|(_, s)| s.repo == fs)
            .map(|(m, _)| m.clone())
            .collect();
        at.extend(
            bound
                .iter()
                .filter(|(repo, _)| repo == fs)
                .map(|(_, root)| format!("{root} (bound)")),
        );
        at.extend(
            tracked
                .values()
                .filter(|attachment| attachment.filesystem_id == fs)
                .map(|attachment| format!("{} (tracked)", attachment.root)),
        );
        at
    };
    if output_json {
        let out: Vec<serde_json::Value> = listing
            .repos
            .iter()
            .map(|r| {
                serde_json::json!({
                    "name": r.name,
                    "status": r.status,
                    "attached_at": attached(&r.name),
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }
    if listing.repos.is_empty() {
        println!("No filesystems. Create one with: tl fs create <name>");
        return Ok(());
    }
    let mut table = new_table(&["Filesystem", "Status", "Attached"]);
    for r in &listing.repos {
        let at = attached(&r.name);
        table.add_row(vec![
            Cell::new(&r.name),
            Cell::new(&r.status),
            Cell::new(if at.is_empty() {
                "-".to_string()
            } else {
                at.join(", ")
            }),
        ]);
    }
    println!("{table}");
    println!("Sessions of one filesystem: tl fs ls <filesystem>");
    Ok(())
}

/// `tl fs rm <name>` — delete a filesystem and everything in it. Fail-closed against local
/// attachments: a live mount or tracked directory of this filesystem on this machine must be
/// detached first, or the delete would strand a running daemon (or a binding's future saves)
/// against a dead repo.
pub async fn rm_filesystem(ctx: &CliContext, name: &str, force: bool) -> Result<()> {
    let session = FsSession::open(ctx, None).await?;
    let (user, token) = session.creds();
    // Authoritative point-read: read-your-writes for a just-created filesystem, and the
    // 404/kind answers never come from a stale listing.
    let meta = match session
        .client
        .repo_meta_with_credential(&session.project_id, name, user, token)
        .await
    {
        Ok(meta) => meta.into_inner(),
        Err(tensorlake::error::SdkError::ServerError { status, .. }) if status.as_u16() == 404 => {
            return Err(CliError::usage(format!(
                "no filesystem named {name:?} (see: tl fs ls)"
            )));
        }
        Err(e) => return Err(e.into()),
    };
    if !meta.is_filesystem() {
        return Err(CliError::usage(format!(
            "{name} is a repository, not a filesystem — use: tl git rm {name}"
        )));
    }
    if let Some((mountpoint, _, _, alive)) = local_mount_states()
        .into_iter()
        .find(|(_, _, state, _)| state.repo == name)
    {
        return Err(CliError::usage(format!(
            "{name} has {} mount state at {mountpoint}; unmount first: tl fs unmount {mountpoint}",
            if alive {
                "a live"
            } else {
                "recoverable detached"
            }
        )));
    }
    if let Some((_, root)) = plaindir::bound_binding_repos()
        .into_iter()
        .find(|(repo, _)| repo == name)
    {
        return Err(CliError::usage(format!(
            "{name} is tracking {root}; stop first: tl fs unmount {root}"
        )));
    }
    if let Some(attachment) = load_tracked_directories()?
        .into_values()
        .find(|attachment| attachment.filesystem_id == name)
    {
        return Err(CliError::usage(format!(
            "{name} is tracking {}; stop first: tl fs unmount {}",
            attachment.root, attachment.root
        )));
    }
    if !force {
        // The session count is prompt garnish — skip the round trip entirely under --force
        // and tolerate its absence.
        let sessions = fleet_workspaces(&session, Some(name), None)
            .await
            .map(|rows| rows.len())
            .unwrap_or(0);
        let prompt = format!(
            "Delete filesystem {name} and all its saves{}?",
            if sessions > 0 {
                format!(" ({sessions} session(s) die with it)")
            } else {
                String::new()
            }
        );
        let confirmed = dialoguer::Confirm::new()
            .with_prompt(prompt)
            .default(false)
            .interact()
            .unwrap_or(false);
        if !confirmed {
            println!("Aborted.");
            return Ok(());
        }
    }
    session
        .client
        .delete_repo_with_credential(&session.project_id, name, user, token)
        .await?;
    println!("Deleted filesystem {name}.");
    Ok(())
}

/// `tl fs token <name>` — mint the narrow credential a sandbox (or any remote environment)
/// needs to attach this one filesystem. Everything on the attach path — mount, saves,
/// history-by-follow — works under it; project-scope surfaces (ls, history, rm) do not.
pub async fn token(ctx: &CliContext, name: &str, output_json: bool) -> Result<()> {
    let project_id = crate::commands::git::project_id(ctx)?;
    let client = crate::commands::git::artifact_storage_client(ctx)?;
    let credential = client
        .mint_token_for_repo(&project_id, Some(name))
        .await
        .map_err(crate::commands::git::map_sdk_error)?
        .into_inner();
    if output_json {
        println!("{}", serde_json::to_string_pretty(&credential)?);
        return Ok(());
    }
    println!(
        "Scoped credential for filesystem {name} (expires {}):",
        credential.expires_at
    );
    println!("  {}", credential.token);
    println!();
    println!("Attach from any sandbox with FUSE:");
    println!("  export TENSORLAKE_GIT_TOKEN={}", credential.token);
    println!("  tl fs mount {name} <path>");
    Ok(())
}

const COLD_PUSH_STATE_FORMAT: u16 = 1;
const COLD_PUSH_DIRTY_SENTINEL: &str = "__tensorlake_cold_import_root__";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ColdPushStoreIdentity {
    format_ver: u16,
    api_url: String,
    project_id: String,
    filesystem_id: String,
    source_root: String,
    store_uuid: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct TrackedDirectoryAttachment {
    format_ver: u16,
    root: String,
    api_url: String,
    project_id: String,
    organization_id: Option<String>,
    filesystem_id: String,
    state_dir: PathBuf,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ColdPushCapture {
    format_ver: u16,
    source_root: String,
    mode: ColdPushCaptureMode,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum ColdPushCaptureMode {
    /// The unavoidable cold scan has already prepared and uploaded an immutable candidate. The
    /// candidate lives in this atomic frozen-generation record until `mark_prepared` installs the
    /// indexed copy, closing the crash window without copying the whole source tree.
    Full {
        candidate: NativePreparedSnapshotCandidate,
        baselines: Vec<ColdPushObservedPath>,
    },
    /// A repeated push captured only changed/new path bytes into generation-owned immutable
    /// sources. Unchanged files were never opened.
    Delta {
        upserts: Vec<ColdPushCapturedUpsert>,
        deletes: Vec<String>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ColdPushObservedPath {
    path: String,
    identity: local_state::FileIdentity,
    observed_at_secs: i64,
    observed_at_nanos: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ColdPushCapturedUpsert {
    path: String,
    source: PathBuf,
    identity: local_state::FileIdentity,
    observed_at_secs: i64,
    observed_at_nanos: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ColdPushOutcome {
    operation_id: String,
    snapshot_id: String,
    files: usize,
    logical_bytes: u64,
    uploaded_bytes: u64,
    transport: String,
    recovered: bool,
    #[serde(default)]
    unchanged: bool,
}

fn cold_push_state_root() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("share")
        .join("tensorlake")
        .join("pushes")
}

fn tracked_directories_registry_path() -> PathBuf {
    crate::config::files::config_dir().join("tracked-directories.json")
}

fn tracked_directories_registry_lock() -> Result<std::fs::File> {
    std::fs::create_dir_all(crate::config::files::config_dir())?;
    plaindir::flock_exclusive(
        &crate::config::files::config_dir().join("tracked-directories.lock"),
        true,
    )?
    .ok_or_else(|| CliError::usage("could not lock the tracked-directory registry"))
}

fn load_tracked_directories() -> Result<BTreeMap<String, TrackedDirectoryAttachment>> {
    let path = tracked_directories_registry_path();
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(error) => return Err(error.into()),
    };
    let attachments: BTreeMap<String, TrackedDirectoryAttachment> = serde_json::from_slice(&raw)
        .map_err(|error| {
            CliError::usage(format!(
                "tracked-directory registry {} is corrupt: {error}",
                path.display()
            ))
        })?;
    for (root, attachment) in &attachments {
        if attachment.format_ver != COLD_PUSH_STATE_FORMAT || attachment.root != *root {
            return Err(CliError::usage(format!(
                "tracked-directory registry entry {root} has an unsupported or mismatched format"
            )));
        }
    }
    Ok(attachments)
}

fn save_tracked_directories(
    attachments: &BTreeMap<String, TrackedDirectoryAttachment>,
) -> Result<()> {
    std::fs::create_dir_all(crate::config::files::config_dir())?;
    plaindir::write_atomic(
        &tracked_directories_registry_path(),
        &serde_json::to_vec_pretty(attachments)?,
    )?;
    Ok(())
}

fn register_tracked_directory(
    ctx: &CliContext,
    root: &Path,
    filesystem_id: &str,
    project_id: &str,
    state_dir: &Path,
) -> Result<()> {
    let root = root.to_string_lossy().into_owned();
    let _lock = tracked_directories_registry_lock()?;
    let mut attachments = load_tracked_directories()?;
    for (other_root, attachment) in &attachments {
        let other = Path::new(other_root);
        let candidate = Path::new(&root);
        if candidate.starts_with(other) || other.starts_with(candidate) {
            if other_root == &root
                && attachment.api_url == ctx.api_url
                && attachment.project_id == project_id
                && attachment.filesystem_id == filesystem_id
                && attachment.state_dir == state_dir
            {
                return Ok(());
            }
            return Err(CliError::usage(format!(
                "{} overlaps tracked directory {} for filesystem {}; stop tracking it with `tl \
                 fs unmount {}` before attaching another filesystem",
                root, other_root, attachment.filesystem_id, other_root
            )));
        }
    }
    attachments.insert(
        root.clone(),
        TrackedDirectoryAttachment {
            format_ver: COLD_PUSH_STATE_FORMAT,
            root,
            api_url: ctx.api_url.clone(),
            project_id: project_id.to_string(),
            organization_id: ctx.organization_id.clone(),
            filesystem_id: filesystem_id.to_string(),
            state_dir: state_dir.to_path_buf(),
        },
    );
    save_tracked_directories(&attachments)
}

fn tracked_directory_for(path: &Path) -> Result<Option<TrackedDirectoryAttachment>> {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    Ok(load_tracked_directories()?
        .into_values()
        .filter(|attachment| path.starts_with(&attachment.root))
        .max_by_key(|attachment| Path::new(&attachment.root).components().count()))
}

fn remove_tracked_directory(root: &str) -> Result<Option<TrackedDirectoryAttachment>> {
    let _lock = tracked_directories_registry_lock()?;
    let mut attachments = load_tracked_directories()?;
    let removed = attachments.remove(root);
    save_tracked_directories(&attachments)?;
    Ok(removed)
}

fn open_tracked_directory_state(
    attachment: &TrackedDirectoryAttachment,
) -> Result<local_state::LocalStateReader> {
    let identity = read_cold_push_identity(&attachment.state_dir.join("identity.json"))?;
    local_state::LocalState::open_existing(
        attachment.state_dir.join(local_state::LOCAL_STATE_FILE),
        local_state::LocalStateIdentity {
            project_id: identity.project_id,
            filesystem: identity.filesystem_id,
            workspace_id: format!(
                "cold-push:{}",
                cold_push_state_key(
                    &identity.api_url,
                    &attachment.project_id,
                    &attachment.filesystem_id,
                    Path::new(&attachment.root),
                )
            ),
            store_uuid: identity.store_uuid,
        },
    )
    .map_err(|error| cold_push_state_error(&attachment.state_dir, error))
}

fn cold_push_state_key(
    api_url: &str,
    project_id: &str,
    filesystem_id: &str,
    root: &Path,
) -> String {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    for component in [
        api_url.as_bytes(),
        project_id.as_bytes(),
        filesystem_id.as_bytes(),
        root.as_os_str().as_encoded_bytes(),
    ] {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component);
    }
    hex::encode(digest.finalize())
}

fn cold_push_state_error(state_dir: &Path, error: impl std::fmt::Display) -> CliError {
    CliError::usage(format!(
        "cold-push recovery state {} is unavailable: {error}; refusing to start a second \
         publication attempt. Preserve this directory for inspection, or remove it only after \
         confirming the prior push did not publish.",
        state_dir.display()
    ))
}

fn read_cold_push_identity(path: &Path) -> Result<ColdPushStoreIdentity> {
    let raw = std::fs::read(path).map_err(|error| {
        CliError::usage(format!(
            "cannot read cold-push identity {}: {error}; refusing to guess",
            path.display()
        ))
    })?;
    let identity: ColdPushStoreIdentity = serde_json::from_slice(&raw).map_err(|error| {
        CliError::usage(format!(
            "cold-push identity {} is corrupt ({error}); refusing to guess",
            path.display()
        ))
    })?;
    if identity.format_ver != COLD_PUSH_STATE_FORMAT {
        return Err(CliError::usage(format!(
            "cold-push identity {} uses unsupported format {} (this client supports {})",
            path.display(),
            identity.format_ver,
            COLD_PUSH_STATE_FORMAT
        )));
    }
    Ok(identity)
}

fn open_cold_push_state_at(
    state_root: &Path,
    api_url: &str,
    project_id: &str,
    filesystem_id: &str,
    root: &Path,
) -> Result<(PathBuf, local_state::LocalState)> {
    let key = cold_push_state_key(api_url, project_id, filesystem_id, root);
    let state_dir = state_root.join(&key);
    std::fs::create_dir_all(&state_dir)?;
    let identity_path = state_dir.join("identity.json");
    let wanted = ColdPushStoreIdentity {
        format_ver: COLD_PUSH_STATE_FORMAT,
        api_url: api_url.to_string(),
        project_id: project_id.to_string(),
        filesystem_id: filesystem_id.to_string(),
        source_root: root.to_string_lossy().into_owned(),
        store_uuid: uuid::Uuid::new_v4().to_string(),
    };
    let (identity, identity_created) = match std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&identity_path)
    {
        Ok(mut file) => {
            use std::io::Write;

            let encoded = serde_json::to_vec_pretty(&wanted)?;
            file.write_all(&encoded)?;
            file.sync_all()?;
            std::fs::File::open(&state_dir)?.sync_all()?;
            (wanted, true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            (read_cold_push_identity(&identity_path)?, false)
        }
        Err(error) => return Err(error.into()),
    };
    if identity.api_url != api_url
        || identity.project_id != project_id
        || identity.filesystem_id != filesystem_id
        || identity.source_root != root.to_string_lossy()
    {
        return Err(CliError::usage(format!(
            "cold-push state {} belongs to a different source or remote; refusing to reuse it",
            state_dir.display()
        )));
    }
    let store_identity = local_state::LocalStateIdentity {
        project_id: project_id.to_string(),
        filesystem: filesystem_id.to_string(),
        workspace_id: format!("cold-push:{key}"),
        store_uuid: identity.store_uuid,
    };
    let database_path = state_dir.join(local_state::LOCAL_STATE_FILE);
    if !identity_created && !database_path.exists() {
        return Err(CliError::usage(format!(
            "tracked-directory identity {} exists but its durable database is missing; refusing \
             to treat the directory as a first push because the prior remote baseline is \
             unprovable. Preserve {}, then remove that state directory only after inspecting the \
             filesystem head.",
            identity_path.display(),
            state_dir.display(),
        )));
    }
    let store = local_state::LocalState::open(database_path, store_identity)
        .map_err(|error| cold_push_state_error(&state_dir, error))?;
    Ok((state_dir, store))
}

fn cold_push_candidate(
    store: &local_state::LocalState,
    generation: u64,
    state_dir: &Path,
) -> Result<Option<NativePreparedSnapshotCandidate>> {
    store
        .prepared(generation)
        .map_err(|error| cold_push_state_error(state_dir, error))?
        .map(|prepared| {
            serde_json::from_slice(&prepared.candidate).map_err(|error| {
                CliError::usage(format!(
                    "cold-push candidate in {} is corrupt ({error}); refusing to re-scan and \
                     risk publishing different bytes under the same request",
                    state_dir.display()
                ))
            })
        })
        .transpose()
}

fn cold_push_outcome_from_candidate(
    candidate: &NativePreparedSnapshotCandidate,
    request_id: String,
    snapshot_id: String,
    recovered: bool,
) -> ColdPushOutcome {
    ColdPushOutcome {
        operation_id: request_id,
        snapshot_id,
        files: candidate.files(),
        logical_bytes: candidate.logical_bytes(),
        uploaded_bytes: candidate.uploaded_bytes(),
        transport: "prepared_journal".to_string(),
        recovered,
        unchanged: false,
    }
}

pub(crate) fn next_native_publish_operation_id(
    request_id: &str,
    next_attempt: u32,
    base_snapshot_id: &str,
    root_id: &str,
) -> String {
    use sha2::{Digest as _, Sha256};

    let mut digest = Sha256::new();
    for component in [
        request_id.as_bytes(),
        &next_attempt.to_be_bytes(),
        base_snapshot_id.as_bytes(),
        root_id.as_bytes(),
    ] {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component);
    }
    hex::encode(digest.finalize())
}

#[derive(Debug)]
struct ColdPushScan {
    paths: Vec<ColdPushObservedPath>,
}

#[derive(Debug)]
struct ColdPushDeltaPlan {
    upserts: Vec<ColdPushObservedPath>,
    deletes: Vec<String>,
}

fn cold_push_now_stamp() -> (i64, i64) {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| (duration.as_secs() as i64, duration.subsec_nanos() as i64))
        .unwrap_or((0, 0))
}

#[cfg(unix)]
fn cold_push_file_identity(metadata: &std::fs::Metadata) -> local_state::FileIdentity {
    use std::os::unix::fs::MetadataExt;

    local_state::FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.size(),
        mtime_secs: metadata.mtime(),
        mtime_nanos: metadata.mtime_nsec(),
        ctime_secs: metadata.ctime(),
        ctime_nanos: metadata.ctime_nsec(),
        mode: metadata.mode(),
    }
}

#[cfg(not(unix))]
fn cold_push_file_identity(metadata: &std::fs::Metadata) -> local_state::FileIdentity {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| (duration.as_secs() as i64, duration.subsec_nanos() as i64))
        .unwrap_or((0, 0));
    local_state::FileIdentity {
        device: 0,
        inode: 0,
        size: metadata.len(),
        mtime_secs: modified.0,
        mtime_nanos: modified.1,
        ctime_secs: modified.0,
        ctime_nanos: modified.1,
        mode: if metadata.is_dir() {
            0o040755
        } else {
            0o100644
        },
    }
}

fn scan_cold_push_tree(root: &Path) -> Result<ColdPushScan> {
    let (observed_at_secs, observed_at_nanos) = cold_push_now_stamp();
    let mut ignore = SnapshotIgnore::new(root);
    let mut paths = Vec::new();
    scan_cold_push_dir(
        root,
        "",
        &mut ignore,
        observed_at_secs,
        observed_at_nanos,
        &mut paths,
    )?;
    paths.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(ColdPushScan { paths })
}

fn scan_cold_push_dir(
    root: &Path,
    rel_dir: &str,
    ignore: &mut SnapshotIgnore,
    observed_at_secs: i64,
    observed_at_nanos: i64,
    paths: &mut Vec<ColdPushObservedPath>,
) -> Result<()> {
    let absolute = if rel_dir.is_empty() {
        root.to_path_buf()
    } else {
        root.join(rel_dir)
    };
    let entries = std::fs::read_dir(&absolute).map_err(|error| {
        CliError::usage(format!(
            "cannot read directory {}: {error}; aborting the push because a skipped subtree \
             would be interpreted as deletion",
            absolute.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            CliError::usage(format!(
                "cannot read an entry of {}: {error}; aborting the push",
                absolute.display()
            ))
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(CliError::usage(format!(
                "{} has a non-UTF-8 name; filesystem snapshot paths are UTF-8",
                absolute.join(&name).display()
            )));
        };
        let path = if rel_dir.is_empty() {
            name.to_string()
        } else {
            format!("{rel_dir}/{name}")
        };
        let source = entry.path();
        let metadata = std::fs::symlink_metadata(&source).map_err(|error| {
            CliError::usage(format!(
                "cannot stat {}: {error}; aborting the push",
                source.display()
            ))
        })?;
        let file_type = metadata.file_type();
        if ignore.is_ignored(&path, file_type.is_dir())? {
            continue;
        }
        if !(file_type.is_dir() || file_type.is_file() || file_type.is_symlink()) {
            return Err(CliError::usage(format!(
                "{} is not a regular file, directory, or symlink; remove or ignore it",
                source.display()
            )));
        }
        paths.push(ColdPushObservedPath {
            path: path.clone(),
            identity: cold_push_file_identity(&metadata),
            observed_at_secs,
            observed_at_nanos,
        });
        if file_type.is_dir() {
            scan_cold_push_dir(
                root,
                &path,
                ignore,
                observed_at_secs,
                observed_at_nanos,
                paths,
            )?;
        }
    }
    Ok(())
}

fn cold_push_identity_is_proven_clean(
    baseline: &local_state::SealedBaseline,
    current: &ColdPushObservedPath,
) -> bool {
    let local_state::SealedPathState::Upsert { identity, .. } = &baseline.state else {
        return false;
    };
    if identity != &current.identity {
        return false;
    }
    let (Some(observed_secs), Some(observed_nanos)) =
        (baseline.observed_at_secs, baseline.observed_at_nanos)
    else {
        return false;
    };
    let observed = (observed_secs, observed_nanos);
    (identity.mtime_secs, identity.mtime_nanos) < observed
        && (identity.ctime_secs, identity.ctime_nanos) < observed
}

fn cold_push_is_directory(identity: &local_state::FileIdentity) -> bool {
    #[cfg(unix)]
    {
        identity.mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32
    }
    #[cfg(not(unix))]
    {
        identity.mode & 0o170000 == 0o040000
    }
}

fn plan_cold_push_delta(
    root: &Path,
    scan: &ColdPushScan,
    baselines: &[local_state::SealedBaseline],
) -> Result<ColdPushDeltaPlan> {
    let baseline_by_path: BTreeMap<&str, &local_state::SealedBaseline> = baselines
        .iter()
        .map(|baseline| (baseline.path.as_str(), baseline))
        .collect();
    let scanned_paths: BTreeSet<&str> =
        scan.paths.iter().map(|entry| entry.path.as_str()).collect();
    let upserts = scan
        .paths
        .iter()
        .filter(|entry| {
            !baseline_by_path
                .get(entry.path.as_str())
                .is_some_and(|baseline| cold_push_identity_is_proven_clean(baseline, entry))
        })
        .cloned()
        .collect();

    let mut ignore = SnapshotIgnore::new(root);
    let mut deletes = Vec::new();
    for baseline in baselines {
        if scanned_paths.contains(baseline.path.as_str()) {
            continue;
        }
        let local_state::SealedPathState::Upsert { identity, .. } = &baseline.state else {
            continue;
        };
        // Newly ignored is intentionally not newly deleted: ignore rules gate tracking, so the
        // last published remote entry remains in place until it is explicitly visible+absent.
        if ignore.is_ignored(&baseline.path, cold_push_is_directory(identity))? {
            continue;
        }
        deletes.push(baseline.path.clone());
    }
    deletes.sort();
    Ok(ColdPushDeltaPlan { upserts, deletes })
}

fn cold_push_baseline(
    observed: &ColdPushObservedPath,
    snapshot_id: &str,
) -> local_state::SealedBaseline {
    local_state::SealedBaseline::upsert_observed(
        observed.path.clone(),
        snapshot_id,
        observed.identity.clone(),
        None,
        observed.observed_at_secs,
        observed.observed_at_nanos,
    )
}

fn cold_push_candidate_observations(
    candidate: &NativePreparedSnapshotCandidate,
) -> Vec<ColdPushObservedPath> {
    candidate
        .source_observations()
        .iter()
        .map(|observation| ColdPushObservedPath {
            path: observation.path.clone(),
            identity: local_state::FileIdentity {
                device: observation.device,
                inode: observation.inode,
                size: observation.size,
                mtime_secs: observation.mtime_secs,
                mtime_nanos: observation.mtime_nanos,
                ctime_secs: observation.ctime_secs,
                ctime_nanos: observation.ctime_nanos,
                mode: observation.mode,
            },
            observed_at_secs: observation.observed_at_secs,
            observed_at_nanos: observation.observed_at_nanos,
        })
        .collect()
}

fn cold_push_retirement_baselines(
    capture: &ColdPushCapture,
    snapshot_id: &str,
) -> (Vec<local_state::SealedBaseline>, Vec<String>) {
    match &capture.mode {
        ColdPushCaptureMode::Full { baselines, .. } => (
            baselines
                .iter()
                .map(|observed| cold_push_baseline(observed, snapshot_id))
                .collect(),
            Vec::new(),
        ),
        ColdPushCaptureMode::Delta { upserts, deletes } => (
            upserts
                .iter()
                .map(|upsert| {
                    local_state::SealedBaseline::upsert_observed(
                        upsert.path.clone(),
                        snapshot_id,
                        upsert.identity.clone(),
                        None,
                        upsert.observed_at_secs,
                        upsert.observed_at_nanos,
                    )
                })
                .collect(),
            deletes.clone(),
        ),
    }
}

async fn durable_cold_push(
    ctx: &CliContext,
    session: &FsSession,
    root: &Path,
    name: &str,
    message: Option<&str>,
    progress: std::sync::Arc<dyn Fn(NativePushEvent) + Send + Sync>,
) -> Result<ColdPushOutcome> {
    use local_state::{
        GenerationState, LegacyImport, LegacyMutation, PreparedGeneration, PublishRequest,
    };

    let (state_dir, store) = open_cold_push_state_at(
        &cold_push_state_root(),
        &ctx.api_url,
        &session.project_id,
        name,
        root,
    )?;
    let _writer_lock = try_local_state_writer_lock(&state_dir)?.ok_or_else(|| {
        CliError::usage(format!(
            "another `tl fs push` owns the tracked-directory state at {}; wait for it to finish \
             and retry",
            state_dir.display()
        ))
    })?;
    register_tracked_directory(ctx, root, name, &session.project_id, &state_dir)?;
    let (user, token) = session.creds();

    let completed = store
        .completed_publish_requests()
        .map_err(|error| cold_push_state_error(&state_dir, error))?
        .into_iter();
    // Retirement and the bounded completion receipt are one transaction. If the process died
    // after that commit but before the CLI delivered the result, return the original success.
    // Acknowledged receipts remain as the empty-tree-safe "this tracked root was initialized"
    // marker and are not replayed.
    if let Some(completed) = completed.clone().filter(|row| !row.acknowledged).last() {
        let mut outcome: ColdPushOutcome =
            serde_json::from_slice(&completed.response).map_err(|error| {
                CliError::usage(format!(
                    "completed cold-push receipt in {} is corrupt ({error}); the server result \
                     was adopted, but this client refuses to fabricate a response",
                    state_dir.display()
                ))
            })?;
        outcome.recovered = true;
        return Ok(outcome);
    }
    let previously_initialized = completed.count() > 0;

    if store
        .needs_legacy_import()
        .map_err(|error| cold_push_state_error(&state_dir, error))?
    {
        let head = session
            .client
            .native_head_with_credential(&session.project_id, name, user, token)
            .await?;
        store
            .import_legacy_once(LegacyImport {
                base_snapshot: head.snapshot_id,
                mutations: vec![LegacyMutation::Upsert {
                    path: COLD_PUSH_DIRTY_SENTINEL.to_string(),
                    min_write_offset: 0,
                }],
            })
            .map_err(|error| cold_push_state_error(&state_dir, error))?;
    }

    #[cfg(unix)]
    {
        let owned = store
            .artifacts()
            .map_err(|error| cold_push_state_error(&state_dir, error))?
            .into_iter()
            .map(|artifact| artifact.generation)
            .collect();
        generation_capture::reclaim_orphan_generation_captures(&state_dir, &owned)?;
    }

    let mut generations = store
        .generations()
        .map_err(|error| cold_push_state_error(&state_dir, error))?;
    generations.sort_by_key(|generation| generation.generation);
    let pending_generation = generations
        .iter()
        .find(|generation| generation.state != GenerationState::Open)
        .map(|generation| generation.generation);
    let generation = if let Some(generation) = pending_generation {
        generation
    } else {
        let baselines = store
            .sealed_baselines()
            .map_err(|error| cold_push_state_error(&state_dir, error))?;
        let active_generation = generations
            .iter()
            .find(|generation| generation.state == GenerationState::Open)
            .ok_or_else(|| {
                CliError::usage(format!(
                    "cold-push state {} has no open generation",
                    state_dir.display()
                ))
            })?
            .generation;

        if baselines.is_empty() && !previously_initialized {
            // Keep the high-throughput cold path intact: scan/hash/compress/upload stay pipelined
            // directly from the source tree. The fully prepared immutable candidate is then
            // committed in the same transaction that freezes the generation, so a crash never
            // leaves a frozen generation whose bytes must be guessed from a changed live tree.
            let base_snapshot_id = generations
                .iter()
                .find(|generation| generation.generation == active_generation)
                .and_then(|generation| generation.base_snapshot.clone());
            let preparation_operation_id = store
                .ensure_preparation_operation_id(active_generation)
                .map_err(|error| cold_push_state_error(&state_dir, error))?;
            let candidate = session
                .client
                .prepare_native_directory_snapshot_candidate_with_operation_id_and_credential(
                    &session.project_id,
                    name,
                    root,
                    user,
                    token,
                    NativePushOptions {
                        expected_snapshot_id: base_snapshot_id.clone(),
                        progress: Some(progress.clone()),
                        ..NativePushOptions::default()
                    },
                    preparation_operation_id,
                )
                .await?;
            let capture = ColdPushCapture {
                format_ver: COLD_PUSH_STATE_FORMAT,
                source_root: root.to_string_lossy().into_owned(),
                mode: ColdPushCaptureMode::Full {
                    candidate: candidate.clone(),
                    baselines: cold_push_candidate_observations(&candidate),
                },
            };
            let generation = store
                .freeze_current_with_capture(serde_json::to_vec(&capture)?)
                .map_err(|error| cold_push_state_error(&state_dir, error))?
                .ok_or_else(|| {
                    CliError::usage(format!(
                        "cold-push state {} unexpectedly contains no imported namespace",
                        state_dir.display()
                    ))
                })?
                .generation;
            debug_assert_eq!(generation, active_generation);
            let candidate_bytes = serde_json::to_vec(&candidate)?;
            use sha2::{Digest, Sha256};
            let mut digest = Sha256::new();
            digest.update(root.as_os_str().as_encoded_bytes());
            digest.update(candidate.root_id().as_bytes());
            store
                .mark_prepared(PreparedGeneration::new(
                    generation,
                    base_snapshot_id,
                    candidate.root_id(),
                    hex::encode(digest.finalize()),
                    candidate_bytes,
                ))
                .map_err(|error| cold_push_state_error(&state_dir, error))?;
            generation
        } else {
            let scan_root = root.to_path_buf();
            let scan = tokio::task::spawn_blocking(move || scan_cold_push_tree(&scan_root))
                .await
                .map_err(|error| {
                    CliError::usage(format!("tracked-directory scanner failed: {error}"))
                })??;
            let delta = plan_cold_push_delta(root, &scan, &baselines)?;
            if delta.upserts.is_empty() && delta.deletes.is_empty() {
                let snapshot_id = generations
                    .iter()
                    .find(|generation| generation.generation == active_generation)
                    .and_then(|generation| generation.base_snapshot.clone())
                    .ok_or_else(|| {
                        CliError::usage(
                            "tracked directory is clean but its adopted snapshot is missing",
                        )
                    })?;
                return Ok(ColdPushOutcome {
                    operation_id: String::new(),
                    snapshot_id,
                    files: 0,
                    logical_bytes: 0,
                    uploaded_bytes: 0,
                    transport: "local-change-index".to_string(),
                    recovered: false,
                    unchanged: true,
                });
            }
            store
                .ensure_preparation_operation_id(active_generation)
                .map_err(|error| cold_push_state_error(&state_dir, error))?;
            // The durable mutations precede byte capture. A crash while the generation is still
            // Open may reconcile again; once Frozen, only generation-owned immutable paths are
            // consulted.
            for upsert in &delta.upserts {
                store
                    .record_upsert(&upsert.path, 0)
                    .map_err(|error| cold_push_state_error(&state_dir, error))?;
            }
            for delete in &delta.deletes {
                store
                    .record_delete(delete)
                    .map_err(|error| cold_push_state_error(&state_dir, error))?;
            }
            #[cfg(unix)]
            let captured = {
                store
                    .claim_generation_capture(active_generation)
                    .map_err(|error| cold_push_state_error(&state_dir, error))?;
                let captured = generation_capture::capture_generation_upserts(
                    &state_dir,
                    active_generation,
                    delta
                        .upserts
                        .iter()
                        .map(|entry| (entry.path.clone(), root.join(&entry.path))),
                )?;
                let bytes =
                    generation_capture::generation_capture_bytes(&state_dir, active_generation)?;
                store
                    .set_generation_capture_bytes(active_generation, bytes)
                    .map_err(|error| cold_push_state_error(&state_dir, error))?;
                captured
            };
            #[cfg(not(unix))]
            let captured: Vec<NativeLocalUpsert> = delta
                .upserts
                .iter()
                .map(|entry| NativeLocalUpsert {
                    path: entry.path.clone(),
                    source: root.join(&entry.path),
                })
                .collect();
            let identity_by_path: BTreeMap<&str, &ColdPushObservedPath> = delta
                .upserts
                .iter()
                .map(|entry| (entry.path.as_str(), entry))
                .collect();
            let captured = captured
                .into_iter()
                .map(|upsert| {
                    let observed = identity_by_path.get(upsert.path.as_str()).ok_or_else(|| {
                        CliError::usage(format!(
                            "captured path {} is absent from its frozen scan",
                            upsert.path
                        ))
                    })?;
                    Ok(ColdPushCapturedUpsert {
                        path: upsert.path,
                        source: upsert.source,
                        identity: observed.identity.clone(),
                        observed_at_secs: observed.observed_at_secs,
                        observed_at_nanos: observed.observed_at_nanos,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let capture = ColdPushCapture {
                format_ver: COLD_PUSH_STATE_FORMAT,
                source_root: root.to_string_lossy().into_owned(),
                mode: ColdPushCaptureMode::Delta {
                    upserts: captured,
                    deletes: delta.deletes,
                },
            };
            let generation = store
                .freeze_current_with_capture(serde_json::to_vec(&capture)?)
                .map_err(|error| cold_push_state_error(&state_dir, error))?
                .ok_or_else(|| {
                    CliError::usage(format!(
                        "tracked push {} lost its just-recorded mutations",
                        state_dir.display()
                    ))
                })?
                .generation;
            debug_assert_eq!(generation, active_generation);
            generation
        }
    };
    let generation_record = store
        .generation(generation)
        .map_err(|error| cold_push_state_error(&state_dir, error))?
        .ok_or_else(|| {
            CliError::usage(format!(
                "cold-push state {} lost generation {generation}; refusing to guess",
                state_dir.display()
            ))
        })?;
    let existing_request = store
        .publish_requests()
        .map_err(|error| cold_push_state_error(&state_dir, error))?
        .into_iter()
        .find(|request| request.generation == generation);
    let capture: ColdPushCapture = store
        .frozen_capture(generation)
        .map_err(|error| cold_push_state_error(&state_dir, error))?
        .ok_or_else(|| {
            CliError::usage(format!(
                "tracked push generation {generation} in {} has no frozen capture",
                state_dir.display()
            ))
        })
        .and_then(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                CliError::usage(format!(
                    "tracked push capture in {} is corrupt ({error}); refusing to guess",
                    state_dir.display()
                ))
            })
        })?;
    if capture.format_ver != COLD_PUSH_STATE_FORMAT || capture.source_root != root.to_string_lossy()
    {
        return Err(CliError::usage(format!(
            "tracked push capture in {} belongs to a different source or format",
            state_dir.display()
        )));
    }

    if generation_record.state == GenerationState::Published {
        let snapshot_id = generation_record.published_snapshot.ok_or_else(|| {
            CliError::usage(format!(
                "published cold-push generation {generation} in {} has no snapshot id",
                state_dir.display()
            ))
        })?;
        let candidate = cold_push_candidate(&store, generation, &state_dir)?.ok_or_else(|| {
            CliError::usage(format!(
                "published cold-push generation {generation} in {} has no prepared candidate",
                state_dir.display()
            ))
        })?;
        let request_id = existing_request
            .as_ref()
            .map(|request| request.request_id.clone())
            .unwrap_or_else(|| candidate.preparation_operation_id.clone());
        let outcome =
            cold_push_outcome_from_candidate(&candidate, request_id, snapshot_id.clone(), true);
        let completed_response = serde_json::to_vec(&outcome)?;
        let (baseline_updates, baseline_removals) =
            cold_push_retirement_baselines(&capture, &snapshot_id);
        store
            .retire_published(
                generation,
                &snapshot_id,
                &baseline_updates,
                &baseline_removals,
                completed_response,
            )
            .map_err(|error| cold_push_state_error(&state_dir, error))?;
        #[cfg(unix)]
        if let Err(error) = generation_capture::retire_generation_capture(&state_dir, generation) {
            eprintln!(
                "warning: snapshot was adopted, but generation capture {generation} could not be \
                 reclaimed: {error}"
            );
        }
        return Ok(outcome);
    }

    let base_snapshot_id = generation_record.base_snapshot.clone();
    let preparation_operation_id = generation_record
        .preparation_operation_id
        .clone()
        .ok_or_else(|| {
            CliError::usage(format!(
                "tracked push generation {generation} in {} has no durable preparation operation \
                 id; refusing an ambiguous retry",
                state_dir.display()
            ))
        })?;
    let candidate = match cold_push_candidate(&store, generation, &state_dir)? {
        Some(candidate) => candidate,
        None => {
            let candidate = match &capture.mode {
                ColdPushCaptureMode::Full { candidate, .. } => candidate.clone(),
                ColdPushCaptureMode::Delta { upserts, deletes } => {
                    let base = base_snapshot_id.as_deref().ok_or_else(|| {
                        CliError::usage(
                            "incremental tracked push requires an adopted base snapshot",
                        )
                    })?;
                    let segment_staging = state_dir
                        .join("staging")
                        .join("generations")
                        .join(generation.to_string())
                        .join("segments");
                    match tokio::fs::remove_dir_all(&segment_staging).await {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            return Err(CliError::usage(format!(
                                "cleaning tracked generation segment staging failed: {error}"
                            )));
                        }
                    }
                    tokio::fs::create_dir_all(&segment_staging).await?;
                    session
                        .client
                        .prepare_native_snapshot_candidate_with_operation_and_staging_directory(
                            &session.project_id,
                            name,
                            base,
                            NativeChangeSet {
                                upserts: upserts
                                    .iter()
                                    .map(|upsert| NativeLocalUpsert {
                                        path: upsert.path.clone(),
                                        source: upsert.source.clone(),
                                    })
                                    .collect(),
                                deletes: deletes.clone(),
                                renames: Vec::new(),
                            },
                            user,
                            token,
                            preparation_operation_id.clone(),
                            Some(segment_staging),
                        )
                        .await?
                }
            };
            let candidate_bytes = serde_json::to_vec(&candidate)?;
            use sha2::{Digest, Sha256};

            let mut digest = Sha256::new();
            digest.update(root.as_os_str().as_encoded_bytes());
            digest.update(candidate.root_id().as_bytes());
            let source_fingerprint = hex::encode(digest.finalize());
            store
                .mark_prepared(PreparedGeneration::new(
                    generation,
                    base_snapshot_id.clone(),
                    candidate.root_id(),
                    source_fingerprint,
                    candidate_bytes,
                ))
                .map_err(|error| cold_push_state_error(&state_dir, error))?;
            candidate
        }
    };

    let request = match existing_request {
        Some(request) => request,
        None => {
            let request = PublishRequest::new(
                uuid::Uuid::new_v4().to_string(),
                generation,
                message.unwrap_or_default(),
                false,
                chrono::Utc::now().timestamp_millis().max(0) as u64,
            );
            store
                .put_publish_request(request.clone())
                .map_err(|error| cold_push_state_error(&state_dir, error))?;
            request
        }
    };
    if let Some(failure) = request.failure.as_deref() {
        return Err(CliError::usage(format!(
            "tracked-directory snapshot request {} failed permanently: {failure}. Evidence is \
             retained in `tl fs doctor`",
            request.request_id
        )));
    }
    let source_fingerprint = store
        .prepared(generation)
        .map_err(|error| cold_push_state_error(&state_dir, error))?
        .ok_or_else(|| {
            CliError::usage(format!(
                "tracked push generation {generation} in {} lost its prepared record",
                state_dir.display()
            ))
        })?
        .source_fingerprint;
    let published = publish_durable_native_candidate(
        &session.client,
        &session.project_id,
        name,
        &store,
        generation,
        &source_fingerprint,
        candidate,
        request,
        user,
        token,
        None,
        Some(progress.clone()),
        |candidate| serde_json::to_vec(candidate).map_err(Into::into),
    )
    .await?;
    let report = published.report;
    let outcome = ColdPushOutcome {
        operation_id: report.operation_id,
        snapshot_id: report.snapshot_id.clone(),
        files: report.files,
        logical_bytes: report.logical_bytes,
        uploaded_bytes: report.uploaded_bytes,
        transport: report.transport,
        recovered: false,
        unchanged: false,
    };
    let completed_response = serde_json::to_vec(&outcome)?;
    let (baseline_updates, baseline_removals) =
        cold_push_retirement_baselines(&capture, &report.snapshot_id);
    store
        .retire_published(
            generation,
            &report.snapshot_id,
            &baseline_updates,
            &baseline_removals,
            completed_response,
        )
        .map_err(|error| cold_push_state_error(&state_dir, error))?;
    #[cfg(unix)]
    if let Err(error) = generation_capture::retire_generation_capture(&state_dir, generation) {
        eprintln!(
            "warning: snapshot was adopted, but generation capture {generation} could not be \
             reclaimed: {error}"
        );
    }
    Ok(outcome)
}

fn acknowledge_cold_push(
    ctx: &CliContext,
    session: &FsSession,
    root: &Path,
    name: &str,
    request_id: &str,
) -> Result<()> {
    if request_id.is_empty() {
        return Ok(());
    }
    let (state_dir, store) = open_cold_push_state_at(
        &cold_push_state_root(),
        &ctx.api_url,
        &session.project_id,
        name,
        root,
    )?;
    store
        .acknowledge_completed_publish_request(request_id)
        .map_err(|error| cold_push_state_error(&state_dir, error))
}

/// `tl fs push <dir> <name>` — upload a folder into a filesystem as one save, no mount.
///
/// Its first invocation keeps the pipelined cold-import path. The durable local generation store
/// then becomes a strict reconciliation index for the same canonical source+remote tuple, so
/// later invocations stat the tree but read/hash/upload only changed files.
pub async fn push_dir(
    ctx: &CliContext,
    dir: &Path,
    name: &str,
    message: Option<&str>,
) -> Result<()> {
    let dir = dir.canonicalize().map_err(|error| {
        CliError::usage(format!(
            "cannot resolve directory {}: {error}",
            dir.display()
        ))
    })?;
    if !dir.is_dir() {
        return Err(CliError::usage(format!(
            "{} is not a directory",
            dir.display()
        )));
    }
    let dir_text = dir.to_str().ok_or_else(|| {
        CliError::usage(format!(
            "tracked directory root {} is not valid UTF-8; native filesystem paths and durable \
             attachment identities require lossless UTF-8",
            dir.display()
        ))
    })?;
    let protected_roots = [
        Some(cold_push_state_root()),
        Some(daemon::state_dir_root()),
        Some(crate::config::files::config_dir()),
    ];
    for protected in protected_roots
        .into_iter()
        .flatten()
        .map(|path| path.canonicalize().unwrap_or(path))
    {
        if dir.starts_with(&protected) || protected.starts_with(&dir) {
            return Err(CliError::usage(format!(
                "{} overlaps Tensorlake's local state at {}; refusing to snapshot a live \
                 journal/configuration tree",
                dir.display(),
                protected.display(),
            )));
        }
    }
    plaindir::assert_no_overlap(dir_text)?;
    let session = FsSession::open(ctx, Some(name)).await?;
    let started = std::time::Instant::now();
    let bar = indicatif::ProgressBar::new_spinner();
    bar.enable_steady_tick(std::time::Duration::from_millis(120));
    bar.set_message("scanning files once...");
    let progress_bar = bar.clone();
    let progress = std::sync::Arc::new(move |event| {
        match event {
        NativePushEvent::Scanned {
            files,
            logical_bytes,
            segments,
            ..
        } => progress_bar.set_message(format!(
            "scanned {files} files ({}, {segments} aggregate segment(s)); negotiating...",
            format_bytes(logical_bytes)
        )),
        NativePushEvent::Negotiated {
            missing_segments,
            total_segments,
            transport,
        } => progress_bar.set_message(format!(
            "uploading {missing_segments} of {total_segments} aggregate segment(s) via {transport}..."
        )),
        NativePushEvent::Uploaded { .. } => {
            progress_bar.set_message("validating save metadata...")
        }
        NativePushEvent::Verifying { .. } => {
            progress_bar.set_message("validating save metadata...")
        }
        NativePushEvent::Published { .. } => progress_bar.set_message("save published"),
    }
    });
    let result = durable_cold_push(ctx, &session, &dir, name, message, progress).await;
    bar.finish_and_clear();
    let report = result?;
    if report.unchanged {
        println!(
            "No changes; filesystem remains at {} (strict reconciliation walk, no file reads or uploads).",
            short_id(&report.snapshot_id)
        );
        return Ok(());
    }
    let elapsed = started.elapsed();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        report.logical_bytes as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    println!(
        "Saved {} ({} file(s), {} logical, {} uploaded via {} in {}; {}/s; operation {})",
        short_id(&report.snapshot_id),
        report.files,
        format_bytes(report.logical_bytes),
        format_bytes(report.uploaded_bytes),
        report.transport,
        fmt_dur(elapsed),
        format_bytes(throughput as u64),
        report.operation_id,
    );
    if report.recovered {
        println!("  recovered a previously published save from durable local state");
    }
    if let Err(error) = acknowledge_cold_push(ctx, &session, &dir, name, &report.operation_id) {
        // The snapshot is already published. Leaving the receipt unacknowledged is deliberately
        // fail-safe: the next invocation replays this success instead of risking a second publish.
        eprintln!("warning: could not acknowledge the local completion receipt: {error}");
    }
    Ok(())
}

/// `tl fs history` — the save timeline of a filesystem (target: a filesystem name or a
/// mounted/tracked directory; default: the attachment containing the CWD).
pub async fn history(
    ctx: &CliContext,
    target: Option<&str>,
    limit: usize,
    output_json: bool,
) -> Result<()> {
    // A target that is a known local attachment (or none, meaning the CWD's) names its backing
    // filesystem; anything else is a filesystem name. Attachments are mounts OR tracked
    // (pushed) directories — the latter have binding state, not daemon mount state.
    let target_is_attachment = target
        .map(|target| is_registered_mount(Path::new(target)))
        .transpose()?
        .unwrap_or(true);
    let fs_name = match target {
        Some(t) if !target_is_attachment => t.to_string(),
        other => {
            let path = resolve_mount_path(other.map(PathBuf::from))?;
            if let Some(attachment) = tracked_directory_for(&path)? {
                attachment.filesystem_id
            } else {
                match plaindir::binding_for_lenient(&path) {
                    Some((root, _)) => {
                        return Err(CliError::usage(format!(
                            "{root} uses the removed pre-release Git-backed directory binding. Stop \
                         tracking it with `tl fs unmount {root}`, then attach the native engine \
                         with `tl fs push {root} <filesystem>`."
                        )));
                    }
                    None => {
                        let (_, state_dir) = state_dir_for(&path)?;
                        daemon::load_mount_state(&state_dir)?.repo
                    }
                }
            }
        }
    };
    let session = FsSession::open(ctx, Some(&fs_name)).await?;
    let (user, token) = session.creds();
    // Snapshot ownership is keyed by content id and therefore not chronological. The atomic
    // head-event journal is server-ordered newest-first and is the filesystem's visible save
    // timeline (including restores and workspace promotions).
    let events = session
        .client
        .native_head_events_with_credential(&session.project_id, &fs_name, limit, user, token)
        .await?;
    let snapshots: Vec<Result<NativeSnapshotInfo>> =
        futures::stream::iter(events.into_iter().map(|event| event.snapshot_id))
            .map(|snapshot_id| {
                let client = session.client.clone();
                let project_id = session.project_id.clone();
                let fs_name = fs_name.clone();
                let user = user.to_string();
                let token = token.to_string();
                async move {
                    client
                        .native_snapshot_with_credential(
                            &project_id,
                            &fs_name,
                            &snapshot_id,
                            &user,
                            &token,
                        )
                        .await
                        .map_err(Into::into)
                }
            })
            .buffered(8)
            .collect()
            .await;
    let saves: Vec<NativeSnapshotInfo> = snapshots.into_iter().collect::<Result<_>>()?;
    if output_json {
        let saves: Vec<_> = saves
            .iter()
            .map(|save| {
                serde_json::json!({
                    "save_id": save.snapshot_id,
                    "filesystem_id": save.filesystem_id,
                    "root_id": save.root,
                    "parent_save_ids": save.parents,
                    "created_at_ms": save.created_at_ms,
                    "principal": save.principal,
                    "message": save.message,
                    "operation_id": save.operation_id,
                    "pinned": save.pinned,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&saves)?);
        return Ok(());
    }
    if saves.is_empty() {
        println!("No saves yet. Mount and `tl fs snapshot`, or `tl fs push <dir> {fs_name}`.");
        return Ok(());
    }
    let mut table = new_table(&["When", "Who", "Message", "Pinned", "Save"]);
    for snapshot in &saves {
        table.add_row(vec![
            Cell::new(age_display(snapshot.created_at_ms / 1000)),
            Cell::new(if snapshot.principal.is_empty() {
                "-"
            } else {
                &snapshot.principal
            }),
            Cell::new(if snapshot.message.is_empty() {
                "-"
            } else {
                &snapshot.message
            }),
            Cell::new(if snapshot.pinned { "yes" } else { "" }),
            Cell::new(short_id(&snapshot.snapshot_id)),
        ]);
    }
    println!("{table}");
    Ok(())
}

pub async fn set_snapshot_pin(
    ctx: &CliContext,
    file_system: &str,
    version: &str,
    pinned: bool,
) -> Result<()> {
    let session = FsSession::open(ctx, Some(file_system)).await?;
    let (user, token) = session.creds();
    let snapshot_id = session
        .client
        .resolve_native_snapshot_id_with_credential(
            &session.project_id,
            file_system,
            version,
            user,
            token,
        )
        .await?;
    let state = if pinned {
        session
            .client
            .pin_native_snapshot_with_credential(
                &session.project_id,
                file_system,
                &snapshot_id,
                user,
                token,
            )
            .await?
    } else {
        session
            .client
            .unpin_native_snapshot_with_credential(
                &session.project_id,
                file_system,
                &snapshot_id,
                user,
                token,
            )
            .await?
    };
    println!(
        "{} save {} in filesystem {}.",
        if state.pinned { "Pinned" } else { "Unpinned" },
        short_id(&state.snapshot_id),
        file_system
    );
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes / KIB)
    } else {
        format!("{} B", bytes as u64)
    }
}

/// `tl fs mount <name>[:<save>] <path>` — the filesystem-surface mount: the live filesystem is
/// writable with autosave, while an explicit retained save is a fixed read-only time-travel
/// view. Detached local writer sessions auto-resume. Filesystems use the native `/fs` wire.
pub async fn mount_filesystem(
    ctx: &CliContext,
    target: &str,
    path: &Path,
    ro: bool,
    foreground: bool,
    trace_ops: bool,
    log_level: &str,
) -> Result<()> {
    let historical = target.contains(':');
    // A drive doesn't lose your work: writable filesystem mounts always autosave.
    let autosave = (!ro && !historical).then_some(FS_AUTOSAVE_DEFAULT_SECS);
    // Auto-resume first — no server round trip. A detached local session of this filesystem
    // (mount state present, daemon dead) picks up where it left off: "run the same command
    // again" is the crash recovery. WritePolicy::Auto keeps the single-writer downgrade.
    if !ro
        && !historical
        && let Some((workspace_id, state_dir)) = detached_local_session(target)
    {
        println!(
            "Resuming detached session {} of filesystem {target}.",
            short_id(&workspace_id)
        );
        // The repo is KNOWN from the local mount state: attach through the per-repo
        // endpoint so the whole path works under a repo-scoped credential (the sandbox
        // recipe) — never through name resolution, which would treat the id as a repo.
        // The SELECTED state dir travels with the id: recomputing it via allocation
        // could pick a different dead registration of the same workspace (the canonical
        // dir when the newest overlay lives in a suffixed one) and strand unsealed work.
        return mount(
            ctx,
            target,
            path,
            WritePolicy::Auto,
            None,
            autosave,
            true,
            None,
            Some(Resume {
                workspace_id: &workspace_id,
                state_dir: Some(state_dir),
            }),
            foreground,
            trace_ops,
            log_level,
        )
        .await;
    }
    mount(
        ctx,
        target,
        path,
        if ro || historical {
            WritePolicy::Ro
        } else {
            WritePolicy::Auto
        },
        None,
        autosave,
        true,
        None,
        None,
        foreground,
        trace_ops,
        log_level,
    )
    .await
}

/// A known-repo workspace attach for [`mount`]: skip name resolution entirely, and — when a
/// crash-resume selected a specific detached overlay — reopen exactly that state dir.
pub(crate) struct Resume<'a> {
    pub(crate) workspace_id: &'a str,
    pub(crate) state_dir: Option<PathBuf>,
}

fn local_state_uuid_for_mount(
    native_filesystem: bool,
    read_only: bool,
    resume_state_dir: Option<&Path>,
) -> Option<String> {
    if !native_filesystem || read_only {
        return None;
    }
    Some(
        resume_state_dir
            .and_then(|dir| daemon::load_mount_state(dir).ok())
            .and_then(|state| state.local_state_uuid)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
    )
}

/// A detached local session of `repo`: registered mount state on this machine whose daemon is
/// no longer alive. Newest state-dir mtime wins when several are detached — a heuristic proxy
/// for "the one you used last" (all fs sessions publish to the same target, so resuming an
/// older sibling loses nothing published; only its local overlay differs).
fn detached_local_session(repo: &str) -> Option<(String, PathBuf)> {
    let mut candidates: Vec<(std::time::SystemTime, String, PathBuf)> = local_mount_states()
        .into_iter()
        .filter_map(|(_, state_dir, state, alive)| {
            if alive || state.repo != repo || state.read_only() {
                return None;
            }
            let modified = std::fs::metadata(&state_dir)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            Some((modified, state.workspace_id, state_dir))
        })
        .collect();
    candidates.sort_by_key(|(modified, ..)| std::cmp::Reverse(*modified));
    candidates.into_iter().next().map(|(_, id, dir)| (id, dir))
}

/// `tl git mount <repo>[:<ref>]` — the repository-surface mount: explicit checkpointing, no
/// autosave, publish only with --publish, --workspace reattaches an existing workspace.
#[allow(clippy::too_many_arguments)]
pub async fn mount_repo(
    ctx: &CliContext,
    target: &str,
    workspace: Option<&str>,
    path: &Path,
    ro: bool,
    publish: bool,
    foreground: bool,
    trace_ops: bool,
    log_level: &str,
) -> Result<()> {
    let (repo_source, subtree) = parse_git_mount_target(target)?;
    if publish && !repo_source.contains(':') {
        return Err(CliError::usage(
            "--publish needs the branch to land on: tl git mount <repo>:<branch> ...",
        ));
    }
    let mode = if ro {
        WritePolicy::Ro
    } else {
        WritePolicy::Auto
    };
    let shared_target = publish.then(|| {
        repo_source
            .split_once(':')
            .map(|(_, base)| base.to_string())
            .expect("guarded above")
    });
    mount(
        ctx,
        &repo_source,
        path,
        mode,
        shared_target,
        None,
        false,
        subtree,
        // --workspace names an id inside `target`'s repo: per-repo attach, repo-scoped-safe.
        workspace.map(|id| Resume {
            workspace_id: id,
            state_dir: None,
        }),
        foreground,
        trace_ops,
        log_level,
    )
    .await
}

/// Split the repository source from the optional subtree selector. A subtree is always a
/// canonical repository-relative directory; keeping this parser at the command boundary means
/// state files and mount-core options never have to represent ambiguous spellings.
fn parse_git_mount_target(target: &str) -> Result<(String, Option<String>)> {
    let Some((source, subtree)) = target.split_once("//") else {
        if target.is_empty() {
            return Err(CliError::usage("repository name cannot be empty"));
        }
        return Ok((target.to_string(), None));
    };
    if source.is_empty()
        || subtree.is_empty()
        || subtree.starts_with('/')
        || subtree.ends_with('/')
        || subtree.split('/').any(|component| {
            component.is_empty()
                || component == "."
                || component == ".."
                || component.as_bytes().contains(&0)
        })
    {
        return Err(CliError::usage(format!(
            "invalid subtree mount target {target:?}; use repo[:ref]//path/to/directory"
        )));
    }
    Ok((source.to_string(), Some(subtree.to_string())))
}

/// Turn the server's explicit policy into the mount-core follow target, rejecting incoherent or
/// future wire values instead of silently changing a live source into a pinned one.
fn git_mount_follow_ref(source: &GitMountSource) -> Result<Option<String>> {
    match (
        source.kind.as_str(),
        source.follow_policy.as_str(),
        source.canonical_ref.as_deref(),
    ) {
        ("branch", "follow", Some(name)) if name.starts_with("refs/heads/") => {
            Ok(Some(name.to_string()))
        }
        ("tag", "follow", Some(name)) if name.starts_with("refs/tags/") => {
            Ok(Some(name.to_string()))
        }
        ("commit", "pinned", None) => Ok(None),
        _ => Err(CliError::usage(
            "server returned an incoherent repository mount source policy; update the server and \
             CLI together",
        )),
    }
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
    // Filesystems are born with a genesis save (server-minted, kind=filesystem) and legacy
    // repos may have real history — seeding those would stack a pointless empty commit on
    // every first bind. Only a genuinely unborn default branch needs the seed.
    let refs = session
        .client
        .list_refs_with_credential(&session.project_id, repo, user, token)
        .await?
        .into_inner();
    let full_ref = format!("refs/heads/{default_branch}");
    if refs.refs.iter().any(|r| r.name == full_ref) {
        return Ok(());
    }
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

/// Serialize registry mutations across processes. Every writer is load-modify-save; without
/// this, two concurrent `tl fs mount`s read the same table and the last save silently drops
/// the other's entry — stranding a LIVE mount outside ls/status/unmount. Readers stay
/// lock-free: [`plaindir::write_atomic`]'s rename means they see a complete document or the
/// previous one, never a torn write (which `registry_load` would misread as an empty
/// registry). Blocking acquire — the critical section is a small file rewrite.
fn registry_lock() -> Result<std::fs::File> {
    std::fs::create_dir_all(crate::config::files::config_dir())?;
    plaindir::flock_exclusive(
        &crate::config::files::config_dir().join("mounts.lock"),
        true,
    )?
    .ok_or_else(|| CliError::usage("could not lock the mount registry"))
}

fn registry_save(table: &toml::map::Map<String, toml::Value>) -> Result<()> {
    std::fs::create_dir_all(crate::config::files::config_dir())?;
    plaindir::write_atomic(
        &mounts_registry_path(),
        toml::to_string_pretty(&toml::Value::Table(table.clone()))?.as_bytes(),
    )
}

fn registry_add(mountpoint: &str, state_dir: &Path) -> Result<()> {
    let _lock = registry_lock()?;
    let mut table = registry_load();
    // One state dir, one mountpoint: resuming a detached session at a NEW path must retire
    // the old path's binding, or management commands against the stale mountpoint report
    // the live session twice — and can shut down or delete the new mount's state.
    let dir = state_dir.to_string_lossy().into_owned();
    table.retain(|_, v| v.as_str() != Some(dir.as_str()));
    table.insert(mountpoint.to_string(), toml::Value::String(dir));
    registry_save(&table)
}

fn registry_remove(mountpoint: &str) -> Result<()> {
    let _lock = registry_lock()?;
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

/// Resolve doctor targets even after the live mount registry entry has been removed.
///
/// Repair is specifically for detached state, so requiring the live registry would make the
/// command unreachable in the state where it is safe to run. An explicit state-directory path
/// wins; otherwise scan only the managed mount-state root for an exact persisted mountpoint match.
/// Normal path-addressed commands continue to use [`state_dir_for`] and retain their stricter
/// live-registration semantics.
fn doctor_state_dir_for_in(path: &Path, state_root: &Path) -> Result<(String, PathBuf)> {
    let explicit_state = path.join("state.json");
    match std::fs::symlink_metadata(&explicit_state) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(CliError::usage(format!(
                    "mount state at {} is not a regular file",
                    explicit_state.display()
                )));
            }
            let state = daemon::load_mount_state(path).map_err(|error| {
                CliError::usage(format!(
                    "cannot read mount state at {}: {error}",
                    explicit_state.display()
                ))
            })?;
            let state_dir = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            return Ok((canonical_mountpoint(&state.mountpoint)?, state_dir));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(CliError::usage(format!(
                "cannot inspect possible mount state at {}: {error}",
                explicit_state.display()
            )));
        }
    }
    if let Ok(found) = state_dir_for(path) {
        return Ok(found);
    }

    let mountpoint = canonical_mountpoint(path)?;
    let entries = match std::fs::read_dir(state_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(not_a_mount_error(format!(
                "{mountpoint} is not a live tl fs mount and no detached local state matches it"
            )));
        }
        Err(error) => {
            return Err(CliError::usage(format!(
                "cannot scan detached mount state at {}: {error}",
                state_root.display()
            )));
        }
    };
    let mut matches = Vec::new();
    for entry in entries {
        let entry = entry?;
        let state_dir = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let Ok(state) = daemon::load_mount_state(&state_dir) else {
            // Unrelated corrupt state cannot prevent repairing the explicitly named mount. A
            // caller can pass that state directory itself to diagnose its state.json.
            continue;
        };
        let Ok(saved_mountpoint) = canonical_mountpoint(&state.mountpoint) else {
            continue;
        };
        if saved_mountpoint == mountpoint {
            matches.push(state_dir);
        }
    }
    matches.sort();
    match matches.as_slice() {
        [state_dir] => Ok((mountpoint, state_dir.clone())),
        [] => Err(not_a_mount_error(format!(
            "{mountpoint} is not a live tl fs mount and no detached local state matches it; pass \
             the state directory under {} explicitly if its state.json is damaged",
            state_root.display()
        ))),
        _ => Err(CliError::usage(format!(
            "multiple detached mount-state directories claim {mountpoint}: {}; pass the intended \
             state directory explicitly",
            matches
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

fn doctor_state_dir_for(path: &Path) -> Result<(String, PathBuf)> {
    doctor_state_dir_for_in(path, &daemon::state_dir_root())
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
/// disambiguate optional positional args (a path-addressed command's optional PATH) —
/// a binding here resolves as the command's path, whose dispatch then answers with the
/// binding-appropriate behavior (or a clear v1 "not supported").
pub fn is_registered_mount(path: &Path) -> Result<bool> {
    Ok(state_dir_for(path).is_ok()
        || tracked_directory_for(path)?.is_some()
        || plaindir::binding_for_lenient(path).is_some())
}

pub fn is_tracked_directory(path: &Path) -> Result<bool> {
    Ok(tracked_directory_for(path)?.is_some())
}

/// Enforce the product boundary for path-addressed `tl fs` commands before shared mount helpers
/// run. The mount implementation is shared with `tl git`, but their state machines are not.
pub fn require_native_filesystem_attachment(path: &Path) -> Result<()> {
    if tracked_directory_for(path)?.is_some() {
        return Ok(());
    }
    if let Some((root, _)) = plaindir::binding_for_lenient(path) {
        return Err(CliError::usage(format!(
            "{root} uses the removed pre-release Git-backed directory binding. Stop tracking it \
             with `tl fs unmount {root}`, then attach the native engine with `tl fs push {root} \
             <filesystem>`."
        )));
    }
    match state_dir_for(path) {
        Ok((mountpoint, state_dir)) => {
            let state = daemon::load_mount_state(&state_dir)?;
            if state.native_filesystem {
                Ok(())
            } else {
                Err(CliError::usage(format!(
                    "{mountpoint} is a repository mount; use `tl git` commands for it"
                )))
            }
        }
        Err(_error) if daemon::still_mounted(path) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Mirror of [`require_native_filesystem_attachment`] for `tl git` mount verbs.
pub fn require_repository_mount_attachment(path: &Path) -> Result<()> {
    if let Some(attachment) = tracked_directory_for(path)? {
        return Err(CliError::usage(format!(
            "{} is a tracked native filesystem directory; use `tl fs` commands for it",
            attachment.root
        )));
    }
    if let Some((root, _)) = plaindir::binding_for_lenient(path) {
        return Err(CliError::usage(format!(
            "{root} is a removed pre-release filesystem binding; detach it with `tl fs unmount \
             {root}`"
        )));
    }
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.native_filesystem {
        Err(CliError::usage(format!(
            "{mountpoint} is a native filesystem mount; use `tl fs` commands for it"
        )))
    } else {
        Ok(())
    }
}

/// The registered mountpoint or bound directory containing the current directory (the
/// deepest one, for nesting). This is what path-addressed commands operate on when no path
/// argument is given.
pub fn mount_containing_cwd() -> Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let cwd = cwd.canonicalize().unwrap_or(cwd);
    let mut roots: Vec<PathBuf> = registry_load().keys().map(PathBuf::from).collect();
    roots.extend(load_tracked_directories()?.into_keys().map(PathBuf::from));
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

/// Resolve the intentional CLI ambiguity in commands that accept either `[PATH] [TARGET]` or a
/// repository name: explicit/existing paths and registered mounts are paths; everything else is
/// a ref/repository token.
pub fn positional_is_mount_path(value: &Path) -> Result<bool> {
    Ok(is_registered_mount(value)? || is_path_shaped(value))
}

/// Guard for `tl git promote <branch>` / `tl fs restore <version>` with the mount path omitted:
/// when the sole positional is itself a mounted directory (or an explicit path), the user
/// almost certainly forgot the branch/version — without this, promote would publish the CWD
/// mount onto a branch literally named after the directory.
pub fn reject_mount_like_positional(value: &str, what: &str, usage: &str) -> Result<()> {
    let as_path = Path::new(value);
    if is_registered_mount(as_path)? || is_path_shaped(as_path) {
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
pub fn hydrate_scope_from_mount(ctx: &mut CliContext, path: &Path) -> Result<()> {
    if ctx.effective_project_id().is_some() {
        return Ok(());
    }
    if let Some(attachment) = tracked_directory_for(path)? {
        ctx.api_url = attachment.api_url;
        ctx.project_id = Some(attachment.project_id);
        if ctx.organization_id.is_none() {
            ctx.organization_id = attachment.organization_id;
        }
        return Ok(());
    }
    // Plain-directory bindings carry the same scope record as mounts (binding.json).
    if let Some((_, binding_state)) = plaindir::binding_for_lenient(path)
        && let Ok(binding) = plaindir::load_binding(&binding_state)
    {
        ctx.project_id = Some(binding.project_id);
        if ctx.organization_id.is_none() {
            ctx.organization_id = binding.organization_id;
        }
        return Ok(());
    }
    let Ok((_, state_dir)) = state_dir_for(path) else {
        return Ok(());
    };
    let Ok(state) = daemon::load_mount_state(&state_dir) else {
        return Ok(());
    };
    ctx.project_id = Some(state.project_id);
    if ctx.organization_id.is_none() {
        ctx.organization_id = state.organization_id;
    }
    Ok(())
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

/// Every locally registered mount: `(mountpoint, state dir, state, daemon alive)`. The single
/// scan behind both the live view (attachment columns, single-writer checks) and the detached
/// view (auto-resume), so the two can never disagree on what "alive" means.
fn local_mount_states() -> Vec<(String, PathBuf, MountState, bool)> {
    registry_load()
        .iter()
        .filter_map(|(mountpoint, state_dir)| {
            let state_dir = PathBuf::from(state_dir.as_str()?);
            let state = daemon::load_mount_state(&state_dir).ok()?;
            let alive = daemon::daemon_pid(&state_dir).is_some_and(daemon_alive);
            Some((mountpoint.clone(), state_dir, state, alive))
        })
        .collect()
}

/// Local mounts whose daemon is still running: `(mountpoint, state)`.
fn live_mounts() -> Vec<(String, MountState)> {
    local_mount_states()
        .into_iter()
        .filter_map(|(mountpoint, _, state, alive)| alive.then_some((mountpoint, state)))
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
///
/// A registration only HOLDS its dir while its daemon is alive: a crashed mount's on-disk
/// registry entry survives the crash, and treating it as a hold pushed the crash-resume onto
/// a fresh suffixed dir — stranding the unsealed local overlay in the old one, which is the
/// exact loss "run the same command again" recovery exists to prevent.
fn alloc_state_dir(workspace_id: &str, skip: &std::collections::HashSet<PathBuf>) -> PathBuf {
    let registered: std::collections::HashSet<PathBuf> = registry_load()
        .values()
        .filter_map(|v| v.as_str().map(PathBuf::from))
        .filter(|dir| daemon::daemon_pid(dir).is_some_and(daemon_alive))
        .collect();
    let root = daemon::state_dir_root();
    let mut n = 1u32;
    loop {
        let candidate = if n == 1 {
            root.join(workspace_id)
        } else {
            root.join(format!("{workspace_id}.{n}"))
        };
        if !registered.contains(&candidate) && !skip.contains(&candidate) {
            return candidate;
        }
        n += 1;
    }
}

/// Stake `dir` for THIS mount, atomically, before any state lands in it. The aliveness
/// probe alone is racy: a daemon writes its pid only after credentials, server round
/// trips, and the FUSE attach — a multi-second window in which a second mount of the same
/// workspace sees the dir as free and two daemons end up sharing one overlay. The claim
/// is an exclusive flock on `daemon.pid` holding the CLI's OWN pid; the daemon overwrites
/// it once serving, and the caller keeps the returned guard (and its lock) until then.
/// `None` = another process holds the claim or a live daemon owns the dir.
fn claim_state_dir(dir: &Path) -> Result<Option<std::fs::File>> {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    std::fs::create_dir_all(dir)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join("daemon.pid"))?;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Ok(None);
    }
    // The lock arbitrates concurrent CLAIMS; a running daemon holds no flock, so its
    // liveness is checked separately (its pid is what the file holds).
    if daemon::daemon_pid(dir).is_some_and(daemon_alive) {
        return Ok(None);
    }
    let mut file = file;
    file.set_len(0)?;
    write!(file, "{}", std::process::id())?;
    file.sync_all()?;
    Ok(Some(file))
}

// ---------------------------------------------------------------------------------------------
// Mount / unmount
// ---------------------------------------------------------------------------------------------

/// `tl fs mount <target> <path>` — how workspaces are born and revived.
/// `<file-system>[:<ref-or-commit>]` creates a new workspace on that file system;
/// `<workspace-id>` (or a unique prefix; see `tl fs ls`) mounts an existing one, resuming at
/// its last snapshot. Reads stream lazily; nothing is copied to disk up front.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub async fn mount(
    ctx: &CliContext,
    target: &str,
    path: &Path,
    mode: WritePolicy,
    shared_target: Option<String>,
    auto_commit_interval_secs: Option<u64>,
    // The surface speaking: true = `tl fs` (drives, sessions, saves), false = `tl git`
    // (workspaces, snapshots, branches). One engine, two vocabularies — the fs surface
    // keeps branch, commit, ref, and merge terminology out of its output.
    fs_surface: bool,
    // Git-only repository-relative directory exposed as the mount root. Local overlay and
    // status paths stay relative to it; snapshot submission restores this prefix.
    subtree: Option<String>,
    // A workspace KNOWN to live in `target`'s repo (auto-resume, `--workspace`): attach
    // through the per-repo, principal-checked endpoint directly. Never routed through name
    // resolution — that path treats the id as a repo name and then falls back to a
    // project-wide fleet search, both of which a repo-scoped credential (the sandbox attach
    // recipe) rightly 403s.
    resume: Option<Resume<'_>>,
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
    if fs_surface && subtree.is_some() {
        return Err(CliError::usage(
            "subtree selectors are a repository-mount feature; use `tl git mount repo//path`",
        ));
    }
    // The CLI's own phase timings (`phase=… "mount timing"`, debug level) surface on stderr
    // through the same subscriber the daemon uses for daemon.log — pass `--log-level debug`
    // to see them; the default "info" keeps mount's stderr clean for scripts.
    daemon::init_logging(log_level)?;
    // The two surfaces' words for the same machinery; every user-facing line below must use
    // these (or branch on fs_surface) so `tl fs` never says workspace/snapshot/branch.
    let (unit, saves) = if fs_surface {
        ("session", "saves")
    } else {
        ("workspace", "snapshots")
    };
    let started = std::time::Instant::now();
    #[cfg(target_os = "macos")]
    ensure_fskit_ready().await?;
    #[cfg(target_os = "linux")]
    {
        ensure_fuse_ready()?;
        // A crashed mount leaves its dead FUSE attachment in place; the kernel answers the
        // mountpoint with ENOTCONN forever. Detach it lazily before mounting — otherwise the
        // new mount stacks over the corpse, and unmounting later resurfaces it.
        if std::fs::metadata(path).is_err()
            && matches!(
                std::fs::symlink_metadata(path),
                Err(ref e) if matches!(
                    e.raw_os_error(),
                    Some(libc::ENOTCONN) | Some(libc::EIO) | Some(libc::ENXIO)
                )
            )
        {
            for cmd in ["fusermount3", "fusermount"] {
                if std::process::Command::new(cmd)
                    .args(["-uz", &path.to_string_lossy()])
                    .status()
                    .is_ok_and(|st| st.success())
                {
                    eprintln!("detached a dead previous mount at {}", path.display());
                    break;
                }
            }
        }
    }
    let (name, base) = match target.split_once(':') {
        Some((name, base)) => (name, Some(base.to_string())),
        None => (target, None),
    };
    if fs_surface && base.is_some() && mode == WritePolicy::Rw {
        return Err(CliError::usage(
            "a historical filesystem save is immutable; omit `--mode rw` to mount it read-only",
        ));
    }
    if shared_target.is_some() && mode == WritePolicy::Ro {
        return Err(CliError::usage(
            "publishing every snapshot cannot be combined with a read-only mount",
        ));
    }
    if auto_commit_interval_secs.is_some() && mode == WritePolicy::Ro {
        return Err(CliError::usage(
            "automatic snapshots seal local writes; a read-only mount has none",
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
    let pinned_native_snapshot = if fs_surface {
        match base.as_deref() {
            Some(prefix) => Some(
                session
                    .client
                    .resolve_native_snapshot_id_with_credential(
                        &session.project_id,
                        name,
                        prefix,
                        user,
                        token,
                    )
                    .await?,
            ),
            None => None,
        }
    } else {
        None
    };

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
        /// Stateless read-only repository view. The UUID is a presence heartbeat id, not a
        /// workspace and never a GC root.
        ReadOnly(String, GitMountSource),
    }
    let resume_state_dir = resume.as_ref().and_then(|r| r.state_dir.clone());
    let resolved = if fs_surface {
        if resume.is_some() && pinned_native_snapshot.is_some() {
            return Err(CliError::usage(
                "a historical save cannot be combined with a resumed session; mount the save \
                 without `--workspace`",
            ));
        }
        if let Some(resume) = resume.as_ref() {
            let workspace = session
                .client
                .native_workspace_with_credential(
                    &session.project_id,
                    name,
                    resume.workspace_id,
                    user,
                    token,
                )
                .await
                .map_err(|error| {
                    CliError::usage(format!(
                        "resuming session {} of {name} failed: {error}",
                        short_id(resume.workspace_id)
                    ))
                })?;
            Resolved::Attached(name.to_string(), native_workspace_to_mount_info(workspace))
        } else {
            let workspace = session
                .client
                .create_native_workspace_with_credential(
                    &session.project_id,
                    name,
                    pinned_native_snapshot.as_deref(),
                    mode == WritePolicy::Ro || pinned_native_snapshot.is_some(),
                    user,
                    token,
                )
                .await?;
            Resolved::Created(native_workspace_to_mount_info(workspace))
        }
    } else {
        if mode == WritePolicy::Ro && resume.is_none() {
            let source = session
                .client
                .resolve_git_mount_source(
                    &session.project_id,
                    name,
                    user,
                    token,
                    base.as_deref(),
                    subtree.as_deref(),
                )
                .await?
                .into_inner();
            Resolved::ReadOnly(uuid::Uuid::new_v4().to_string(), source)
        } else {
            let create_req = CreateWorkspaceRequest {
                base: base.clone(),
                shared_target: shared_target.clone(),
                // The server applies product semantics from the repo's authoritative kind: an
                // fs-surface create gets its publish target filled server-side (and a repository is
                // rejected with the right command). Clients never pre-read listings to decide this.
                surface: fs_surface.then(|| "filesystem".to_string()),
                read_only: mode == WritePolicy::Ro,
                ..Default::default()
            };
            // One create call site for both the first attempt and the post-seed retry, so the two can
            // never drift apart.
            let try_create = || {
                session
                    .client
                    .create_workspace(&session.project_id, name, user, token, &create_req)
            };
            if let Some(id) = resume.map(|r| r.workspace_id) {
                let ws = session
                    .client
                    .get_workspace(&session.project_id, name, user, token, id)
                    .await
                    .map_err(|e| {
                        CliError::usage(format!(
                            "resuming {} {} of {name} failed: {e} (start fresh: tl fs mount {name} \
                     <path>)",
                            unit,
                            short_id(id),
                        ))
                    })?
                    .into_inner();
                Resolved::Attached(name.to_string(), ws)
            } else {
                match try_create().await {
                    Ok(ws) => Resolved::Created(ws.into_inner()),
                    Err(e) => match create_recovery(&e) {
                        // No file system by this name and no branch was named: try it as a workspace id.
                        Some(CreateRecovery::RepoMissing) if base.is_none() => {
                            match resolve_workspace(&session, name).await? {
                                Some((repo, ws)) => Resolved::Attached(repo, ws),
                                None if fs_surface => {
                                    return Err(CliError::usage(format!(
                                        "no filesystem named {name:?} (see: tl fs ls; create one: tl fs \
                             create {name})"
                                    )));
                                }
                                None => {
                                    return Err(CliError::usage(format!(
                                        "no repo or workspace matches {name:?}. See `tl git ls`, or \
                             create the repo first: tl git create {name}"
                                    )));
                                }
                            }
                        }
                        Some(CreateRecovery::RepoMissing) if fs_surface => {
                            return Err(CliError::usage(format!(
                                "no filesystem named {name:?} (see: tl fs ls; create one: tl fs create \
                     {name})"
                            )));
                        }
                        Some(CreateRecovery::RepoMissing) => {
                            return Err(CliError::usage(format!(
                                "no repo named {name:?}; create it first: tl git create {name}"
                            )));
                        }
                        // Seed an unborn default branch whether it is implied OR named (either spelling):
                        // a fresh `tl git create` repo has no commits, and `tl fs mount repo:main` used to
                        // fail with `base "main" does not resolve to a commit` while plain
                        // `tl fs mount repo` worked. Writable mounts only — a read-only view must never
                        // write to the server (and with read-scoped credentials the seed push would fail
                        // opaquely); ro keeps the clear server error. Other branch names stay strict —
                        // seeding cannot conjure them.
                        Some(CreateRecovery::BaseUnresolved) if fs_surface => return Err(e.into()),
                        Some(CreateRecovery::BaseUnresolved) => {
                            let file_systems = session
                                .client
                                .list_repos_with_credential(&session.project_id, None, user, token)
                                .await?
                                .into_inner();
                            let Some(fs) = file_systems.repos.iter().find(|r| r.name == name)
                            else {
                                return Err(e.into());
                            };
                            let default_branch = fs.default_branch.clone();
                            let names_default = base.as_deref().is_none_or(|b| {
                                b == default_branch
                                    || b.strip_prefix("refs/heads/") == Some(&default_branch)
                            });
                            if !names_default || mode == WritePolicy::Ro {
                                return Err(e.into());
                            }
                            ensure_seeded(&session, &default_branch, name).await?;
                            Resolved::Created(try_create().await?.into_inner())
                        }
                        None => return Err(e.into()),
                    },
                }
            }
        }
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
    let (repo, ws, attached, read_only, follow_ref, start_oid, git_mount_source) = match resolved {
        Resolved::ReadOnly(presence_id, source) => {
            // Branches and tags follow the exact canonical namespace returned by the server;
            // only raw commits are pinned.
            let follow_ref = git_mount_follow_ref(&source)?;
            let view_ref = follow_ref
                .clone()
                .unwrap_or_else(|| source.resolved_commit.clone());
            let start_oid = Some(source.resolved_commit.clone());
            let ws = WorkspaceInfo {
                id: presence_id,
                ref_name: view_ref,
                principal: String::new(),
                base: source.resolved_commit.clone(),
                base_ref: source.canonical_ref.clone(),
                head: source.resolved_commit.clone(),
                created_at_secs: 0,
                lease_secs: 0,
                lease_due_ms: None,
                pinned: false,
                shared_target: None,
            };
            (
                name.to_string(),
                ws,
                false,
                true,
                follow_ref,
                start_oid,
                Some(source),
            )
        }
        Resolved::Attached(repo, ws) => {
            if shared_target.is_some() {
                return Err(CliError::usage(
                    "publish-on-snapshot is chosen when the workspace is created; this attach \
                     resumes an existing workspace, which keeps its original setting",
                ));
            }
            // Single-writer by default: a workspace attached elsewhere — live mount OR
            // plain-directory binding (a binding is always a writer) — attaches read-only.
            // There is no override flag; release the other attachment to take writes.
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
                    "{} {unit} is already attached at {at}; mounting read-only (unmount it \
                     there to take writes)",
                    style("note:").yellow(),
                ),
                (Some(at), WritePolicy::Rw) => eprintln!(
                    "{} {unit} is also writable at {at}; two writers race {saves}",
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
            (repo, ws, true, read_only, follow_ref, start_oid, None)
        }
        Resolved::Created(ws) => {
            let read_only = mode == WritePolicy::Ro || pinned_native_snapshot.is_some();
            // What the view follows. Writable workspaces follow their own ref; shared-rw
            // follows the branch it publishes to, so every writer's view converges on the
            // reconciled branch rather than staying pinned to its own snapshots. The publish
            // target comes from the CREATE RESPONSE — for fs-surface sessions the server
            // filled it from the repo's stored default branch. A read-only view follows the
            // named branch (fs surface: HEAD itself, resolved per poll) so new commits
            // appear — except a fixed commit base, which is a pinned view that never
            // advances.
            let follow_ref = if pinned_native_snapshot.is_some() {
                None
            } else if let Some(target) = &ws.shared_target {
                Some(format!("refs/heads/{target}"))
            } else if read_only && fs_surface {
                Some("HEAD".to_string())
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
                None,
            )
        }
    };

    // An attach can resolve read-only implicitly (the workspace is live-mounted elsewhere);
    // nothing server-side was created on that path, so erroring here leaks nothing.
    if auto_commit_interval_secs.is_some() && read_only {
        return Err(CliError::usage(
            "automatic saves need a writable mount; this workspace is attached elsewhere and \
             resolved read-only (unmount it there to take writes)",
        ));
    }

    let mountpoint = canonical_mountpoint(path)?;
    // A resume reopens EXACTLY the state dir it selected (the newest detached overlay);
    // re-allocating could land on a different dead registration of the same workspace.
    // `_state_claim` holds the exclusive claim (see claim_state_dir) until this function
    // returns — by which point the daemon is serving and its own pid holds the dir.
    let local_state_uuid =
        local_state_uuid_for_mount(fs_surface, read_only, resume_state_dir.as_deref());
    let (state_dir, _state_claim) = match resume_state_dir {
        Some(dir) => match claim_state_dir(&dir)? {
            Some(guard) => (dir, guard),
            None => {
                return Err(CliError::usage(format!(
                    "{unit} {} is already being mounted or served by another process",
                    short_id(&ws.id)
                )));
            }
        },
        None => {
            let mut skip: std::collections::HashSet<PathBuf> = Default::default();
            loop {
                let candidate = alloc_state_dir(&ws.id, &skip);
                match claim_state_dir(&candidate)? {
                    Some(guard) => break (candidate, guard),
                    // Raced by a concurrent mount mid-claim: take the next dir.
                    None => {
                        skip.insert(candidate);
                    }
                }
            }
        }
    };
    let (owner_uid, owner_gid) = mount_owner();
    daemon::save_mount_state(
        &state_dir,
        &MountState {
            project_id: session.project_id.clone(),
            organization_id: ctx.effective_organization_id(),
            owner_uid: Some(owner_uid),
            owner_gid: Some(owner_gid),
            repo: repo.clone(),
            subtree: subtree.clone(),
            git_mount_source: git_mount_source.clone(),
            mount_presence_id: git_mount_source.as_ref().map(|_| ws.id.clone()),
            native_filesystem: fs_surface,
            pinned_snapshot: pinned_native_snapshot.clone(),
            workspace_id: ws.id.clone(),
            local_state_uuid,
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
                        "Mounted {unit} {} ({}) at {}{}",
                        short_id(&ws.id),
                        repo,
                        mountpoint,
                        match (read_only, fs_surface) {
                            (true, _) => ", read-only",
                            (false, true) => ", resumed at its last save",
                            (false, false) => ", resumed at its last snapshot",
                        },
                    );
                } else if fs_surface {
                    println!(
                        "Mounted filesystem {} at {} (session {}{})",
                        repo,
                        mountpoint,
                        short_id(&ws.id),
                        if ws.shared_target.is_some() {
                            ", saves publish automatically"
                        } else if pinned_native_snapshot.is_some() {
                            ", read-only, fixed at a historical save"
                        } else if read_only {
                            ", read-only, follows the filesystem"
                        } else {
                            ""
                        },
                    );
                } else if let Some(source) = git_mount_source.as_ref() {
                    println!(
                        "Mounted read-only {}{} at {} (stateless; no workspace created)",
                        repo,
                        subtree
                            .as_deref()
                            .map(|path| format!("//{path}"))
                            .unwrap_or_default(),
                        mountpoint,
                    );
                    println!(
                        "Source: {}{} at {}",
                        source.kind,
                        source
                            .canonical_ref
                            .as_deref()
                            .map(|name| format!(" {name}"))
                            .unwrap_or_default(),
                        &source.resolved_commit[..source.resolved_commit.len().min(12)],
                    );
                } else {
                    println!(
                        "Mounted {}:{} at {} (workspace {}{})",
                        repo,
                        ws.base_ref.as_deref().unwrap_or(&ws.base[..12]),
                        mountpoint,
                        short_id(&ws.id),
                        if ws.shared_target.is_some() {
                            ", snapshots auto-publish to the branch"
                        } else if read_only {
                            ", read-only, follows the branch"
                        } else {
                            ""
                        },
                    );
                }
                if read_only {
                    if fs_surface {
                        let save = resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?");
                        if pinned_native_snapshot.is_some() {
                            println!(
                                "Reading historical save {save}; this view does not follow new \
                                 saves."
                            );
                        } else {
                            println!(
                                "Reading save {save}; new saves appear as the filesystem advances."
                            );
                        }
                    } else if git_mount_source
                        .as_ref()
                        .is_some_and(|source| source.kind == "commit")
                    {
                        println!(
                            "Reading pinned commit {}; this view does not follow a ref.",
                            resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                        );
                    } else {
                        println!(
                            "Reading commit {}; new commits appear as the followed ref advances.",
                            resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                        );
                    }
                } else if fs_surface {
                    println!(
                        "At save {}. Changes save automatically; tl fs snapshot {} makes a \
                         named save.",
                        resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                        mountpoint,
                    );
                } else {
                    println!(
                        "Lower commit {}. Work in the mount, then: tl git snapshot {}",
                        resp.get("commit").and_then(|c| c.as_str()).unwrap_or("?"),
                        path.display()
                    );
                }
                if let Some(secs) = auto_commit_interval_secs {
                    if fs_surface {
                        println!("Autosave: changes save every {secs}s (async).");
                    } else {
                        println!(
                            "Auto-commit: local changes seal into a snapshot every {secs}s \
                             (async)."
                        );
                    }
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
                    if fs_surface {
                        let _ = session
                            .client
                            .delete_native_workspace_with_credential(
                                &session.project_id,
                                &repo,
                                &ws.id,
                                user,
                                token,
                            )
                            .await;
                    } else if git_mount_source.is_some() {
                        let _ = session
                            .client
                            .delete_git_mount_presence(
                                &session.project_id,
                                &repo,
                                user,
                                token,
                                &ws.id,
                            )
                            .await;
                    } else {
                        let _ = session
                            .client
                            .delete_workspace(&session.project_id, &repo, user, token, &ws.id)
                            .await;
                    }
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

async fn refuse_while_native_restore_active(
    state_dir: &Path,
    mountpoint: &str,
    state: &MountState,
    action: &str,
) -> Result<()> {
    if !state.native_filesystem || state.local_state_uuid.is_none() {
        return Ok(());
    }
    let operation = match daemon::control(state_dir, "restore-status").await {
        Ok(reply) => reply
            .get("operation")
            .cloned()
            .map(serde_json::from_value::<Option<local_state::RestoreOperation>>)
            .transpose()
            .map_err(|error| {
                CliError::usage(format!(
                    "cannot read durable restore status for {mountpoint}: {error}"
                ))
            })?
            .flatten(),
        Err(_) => {
            let identity = local_state_doctor_identity(mountpoint, state)?;
            local_state::LocalState::open_existing(
                state_dir.join(local_state::LOCAL_STATE_FILE),
                identity,
            )
            .map_err(|error| {
                CliError::usage(format!(
                    "cannot verify durable restore status for {mountpoint}: {error}"
                ))
            })?
            .active_restore()
            .map_err(|error| {
                CliError::usage(format!(
                    "cannot read durable restore status for {mountpoint}: {error}"
                ))
            })?
        }
    };
    if let Some(operation) = operation {
        return Err(CliError::usage(format!(
            "cannot {action} while restore {} to {} is in progress. Resume it with `tl fs \
             restore {mountpoint} {}`; the mount remains write-fenced until the durable restore \
             is adopted.",
            short_id(&operation.request_id),
            short_id(&operation.target_snapshot_id),
            operation.target_snapshot_id,
        )));
    }
    Ok(())
}

fn native_lifecycle_inflight(lifecycle: &LocalStateDoctorLifecycle) -> Option<String> {
    if let Some(restore) = lifecycle.active_restore.as_ref() {
        return Some(format!(
            "restore {} to {}",
            short_id(&restore.request_id),
            short_id(&restore.target_snapshot_id)
        ));
    }
    if let Some(generation) = lifecycle
        .generations
        .iter()
        .find(|generation| generation.state != "open")
    {
        return Some(format!(
            "snapshot generation {} ({})",
            generation.generation, generation.state
        ));
    }
    lifecycle
        .completed_publish_requests
        .iter()
        .find(|request| !request.acknowledged)
        .map(|request| {
            format!(
                "undelivered snapshot response {}",
                short_id(&request.request_id)
            )
        })
}

async fn native_lifecycle_for_mount(
    state_dir: &Path,
    mountpoint: &str,
    state: &MountState,
) -> Result<LocalStateDoctorLifecycle> {
    match daemon::control(state_dir, "doctor-local-state").await {
        Ok(response) => serde_json::from_value(response).map_err(|error| {
            CliError::usage(format!(
                "cannot decode durable snapshot lifecycle for {mountpoint}: {error}"
            ))
        }),
        Err(_) => {
            let identity = local_state_doctor_identity(mountpoint, state)?;
            let store = local_state::LocalState::open_existing(
                state_dir.join(local_state::LOCAL_STATE_FILE),
                identity,
            )
            .map_err(|error| {
                CliError::usage(format!(
                    "cannot verify durable snapshot lifecycle for {mountpoint}: {error}"
                ))
            })?;
            local_state_doctor_lifecycle(&store).map_err(|error| {
                CliError::usage(format!(
                    "cannot read durable snapshot lifecycle for {mountpoint}: {error}"
                ))
            })
        }
    }
}

pub async fn unmount(
    ctx: &CliContext,
    path: &Path,
    delete: bool,
    discard_local: bool,
) -> Result<()> {
    if let Some(attachment) = tracked_directory_for(path)? {
        if delete || discard_local {
            return Err(CliError::usage(
                "a tracked ordinary directory is your own files; unmount only stops local \
                 tracking (--discard/--delete do not apply). Use `tl fs rm` after unmount if the \
                 filesystem itself should be deleted",
            ));
        }
        let _writer_guard =
            try_local_state_writer_lock(&attachment.state_dir)?.ok_or_else(|| {
                CliError::usage(format!(
                    "{} is currently saving; wait for `tl fs push` to finish before unmounting",
                    attachment.root
                ))
            })?;
        let store = open_tracked_directory_state(&attachment)?;
        let lifecycle = local_state_doctor_lifecycle(&store).map_err(|error| {
            CliError::usage(format!(
                "cannot inspect tracked-directory snapshot lifecycle before unmount: {error}"
            ))
        })?;
        if let Some(inflight) = native_lifecycle_inflight(&lifecycle) {
            return Err(CliError::usage(format!(
                "cannot stop tracking {} while {inflight} is unresolved; rerun `tl fs push` to \
                 adopt it first",
                attachment.root
            )));
        }
        remove_tracked_directory(&attachment.root)?;
        println!(
            "Stopped tracking {} for filesystem {}. Local files were not changed; the durable \
             change index remains at {} and is reused by a later `tl fs push`.",
            attachment.root,
            attachment.filesystem_id,
            attachment.state_dir.display(),
        );
        return Ok(());
    }
    if let Some((root, _)) = plaindir::binding_for_lenient(path) {
        return Err(CliError::usage(format!(
            "{root} is a pushed directory, not a mount; stop tracking it with: tl fs unmount \
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
    if delete && state.mount_presence_id.is_some() {
        return Err(CliError::usage(
            "this is a stateless read-only repository view, so it has no workspace to delete; \
             rerun `tl git unmount` without `--delete`",
        ));
    }
    refuse_while_native_restore_active(&state_dir, &mountpoint, &state, "unmount").await?;
    if state.native_filesystem {
        let lifecycle = native_lifecycle_for_mount(&state_dir, &mountpoint, &state).await?;
        if let Some(inflight) = native_lifecycle_inflight(&lifecycle) {
            return Err(CliError::usage(format!(
                "cannot unmount {mountpoint} while {inflight} is unresolved; wait for the \
                 snapshot worker or retry `tl fs snapshot {mountpoint}`"
            )));
        }
    }
    // Unmount deletes the state dir — the overlay with it. Retained sealed content drops
    // loss-free (its bytes are in workspace history; sealing keeps the overlay, so this is
    // the normal post-snapshot state and must not gate). Anything TRULY losable — unsealed
    // changes, ignored files — needs the explicit flag. Checked before the shutdown so a
    // refusal leaves the mount fully intact.
    if !discard_local && let Some(losable) = overlay_losable_state(&state_dir, &mountpoint).await? {
        return Err(CliError::usage(format!(
            "the mount at {mountpoint} holds {losable}; unmounting would destroy that. Seal \
             unsealed changes first, then re-run:\n  tl fs snapshot {mountpoint}\n  tl fs \
             unmount {mountpoint}\nIgnored files never enter a snapshot — dropping them (and \
             everything else local) takes the flag:\n  tl fs unmount --discard \
             {mountpoint}"
        )));
    }
    // The gate's answer ages while the volume stays writable through teardown; fingerprint
    // now, re-check at the point of no return (state-dir deletion below).
    let local_baseline = if discard_local {
        None
    } else {
        Some(overlay_fingerprint(&state_dir)?)
    };
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
    if let Some(presence_id) = state.mount_presence_id.as_deref() {
        // Presence is observational and TTL-bound, so failure to remove it must not strand a
        // local unmount. A final best-effort DELETE makes clean exits disappear immediately;
        // crashed/offline exits disappear at expiry.
        match FsSession::open(ctx, Some(&state.repo)).await {
            Ok(session) => {
                let (user, token) = session.creds();
                if let Err(error) = session
                    .client
                    .delete_git_mount_presence(
                        &session.project_id,
                        &state.repo,
                        user,
                        token,
                        presence_id,
                    )
                    .await
                {
                    eprintln!(
                        "warning: could not remove read-only mount presence (it will expire): \
                         {error}"
                    );
                }
            }
            Err(error) => eprintln!(
                "warning: could not authenticate to remove read-only mount presence (it will \
                 expire): {error}"
            ),
        }
    }
    if delete {
        let session = FsSession::open(ctx, Some(&state.repo)).await?;
        let (user, token) = session.creds();
        if state.native_filesystem {
            session
                .client
                .delete_native_workspace_with_credential(
                    &session.project_id,
                    &state.repo,
                    &state.workspace_id,
                    user,
                    token,
                )
                .await?;
        } else {
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
    }
    // Point of no return. Writes that landed after the gate's answer (through the mount
    // while it was still attached) would be silently destroyed here — the daemon is gone,
    // but the fingerprint needs no daemon.
    if let Some(baseline) = local_baseline
        && overlay_fingerprint(&state_dir)? != baseline
    {
        return Err(CliError::usage(format!(
            "local changes landed while unmounting; the volume is detached but the local \
             state is kept at {}. Remount to recover and seal them (`tl fs mount \
             {mountpoint}`, then `tl fs snapshot {mountpoint}`), or re-run \
             `tl fs unmount --discard {mountpoint}` to drop them.",
            state_dir.display()
        )));
    }
    std::fs::remove_dir_all(&state_dir)?;
    registry_remove(&mountpoint)?;
    if delete {
        println!(
            "Unmounted {mountpoint} (session {} deleted).",
            short_id(&state.workspace_id)
        );
    } else if state.mount_presence_id.is_some() {
        println!("Unmounted read-only repository view at {mountpoint}.");
    } else if state.native_filesystem {
        println!(
            "Unmounted {mountpoint}. Session {} kept — `tl fs mount {} <path>` resumes it.",
            short_id(&state.workspace_id),
            state.repo,
        );
    } else {
        println!(
            "Unmounted {mountpoint}. Session {} kept — `tl fs mount {} <path>` resumes it \
             (repositories: tl git mount {} <path> --workspace {}).",
            short_id(&state.workspace_id),
            state.repo,
            state.repo,
            short_id(&state.workspace_id),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Snapshot: the overlay is the dirty set.
// ---------------------------------------------------------------------------------------------

/// Walk the overlay state dir: `(upserts, deletes)` as repo paths. Nested `.gitignore` files are
/// the sole authority for paths that do not enumerate.
/// Overlay upserts as `(repo path, upper file, git mode)`.
type OverlayUpserts = Vec<(String, PathBuf, u32)>;

struct SnapshotIgnore {
    mount_root: PathBuf,
    gitignores: HashMap<PathBuf, Gitignore>,
}

impl SnapshotIgnore {
    fn new(mount_root: &Path) -> Self {
        Self {
            mount_root: mount_root.to_path_buf(),
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
            let rel = overlay_rel_path(root, &abs);
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

/// Whether the overlay holds ANY local state — raw upper/wh trees plus pending renames, no
/// classification. This is [`overlay_losable_state`]'s empty fast path (and its daemon-less
/// escape hatch), NOT the destructive-command gate itself: retained sealed content counts as
/// "state" here, so gating a destructive command on this alone recreates the
/// permanently-unsatisfiable refusal the losable gate exists to fix.
///
/// Fails CLOSED: this guards data destruction, so an overlay tree that cannot be read is an
/// error, not "no state" — a permissions hiccup must never wave a destructive command
/// through. Only a missing tree (never-written overlay side) is honestly empty. The explicit
/// `--discard` flag bypasses the check entirely (callers short-circuit before calling).
fn overlay_has_local_state(state_dir: &Path) -> Result<bool> {
    fn any_entry(dir: &Path) -> Result<bool> {
        let read = match std::fs::read_dir(dir) {
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(overlay_unreadable(dir, &e)),
        };
        for entry in read {
            let entry = entry.map_err(|e| overlay_unreadable(dir, &e))?;
            let meta = match entry.path().symlink_metadata() {
                Ok(meta) => meta,
                // Deleted between readdir and stat: honestly not state anymore.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(overlay_unreadable(&entry.path(), &e)),
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

/// The shared fail-closed error for gate walks: this feeds data-destruction decisions, so an
/// unreadable tree is an error naming the bypass flag, never a guess.
fn overlay_unreadable(path: &Path, err: &std::io::Error) -> CliError {
    CliError::usage(format!(
        "cannot verify local overlay state: {} is unreadable ({err}); fix permissions \
         or pass --discard to drop the overlay without checking",
        path.display()
    ))
}

/// One rel-path normalization for every overlay walker. These strings are the join keys
/// against the sealed index and the daemon's dirty view — a walker normalizing differently
/// would classify every retained file "uncovered" and revert the losable gate to the
/// permanently-unsatisfiable behavior it exists to fix.
fn overlay_rel_path(root: &Path, abs: &Path) -> String {
    abs.strip_prefix(root)
        .expect("under root")
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// What dropping the overlay would actually destroy — the gate `unmount` and `restore` check
/// before requiring `--discard`. `None` means everything the overlay holds is retained
/// sealed content (stat-verified against the sealed index, so byte-identical to workspace
/// history — sealing keeps the overlay as the local byte cache, making this the DEFAULT
/// state after every snapshot), which drops loss-free. `Some(reason)` names the truly-losable
/// state: unsealed changes (pending renames included), ignored local-only files and
/// deletions, or overlay entries no seal record vouches for.
///
/// The classification is OFFLINE-capable. Native mounts read sealed baselines and every durable
/// dirty/rename row from snapshot-state.redb; a live daemon serves the same state from its
/// in-memory mirror because it owns redb's writable lock. Legacy repository mounts retain the old
/// sealed.json fallback. Any file modified through the mount either has a durable dirty intent or
/// stat-mismatches its retained baseline and refuses.
///
/// Directory containers are not counted again by the stat walk. Native empty-directory mutations
/// are represented by durable dirty rows; git cannot represent an empty tree.
///
/// Fail-closed: an unreadable tree or an unevaluable ignore rule is an error naming the
/// bypass flag, never a pass.
#[cfg(unix)]
async fn overlay_losable_state(state_dir: &Path, mountpoint: &str) -> Result<Option<String>> {
    let has_overlay = overlay_has_local_state(state_dir)?;
    let mount_state = state_dir
        .join("state.json")
        .exists()
        .then(|| daemon::load_mount_state(state_dir))
        .transpose()?;
    let durable_native = mount_state
        .as_ref()
        .is_some_and(|state| state.native_filesystem && state.local_state_uuid.is_some());
    let (dirty, sealed): (Option<daemon::DirtyReply>, daemon::SealedIndex) = if durable_native {
        match daemon::control(state_dir, "overlay-safety").await {
            Ok(reply) if reply.get("dirty").is_some() && reply.get("sealed").is_some() => {
                let safety: daemon::OverlaySafetyReply =
                    serde_json::from_value(reply).map_err(|error| {
                        CliError::usage(format!(
                            "cannot verify local overlay state: the mount daemon returned invalid \
                             durable safety state ({error}); pass --discard only if losing local \
                             work is intended"
                        ))
                    })?;
                (Some(safety.dirty), safety.sealed)
            }
            _ => {
                // The daemon is absent (or old enough not to serve the safety op). Open redb
                // strictly read-only; this fails closed if a live writer owns the lock, the
                // identity mismatches, or any required table/record is corrupt.
                let mount_state = mount_state
                    .as_ref()
                    .expect("durable native mount has mount state");
                let identity = local_state_doctor_identity(mountpoint, mount_state)?;
                let database_path = state_dir.join(local_state::LOCAL_STATE_FILE);
                let local = local_state::LocalState::open_existing(&database_path, identity)
                    .map_err(|error| {
                        CliError::usage(format!(
                            "cannot verify local overlay state from {}: {error}; pass --discard \
                             only if losing local work is intended",
                            database_path.display()
                        ))
                    })?;
                let recovery = local.recovery_dirty_state().map_err(|error| {
                    CliError::usage(format!(
                        "cannot read durable dirty state from {}: {error}; pass --discard only if \
                         losing local work is intended",
                        database_path.display()
                    ))
                })?;
                let mut upserts = std::collections::BTreeSet::new();
                let mut deletes = std::collections::BTreeSet::new();
                for path in recovery.paths {
                    match path.kind {
                        local_state::DirtyKind::Upsert => {
                            upserts.insert(path.path);
                        }
                        local_state::DirtyKind::Delete => {
                            deletes.insert(path.path);
                        }
                    }
                }
                let renames: std::collections::BTreeSet<(String, String)> = recovery
                    .renames
                    .into_iter()
                    .map(|rename| (rename.from, rename.to))
                    .collect();
                let active = local
                    .generation(recovery.active_generation)
                    .map_err(|error| {
                        CliError::usage(format!(
                            "cannot read durable active generation from {}: {error}",
                            database_path.display()
                        ))
                    })?
                    .ok_or_else(|| {
                        CliError::usage(format!(
                            "cannot verify local overlay state: durable active generation {} is \
                             missing from {}",
                            recovery.active_generation,
                            database_path.display()
                        ))
                    })?;
                let sealed =
                    daemon::sealed_index_from_local_state_reader(&local).map_err(|error| {
                        CliError::usage(format!(
                            "cannot read durable sealed baselines from {}: {error}; pass --discard \
                             only if losing local work is intended",
                            database_path.display()
                        ))
                    })?;
                (
                    Some(daemon::DirtyReply {
                        upserts: upserts.into_iter().collect(),
                        deletes: deletes.into_iter().collect(),
                        renames: renames.into_iter().collect(),
                        commit: active.base_snapshot.unwrap_or_default(),
                    }),
                    sealed,
                )
            }
        }
    } else {
        let dirty = match daemon::control(state_dir, "dirty").await {
            Ok(reply) if reply.get("upserts").is_some() => serde_json::from_value(reply).ok(),
            _ => None,
        };
        (dirty, daemon::SealedIndex::load(state_dir))
    };
    if !has_overlay
        && dirty.as_ref().is_none_or(|dirty| {
            dirty.upserts.is_empty() && dirty.deletes.is_empty() && dirty.renames.is_empty()
        })
    {
        // Truly empty. Also the answer for a never-written mount whose daemon is gone —
        // unmounting that must not demand a remount first.
        return Ok(None);
    }
    let mut ignore = SnapshotIgnore::new(Path::new(mountpoint));
    let renames = match &dirty {
        Some(d) => d.renames.clone(),
        None => pending_renames(state_dir)
            .into_iter()
            .map(|(to, from)| (from, to))
            .collect(),
    };
    let (dirty_upserts, dirty_deletes): (
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
    ) = match &dirty {
        Some(d) => (
            d.upserts.iter().cloned().collect(),
            d.deletes.iter().cloned().collect(),
        ),
        None => Default::default(),
    };
    let rename_sources: std::collections::HashSet<&str> =
        renames.iter().map(|(from, _)| from.as_str()).collect();
    // Dirty paths count once, from the authoritative view (not per on-disk entry): the next
    // seal owns them.
    let mut unsealed = dirty_upserts.len() + dirty_deletes.len() + renames.len();
    let (mut ignored, mut uncovered) = (0usize, 0usize);

    fn check_ignored(ignore: &mut SnapshotIgnore, rel: &str, is_dir: bool) -> Result<bool> {
        ignore.is_ignored(rel, is_dir).map_err(|e| {
            CliError::usage(format!(
                "cannot verify local overlay state: evaluating ignore rules failed ({e}); \
                 fix the ignore file or pass --discard to drop the overlay without \
                 checking"
            ))
        })
    }
    fn classify_upper(
        root: &Path,
        dir: &Path,
        ignore: &mut SnapshotIgnore,
        sealed: &daemon::SealedIndex,
        dirty_upserts: &std::collections::HashSet<String>,
        ignored: &mut usize,
        uncovered: &mut usize,
    ) -> Result<()> {
        let read = match std::fs::read_dir(dir) {
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(overlay_unreadable(dir, &e)),
        };
        for entry in read {
            let entry = entry.map_err(|e| overlay_unreadable(dir, &e))?;
            let abs = entry.path();
            let meta = match abs.symlink_metadata() {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(overlay_unreadable(&abs, &e)),
            };
            let rel = overlay_rel_path(root, &abs);
            if meta.is_dir() && !meta.file_type().is_symlink() {
                if check_ignored(ignore, &rel, true)? {
                    // One ignored SUBTREE, not a recursive file count: walking a large
                    // node_modules merely to report an exact number could dominate unmount
                    // latency, and the decision (ignored content never seals, user keeps or
                    // discards it knowingly) is per-subtree anyway.
                    *ignored += 1;
                } else {
                    classify_upper(
                        root,
                        &abs,
                        ignore,
                        sealed,
                        dirty_upserts,
                        ignored,
                        uncovered,
                    )?;
                }
                continue;
            }
            if dirty_upserts.contains(&rel) {
                continue; // counted via the dirty view
            }
            if sealed
                .upserts
                .get(&rel)
                .is_some_and(|s| daemon::SealedStat::of(&meta) == *s)
            {
                continue; // retained: stat-verified sealed content
            }
            if check_ignored(ignore, &rel, false)? {
                *ignored += 1;
                continue;
            }
            *uncovered += 1;
        }
        Ok(())
    }
    let upper = state_dir.join("upper");
    classify_upper(
        &upper,
        &upper,
        &mut ignore,
        &sealed,
        &dirty_upserts,
        &mut ignored,
        &mut uncovered,
    )?;
    // Whiteouts: dirty delete (counted) → pending-rename source (the rename counts) →
    // sealed tombstone (retained) → ignored (a local-only deletion of a committed file: it
    // can never seal, exactly like an ignored file, so it takes the flag) → uncovered.
    fn classify_wh(
        root: &Path,
        dir: &Path,
        ignore: &mut SnapshotIgnore,
        sealed: &daemon::SealedIndex,
        dirty_deletes: &std::collections::HashSet<String>,
        rename_sources: &std::collections::HashSet<&str>,
        ignored: &mut usize,
        uncovered: &mut usize,
    ) -> Result<()> {
        let read = match std::fs::read_dir(dir) {
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(overlay_unreadable(dir, &e)),
        };
        for entry in read {
            let entry = entry.map_err(|e| overlay_unreadable(dir, &e))?;
            let abs = entry.path();
            let meta = match abs.symlink_metadata() {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(overlay_unreadable(&abs, &e)),
            };
            let rel = overlay_rel_path(root, &abs);
            if meta.is_dir() && !meta.file_type().is_symlink() {
                classify_wh(
                    root,
                    &abs,
                    ignore,
                    sealed,
                    dirty_deletes,
                    rename_sources,
                    ignored,
                    uncovered,
                )?;
                continue;
            }
            if dirty_deletes.contains(&rel) || rename_sources.contains(rel.as_str()) {
                continue;
            }
            if sealed.deletes.contains(&rel) {
                continue; // retained tombstone
            }
            if check_ignored(ignore, &rel, false)? {
                *ignored += 1;
                continue;
            }
            *uncovered += 1;
        }
        Ok(())
    }
    let wh = state_dir.join("wh");
    classify_wh(
        &wh,
        &wh,
        &mut ignore,
        &sealed,
        &dirty_deletes,
        &rename_sources,
        &mut ignored,
        &mut uncovered,
    )?;
    // Offline, FUSE-written dirt has no dirty view to land in — it shows up here as a stat
    // mismatch instead.
    if dirty.is_none() {
        unsealed = 0;
    }
    let mut reasons = Vec::new();
    if unsealed > 0 {
        reasons.push(format!("{unsealed} unsealed local change(s)"));
    }
    if ignored > 0 {
        reasons.push(format!(
            "{ignored} ignored local-only change(s) (never part of any snapshot)"
        ));
    }
    if uncovered > 0 {
        reasons.push(format!(
            "{uncovered} overlay {} not covered by any snapshot{}",
            if uncovered == 1 { "entry" } else { "entries" },
            if dirty.is_none() {
                " (the mount daemon is not running; unsealed changes appear here)"
            } else {
                ""
            },
        ));
    }
    Ok((!reasons.is_empty()).then(|| reasons.join(", ")))
}

/// Mounts don't exist off unix; nothing can reach this gate with real state, and pretending
/// to classify would fork the semantics of a data-destruction check.
#[cfg(not(unix))]
async fn overlay_losable_state(_state_dir: &Path, _mountpoint: &str) -> Result<Option<String>> {
    Err(CliError::usage(
        "tl fs mounts are supported on Linux (FUSE) and macOS (FSKit) only.",
    ))
}

/// A cheap change detector over the raw overlay (upper + wh + pending renames): the
/// unmount/restore gates capture it when they pass, and both commands re-check it at their
/// point of no return — the gate's answer is only as good as the moment it was computed, and
/// seconds (unmount teardown) to minutes (restore's fetch phase) of live writable mount sit
/// between the two. Ignore rules deliberately play no part: ANY change is grounds to abort,
/// new ignored files included — and needing no rule evaluation also makes it usable after
/// the daemon is gone. Fail-closed on unreadable state.
fn overlay_fingerprint(state_dir: &Path) -> Result<u64> {
    use std::hash::{Hash, Hasher};
    fn collect(
        root: &Path,
        dir: &Path,
        prefix: &str,
        out: &mut Vec<(String, u64, Option<std::time::SystemTime>)>,
    ) -> Result<()> {
        let read = match std::fs::read_dir(dir) {
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(overlay_unreadable(dir, &e)),
        };
        for entry in read {
            let entry = entry.map_err(|e| overlay_unreadable(dir, &e))?;
            let abs = entry.path();
            let meta = match abs.symlink_metadata() {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(overlay_unreadable(&abs, &e)),
            };
            if meta.is_dir() && !meta.file_type().is_symlink() {
                collect(root, &abs, prefix, out)?;
            } else {
                out.push((
                    format!("{prefix}{}", overlay_rel_path(root, &abs)),
                    meta.len(),
                    meta.modified().ok(),
                ));
            }
        }
        Ok(())
    }
    let mut entries = Vec::new();
    collect(
        &state_dir.join("upper"),
        &state_dir.join("upper"),
        "u/",
        &mut entries,
    )?;
    collect(
        &state_dir.join("wh"),
        &state_dir.join("wh"),
        "w/",
        &mut entries,
    )?;
    entries.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (rel, len, mtime) in &entries {
        rel.hash(&mut hasher);
        len.hash(&mut hasher);
        if let Some(mtime) = mtime
            && let Ok(since_epoch) = mtime.duration_since(std::time::UNIX_EPOCH)
        {
            since_epoch.hash(&mut hasher);
        }
    }
    pending_renames(state_dir).hash(&mut hasher);
    Ok(hasher.finish())
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

/// A mount's local-change picture. `exact: true` means the daemon's sealer answered — the
/// dirty set is exactly what the next snapshot would publish (same dirty index, same ignore
/// rules, same resolution walk), and retained/ignored account for every other upper file.
/// `exact: false` is the daemon-down fallback: a raw upper walk that cannot distinguish
/// unsealed dirt from retained sealed content, so it may over-report.
struct LocalChanges {
    upserts: Vec<String>,
    deletes: Vec<String>,
    /// Pending committed-directory renames, `(from, to)`.
    renames: Vec<(String, String)>,
    /// Upper files sealed into a snapshot and kept as the local byte cache.
    retained: usize,
    /// Ignored local-only files (never enter a snapshot and survive generation-safe cleanup).
    ignored: usize,
    exact: bool,
}

impl LocalChanges {
    fn dirty(&self) -> usize {
        self.upserts.len() + self.deletes.len() + self.renames.len()
    }
}

/// The one authoritative answer to "what is dirty": ask the daemon's sealer (`dirty` control
/// op); fall back to the raw overlay walk only when the daemon is not answering. Every
/// dirt-consulting command (`status`, `promote`, `sync`, `diff`) routes through here so none
/// of them can disagree with what `tl fs snapshot` would actually seal.
async fn local_changes(state_dir: &Path, mountpoint: &str) -> Result<LocalChanges> {
    if let Ok(reply) = daemon::control(state_dir, "dirty").await
        && reply.get("ok").and_then(|v| v.as_bool()) == Some(true)
        // Every DirtyReply field is serde-defaulted, so a bare `{"ok":true}` ack (a handler
        // that acknowledges an op it never implemented) would otherwise parse as an EXACT
        // all-clean answer; requiring the payload field keeps that failure in the labeled
        // fallback instead.
        && reply.get("upserts").is_some()
        && let Ok(dirty) = serde_json::from_value::<daemon::DirtyReply>(reply)
    {
        // Retained/ignored accounting: everything in the upper that is not dirty is either
        // sealed-and-kept or ignored. The walk is local disk, and the ignore rules are the
        // same ones the sealer applies.
        let dirty_set: std::collections::HashSet<&str> =
            dirty.upserts.iter().map(String::as_str).collect();
        let mut ignore = SnapshotIgnore::new(Path::new(mountpoint));
        let (mut retained, mut ignored) = (0usize, 0usize);
        let upper = state_dir.join("upper");
        let mut stack = vec![upper.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(read) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in read.flatten() {
                let abs = entry.path();
                let Ok(meta) = abs.symlink_metadata() else {
                    continue;
                };
                let rel = overlay_rel_path(&upper, &abs);
                let is_dir = meta.is_dir() && !meta.file_type().is_symlink();
                if ignore.is_ignored(&rel, is_dir).unwrap_or(false) {
                    // An ignored directory's whole subtree is ignored — count its files.
                    ignored += if is_dir { count_files_under(&abs) } else { 1 };
                    continue;
                }
                if is_dir {
                    stack.push(abs);
                } else if !dirty_set.contains(rel.as_str()) {
                    retained += 1;
                }
            }
        }
        return Ok(LocalChanges {
            upserts: dirty.upserts,
            deletes: dirty.deletes,
            renames: dirty.renames,
            retained,
            ignored,
            exact: true,
        });
    }
    let (upserts, deletes) = enumerate_overlay(state_dir, Path::new(mountpoint))?;
    Ok(LocalChanges {
        upserts: upserts.into_iter().map(|(rel, _, _)| rel).collect(),
        deletes,
        renames: pending_renames(state_dir)
            .into_iter()
            .map(|(to, from)| (from, to))
            .collect(),
        retained: 0,
        ignored: 0,
        exact: false,
    })
}

/// Every file/symlink under `dir`, recursively — [`local_changes`]'s ignored-subtree counter.
fn count_files_under(dir: &Path) -> usize {
    let mut n = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(read) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in read.flatten() {
            let Ok(meta) = entry.path().symlink_metadata() else {
                continue;
            };
            if meta.is_dir() && !meta.file_type().is_symlink() {
                stack.push(entry.path());
            } else {
                n += 1;
            }
        }
    }
    n
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
    if let Some(attachment) = tracked_directory_for(path)? {
        if clear {
            return Err(CliError::usage(
                "--clear trims a mount's retained byte cache; a tracked ordinary directory has \
                 no overlay cache to trim",
            ));
        }
        return push_dir(
            ctx,
            Path::new(&attachment.root),
            &attachment.filesystem_id,
            message,
        )
        .await;
    }
    // A plain-directory binding snapshots by scanning the directory against its stat index;
    // there is no overlay, so the mount-only --clear flag has nothing to drop.
    if let Some((root, _)) = plaindir::binding_for_lenient(path) {
        return Err(CliError::usage(format!(
            "{root} uses the removed pre-release Git-backed directory binding. Stop tracking it \
             with `tl fs unmount {root}`, then attach the native engine with `tl fs push {root} \
             <filesystem>`."
        )));
    }
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    refuse_while_native_restore_active(&state_dir, &mountpoint, &state, "snapshot").await?;
    if state.read_only() {
        if state.native_filesystem {
            let view = state
                .pinned_snapshot
                .as_deref()
                .map(|save| format!("fixed at save {}", short_id(save)))
                .unwrap_or_else(|| "following the filesystem's current state".to_string());
            return Err(CliError::usage(format!(
                "this is a read-only filesystem view {view}; there is nothing to save"
            )));
        }
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
    let (outcome, cleared, completed_request_id) =
        seal_via_daemon(&state_dir, &mountpoint, message, clear, &bar).await?;
    let total = started.elapsed();
    bar.finish_and_clear();
    let sealed = match outcome {
        DaemonSealOutcome::Clean => {
            println!("{}", clean_snapshot_message(cleared));
            acknowledge_daemon_snapshot(&state_dir, completed_request_id.as_deref()).await;
            return Ok(());
        }
        DaemonSealOutcome::Pending { watermark } => {
            if clear {
                println!(
                    "Snapshot preparation queued at dirty watermark {watermark}; the mount daemon \
                     will publish it and then generation-safely clear its retained paths in the \
                     background."
                );
            } else {
                println!(
                    "Snapshot preparation queued at dirty watermark {watermark}; the mount daemon \
                     will publish it in the background."
                );
            }
            return Ok(());
        }
        DaemonSealOutcome::Sealed(sealed) => sealed,
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
    acknowledge_daemon_snapshot(&state_dir, completed_request_id.as_deref()).await;
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

enum DaemonSealOutcome {
    Clean,
    Pending { watermark: u64 },
    Sealed(DaemonSeal),
}

async fn acknowledge_daemon_snapshot(state_dir: &Path, request_id: Option<&str>) {
    let Some(request_id) = request_id else {
        return;
    };
    if let Err(error) = daemon::control_with(
        state_dir,
        "ack-snapshot",
        serde_json::json!({ "request_id": request_id }),
    )
    .await
    {
        // The snapshot is already published. Keeping the receipt unacknowledged is fail-safe:
        // the next matching manual invocation replays the exact success.
        eprintln!(
            "warning: snapshot succeeded but its local response receipt could not be acknowledged: \
             {error}"
        );
    }
}

/// What a clean (nothing-to-seal) snapshot prints. Never claims a clean workspace when a
/// requested clear actually dropped retained paths from an earlier save.
fn clean_snapshot_message(cleared: Option<usize>) -> String {
    match cleared {
        Some(n) if n > 0 => format!(
            "Nothing new to snapshot; cleared {n} locally retained path(s) from the overlay."
        ),
        _ => "Nothing to snapshot: workspace is clean.".to_string(),
    }
}

/// Seal through the mount daemon's sealer — the SAME machinery (and state) as auto-commit,
/// which is what makes manual snapshots correct: the shared dirty watermark means an
/// auto-commit mount never re-publishes manually sealed paths (and vice versa), only paths
/// touched since the last seal are pushed instead of the whole ever-dirty upper, and deletes
/// racing a seal go through the sealer's resurrection tombstone guard. A dirty generation
/// that has not finished background preparation returns [`DaemonSealOutcome::Pending`]
/// immediately; no scan, hash, compression, or upload runs on this control request.
///
/// The daemon advances the lower to the sealed commit before replying, so the mount serves
/// the new snapshot when this returns; the reply also drains the banked probe backlog, which
/// macOS converges here (Linux rode the FUSE notifier inside the daemon). The `seal` op is
/// line-streaming: progress events narrate onto `bar` until the final reply line arrives.
///
/// `clear` rides the seal request itself. Native mounts trim only retained paths owned by the
/// published generation, preserving later writes and ignored/local-only content. Repository
/// mounts retain their legacy whole-overlay behavior.
async fn seal_via_daemon(
    state_dir: &Path,
    mountpoint: &str,
    message: Option<&str>,
    clear: bool,
    bar: &indicatif::ProgressBar,
) -> Result<(DaemonSealOutcome, Option<usize>, Option<String>)> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let request = daemon::SealRequest {
        request_id: Some(request_id.clone()),
        message: message.map(str::to_string),
        clear,
    };
    let request_value = serde_json::to_value(&request)?;
    let resp = match daemon::control_streaming(state_dir, "seal", request_value.clone(), |event| {
        bar.set_message(event.to_string())
    })
    .await
    {
        Ok(response) => response,
        Err(first_error) => {
            bar.set_message("reconnecting to recover snapshot result...");
            daemon::control_streaming(state_dir, "seal", request_value, |event| {
                bar.set_message(event.to_string())
            })
            .await
            .map_err(|second_error| {
                CliError::usage(format!(
                    "snapshot request {request_id} lost its first daemon response ({first_error}) \
                     and recovery also failed: {second_error}"
                ))
            })?
        }
    };
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
    if reply.pending {
        if reply.clean {
            return Err(CliError::usage(
                "the mount daemon sent a contradictory seal reply (both clean and pending)",
            ));
        }
        let watermark = reply.pending_watermark.ok_or_else(|| {
            CliError::usage(
                "the mount daemon's pending seal reply is missing \"pending_watermark\"",
            )
        })?;
        return Ok((DaemonSealOutcome::Pending { watermark }, None, None));
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
        return Ok((
            DaemonSealOutcome::Clean,
            cleared,
            reply.completed_request_id,
        ));
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
        DaemonSealOutcome::Sealed(DaemonSeal {
            commit: reply.commit,
            files: sealed_field("files", reply.files)?,
            chunks_uploaded: sealed_field("chunks_uploaded", reply.chunks_uploaded)?,
            chunks_total: sealed_field("chunks_total", reply.chunks_total)?,
            push_ms: reply.push_ms,
        }),
        cleared,
        reply.completed_request_id,
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
    refuse_while_native_restore_active(&state_dir, &mountpoint, &state, "promote").await?;
    if state.read_only() {
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; there is nothing to promote",
            state.follow_ref.as_deref().unwrap_or("the branch"),
        )));
    }
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;
    let changes = local_changes(&state_dir, &mountpoint).await?;
    if changes.dirty() > 0 {
        eprintln!(
            "{} {} local change(s) not in any snapshot; promoting the last snapshot only. Run `tl fs snapshot` first to include them.{}",
            style("note:").yellow(),
            changes.dirty(),
            if changes.exact {
                ""
            } else {
                " (The mount daemon did not answer the dirty query; the count may include \
                 already-sealed files.)"
            },
        );
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
                            "rebase the workspace onto {branch}, resolve, and promote again:\n  tl git rebase {} {branch}\n  # fix the conflict markers, then\n  tl git snapshot {} && tl git promote {} {branch} --merge",
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

/// Rebase: pull the target branch into a behind workspace — one server-side rebase-style merge
/// commit on the target head; the mount's lower layer then advances to it. Under the default
/// materialize policy conflicts land as diff3 markers in the workspace files; resolve them and
/// snapshot. Local overlay changes would shadow synced content (markers included), so the
/// pre-flight is: refuse while anything is DIRTY (fix: `tl fs snapshot` — non-destructive),
/// then ask the daemon to `trim` retained sealed content out of the overlay (safe: those bytes
/// are in workspace history) so nothing sealed shadows the pull either. Ignored local-only
/// files survive; a sealed file held open by a live writer blocks the sync by name.
pub async fn git_rebase(
    ctx: &CliContext,
    path: &Path,
    target: &str,
    fail_on_conflict: bool,
    message: Option<&str>,
) -> Result<()> {
    if plaindir::binding_for_lenient(path).is_some() {
        return Err(CliError::usage(
            "rebase is not supported for plain-directory bindings in v1 (there is no mount to \
             materialize rebased content into); v1 bindings are single-writer capture only",
        ));
    }
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    refuse_while_native_restore_active(&state_dir, &mountpoint, &state, "rebase").await?;
    if state.read_only() {
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; it cannot be rebased",
            state.follow_ref.as_deref().unwrap_or("the branch"),
        )));
    }
    if let Some(target_ref) = state.follow_ref.as_deref() {
        return Err(CliError::usage(format!(
            "this workspace publishes every snapshot to {target_ref} and serves that branch; \
             rebasing only its private workspace ref would make the result invisible in the \
             mount. Snapshot normally (server reconciliation incorporates branch advances), or \
             create a non-publish workspace for an explicit rebase"
        )));
    }
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;
    let changes = local_changes(&state_dir, &mountpoint).await?;
    if !changes.exact {
        // The trim pre-flight and the post-sync refresh both need a live daemon; without the
        // exact dirty answer a raw-walk count would also re-introduce the old false refusal
        // on retained files.
        return Err(CliError::usage(
            "the mount daemon is not answering the dirty query (not running, or it predates \
             this CLI); remount with `tl git mount` and retry the rebase",
        ));
    }
    if changes.dirty() > 0 {
        return Err(CliError::usage(format!(
            "{} local change(s) would shadow rebased content. Snapshot them first: `tl git snapshot {}`, then rerun the rebase.",
            changes.dirty(),
            path.display(),
        )));
    }
    // Retained sealed content shadows pulled bytes exactly like dirt would — but it is all in
    // workspace history, so the daemon can drop it without losing anything. Ignored files
    // survive the trim (the pull may still be shadowed where an ignored file collides with a
    // synced path — local wins there, same as before).
    let trim = daemon::control(&state_dir, "trim").await?;
    let trim: daemon::TrimReply = serde_json::from_value(trim)
        .map_err(|e| CliError::usage(format!("the daemon's trim reply did not parse: {e}")))?;
    if !trim.held_open.is_empty() {
        return Err(CliError::usage(format!(
            "{} sealed file(s) could not be released (held open by a running process, or the \
             drop failed — first: {}); resolve that and rerun the rebase",
            trim.held_open.len(),
            trim.held_open[0],
        )));
    }
    let (user, token) = session.creds();
    let recovering =
        target.starts_with(&format!("refs/workspaces/{}/presync/", state.workspace_id));
    let request = RebaseWorkspaceRequest {
        target: Some(target.to_string()),
        policy: fail_on_conflict.then(|| "fail".to_string()),
        message: message.map(str::to_string),
        ..Default::default()
    };
    // Same 425 contract as promote: a rebase issued right behind a snapshot can catch the
    // commit index still materializing.
    let resp = {
        let deadline = std::time::Instant::now() + TOO_EARLY_DEADLINE;
        loop {
            match session
                .client
                .workspace_rebase(
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
    let recovery_ref = resp.recovery_ref.clone();
    let mut resp = resp.result;
    let mut outside_conflicts = Vec::new();
    if let Some(prefix) = state.subtree.as_deref() {
        let child_prefix = format!("{prefix}/");
        resp.conflicts = resp
            .conflicts
            .into_iter()
            .filter_map(|mut conflict| {
                let repo_path = conflict.path.clone();
                let projected = if repo_path == prefix {
                    ".".to_string()
                } else {
                    match repo_path.strip_prefix(&child_prefix) {
                        Some(path) => path.to_string(),
                        None => {
                            outside_conflicts.push(conflict);
                            return None;
                        }
                    }
                };
                conflict.path = projected;
                Some(conflict)
            })
            .collect();
    }
    if resp.up_to_date {
        println!(
            "Already up to date with {}.",
            &resp.target_head[..resp.target_head.len().min(12)]
        );
        return Ok(());
    }
    if !resp.clean && fail_on_conflict {
        eprintln!(
            "{} rebase conflicts on {} path(s); the workspace is unchanged:",
            style("error:").red(),
            resp.conflicts.len() + outside_conflicts.len(),
        );
        for c in &resp.conflicts {
            eprintln!("  {:<14} {}", style(&c.kind).yellow(), c.path);
        }
        for c in &outside_conflicts {
            eprintln!(
                "  {:<14} {} (outside mounted subtree)",
                style(&c.kind).yellow(),
                c.path
            );
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
                 rebase can transiently answer ENOENT (kernel cache, ~30s)",
                style("warning:").yellow(),
            );
        }
    }
    if recovering {
        println!(
            "Recovered retained chain {} at {}.",
            target,
            &resp.workspace_head[..resp.workspace_head.len().min(12)],
        );
    } else {
        println!(
            "Rebased onto {} ({} path(s) changed){}.",
            &resp.target_head[..resp.target_head.len().min(12)],
            resp.changed_paths,
            if resp.fast_forwarded {
                "; workspace fast-forwarded"
            } else {
                ""
            },
        );
    }
    if !resp.conflicts.is_empty() || !outside_conflicts.is_empty() {
        println!(
            "{} {} conflict(s) materialized as diff3 markers:",
            style("note:").yellow(),
            resp.conflicts.len() + outside_conflicts.len(),
        );
        for c in &resp.conflicts {
            println!("  {:<14} {}", style(&c.kind).yellow(), c.path);
        }
        for c in &outside_conflicts {
            println!(
                "  {:<14} {} (outside mounted subtree)",
                style(&c.kind).yellow(),
                c.path
            );
        }
        if outside_conflicts.is_empty() {
            println!(
                "Resolve the markers, then `tl git snapshot {}`.",
                path.display()
            );
        } else {
            println!(
                "Resolve every marker by resuming this workspace in a full-repository mount (or \
                 a subtree that contains the listed paths), then snapshot it."
            );
        }
    }
    if let Some(recovery_ref) = recovery_ref {
        println!("Retained the replaced chain at {recovery_ref}.");
    }
    Ok(())
}

/// Refresh the current repository view, or switch a pristine workspace to another source. The
/// server refuses any switch that would rewrite a snapshot chain and points the caller at
/// `tl git rebase`; this client also refuses unsnapshotted local changes before making the call.
pub async fn git_sync(ctx: &CliContext, path: &Path, target: Option<&str>) -> Result<()> {
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.native_filesystem {
        return Err(CliError::usage(
            "this is a filesystem mount; use `tl fs status` or `tl fs snapshot`",
        ));
    }

    if state.read_only() {
        let refresh = if let Some(target) = target {
            let previous_presence = state.mount_presence_id.as_deref().ok_or_else(|| {
                CliError::usage(
                    "this is a read-only attachment to a durable workspace; switching it would \
                     move the active writer's view. Mount a stateless view with `--ro` instead",
                )
            })?;
            let session = FsSession::open(ctx, Some(&state.repo)).await?;
            let (user, token) = session.creds();
            let source = session
                .client
                .resolve_git_mount_source(
                    &session.project_id,
                    &state.repo,
                    user,
                    token,
                    Some(target),
                    state.subtree.as_deref(),
                )
                .await?
                .into_inner();
            // Presence rows bind their source immutably. A source switch gets a fresh session id
            // so a concurrent/stale heartbeat can never rewrite the identity of the old view.
            let presence_id = uuid::Uuid::new_v4().to_string();
            let refresh = daemon::control_with(
                &state_dir,
                "switch-source",
                serde_json::json!({
                    "source": source.clone(),
                    "presence_id": presence_id.clone(),
                }),
            )
            .await?;
            if let Err(error) = session
                .client
                .record_git_mount_presence(
                    &session.project_id,
                    &state.repo,
                    user,
                    token,
                    &presence_id,
                    &tensorlake::artifact_storage::workspaces::RecordGitMountPresenceRequest {
                        source: &source,
                        mounted_on: &mountpoint,
                        ttl_seconds: None,
                    },
                )
                .await
            {
                eprintln!(
                    "{} the view switched, but its presence update will wait for the next \
                     heartbeat: {error}",
                    style("warning:").yellow(),
                );
            }
            if let Err(error) = session
                .client
                .delete_git_mount_presence(
                    &session.project_id,
                    &state.repo,
                    user,
                    token,
                    previous_presence,
                )
                .await
            {
                eprintln!(
                    "{} the old mount presence will disappear at expiry: {error}",
                    style("warning:").yellow(),
                );
            }
            refresh
        } else {
            daemon::control(&state_dir, "refresh").await?
        };
        let (expect, _, _) = parse_refresh_probes(&refresh);
        if cfg!(target_os = "macos") && !expect.is_empty() {
            let changed: BTreeSet<String> = expect.keys().cloned().collect();
            converge_kernel_view(Path::new(&mountpoint), &changed, &expect);
        }
        let commit = refresh
            .get("commit")
            .and_then(serde_json::Value::as_str)
            .or(state.start_oid.as_deref())
            .unwrap_or("unknown");
        println!(
            "{} {} at {}.",
            if target.is_some() {
                "Switched"
            } else {
                "Refreshed"
            },
            state.repo,
            short_id(commit)
        );
        return Ok(());
    }

    if target.is_some()
        && let Some(target_ref) = state.follow_ref.as_deref()
    {
        return Err(CliError::usage(format!(
            "this workspace is fixed to its publish target {target_ref}; retargeting it would \
             leave the live mount serving the old branch. Create a new publish workspace for \
             the other target, or run `tl git sync` without a target to refresh this one"
        )));
    }

    let changes = local_changes(&state_dir, &mountpoint).await?;
    if !changes.exact {
        return Err(CliError::usage(
            "the mount daemon is not answering the dirty query; remount with `tl git mount` \
             and retry the sync",
        ));
    }
    if changes.dirty() > 0 {
        return Err(CliError::usage(format!(
            "{} local change(s) would shadow the switched view. Snapshot them first with \
             `tl git snapshot {}`.",
            changes.dirty(),
            path.display(),
        )));
    }

    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;
    let (user, token) = session.creds();
    let request = SyncWorkspaceRequest {
        target: target.map(str::to_string),
        ..Default::default()
    };
    let response = session
        .client
        .workspace_sync(
            &session.project_id,
            &state.repo,
            user,
            token,
            &state.workspace_id,
            &request,
        )
        .await?
        .into_inner();
    let _ = daemon::control(&state_dir, "refresh").await?;
    if !response.changed {
        println!("Already at {}.", short_id(&response.target_head));
    } else {
        println!(
            "Switched pristine workspace {} to {} at {}.",
            short_id(&state.workspace_id),
            response
                .target_ref
                .as_deref()
                .unwrap_or("the requested commit"),
            short_id(&response.target_head),
        );
    }
    Ok(())
}

#[derive(Debug)]
struct RepairOverlayImage {
    upserts: Vec<String>,
    deletes: Vec<String>,
    renames: Vec<(String, String)>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LocalStateRepairManifest {
    format_ver: u16,
    created_at_ms: u64,
    mountpoint: String,
    database_path: PathBuf,
    backup_path: PathBuf,
    original_database_present: bool,
    base_snapshot: Option<String>,
    upserts: usize,
    deletes: usize,
    renames: usize,
}

#[derive(Debug, serde::Serialize)]
struct LocalStateRepairReport {
    status: &'static str,
    filesystem: String,
    mountpoint: String,
    state_directory: PathBuf,
    database_path: PathBuf,
    backup_path: PathBuf,
    original_database_present: bool,
    base_snapshot: Option<String>,
    upserts: usize,
    deletes: usize,
    renames: usize,
    #[serde(flatten)]
    lifecycle: LocalStateDoctorLifecycle,
}

fn repair_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn repair_rel_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root).map_err(|_| {
        CliError::usage(format!(
            "{} escaped repair overlay root {}",
            path.display(),
            root.display()
        ))
    })?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    CliError::usage(format!(
                        "repair cannot preserve the non-UTF-8 overlay path {}; no journal was \
                         replaced",
                        path.display()
                    ))
                })?;
                if part.is_empty() {
                    return Err(CliError::usage(format!(
                        "repair found an empty overlay path component under {}; no journal was \
                         replaced",
                        root.display()
                    )));
                }
                parts.push(part);
            }
            _ => {
                return Err(CliError::usage(format!(
                    "repair found a non-normal overlay path {}; no journal was replaced",
                    path.display()
                )));
            }
        }
    }
    if parts.is_empty() {
        return Err(CliError::usage(format!(
            "repair attempted to journal the overlay root {}; no journal was replaced",
            root.display()
        )));
    }
    Ok(parts.join("/"))
}

fn repair_walk_overlay_tree(
    root: &Path,
    include_directories: bool,
    label: &str,
) -> Result<Vec<String>> {
    let metadata = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(CliError::usage(format!(
                "cannot inspect repair {label} at {}: {error}; no journal was replaced",
                root.display()
            )));
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(CliError::usage(format!(
            "repair {label} at {} is not a real directory; no journal was replaced",
            root.display()
        )));
    }

    fn walk(
        root: &Path,
        directory: &Path,
        include_directories: bool,
        label: &str,
        paths: &mut Vec<String>,
    ) -> Result<()> {
        let mut entries = std::fs::read_dir(directory)
            .map_err(|error| {
                CliError::usage(format!(
                    "cannot enumerate repair {label} directory {}: {error}; no journal was \
                     replaced",
                    directory.display()
                ))
            })?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|error| {
                CliError::usage(format!(
                    "cannot enumerate repair {label} directory {}: {error}; no journal was \
                     replaced",
                    directory.display()
                ))
            })?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
                CliError::usage(format!(
                    "cannot inspect repair {label} entry {}: {error}; no journal was replaced",
                    path.display()
                ))
            })?;
            let file_type = metadata.file_type();
            if file_type.is_dir() && !file_type.is_symlink() {
                if include_directories {
                    paths.push(repair_rel_path(root, &path)?);
                }
                walk(root, &path, include_directories, label, paths)?;
            } else if file_type.is_file() || file_type.is_symlink() {
                paths.push(repair_rel_path(root, &path)?);
            } else {
                return Err(CliError::usage(format!(
                    "repair found unsupported local object {} in {label}; only regular files, \
                     directories, and symlinks are safe to journal",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    let mut paths = Vec::new();
    walk(root, root, include_directories, label, &mut paths)?;
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn validate_repair_journal_path(path: &str, label: &str) -> Result<()> {
    let candidate = Path::new(path);
    if path.is_empty()
        || candidate.is_absolute()
        || candidate.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::CurDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        return Err(CliError::usage(format!(
            "repair found invalid {label} path {path:?}; no journal was replaced"
        )));
    }
    if candidate
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CliError::usage(format!(
            "repair found invalid {label} path {path:?}; no journal was replaced"
        )));
    }
    Ok(())
}

fn repair_redirects(state_dir: &Path) -> Result<Vec<(String, String)>> {
    let path = state_dir.join("redirects.json");
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(CliError::usage(format!(
                "cannot inspect repair redirect state at {}: {error}; no journal was replaced",
                path.display()
            )));
        }
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(CliError::usage(format!(
            "repair redirect state at {} is not a regular file; no journal was replaced",
            path.display()
        )));
    }
    let raw = std::fs::read(&path)?;
    let redirects: HashMap<String, String> = serde_json::from_slice(&raw).map_err(|error| {
        CliError::usage(format!(
            "repair redirect state at {} is corrupt ({error}); no journal was replaced",
            path.display()
        ))
    })?;
    let mut entries = Vec::with_capacity(redirects.len());
    for (to, from) in redirects {
        validate_repair_journal_path(&to, "redirect destination")?;
        validate_repair_journal_path(&from, "redirect source")?;
        if from == to {
            return Err(CliError::usage(format!(
                "repair found a self-referential redirect {from:?}; no journal was replaced"
            )));
        }
        entries.push((from, to));
    }
    entries.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
    Ok(entries)
}

fn repair_overlay_image(state_dir: &Path) -> Result<RepairOverlayImage> {
    let upserts: std::collections::BTreeSet<String> =
        repair_walk_overlay_tree(&state_dir.join("upper"), true, "upper tree")?
            .into_iter()
            .collect();
    let mut deletes: std::collections::BTreeSet<String> =
        repair_walk_overlay_tree(&state_dir.join("wh"), false, "whiteout tree")?
            .into_iter()
            .collect();
    // A recreated upper entry shadows an old same-path whiteout. Descendant whiteouts remain
    // meaningful and are retained.
    deletes.retain(|path| !upserts.contains(path));
    Ok(RepairOverlayImage {
        upserts: upserts.into_iter().collect(),
        deletes: deletes.into_iter().collect(),
        renames: repair_redirects(state_dir)?,
    })
}

fn copy_repair_metadata_file(state_dir: &Path, backup_dir: &Path, name: &str) -> Result<()> {
    let source = state_dir.join(name);
    let metadata = match std::fs::symlink_metadata(&source) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(CliError::usage(format!(
            "repair evidence {} is not a regular file; refusing to replace the journal",
            source.display()
        )));
    }
    let destination = backup_dir.join(name);
    std::fs::copy(&source, &destination)?;
    std::fs::File::open(&destination)?.sync_all()?;
    Ok(())
}

fn write_repair_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let encoded = serde_json::to_vec_pretty(value)?;
    plaindir::write_atomic(path, &encoded)?;
    Ok(())
}

fn repair_database_base_snapshot(
    database_path: &Path,
    identity: local_state::LocalStateIdentity,
    explicit_base: Option<&str>,
) -> Result<Option<String>> {
    let explicit_base = explicit_base
        .map(|base| {
            if base == "empty" {
                Ok(None)
            } else if base.trim().is_empty() {
                Err(CliError::usage(
                    "--base must be a native save ID, or `empty` for a filesystem with no saves",
                ))
            } else {
                Ok(Some(base.to_string()))
            }
        })
        .transpose()?;
    let metadata = match std::fs::symlink_metadata(database_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return explicit_base.ok_or_else(|| {
                CliError::usage(format!(
                    "durable local snapshot state is missing at {}; the repair baseline cannot \
                     be proven. Retry with `--base <SAVE_ID>` (or `--base empty` only for a \
                     filesystem that has never had a save)",
                    database_path.display()
                ))
            });
        }
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(CliError::usage(format!(
            "durable local snapshot state at {} is not a regular file; refusing to replace it",
            database_path.display()
        )));
    }
    match local_state::LocalState::open_existing(database_path, identity) {
        Ok(reader) => {
            let active = reader.active_generation().map_err(|error| {
                CliError::usage(format!(
                    "cannot read the active generation from {}: {error}",
                    database_path.display()
                ))
            })?;
            let base = reader
                .generation(active)
                .map_err(|error| {
                    CliError::usage(format!(
                        "cannot read active generation {active} from {}: {error}",
                        database_path.display()
                    ))
                })?
                .and_then(|generation| generation.base_snapshot);
            if let Some(explicit) = explicit_base
                && explicit != base
            {
                return Err(CliError::usage(format!(
                    "--base does not match the baseline proven by the readable journal at {}",
                    database_path.display()
                )));
            }
            Ok(base)
        }
        Err(error) => {
            let detail = error.to_string().to_ascii_lowercase();
            if detail.contains("already open")
                || detail.contains("database lock")
                || detail.contains("resource temporarily unavailable")
            {
                return Err(CliError::usage(format!(
                    "durable local snapshot state at {} still has a live reader/writer ({error}); \
                     refusing offline repair",
                    database_path.display()
                )));
            }
            explicit_base.ok_or_else(|| {
                CliError::usage(format!(
                    "durable local snapshot state at {} cannot prove its baseline ({error}). \
                     Refusing to guess from attach-time mount metadata; retry with `--base \
                     <SAVE_ID>` (or `--base empty` only for a filesystem that has never had a \
                     save)",
                    database_path.display()
                ))
            })
        }
    }
}

fn repair_local_state_journal(
    mountpoint: String,
    state_dir: PathBuf,
    state: &MountState,
    explicit_base: Option<&str>,
) -> Result<LocalStateRepairReport> {
    if !state.native_filesystem {
        return Err(CliError::usage(
            "`tl fs doctor --repair-journal` only repairs native filesystem mounts",
        ));
    }
    if state.read_only() || state.local_state_uuid.is_none() {
        return Err(CliError::usage(
            "this mount has no writable native mutation journal to repair",
        ));
    }
    if daemon::daemon_pid(&state_dir).is_some_and(daemon_alive)
        || live_daemon_for(&mountpoint).is_some()
        || daemon::still_mounted(Path::new(&mountpoint))
    {
        return Err(CliError::usage(format!(
            "{mountpoint} is still mounted or has a live daemon; unmount it before repairing the \
             journal"
        )));
    }
    let _writer_guard = try_local_state_writer_lock(&state_dir)?.ok_or_else(|| {
        CliError::usage(format!(
            "native local snapshot state at {} has a live writer; refusing offline repair",
            state_dir.display()
        ))
    })?;
    // Close the check-vs-lock race: a previous-version daemon does not own the new writer flock,
    // so repeat every legacy liveness probe after acquiring it.
    if daemon::daemon_pid(&state_dir).is_some_and(daemon_alive)
        || live_daemon_for(&mountpoint).is_some()
        || daemon::still_mounted(Path::new(&mountpoint))
    {
        return Err(CliError::usage(format!(
            "{mountpoint} became live while journal repair was starting; no journal was replaced"
        )));
    }

    let identity = local_state_doctor_identity(&mountpoint, state)?;
    let database_path = state_dir.join(local_state::LOCAL_STATE_FILE);
    if let Ok(reader) = local_state::LocalState::open_existing(&database_path, identity.clone()) {
        let lifecycle = local_state_doctor_lifecycle(&reader).map_err(|error| {
            CliError::usage(format!(
                "cannot inspect the existing lifecycle before repair: {error}"
            ))
        })?;
        if let Some(inflight) = native_lifecycle_inflight(&lifecycle) {
            return Err(CliError::usage(format!(
                "refusing to replace the journal while {inflight} is unresolved; resume/adopt \
                 that operation before repair"
            )));
        }
    }
    let base_snapshot =
        repair_database_base_snapshot(&database_path, identity.clone(), explicit_base)?;
    let image = repair_overlay_image(&state_dir)?;
    let original_database_present = database_path.exists();
    let created_at_ms = repair_timestamp_ms();
    let backup_path = state_dir
        .join("repair-backups")
        .join(format!("{created_at_ms}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&backup_path)?;

    let manifest = LocalStateRepairManifest {
        format_ver: 1,
        created_at_ms,
        mountpoint: mountpoint.clone(),
        database_path: database_path.clone(),
        backup_path: backup_path.clone(),
        original_database_present,
        base_snapshot: base_snapshot.clone(),
        upserts: image.upserts.len(),
        deletes: image.deletes.len(),
        renames: image.renames.len(),
    };
    let marker_path = state_dir.join(LOCAL_STATE_REPAIR_MARKER);
    let prior_repair_marker = marker_path.exists();
    let backup_result = (|| -> Result<()> {
        if prior_repair_marker {
            copy_repair_metadata_file(&state_dir, &backup_path, LOCAL_STATE_REPAIR_MARKER)?;
        }
        for name in [
            "state.json",
            LOCAL_STATE_FORMAT_MARKER,
            "redirects.json",
            "sealed.json",
            "sealed.json.tmp",
            "native-prepared.json",
            "native-prepared.json.tmp",
            "native-seal-request.json",
            "native-seal-request.json.tmp",
        ] {
            copy_repair_metadata_file(&state_dir, &backup_path, name)?;
        }
        if original_database_present {
            copy_repair_metadata_file(&state_dir, &backup_path, local_state::LOCAL_STATE_FILE)?;
        } else {
            let missing = backup_path.join(format!("{}.missing", local_state::LOCAL_STATE_FILE));
            std::fs::File::create(&missing)?.sync_all()?;
        }
        write_repair_json(&backup_path.join("manifest.json"), &manifest)?;
        std::fs::File::open(&backup_path)?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = backup_result {
        return Err(error);
    }
    // The original database and supporting metadata are durable in the export before repair
    // marks or replaces anything in the live state directory.
    write_repair_json(&marker_path, &manifest)?;

    let temporary_database = state_dir.join(format!(
        ".{}.repair-{}.tmp",
        local_state::LOCAL_STATE_FILE,
        uuid::Uuid::new_v4()
    ));
    let build_result = (|| -> Result<()> {
        let store = local_state::LocalState::open(&temporary_database, identity.clone()).map_err(
            |error| {
                CliError::usage(format!(
                    "cannot create replacement journal {}: {error}",
                    temporary_database.display()
                ))
            },
        )?;
        let mut mutations =
            Vec::with_capacity(image.upserts.len() + image.deletes.len() + image.renames.len());
        mutations.extend(image.upserts.iter().cloned().map(|path| {
            local_state::LegacyMutation::Upsert {
                path,
                min_write_offset: 0,
            }
        }));
        mutations.extend(
            image
                .deletes
                .iter()
                .cloned()
                .map(|path| local_state::LegacyMutation::Delete { path }),
        );
        mutations.extend(
            image
                .renames
                .iter()
                .cloned()
                .map(|(from, to)| local_state::LegacyMutation::Rename { from, to }),
        );
        store
            .import_legacy_once(local_state::LegacyImport {
                base_snapshot: base_snapshot.clone(),
                mutations,
            })
            .map_err(|error| {
                CliError::usage(format!(
                    "cannot build conservative replacement journal: {error}"
                ))
            })?;
        drop(store);
        let reader = local_state::LocalState::open_existing(&temporary_database, identity.clone())
            .map_err(|error| {
                CliError::usage(format!(
                    "replacement journal failed strict validation before adoption: {error}"
                ))
            })?;
        let generations = reader.generations().map_err(|error| {
            CliError::usage(format!(
                "replacement journal generation table failed validation: {error}"
            ))
        })?;
        if generations.len() != 1
            || generations[0].generation != 1
            || generations[0].state != local_state::GenerationState::Open
        {
            return Err(CliError::usage(
                "replacement journal did not contain exactly one open generation",
            ));
        }
        Ok(())
    })();
    if let Err(error) = build_result {
        let _ = std::fs::remove_file(&temporary_database);
        if prior_repair_marker {
            let _ = std::fs::copy(backup_path.join(LOCAL_STATE_REPAIR_MARKER), &marker_path);
        } else {
            let _ = std::fs::remove_file(&marker_path);
        }
        let _ = std::fs::File::open(&state_dir).and_then(|directory| directory.sync_all());
        return Err(error);
    }

    // From here onward an interruption must leave the marker in place so daemon startup fails
    // closed. The original database and every prior generation capture are already exported.
    let staging = state_dir.join("staging");
    if let Ok(metadata) = std::fs::symlink_metadata(&staging) {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(CliError::usage(format!(
                "generation staging path {} is not a real directory; original journal preserved \
                 at {} and repair marker retained",
                staging.display(),
                backup_path.display()
            )));
        }
        std::fs::rename(&staging, backup_path.join("staging"))?;
        std::fs::File::open(&backup_path)?.sync_all()?;
    }
    std::fs::rename(&temporary_database, &database_path).map_err(|error| {
        CliError::usage(format!(
            "adopting replacement journal failed ({error}); original state is preserved at {} \
             and the repair marker prevents mounting",
            backup_path.display()
        ))
    })?;
    std::fs::File::open(&state_dir)?.sync_all()?;

    let repaired =
        local_state::LocalState::open_existing(&database_path, identity).map_err(|error| {
            CliError::usage(format!(
                "adopted replacement journal failed validation ({error}); evidence is preserved \
                 at {} and the repair marker prevents mounting",
                backup_path.display()
            ))
        })?;
    let lifecycle = local_state_doctor_lifecycle(&repaired).map_err(|error| {
        CliError::usage(format!(
            "adopted replacement journal lifecycle is unreadable ({error}); evidence is preserved \
             at {} and the repair marker prevents mounting",
            backup_path.display()
        ))
    })?;
    drop(repaired);
    std::fs::remove_file(&marker_path)?;
    std::fs::File::open(&state_dir)?.sync_all()?;

    Ok(LocalStateRepairReport {
        status: "repaired",
        filesystem: state.repo.clone(),
        mountpoint,
        state_directory: state_dir,
        database_path,
        backup_path,
        original_database_present,
        base_snapshot,
        upserts: image.upserts.len(),
        deletes: image.deletes.len(),
        renames: image.renames.len(),
        lifecycle,
    })
}

#[derive(Debug, serde::Serialize)]
struct LocalStateDoctorReport {
    status: &'static str,
    filesystem: String,
    mountpoint: String,
    state_directory: PathBuf,
    database_path: PathBuf,
    database_bytes: u64,
    daemon_running: bool,
    #[serde(flatten)]
    lifecycle: LocalStateDoctorLifecycle,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LocalStateDoctorLifecycle {
    identity: local_state::LocalStateIdentity,
    active_generation: u64,
    sealed_baseline_paths: usize,
    ordered_mutation_intents: usize,
    staging_artifacts: usize,
    staging_bytes: u64,
    oldest_unretired_generation: Option<u64>,
    active_restore: Option<local_state::RestoreOperation>,
    failed_restore: Option<local_state::RestoreOperation>,
    completed_publish_requests: Vec<LocalStateDoctorCompletedRequest>,
    generations: Vec<LocalStateDoctorGeneration>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LocalStateDoctorGeneration {
    generation: u64,
    state: String,
    base_snapshot: Option<String>,
    dirty_paths: usize,
    rename_intents: usize,
    ordered_mutation_intents: usize,
    prepared: Option<LocalStateDoctorPrepared>,
    publish_requests: Vec<LocalStateDoctorRequest>,
    published_snapshot: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LocalStateDoctorPrepared {
    root_id: String,
    source_fingerprint: String,
    candidate_bytes: usize,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LocalStateDoctorRequest {
    request_id: String,
    message: String,
    clear_after_publish: bool,
    created_at_ms: u64,
    failure: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LocalStateDoctorCompletedRequest {
    request_id: String,
    generation: u64,
    snapshot_id: String,
    clear_after_publish: bool,
    created_at_ms: u64,
    response_bytes: usize,
    #[serde(default)]
    acknowledged: bool,
}

fn local_state_generation_name(state: local_state::GenerationState) -> &'static str {
    match state {
        local_state::GenerationState::Open => "open",
        local_state::GenerationState::Frozen => "frozen",
        local_state::GenerationState::Prepared => "prepared",
        local_state::GenerationState::PublishRequested => "publish_requested",
        local_state::GenerationState::Published => "published",
    }
}

trait LocalStateDoctorSource {
    fn doctor_identity(&self) -> &local_state::LocalStateIdentity;
    fn doctor_active_generation(&self) -> local_state::Result<u64>;
    fn doctor_generations(&self) -> local_state::Result<Vec<local_state::GenerationRecord>>;
    fn doctor_recovery_dirty_state(&self) -> local_state::Result<local_state::RecoveryDirtyState>;
    fn doctor_prepared(
        &self,
        generation: u64,
    ) -> local_state::Result<Option<local_state::PreparedGeneration>>;
    fn doctor_publish_requests(&self) -> local_state::Result<Vec<local_state::PublishRequest>>;
    fn doctor_completed_publish_requests(
        &self,
    ) -> local_state::Result<Vec<local_state::CompletedPublishRequest>>;
    fn doctor_sealed_baselines(&self) -> local_state::Result<Vec<local_state::SealedBaseline>>;
    fn doctor_active_restore(&self) -> local_state::Result<Option<local_state::RestoreOperation>>;
    fn doctor_failed_restore(&self) -> local_state::Result<Option<local_state::RestoreOperation>>;
    fn doctor_artifacts(&self) -> local_state::Result<Vec<local_state::ArtifactOwnership>>;
}

impl LocalStateDoctorSource for local_state::LocalState {
    fn doctor_identity(&self) -> &local_state::LocalStateIdentity {
        self.identity()
    }

    fn doctor_active_generation(&self) -> local_state::Result<u64> {
        self.active_generation()
    }

    fn doctor_generations(&self) -> local_state::Result<Vec<local_state::GenerationRecord>> {
        self.generations()
    }

    fn doctor_recovery_dirty_state(&self) -> local_state::Result<local_state::RecoveryDirtyState> {
        self.recovery_dirty_state()
    }

    fn doctor_prepared(
        &self,
        generation: u64,
    ) -> local_state::Result<Option<local_state::PreparedGeneration>> {
        self.prepared(generation)
    }

    fn doctor_publish_requests(&self) -> local_state::Result<Vec<local_state::PublishRequest>> {
        self.publish_requests()
    }

    fn doctor_completed_publish_requests(
        &self,
    ) -> local_state::Result<Vec<local_state::CompletedPublishRequest>> {
        self.completed_publish_requests()
    }

    fn doctor_sealed_baselines(&self) -> local_state::Result<Vec<local_state::SealedBaseline>> {
        self.sealed_baselines()
    }

    fn doctor_active_restore(&self) -> local_state::Result<Option<local_state::RestoreOperation>> {
        self.active_restore()
    }

    fn doctor_failed_restore(&self) -> local_state::Result<Option<local_state::RestoreOperation>> {
        self.failed_restore()
    }

    fn doctor_artifacts(&self) -> local_state::Result<Vec<local_state::ArtifactOwnership>> {
        self.artifacts()
    }
}

impl LocalStateDoctorSource for local_state::LocalStateReader {
    fn doctor_identity(&self) -> &local_state::LocalStateIdentity {
        self.identity()
    }

    fn doctor_active_generation(&self) -> local_state::Result<u64> {
        self.active_generation()
    }

    fn doctor_generations(&self) -> local_state::Result<Vec<local_state::GenerationRecord>> {
        self.generations()
    }

    fn doctor_recovery_dirty_state(&self) -> local_state::Result<local_state::RecoveryDirtyState> {
        self.recovery_dirty_state()
    }

    fn doctor_prepared(
        &self,
        generation: u64,
    ) -> local_state::Result<Option<local_state::PreparedGeneration>> {
        self.prepared(generation)
    }

    fn doctor_publish_requests(&self) -> local_state::Result<Vec<local_state::PublishRequest>> {
        self.publish_requests()
    }

    fn doctor_completed_publish_requests(
        &self,
    ) -> local_state::Result<Vec<local_state::CompletedPublishRequest>> {
        self.completed_publish_requests()
    }

    fn doctor_sealed_baselines(&self) -> local_state::Result<Vec<local_state::SealedBaseline>> {
        self.sealed_baselines()
    }

    fn doctor_active_restore(&self) -> local_state::Result<Option<local_state::RestoreOperation>> {
        self.active_restore()
    }

    fn doctor_failed_restore(&self) -> local_state::Result<Option<local_state::RestoreOperation>> {
        self.failed_restore()
    }

    fn doctor_artifacts(&self) -> local_state::Result<Vec<local_state::ArtifactOwnership>> {
        self.artifacts()
    }
}

fn local_state_doctor_lifecycle(
    store: &impl LocalStateDoctorSource,
) -> local_state::Result<LocalStateDoctorLifecycle> {
    let active_generation = store.doctor_active_generation()?;
    let requests = store.doctor_publish_requests()?;
    let recovery = store.doctor_recovery_dirty_state()?;
    let mut generations = Vec::new();
    for generation in store.doctor_generations()? {
        let prepared = store
            .doctor_prepared(generation.generation)?
            .map(|prepared| LocalStateDoctorPrepared {
                root_id: prepared.root_id,
                source_fingerprint: prepared.source_fingerprint,
                candidate_bytes: prepared.candidate.len(),
            });
        let publish_requests = requests
            .iter()
            .filter(|request| request.generation == generation.generation)
            .map(|request| LocalStateDoctorRequest {
                request_id: request.request_id.clone(),
                message: request.message.clone(),
                clear_after_publish: request.clear_after_publish,
                created_at_ms: request.created_at_ms,
                failure: request.failure.clone(),
            })
            .collect();
        generations.push(LocalStateDoctorGeneration {
            generation: generation.generation,
            state: local_state_generation_name(generation.state).to_string(),
            base_snapshot: generation.base_snapshot,
            dirty_paths: recovery
                .paths
                .iter()
                .filter(|path| path.generation == generation.generation)
                .count(),
            rename_intents: recovery
                .renames
                .iter()
                .filter(|rename| rename.generation == generation.generation)
                .count(),
            ordered_mutation_intents: recovery
                .intents
                .iter()
                .filter(|intent| intent.generation == generation.generation)
                .count(),
            prepared,
            publish_requests,
            published_snapshot: generation.published_snapshot,
        });
    }
    let artifacts = store.doctor_artifacts()?;
    Ok(LocalStateDoctorLifecycle {
        identity: store.doctor_identity().clone(),
        active_generation,
        sealed_baseline_paths: store.doctor_sealed_baselines()?.len(),
        ordered_mutation_intents: recovery.intents.len(),
        staging_artifacts: artifacts.len(),
        staging_bytes: artifacts.iter().map(|artifact| artifact.bytes).sum(),
        oldest_unretired_generation: generations
            .iter()
            .filter(|generation| generation.state != "open")
            .map(|generation| generation.generation)
            .min(),
        active_restore: store.doctor_active_restore()?,
        failed_restore: store.doctor_failed_restore()?,
        completed_publish_requests: store
            .doctor_completed_publish_requests()?
            .into_iter()
            .map(|completed| LocalStateDoctorCompletedRequest {
                request_id: completed.request.request_id,
                generation: completed.request.generation,
                snapshot_id: completed.snapshot_id,
                clear_after_publish: completed.request.clear_after_publish,
                created_at_ms: completed.request.created_at_ms,
                response_bytes: completed.response.len(),
                acknowledged: completed.acknowledged,
            })
            .collect(),
        generations,
    })
}

fn local_state_doctor_identity(
    mountpoint: &str,
    state: &MountState,
) -> Result<local_state::LocalStateIdentity> {
    if !state.native_filesystem {
        return Err(CliError::usage(format!(
            "{mountpoint} is a repository mount; `tl fs doctor` only inspects native filesystem \
             mounts"
        )));
    }
    let Some(store_uuid) = state.local_state_uuid.clone() else {
        let detail = if state.read_only() {
            "this is a read-only mount and has no local mutation journal"
        } else {
            "its mount state predates the durable local journal identity"
        };
        return Err(CliError::usage(format!(
            "{mountpoint} has no durable local snapshot state: {detail}. Nothing was treated as \
             clean and no repair was attempted."
        )));
    };
    Ok(local_state::LocalStateIdentity {
        project_id: state.project_id.clone(),
        filesystem: state.repo.clone(),
        workspace_id: state.workspace_id.clone(),
        store_uuid,
    })
}

fn local_state_database_info(state_dir: &Path) -> Result<(PathBuf, u64)> {
    let database_path = state_dir.join(local_state::LOCAL_STATE_FILE);
    let database_bytes = match std::fs::metadata(&database_path) {
        Ok(metadata) if metadata.is_file() => metadata.len(),
        Ok(_) => {
            return Err(CliError::usage(format!(
                "durable local snapshot state at {} is not a regular file; refusing to inspect \
                 or replace it",
                database_path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CliError::usage(format!(
                "durable local snapshot state is missing at {}; refusing to treat this mount as \
                 clean. No repair was attempted.",
                database_path.display()
            )));
        }
        Err(error) => {
            return Err(CliError::usage(format!(
                "cannot inspect durable local snapshot state at {}: {error}",
                database_path.display()
            )));
        }
    };
    Ok((database_path, database_bytes))
}

fn inspect_local_state(
    mountpoint: String,
    state_dir: PathBuf,
    state: &MountState,
) -> Result<LocalStateDoctorReport> {
    let identity = local_state_doctor_identity(&mountpoint, state)?;
    let (database_path, database_bytes) = local_state_database_info(&state_dir)?;
    let store =
        local_state::LocalState::open_existing(&database_path, identity).map_err(|error| {
            CliError::usage(format!(
                "durable local snapshot state at {} failed validation: {error}. Refusing to treat \
             this mount as clean; no repair was attempted.",
                database_path.display()
            ))
        })?;
    let lifecycle = local_state_doctor_lifecycle(&store).map_err(|error| {
        CliError::usage(format!(
            "cannot inspect lifecycle records in {}: {error}",
            database_path.display()
        ))
    })?;
    Ok(LocalStateDoctorReport {
        status: "healthy",
        filesystem: state.repo.clone(),
        mountpoint,
        state_directory: state_dir.clone(),
        database_path,
        database_bytes,
        daemon_running: false,
        lifecycle,
    })
}

/// Inspect a managed native mount's durable local state. The default mode never contacts the
/// server, publishes a snapshot, repairs state, or walks the overlay. `repair_journal` remains
/// local-only but explicitly exports and replaces the journal from a conservative raw-overlay
/// walk while the mount is detached.
pub async fn doctor(
    path: &Path,
    output_json: bool,
    repair_journal: bool,
    repair_base: Option<&str>,
) -> Result<()> {
    if let Some(attachment) = tracked_directory_for(path)? {
        if repair_journal {
            return Err(CliError::usage(
                "tracked ordinary directories reconcile from their source tree on the next \
                 snapshot; offline --repair-journal is only for managed mount overlays",
            ));
        }
        let store = open_tracked_directory_state(&attachment)?;
        let lifecycle = local_state_doctor_lifecycle(&store).map_err(|error| {
            CliError::usage(format!(
                "cannot inspect tracked-directory lifecycle records in {}: {error}",
                attachment.state_dir.display()
            ))
        })?;
        let database_path = attachment.state_dir.join(local_state::LOCAL_STATE_FILE);
        let database_bytes = std::fs::metadata(&database_path)?.len();
        let report = LocalStateDoctorReport {
            status: "healthy",
            filesystem: attachment.filesystem_id,
            mountpoint: attachment.root,
            state_directory: attachment.state_dir,
            database_path,
            database_bytes,
            daemon_running: false,
            lifecycle,
        };
        if output_json {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            println!("{} {}", style("status:").dim(), style("healthy").green());
            println!("{} {}", style("filesystem:").dim(), report.filesystem);
            println!("{} tracked directory", style("attachment:").dim());
            println!("{} {}", style("root:").dim(), report.mountpoint);
            println!(
                "{} {} ({} bytes)",
                style("database:").dim(),
                report.database_path.display(),
                report.database_bytes
            );
            println!(
                "{} {} ({} sealed path baseline(s))",
                style("active generation:").dim(),
                report.lifecycle.active_generation,
                report.lifecycle.sealed_baseline_paths
            );
            println!(
                "{} {}",
                style("pending generations:").dim(),
                report
                    .lifecycle
                    .generations
                    .iter()
                    .filter(|generation| generation.state != "open")
                    .count()
            );
            println!(
                "{} intents={} artifacts={} staging_bytes={}",
                style("local snapshot state:").dim(),
                report.lifecycle.ordered_mutation_intents,
                report.lifecycle.staging_artifacts,
                report.lifecycle.staging_bytes,
            );
            if let Some(restore) = &report.lifecycle.active_restore {
                println!(
                    "{} {} -> {}",
                    style("active restore:").dim(),
                    restore.request_id,
                    restore.target_snapshot_id,
                );
            }
            if let Some(restore) = &report.lifecycle.failed_restore {
                println!(
                    "{} {}: {}",
                    style("failed restore:").dim(),
                    restore.request_id,
                    restore.failure.as_deref().unwrap_or("unknown failure"),
                );
            }
        }
        return Ok(());
    }
    let (mountpoint, state_dir) = doctor_state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir).map_err(|error| {
        CliError::usage(format!(
            "cannot read mount state at {}: {error}",
            state_dir.join("state.json").display()
        ))
    })?;
    if repair_journal {
        let report = repair_local_state_journal(mountpoint, state_dir, &state, repair_base)?;
        if output_json {
            println!("{}", serde_json::to_string_pretty(&report)?);
            return Ok(());
        }
        println!(
            "{} {}",
            style("status:").dim(),
            style(report.status).green()
        );
        println!("{} {}", style("filesystem:").dim(), report.filesystem);
        println!("{} {}", style("mountpoint:").dim(), report.mountpoint);
        println!(
            "{} {}",
            style("database:").dim(),
            report.database_path.display()
        );
        println!(
            "{} {}",
            style("original state backup:").dim(),
            report.backup_path.display()
        );
        println!(
            "{} upserts={} deletes={} renames={} base={}",
            style("rebuilt generation:").dim(),
            report.upserts,
            report.deletes,
            report.renames,
            report.base_snapshot.as_deref().unwrap_or("-"),
        );
        println!(
            "{} {} ({})",
            style("active generation:").dim(),
            report.lifecycle.active_generation,
            local_state_generation_name(local_state::GenerationState::Open)
        );
        return Ok(());
    }
    let daemon_running = daemon::daemon_pid(&state_dir).is_some_and(daemon_alive);
    let report = if daemon_running {
        let expected_identity = local_state_doctor_identity(&mountpoint, &state)?;
        let (database_path, database_bytes) = local_state_database_info(&state_dir)?;
        let response = daemon::control(&state_dir, "doctor-local-state")
            .await
            .map_err(|error| {
                CliError::usage(format!(
                    "the live mount daemon could not inspect its durable local state: {error}. \
                     No repair was attempted."
                ))
            })?;
        let lifecycle: LocalStateDoctorLifecycle =
            serde_json::from_value(response).map_err(|error| {
                CliError::usage(format!(
                    "the live mount daemon returned an invalid local-state report: {error}"
                ))
            })?;
        if lifecycle.identity != expected_identity {
            return Err(CliError::usage(format!(
                "the live mount daemon reported local state for a different mount (expected \
                 {expected_identity:?}, found {:?}); refusing to continue",
                lifecycle.identity
            )));
        }
        LocalStateDoctorReport {
            status: "healthy",
            filesystem: state.repo.clone(),
            mountpoint,
            state_directory: state_dir,
            database_path,
            database_bytes,
            daemon_running: true,
            lifecycle,
        }
    } else {
        inspect_local_state(mountpoint, state_dir, &state)?
    };
    if output_json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!(
        "{} {}",
        style("status:").dim(),
        style(report.status).green()
    );
    println!("{} {}", style("filesystem:").dim(), report.filesystem);
    println!("{} {}", style("mountpoint:").dim(), report.mountpoint);
    println!(
        "{} {}",
        style("state directory:").dim(),
        report.state_directory.display()
    );
    println!(
        "{} {} ({} bytes)",
        style("database:").dim(),
        report.database_path.display(),
        report.database_bytes
    );
    println!(
        "{} project={} filesystem={} workspace={} store={}",
        style("identity:").dim(),
        report.lifecycle.identity.project_id,
        report.lifecycle.identity.filesystem,
        report.lifecycle.identity.workspace_id,
        report.lifecycle.identity.store_uuid,
    );
    println!(
        "{} {}",
        style("daemon:").dim(),
        if report.daemon_running {
            "running"
        } else {
            "not running"
        }
    );
    println!(
        "{} {} ({} sealed path baseline(s))",
        style("active generation:").dim(),
        report.lifecycle.active_generation,
        report.lifecycle.sealed_baseline_paths
    );
    println!(
        "{} intents={} artifacts={} staging_bytes={} oldest_unretired={}",
        style("local snapshot state:").dim(),
        report.lifecycle.ordered_mutation_intents,
        report.lifecycle.staging_artifacts,
        report.lifecycle.staging_bytes,
        report
            .lifecycle
            .oldest_unretired_generation
            .map(|generation| generation.to_string())
            .as_deref()
            .unwrap_or("-"),
    );
    if let Some(restore) = &report.lifecycle.active_restore {
        println!(
            "{} {} target={} expected={}",
            style("active restore:").dim(),
            restore.request_id,
            restore.target_snapshot_id,
            restore.expected_snapshot_id,
        );
    }
    if let Some(restore) = &report.lifecycle.failed_restore {
        println!(
            "{} {} target={} reason={:?}",
            style("failed restore:").dim(),
            restore.request_id,
            restore.target_snapshot_id,
            restore.failure.as_deref().unwrap_or("unknown failure"),
        );
    }
    println!(
        "{} {}",
        style("completed request receipts:").dim(),
        report.lifecycle.completed_publish_requests.len()
    );
    for completed in &report.lifecycle.completed_publish_requests {
        println!(
            "  {}  generation={} snapshot={} clear={} acknowledged={} created_at_ms={} response={} bytes",
            completed.request_id,
            completed.generation,
            completed.snapshot_id,
            completed.clear_after_publish,
            completed.acknowledged,
            completed.created_at_ms,
            completed.response_bytes,
        );
    }
    println!("{}", style("generations:").dim());
    for generation in &report.lifecycle.generations {
        println!(
            "  {}  {}  dirty={} renames={} intents={} base={} published={}",
            generation.generation,
            generation.state,
            generation.dirty_paths,
            generation.rename_intents,
            generation.ordered_mutation_intents,
            generation.base_snapshot.as_deref().unwrap_or("-"),
            generation.published_snapshot.as_deref().unwrap_or("-"),
        );
        if let Some(prepared) = &generation.prepared {
            println!(
                "    prepared root={} source={} candidate={} bytes",
                prepared.root_id, prepared.source_fingerprint, prepared.candidate_bytes
            );
        }
        for request in &generation.publish_requests {
            println!(
                "    request {} clear={} created_at_ms={} failure={:?} message={:?}",
                request.request_id,
                request.clear_after_publish,
                request.created_at_ms,
                request.failure,
                request.message,
            );
        }
    }
    Ok(())
}

async fn tracked_directory_status(
    ctx: &CliContext,
    attachment: TrackedDirectoryAttachment,
    output_json: bool,
) -> Result<()> {
    let root = PathBuf::from(&attachment.root);
    let store = open_tracked_directory_state(&attachment)?;
    let baselines = store
        .sealed_baselines()
        .map_err(|error| cold_push_state_error(&attachment.state_dir, error))?;
    let scan_root = root.clone();
    let scan = tokio::task::spawn_blocking(move || scan_cold_push_tree(&scan_root))
        .await
        .map_err(|error| CliError::usage(format!("tracked-directory scan failed: {error}")))??;
    let delta = plan_cold_push_delta(&root, &scan, &baselines)?;
    let mut generations = store
        .generations()
        .map_err(|error| cold_push_state_error(&attachment.state_dir, error))?;
    generations.sort_by_key(|generation| generation.generation);
    let active = generations
        .iter()
        .find(|generation| generation.state == local_state::GenerationState::Open)
        .ok_or_else(|| {
            CliError::usage(format!(
                "tracked directory {} has no open generation",
                attachment.root
            ))
        })?;
    let session = FsSession::open(ctx, Some(&attachment.filesystem_id)).await?;
    let (user, token) = session.creds();
    let head = session
        .client
        .native_head_with_credential(&session.project_id, &attachment.filesystem_id, user, token)
        .await?;
    let pending: Vec<serde_json::Value> = generations
        .iter()
        .filter(|generation| generation.state != local_state::GenerationState::Open)
        .map(|generation| {
            serde_json::json!({
                "generation": generation.generation,
                "state": local_state_generation_name(generation.state),
                "base_snapshot": generation.base_snapshot,
                "published_snapshot": generation.published_snapshot,
            })
        })
        .collect();
    let report = serde_json::json!({
        "filesystem": attachment.filesystem_id,
        "attachment": {
            "kind": "tracked_directory",
            "root": attachment.root,
            "state_directory": attachment.state_dir,
        },
        "adopted_snapshot": active.base_snapshot.clone(),
        "server_head": head.snapshot_id.clone(),
        "dirty": {
            "upserts": delta.upserts.iter().map(|path| path.path.clone()).collect::<Vec<_>>(),
            "deletes": delta.deletes.clone(),
            "renames": Vec::<serde_json::Value>::new(),
            "exact": true,
        },
        "pending_generations": pending,
        "daemon_running": false,
        "retained": baselines.len(),
        "ignored": serde_json::Value::Null,
    });
    if output_json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    println!(
        "{} {}",
        style("filesystem:").dim(),
        report["filesystem"].as_str().unwrap_or("-")
    );
    println!("{} tracked directory", style("attachment:").dim());
    println!("{} {}", style("root:").dim(), root.display());
    println!(
        "{} {}",
        style("adopted save:").dim(),
        active.base_snapshot.as_deref().map(short_id).unwrap_or("-")
    );
    println!(
        "{} {}",
        style("server head:").dim(),
        head.snapshot_id.as_deref().map(short_id).unwrap_or("-")
    );
    println!(
        "{} {} upsert(s), {} delete(s) (strict metadata reconciliation; regular file bytes were \
         not opened)",
        style("dirty:").dim(),
        delta.upserts.len(),
        report["dirty"]["deletes"].as_array().map_or(0, Vec::len),
    );
    if !pending.is_empty() {
        println!("{} {}", style("pending generations:").dim(), pending.len());
    }
    Ok(())
}

pub async fn status(ctx: &CliContext, path: &Path, output_json: bool) -> Result<()> {
    if let Some(attachment) = tracked_directory_for(path)? {
        return tracked_directory_status(ctx, attachment, output_json).await;
    }
    if let Some((root, _)) = plaindir::binding_for_lenient(path) {
        return Err(CliError::usage(format!(
            "{root} uses the removed pre-release Git-backed directory binding. Stop tracking it \
             with `tl fs unmount {root}`, then attach the native engine with `tl fs push {root} \
             <filesystem>`."
        )));
    }
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    let (user, token) = session.creds();
    let ws = if state.native_filesystem {
        native_workspace_to_mount_info(
            session
                .client
                .native_workspace_with_credential(
                    &session.project_id,
                    &state.repo,
                    &state.workspace_id,
                    user,
                    token,
                )
                .await?,
        )
    } else {
        session
            .client
            .get_workspace(
                &session.project_id,
                &state.repo,
                user,
                token,
                &state.workspace_id,
            )
            .await?
            .into_inner()
    };
    let _ = heartbeat(&session, &state).await;
    let daemon_commit = daemon::control(&state_dir, "ping")
        .await
        .ok()
        .and_then(|r| r.get("commit").and_then(|c| c.as_str().map(str::to_string)));
    let changes = local_changes(&state_dir, &mountpoint).await?;
    // Collisions the followed state materialized that no later save has overwritten. Absent
    // daemon (or a pre-visibility daemon) reads as none — the section only ever adds signal.
    let collisions: Vec<(String, String, String)> = if state.native_filesystem {
        Vec::new()
    } else {
        daemon::control(&state_dir, "conflicts")
            .await
            .ok()
            .and_then(|r| r.get("conflicts").cloned())
            .and_then(|v| serde_json::from_value::<Vec<serde_json::Value>>(v).ok())
            .map(|list| {
                list.into_iter()
                    .filter_map(|c| {
                        Some((
                            c.get("path")?.as_str()?.to_string(),
                            c.get("kind")?.as_str()?.to_string(),
                            c.get("commit")?.as_str()?.to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    if output_json {
        let mut output = serde_json::json!({
            "session": filesystem_session_json(&ws),
            "mounted": daemon_commit.is_some(),
            "log": state_dir.join("daemon.log"),
            "dirty": changes.upserts.iter().cloned()
                .chain(changes.deletes.iter().cloned()).collect::<Vec<_>>(),
            // False when the daemon did not answer the dirty query and the counts fell
            // back to a raw overlay walk (which may include already-saved files).
            "dirty_exact": changes.exact,
            "retained": changes.retained,
            "ignored": changes.ignored,
            "pending_renames": changes.renames
                .iter()
                .map(|(from, to)| serde_json::json!({ "from": from, "to": to }))
                .collect::<Vec<_>>(),
            "unresolved_collisions": collisions
                .iter()
                .map(|(path, kind, commit)| serde_json::json!({
                    "path": path, "kind": kind, "commit": commit,
                }))
                .collect::<Vec<_>>(),
        });
        let state_key = if state.native_filesystem {
            "current_save"
        } else {
            "lower_commit"
        };
        output
            .as_object_mut()
            .expect("status JSON is an object")
            .insert(state_key.to_string(), serde_json::json!(daemon_commit));
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }
    println!("{} {}", style("filesystem:").dim(), state.repo);
    println!(
        "{} {} (created {} ago)",
        style("session:").dim(),
        short_id(&ws.id),
        age_display(ws.created_at_secs)
    );
    // A publish-on-save mount follows its target branch for convergence but is writable —
    // read_only() (not follow_ref presence) decides which mode the user is in.
    if state.read_only() {
        if let Some(snapshot) = state.pinned_snapshot.as_deref() {
            println!(
                "{} read-only, fixed at save {}",
                style("mode:").dim(),
                short_id(snapshot)
            );
        } else if state.native_filesystem {
            println!(
                "{} read-only, follows the filesystem's current state",
                style("mode:").dim()
            );
        } else {
            let followed = state.follow_ref.as_deref().unwrap_or("the current state");
            println!("{} read-only, follows {followed}", style("mode:").dim());
        }
    } else if state.native_filesystem {
        println!(
            "{} writable — every save becomes the filesystem's current state",
            style("mode:").dim()
        );
    } else if let Some(target) = &ws.shared_target {
        println!(
            "{} publishing — every save lands on {target} (view follows it)",
            style("mode:").dim()
        );
    } else {
        println!("{} private (publish with promote)", style("mode:").dim());
    }
    if let Some(secs) = state.auto_commit_interval_secs {
        println!(
            "{} local changes are saved every {secs}s (saved content below is kept \
             locally as the byte cache)",
            style("autosave:").dim()
        );
    }
    match (&daemon_commit, state.native_filesystem) {
        (Some(save), true) => {
            println!("{} serving save {}", style("daemon:").dim(), short_id(save))
        }
        (Some(commit), false) => println!("{} mounted at {commit}", style("daemon:").dim()),
        (None, _) => println!(
            "{} not running (remount with tl fs mount)",
            style("daemon:").dim()
        ),
    }
    println!(
        "{} {}",
        style("log:").dim(),
        state_dir.join("daemon.log").display()
    );
    if !collisions.is_empty() {
        println!(
            "{} {} path(s) — both saves were kept; markers are in the files. Edit and save \
             to resolve:",
            style("unresolved collisions:").yellow(),
            collisions.len(),
        );
        for (path, kind, commit) in &collisions {
            println!(
                "  {:<14} {path} (save {})",
                style(kind).yellow(),
                short_id(commit)
            );
        }
    }
    // A pending rename's source whiteout is a real delete, but showing it next to the R line
    // would read as two changes; the R line carries both sides.
    let deletes: Vec<&String> = changes
        .deletes
        .iter()
        .filter(|p| !changes.renames.iter().any(|(from, _)| &from == p))
        .collect();
    let dirty = changes.upserts.len() + deletes.len() + changes.renames.len();
    if dirty == 0 {
        println!("{} clean", style("local:").dim());
    } else {
        println!("{} {} change(s):", style("local:").dim(), dirty);
        for (from, to) in changes.renames.iter().take(20) {
            println!("  {} {from} -> {to}", style("R").cyan());
        }
        for p in changes.upserts.iter().take(20) {
            println!("  {} {p}", style("M").yellow());
        }
        for p in deletes.iter().take(20) {
            println!("  {} {p}", style("D").red());
        }
        if dirty > 60 {
            println!("  … and more");
        }
        if !changes.exact {
            println!(
                "{} the mount daemon did not answer the dirty query; counts may include \
                 files already included in a save",
                style("note:").yellow()
            );
        }
    }
    if changes.retained > 0 {
        println!(
            "{} {} file(s) included in saves and kept locally as the byte cache \
             (`tl fs snapshot --clear` drops them)",
            style("retained:").dim(),
            changes.retained
        );
    }
    if changes.ignored > 0 {
        println!(
            "{} {} local-only ignored file(s) (never saved)",
            style("ignored:").dim(),
            changes.ignored
        );
    }
    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct GitStatusWorkspace {
    id: String,
    base: String,
    snapshot: String,
    target: Option<String>,
    relationship: String,
    conflicts: Vec<serde_json::Value>,
    conflict_operation: Option<String>,
    retained_chains: Vec<String>,
    retained_chains_truncated: bool,
}

#[derive(Debug, serde::Serialize)]
struct GitStatusLocal {
    changed_paths: usize,
    exact: bool,
}

#[derive(Debug, serde::Serialize)]
struct GitStatusReport {
    format_ver: u16,
    state: String,
    view_type: String,
    repository: String,
    subtree: Option<String>,
    canonical_source: String,
    resolved_commit: Option<String>,
    following: bool,
    connected: bool,
    workspace: Option<GitStatusWorkspace>,
    local: GitStatusLocal,
    next_actions: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
fn classify_git_status(
    source_deleted: bool,
    connected: bool,
    conflict_state: Option<&'static str>,
    read_only: bool,
    following: bool,
    dirty: usize,
    publish_workspace: bool,
    target_advanced: bool,
    snapshotted: bool,
    has_unlanded_snapshots: bool,
) -> (&'static str, Vec<String>) {
    if source_deleted {
        return (
            "source_ref_deleted",
            vec![if publish_workspace {
                "unmount and create a new publish workspace on an existing branch".to_string()
            } else if has_unlanded_snapshots {
                "tl git rebase <REF-OR-COMMIT>".to_string()
            } else {
                "tl git sync <REF-OR-COMMIT>".to_string()
            }],
        );
    }
    if !connected {
        return (
            "server_unreachable_stale_view",
            vec!["restore connectivity, then run tl git sync".to_string()],
        );
    }
    if let Some(conflict_state) = conflict_state {
        return (
            conflict_state,
            vec![
                "resolve conflict markers".to_string(),
                "tl git snapshot".to_string(),
                "tl git promote <BRANCH>".to_string(),
            ],
        );
    }
    if read_only {
        return if following {
            ("read_only_following", vec!["tl git sync".to_string()])
        } else {
            ("read_only_pinned", Vec::new())
        };
    }
    // A local edit always comes before branch relationship advice: rebase/sync intentionally reject
    // dirty overlays, so the only valid next transition is to snapshot first.
    if dirty > 0 {
        return (
            "workspace_locally_dirty",
            vec!["tl git snapshot".to_string()],
        );
    }
    if publish_workspace {
        return (
            "workspace_clean",
            vec!["edit files or run tl git sync".to_string()],
        );
    }
    if target_advanced && snapshotted {
        return (
            "workspace_target_advanced",
            vec!["tl git rebase <REF-OR-COMMIT>".to_string()],
        );
    }
    if has_unlanded_snapshots {
        return (
            "workspace_snapshotted_unpromoted",
            vec![
                "tl git promote <BRANCH>".to_string(),
                "tl git rebase <REF-OR-COMMIT>".to_string(),
            ],
        );
    }
    (
        "workspace_clean",
        vec!["edit files or tl git sync [REF-OR-COMMIT]".to_string()],
    )
}

/// Repository-specific status. This intentionally does not reuse the filesystem renderer: its
/// stable JSON and text name Git workspaces, snapshots, refs, conflicts, and recovery actions.
pub async fn git_status(ctx: &CliContext, path: &Path, output_json: bool) -> Result<()> {
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.native_filesystem {
        return Err(CliError::usage(format!(
            "{} is a native filesystem mount; use `tl fs status {}`",
            mountpoint, mountpoint,
        )));
    }
    let changes = local_changes(&state_dir, &mountpoint).await?;
    let dirty = changes.dirty();
    let daemon_ping = daemon::control(&state_dir, "ping").await.ok();
    let conflicts = daemon::control(&state_dir, "conflicts")
        .await
        .ok()
        .and_then(|reply| reply.get("conflicts").cloned())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();

    // Status must remain useful during an outage. Credential loading/minting and every remote
    // lookup are therefore evidence for `connected`, not `?` exits that hide the stale local view.
    let session = FsSession::open(ctx, Some(&state.repo)).await.ok();
    let expects_workspace = state.mount_presence_id.is_none();
    let workspace = if expects_workspace {
        match session.as_ref() {
            Some(session) => {
                let (user, token) = session.creds();
                session
                    .client
                    .get_workspace(
                        &session.project_id,
                        &state.repo,
                        user,
                        token,
                        &state.workspace_id,
                    )
                    .await
                    .ok()
                    .map(|response| response.into_inner())
            }
            None => None,
        }
    } else {
        None
    };
    let canonical_source = state
        .git_mount_source
        .as_ref()
        .and_then(|source| source.canonical_ref.clone())
        .or_else(|| state.follow_ref.clone())
        .or_else(|| {
            workspace
                .as_ref()
                .and_then(|workspace| workspace.base_ref.clone())
        })
        .or_else(|| workspace.as_ref().map(|workspace| workspace.base.clone()))
        .unwrap_or_else(|| state.ref_name.clone());
    let source_status_result = match session.as_ref() {
        Some(session) if canonical_source.starts_with("refs/") => {
            let (user, token) = session.creds();
            Some(
                session
                    .client
                    .ref_status(
                        &session.project_id,
                        &state.repo,
                        user,
                        token,
                        &canonical_source,
                    )
                    .await
                    .map(|status| status.into_inner()),
            )
        }
        _ => None,
    };
    // A pinned full-commit mount has no ref to probe. Re-resolving its exact commit is a bounded
    // read-only reachability check and also revalidates the selected subtree.
    let pinned_probe_ok = if !expects_workspace && !canonical_source.starts_with("refs/") {
        match session.as_ref() {
            Some(session) => {
                let (user, token) = session.creds();
                session
                    .client
                    .resolve_git_mount_source(
                        &session.project_id,
                        &state.repo,
                        user,
                        token,
                        Some(&canonical_source),
                        state.subtree.as_deref(),
                    )
                    .await
                    .is_ok()
            }
            None => false,
        }
    } else {
        true
    };
    let server_reachable = session.is_some()
        && (!expects_workspace || workspace.is_some())
        && source_status_result
            .as_ref()
            .is_none_or(|result| result.is_ok())
        && pinned_probe_ok;
    let source_status = source_status_result.and_then(|result| result.ok());
    let connected = daemon_ping.is_some() && server_reachable;
    let source_deleted = source_status
        .as_ref()
        .is_some_and(|status| status.oid.is_none());
    let target_oid = source_status.as_ref().and_then(|status| {
        status
            .resolved_commit
            .as_deref()
            .or(status.oid.as_deref())
            .map(str::to_string)
    });
    let snapshotted = workspace
        .as_ref()
        .is_some_and(|workspace| workspace.head != workspace.base);

    // Equality handles the common cases without another request. The ambiguous case needs a real
    // graph relation: a publish reconcile often makes a merge commit that contains the workspace
    // snapshot, so `target != head` is not evidence of divergence.
    let relationship = match (workspace.as_ref(), target_oid.as_deref()) {
        (None, _) => "not_applicable",
        (Some(workspace), None) if workspace.head == workspace.base => "aligned",
        (Some(_), None) => "ahead",
        (Some(workspace), Some(target)) if target == workspace.head => "aligned",
        (Some(workspace), Some(target)) if target == workspace.base => {
            if workspace.head == workspace.base {
                "aligned"
            } else {
                "ahead"
            }
        }
        (Some(workspace), Some(_)) if workspace.head == workspace.base => "behind",
        (Some(workspace), Some(target)) => match session.as_ref() {
            Some(session) => {
                let (user, token) = session.creds();
                match session
                    .client
                    .repo_merge(
                        &session.project_id,
                        &state.repo,
                        user,
                        token,
                        &MergeRequest {
                            ours: target.to_string(),
                            theirs: workspace.head.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(report) if report.already_merged => "behind",
                    Ok(report) if report.fast_forward => "ahead",
                    Ok(_) => "diverged",
                    Err(_) => "unknown",
                }
            }
            None => "unknown",
        },
    };
    let target_advanced = matches!(relationship, "behind" | "diverged");
    let has_unlanded_snapshots =
        snapshotted && matches!(relationship, "ahead" | "diverged" | "unknown");
    let publish_workspace = !state.read_only() && state.follow_ref.is_some();

    let (conflict_operation, retained_chains, retained_chains_truncated) =
        match (session.as_ref(), workspace.as_ref()) {
            (Some(session), Some(workspace)) => {
                let (user, token) = session.creds();
                let conflict_operation = if conflicts.is_empty() {
                    None
                } else {
                    session
                        .client
                        .workspace_log(
                            &session.project_id,
                            &state.repo,
                            user,
                            token,
                            &state.workspace_id,
                            None,
                            1,
                        )
                        .await
                        .ok()
                        .and_then(|page| {
                            page.into_inner()
                                .active_chain
                                .into_iter()
                                .find(|entry| entry.oid == workspace.head && entry.conflicted)
                                .and_then(|entry| entry.operation)
                        })
                };
                // Jump directly to the retained-ref phase. Walking a long active chain first can
                // otherwise make status falsely claim there are no recovery refs.
                let retained = session
                    .client
                    .workspace_log(
                        &session.project_id,
                        &state.repo,
                        user,
                        token,
                        &state.workspace_id,
                        Some("r:"),
                        200,
                    )
                    .await
                    .ok()
                    .map(|page| page.into_inner());
                let truncated = retained.as_ref().is_some_and(|page| page.truncated);
                let refs = retained
                    .into_iter()
                    .flat_map(|page| page.retained_chains)
                    .map(|chain| chain.recovery_ref)
                    .collect();
                (conflict_operation, refs, truncated)
            }
            _ => (None, Vec::new(), false),
        };
    let conflict_state = conflict_operation
        .as_deref()
        .is_some_and(|operation| operation == "rebase" || operation == "workspace_sync")
        .then_some("rebase_conflict")
        .unwrap_or("merge_conflict_requires_sync_rebase");
    let (state_name, next_actions) = classify_git_status(
        source_deleted,
        connected,
        (!conflicts.is_empty()).then_some(conflict_state),
        state.read_only(),
        state.follow_ref.is_some(),
        dirty,
        publish_workspace,
        target_advanced,
        snapshotted,
        has_unlanded_snapshots,
    );
    let workspace_report = if expects_workspace {
        let base = workspace
            .as_ref()
            .map(|workspace| workspace.base.clone())
            .or_else(|| state.start_oid.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let snapshot = workspace
            .as_ref()
            .map(|workspace| workspace.head.clone())
            .or_else(|| {
                daemon_ping.as_ref().and_then(|reply| {
                    reply
                        .get("commit")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
            })
            .unwrap_or_else(|| "unknown".to_string());
        Some(GitStatusWorkspace {
            id: workspace
                .as_ref()
                .map(|workspace| workspace.id.clone())
                .unwrap_or_else(|| state.workspace_id.clone()),
            base,
            snapshot,
            target: workspace
                .as_ref()
                .and_then(|workspace| workspace.shared_target.clone())
                .or_else(|| {
                    state
                        .follow_ref
                        .as_deref()
                        .and_then(|name| name.strip_prefix("refs/heads/"))
                        .map(str::to_string)
                }),
            relationship: relationship.to_string(),
            conflicts,
            conflict_operation,
            retained_chains,
            retained_chains_truncated,
        })
    } else {
        None
    };
    let report = GitStatusReport {
        format_ver: 1,
        state: state_name.to_string(),
        view_type: if state.read_only() {
            "read_only_view".to_string()
        } else {
            "writable_workspace".to_string()
        },
        repository: state.repo.clone(),
        subtree: state.subtree.clone(),
        canonical_source,
        resolved_commit: daemon_ping.as_ref().and_then(|reply| {
            reply
                .get("commit")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        }),
        following: state.follow_ref.is_some(),
        connected,
        workspace: workspace_report,
        local: GitStatusLocal {
            changed_paths: dirty,
            exact: changes.exact,
        },
        next_actions,
    };
    if output_json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        render_git_status(&report);
    }
    Ok(())
}

fn render_git_status(report: &GitStatusReport) {
    println!("{} {}", style("repository:").dim(), report.repository);
    println!("{} {}", style("state:").dim(), report.state);
    println!("{} {}", style("source:").dim(), report.canonical_source);
    if let Some(subtree) = report.subtree.as_deref() {
        println!("{} {subtree}", style("subtree:").dim());
    }
    if let Some(commit) = report.resolved_commit.as_deref() {
        println!("{} {}", style("commit:").dim(), short_id(commit));
    }
    if let Some(workspace) = report.workspace.as_ref() {
        println!("{} {}", style("workspace:").dim(), workspace.id);
        println!("{} {}", style("base:").dim(), short_id(&workspace.base));
        println!(
            "{} {}",
            style("snapshot:").dim(),
            short_id(&workspace.snapshot)
        );
        if let Some(target) = workspace.target.as_deref() {
            println!("{} {target}", style("target:").dim());
        }
        println!(
            "{} {}",
            style("relationship:").dim(),
            workspace.relationship
        );
        if !workspace.conflicts.is_empty() {
            println!(
                "{} {}{}",
                style("conflicts:").yellow(),
                workspace.conflicts.len(),
                workspace
                    .conflict_operation
                    .as_deref()
                    .map(|operation| format!(" (created by {operation})"))
                    .unwrap_or_default()
            );
        }
        if !workspace.retained_chains.is_empty() {
            println!(
                "{} {}{}",
                style("recovery chains:").dim(),
                workspace.retained_chains.join(", "),
                if workspace.retained_chains_truncated {
                    " (more available via tl git log)"
                } else {
                    ""
                }
            );
        }
    }
    println!(
        "{} {} path(s){}",
        style("local changes:").dim(),
        report.local.changed_paths,
        if report.local.exact {
            ""
        } else {
            " (estimate)"
        },
    );
    for action in &report.next_actions {
        println!("{} {action}", style("next:").green());
    }
}

fn mounted_git_subject(subject: Option<&str>) -> Result<Option<(String, PathBuf, MountState)>> {
    let path = match subject {
        None => Some(mount_containing_cwd()?),
        Some(subject) if positional_is_mount_path(Path::new(subject))? => {
            Some(PathBuf::from(subject))
        }
        Some(_) => None,
    };
    let Some(path) = path else {
        return Ok(None);
    };
    let (mountpoint, state_dir) = state_dir_for(&path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.native_filesystem {
        return Err(CliError::usage(
            "the selected path is a native filesystem attachment, not a Git mount",
        ));
    }
    Ok(Some((mountpoint, state_dir, state)))
}

pub async fn git_log(ctx: &CliContext, subject: Option<&str>, output_json: bool) -> Result<()> {
    let mounted = mounted_git_subject(subject)?;
    if mounted
        .as_ref()
        .is_some_and(|(_, _, state)| state.mount_presence_id.is_some())
    {
        return Err(CliError::usage(
            "this is a read-only repository view and has no workspace snapshot chain; use \
             `tl git smartlog` to inspect repository and workspace positions",
        ));
    }
    let repo = mounted
        .as_ref()
        .map(|(_, _, state)| state.repo.as_str())
        .or(subject)
        .ok_or_else(|| CliError::usage("usage: tl git log [PATH|REPO]"))?;
    let session = FsSession::open(ctx, Some(repo)).await?;
    let (user, token) = session.creds();
    let workspaces = if let Some((_, _, state)) = mounted.as_ref() {
        vec![state.workspace_id.clone()]
    } else {
        let mut ids = BTreeSet::new();
        let mut after = None;
        loop {
            let graph = session
                .client
                .repo_smartlog(
                    &session.project_id,
                    repo,
                    user,
                    token,
                    after.as_deref(),
                    200,
                )
                .await?
                .into_inner();
            ids.extend(graph.nodes.into_iter().filter_map(|node| node.workspace_id));
            if !graph.truncated {
                break;
            }
            let Some(next) = graph.next_after else {
                break;
            };
            after = Some(next);
        }
        ids.into_iter().collect()
    };
    let mut pages = Vec::<GitWorkspaceLogPage>::new();
    for workspace in workspaces {
        let mut after = None;
        loop {
            let page = session
                .client
                .workspace_log(
                    &session.project_id,
                    repo,
                    user,
                    token,
                    &workspace,
                    after.as_deref(),
                    200,
                )
                .await?
                .into_inner();
            let next = page.next_after.clone();
            let done = !page.truncated;
            pages.push(page);
            if done {
                break;
            }
            after = next;
        }
    }
    if output_json {
        println!("{}", serde_json::to_string_pretty(&pages)?);
        return Ok(());
    }
    for page in pages {
        println!("{} {}", style("workspace").bold(), page.workspace_id);
        for entry in page.active_chain {
            println!(
                "  {} {}{}",
                short_id(&entry.oid),
                entry.subject,
                if entry.conflicted {
                    " (conflicted)"
                } else {
                    ""
                },
            );
        }
        for retained in page.retained_chains {
            println!(
                "  {} {} -> {} ({})",
                style("retained").yellow(),
                retained.recovery_ref,
                short_id(&retained.head),
                retained.reason,
            );
        }
    }
    Ok(())
}

pub async fn git_smartlog(
    ctx: &CliContext,
    subject: Option<&str>,
    project: bool,
    output_json: bool,
) -> Result<()> {
    let mounted = if project && subject.is_none() {
        None
    } else {
        mounted_git_subject(subject)?
    };
    let repo = mounted
        .as_ref()
        .map(|(_, _, state)| state.repo.as_str())
        .or(subject);
    let session = FsSession::open(ctx, if project { None } else { repo }).await?;
    let workspace = mounted
        .as_ref()
        .map(|(_, _, state)| state.workspace_id.as_str());
    let (user, token) = session.creds();
    let mut pages = Vec::<GitSmartlogPage>::new();
    let mut after = None;
    loop {
        let page = if project {
            session
                .client
                .project_smartlog(
                    &session.project_id,
                    user,
                    token,
                    repo,
                    workspace,
                    after.as_deref(),
                    200,
                )
                .await?
                .into_inner()
        } else {
            let repo = repo
                .ok_or_else(|| CliError::usage("usage: tl git smartlog [PATH|REPO] [--project]"))?;
            session
                .client
                .repo_smartlog(
                    &session.project_id,
                    repo,
                    user,
                    token,
                    after.as_deref(),
                    200,
                )
                .await?
                .into_inner()
        };
        let next = page.next_after.clone();
        let done = !page.truncated;
        pages.push(page);
        if done {
            break;
        }
        after = next;
    }
    if output_json {
        println!("{}", serde_json::to_string_pretty(&pages)?);
        return Ok(());
    }
    for page in pages {
        if let Some(repo) = page.repo.as_deref() {
            println!("{} {repo}", style("repository").bold());
        }
        for node in &page.nodes {
            println!(
                "{} {:<10} {}{}{}",
                node.oid.as_deref().map(short_id).unwrap_or("-"),
                node.kind,
                node.repo
                    .as_deref()
                    .map(|repo| format!("[{repo}] "))
                    .unwrap_or_default(),
                node.label.as_deref().unwrap_or(&node.id),
                node.state
                    .as_deref()
                    .map(|state| format!(" [{state}]"))
                    .unwrap_or_default(),
            );
        }
        for edge in &page.edges {
            println!("  {} {} -> {}", style(&edge.kind).dim(), edge.from, edge.to,);
        }
    }
    Ok(())
}

/// Classify only errors for which retrying the same durable operation cannot make progress.
fn native_operation_error_is_permanent(error: &tensorlake::error::SdkError) -> bool {
    use tensorlake::error::SdkError;
    match error {
        SdkError::ServerError { status, .. } => {
            status.is_client_error()
                && status.as_u16() != 408
                && status.as_u16() != 425
                && status.as_u16() != 429
        }
        SdkError::ClientError(message) => {
            let message = message.to_ascii_lowercase();
            message.contains("filesystem changed to")
                || message.contains("verification rejected")
                || message.contains("invalid")
                || message.contains("unknown native verification state")
        }
        SdkError::Json(_) | SdkError::JsonWithError(_) => true,
        _ => false,
    }
}

struct DurableNativePublish {
    report: NativePushReport,
    candidate: NativePreparedSnapshotCandidate,
    request: local_state::PublishRequest,
    publish_ms: u64,
}

/// One publication/rebase/dead-letter loop shared by mounted and tracked-directory saves.
///
/// Preparation remains workflow-specific because a managed overlay has an authoritative mutation
/// journal while an unmanaged directory requires reconciliation. From a prepared immutable root
/// onward, however, both products use this exact durable CAS state machine.
#[allow(clippy::too_many_arguments)]
async fn publish_durable_native_candidate(
    client: &ArtifactStorageClient,
    project_id: &str,
    filesystem: &str,
    store: &impl local_state::LocalSnapshotStore,
    generation: u64,
    source_fingerprint: &str,
    mut candidate: NativePreparedSnapshotCandidate,
    mut request: local_state::PublishRequest,
    username: &str,
    token: &str,
    workspace_id: Option<String>,
    progress: Option<NativePushProgress>,
    encode_candidate: impl Fn(&NativePreparedSnapshotCandidate) -> Result<Vec<u8>>,
) -> Result<DurableNativePublish> {
    let started = std::time::Instant::now();
    loop {
        let outcome = match client
            .publish_native_snapshot_candidate_outcome_with_credential(
                project_id,
                filesystem,
                candidate.clone(),
                username,
                token,
                NativePushOptions {
                    message: request.message.clone(),
                    expected_snapshot_id: candidate.base_snapshot_id.clone(),
                    workspace_id: workspace_id.clone(),
                    operation_id: Some(request.publish_operation_id.clone()),
                    progress: progress.clone(),
                },
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if native_operation_error_is_permanent(&error) => {
                let reason = error.to_string();
                store
                    .fail_publish_request(&request.request_id, &reason)
                    .map_err(|record_error| {
                        CliError::usage(format!(
                            "native snapshot publication failed permanently ({error}), and \
                             recording its durable failure failed: {record_error}"
                        ))
                    })?;
                return Err(CliError::usage(format!(
                    "native snapshot publication failed permanently and was dead-lettered: \
                     {error}"
                )));
            }
            Err(error) => {
                return Err(CliError::usage(format!(
                    "native snapshot publication failed (will retry idempotently as {}): {error}",
                    request.request_id
                )));
            }
        };
        match outcome {
            tensorlake::artifact_storage::native_fs::NativeCandidatePublishOutcome::Published(
                report,
            ) => {
                store
                    .mark_published(generation, &report.snapshot_id)
                    .map_err(|error| {
                        CliError::usage(format!(
                            "recording published native snapshot failed closed: {error}"
                        ))
                    })?;
                return Ok(DurableNativePublish {
                    report,
                    candidate,
                    request,
                    publish_ms: started.elapsed().as_millis() as u64,
                });
            }
            tensorlake::artifact_storage::native_fs::NativeCandidatePublishOutcome::Conflict {
                snapshot_id,
                actual_snapshot_id: Some(actual),
                ..
            } => {
                tracing::info!(
                    generation,
                    losing_snapshot = %snapshot_id,
                    serialized_winner = %actual,
                    "rebasing exact native change set after publish CAS conflict"
                );
                candidate = if candidate.is_rebaseable() {
                    client
                        .rebase_native_snapshot_candidate_with_credential(
                            project_id, filesystem, &candidate, &actual, username, token,
                        )
                        .await
                        .map_err(|error| {
                            CliError::usage(format!(
                                "native change-set rebase onto {actual} failed: {error}"
                            ))
                        })?
                } else {
                    // The unavoidable first full import already uploaded a complete immutable
                    // root. Serializing it after a concurrently-created head is a metadata-only
                    // last-writer-wins replacement; source bytes are never reopened.
                    candidate.base_snapshot_id = Some(actual.clone());
                    candidate.clone()
                };
                let next_publish_operation_id = next_native_publish_operation_id(
                    &request.request_id,
                    request.publish_attempt.saturating_add(1),
                    &actual,
                    candidate.root_id(),
                );
                request = store
                    .replace_prepared_for_rebase(
                        local_state::PreparedGeneration::new(
                            generation,
                            Some(actual),
                            candidate.root_id(),
                            source_fingerprint,
                            encode_candidate(&candidate)?,
                        ),
                        &next_publish_operation_id,
                    )
                    .map_err(|error| {
                        CliError::usage(format!(
                            "persisting rebased native candidate failed closed: {error}"
                        ))
                    })?;
                let backoff_ms = 25u64.saturating_mul(1u64 << request.publish_attempt.min(4));
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
            tensorlake::artifact_storage::native_fs::NativeCandidatePublishOutcome::Conflict {
                snapshot_id,
                actual_snapshot_id: None,
                ..
            } => {
                store
                    .fail_publish_request(
                        &request.request_id,
                        "publish CAS lost to an empty head; no safe rebase base exists",
                    )
                    .map_err(|error| {
                        CliError::usage(format!(
                            "recording permanent empty-head publish conflict: {error}"
                        ))
                    })?;
                return Err(CliError::usage(format!(
                    "native snapshot {snapshot_id} lost publication to an empty head; refusing \
                     to guess a rebase base (request dead-lettered)"
                )));
            }
        }
    }
}

pub async fn restore(
    ctx: &CliContext,
    path: &Path,
    version: &str,
    discard_local: bool,
) -> Result<()> {
    if let Some(attachment) = tracked_directory_for(path)? {
        return Err(CliError::usage(format!(
            "restore does not replace an ordinary tracked directory in place. Mount the earlier \
             save at another path instead:\n  tl fs mount {}:{} <path>",
            attachment.filesystem_id, version
        )));
    }
    if let Some((root, _)) = plaindir::binding_for_lenient(path) {
        return Err(CliError::usage(format!(
            "{root} uses the removed pre-release Git-backed directory binding. Stop tracking it \
             with `tl fs unmount {root}`, then attach the native engine with `tl fs push {root} \
             <filesystem>`."
        )));
    }
    let (mountpoint, state_dir) = state_dir_for(path)?;
    let state = daemon::load_mount_state(&state_dir)?;
    if state.read_only() {
        if state.native_filesystem {
            return Err(CliError::usage(
                "this is already a read-only filesystem view; restore a writable mount instead",
            ));
        }
        return Err(CliError::usage(format!(
            "this is a read-only mount following {}; there is nothing to restore",
            state.follow_ref.as_deref().unwrap_or("the branch"),
        )));
    }
    // Restore's point of no return drops the ENTIRE overlay. Retained sealed content drops
    // loss-free (the restore refills the view it wants anyway); unsealed changes and ignored
    // files are truly losable and need the explicit flag.
    if !discard_local && let Some(losable) = overlay_losable_state(&state_dir, &mountpoint).await? {
        return Err(CliError::usage(format!(
            "the workspace holds {losable}; restoring would destroy that. Seal unsealed \
             changes first (`tl fs snapshot {path}`), or drop everything local (ignored \
             files included) with the flag:\n  tl fs restore --discard {path} \
             {version}",
            path = path.display(),
        )));
    }
    let session = FsSession::open(ctx, Some(&state.repo)).await?;
    heartbeat(&session, &state).await?;

    // Drop retained byte-cache entries while the workspace is proven clean. Once the durable
    // restore operation begins below, the local journal rejects every new mutation until the
    // daemon has observed the new lower snapshot and adopted it transactionally.
    if state.native_filesystem && !discard_local {
        let trim = daemon::control(&state_dir, "trim").await?;
        let trim: daemon::TrimReply = serde_json::from_value(trim).map_err(|error| {
            CliError::usage(format!(
                "the daemon's restore preflight did not parse: {error}"
            ))
        })?;
        if !trim.held_open.is_empty() {
            return Err(CliError::usage(format!(
                "{} retained file(s) could not be released before restore (first: {}). Close the \
                 process holding the file and retry; the server was not changed.",
                trim.held_open.len(),
                trim.held_open[0],
            )));
        }
    }

    // The gate's answer ages badly while resolving the target. Fingerprint after the retained
    // cache trim, then re-check immediately before entering the durable restore fence.
    let local_baseline = if discard_local {
        None
    } else {
        Some(overlay_fingerprint(&state_dir)?)
    };

    let lower = daemon::control(&state_dir, "ping")
        .await?
        .get("commit")
        .and_then(|c| c.as_str().map(str::to_string))
        .ok_or_else(|| CliError::usage("daemon did not report a commit"))?;
    if state.native_filesystem {
        let (user, token) = session.creds();
        let target = session
            .client
            .resolve_native_snapshot_id_with_credential(
                &session.project_id,
                &state.repo,
                version,
                user,
                token,
            )
            .await?;
        // Catch writes that landed while resolving a short history id before changing the
        // server. The CAS below independently catches concurrent filesystem publication.
        if let Some(baseline) = local_baseline.as_ref()
            && overlay_fingerprint(&state_dir)? != *baseline
        {
            return Err(CliError::usage(format!(
                "local changes landed while the restore was preparing; nothing was changed. \
                 Seal them first (`tl fs snapshot {path}`) and retry, or use --discard.",
                path = path.display(),
            )));
        }
        let operation = daemon::control_with(
            &state_dir,
            "begin_native_restore",
            serde_json::json!({
                "target": target,
                "discard_local": discard_local,
            }),
        )
        .await?;
        let operation: local_state::RestoreOperation =
            serde_json::from_value(operation).map_err(|error| {
                CliError::usage(format!(
                    "the daemon's durable restore operation did not parse: {error}"
                ))
            })?;
        let restored = match operation.completed_snapshot_id.clone() {
            Some(snapshot_id) => snapshot_id,
            None => {
                match session
                    .client
                    .restore_native_snapshot_with_credential(
                        &session.project_id,
                        &state.repo,
                        &state.workspace_id,
                        &operation.target_snapshot_id,
                        &operation.expected_snapshot_id,
                        &operation.request_id,
                        operation.created_at_ms,
                        user,
                        token,
                    )
                    .await
                {
                    Ok(snapshot_id) => snapshot_id,
                    Err(error) => {
                        if native_operation_error_is_permanent(&error) {
                            let reason = error.to_string();
                            daemon::control_with(
                                &state_dir,
                                "fail_native_restore",
                                serde_json::json!({
                                    "request_id": operation.request_id,
                                    "reason": reason,
                                }),
                            )
                            .await
                            .map_err(|dead_letter_error| {
                                CliError::usage(format!(
                                    "restore failed permanently ({error}), and recording the \
                                     durable failure also failed ({dead_letter_error}); the mount \
                                     remains write-fenced"
                                ))
                            })?;
                            return Err(CliError::usage(format!(
                                "restore failed permanently and was dead-lettered: {error}. The \
                                 mount is writable again; rerun restore to retry from the current \
                                 filesystem head"
                            )));
                        }
                        return Err(error.into());
                    }
                }
            }
        };
        if !operation.locally_adopted {
            daemon::control_with(
                &state_dir,
                "adopt_native_restore",
                serde_json::json!({
                    "request_id": operation.request_id,
                    "snapshot": restored.clone(),
                }),
            )
            .await?;
        }
        println!(
            "Restored {} to {} as save {} (no file bytes transferred).",
            path.display(),
            short_id(&target),
            short_id(&restored),
        );
        if let Err(error) = daemon::control_with(
            &state_dir,
            "ack-restore",
            serde_json::json!({ "request_id": operation.request_id }),
        )
        .await
        {
            eprintln!(
                "warning: restore succeeded but its local response receipt could not be \
                 acknowledged: {error}"
            );
        }
        return Ok(());
    }
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
    // Last check before the destructive clear: anything written through the mount during
    // the fetch phase above is unsealed state the gate never saw. Aborting here loses
    // nothing — everything fetched is local, nothing was changed yet.
    if let Some(baseline) = local_baseline
        && overlay_fingerprint(&state_dir)? != baseline
    {
        return Err(CliError::usage(format!(
            "local changes landed while the restore was preparing; nothing was changed. \
             Seal them first (`tl fs snapshot {path}`) and re-run — or re-run with \
             --discard to drop them. (Background seal housekeeping can also move the \
             overlay; if you made no local writes, simply retry.)",
            path = path.display(),
        )));
    }
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
pub(crate) fn write_whiteout(wh: &Path, rel: &str) -> Result<()> {
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
    if state.native_filesystem {
        let (user, token) = session.creds();
        session
            .client
            .native_workspace_heartbeat_with_credential(
                &session.project_id,
                &state.repo,
                &state.workspace_id,
                user,
                token,
            )
            .await?;
        return Ok(());
    }
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

    fn doctor_mount_state(state_dir: &Path) -> MountState {
        MountState {
            project_id: "project-1".to_string(),
            organization_id: None,
            owner_uid: None,
            owner_gid: None,
            repo: "filesystem-1".to_string(),
            subtree: None,
            git_mount_source: None,
            mount_presence_id: None,
            native_filesystem: true,
            pinned_snapshot: None,
            workspace_id: "workspace-1".to_string(),
            local_state_uuid: Some("store-1".to_string()),
            ref_name: "refs/workspaces/workspace-1".to_string(),
            mountpoint: state_dir.join("mount"),
            follow_ref: None,
            read_only: Some(false),
            auto_commit_interval_secs: Some(FS_AUTOSAVE_DEFAULT_SECS),
            start_oid: None,
        }
    }

    #[test]
    fn git_mount_target_parses_subtree_without_changing_source() {
        assert_eq!(
            parse_git_mount_target("monorepo:main//services/auth").unwrap(),
            (
                "monorepo:main".to_string(),
                Some("services/auth".to_string())
            )
        );
        assert_eq!(
            parse_git_mount_target("monorepo//services/auth").unwrap(),
            ("monorepo".to_string(), Some("services/auth".to_string()))
        );
        assert_eq!(
            parse_git_mount_target("monorepo:main").unwrap(),
            ("monorepo:main".to_string(), None)
        );
    }

    #[test]
    fn git_mount_target_rejects_noncanonical_subtrees() {
        for target in [
            "repo//",
            "repo///absolute",
            "repo//path/",
            "repo//a//b",
            "repo//a/./b",
            "repo//a/../b",
        ] {
            assert!(parse_git_mount_target(target).is_err(), "{target:?}");
        }
    }

    #[test]
    fn git_mount_follow_policy_is_explicit_and_namespace_safe() {
        let source = |kind: &str, policy: &str, canonical_ref: Option<&str>| GitMountSource {
            format_ver: 1,
            kind: kind.to_string(),
            follow_policy: policy.to_string(),
            canonical_ref: canonical_ref.map(str::to_string),
            resolved_commit: "1".repeat(40),
            subtree: None,
            root_tree: "2".repeat(40),
        };
        assert_eq!(
            git_mount_follow_ref(&source("branch", "follow", Some("refs/heads/main"))).unwrap(),
            Some("refs/heads/main".to_string())
        );
        assert_eq!(
            git_mount_follow_ref(&source("tag", "follow", Some("refs/tags/v1"))).unwrap(),
            Some("refs/tags/v1".to_string())
        );
        assert_eq!(
            git_mount_follow_ref(&source("commit", "pinned", None)).unwrap(),
            None
        );
        for invalid in [
            source("tag", "follow", Some("refs/heads/v1")),
            source("branch", "pinned", Some("refs/heads/main")),
            source("commit", "follow", None),
            source("commit", "future", None),
        ] {
            assert!(git_mount_follow_ref(&invalid).is_err());
        }
    }

    #[test]
    fn native_local_state_uuid_is_fresh_then_reused_on_resume() {
        assert!(
            local_state_uuid_for_mount(false, false, None).is_none(),
            "repository mounts do not own the native snapshot journal"
        );
        assert!(
            local_state_uuid_for_mount(true, true, None).is_none(),
            "read-only native mounts have no mutation journal"
        );
        let fresh = local_state_uuid_for_mount(true, false, None).unwrap();
        assert!(!fresh.is_empty());

        let temp = tempfile::tempdir().unwrap();
        let mut state = doctor_mount_state(temp.path());
        state.local_state_uuid = Some("persisted-store".to_string());
        daemon::save_mount_state(temp.path(), &state).unwrap();
        assert_eq!(
            local_state_uuid_for_mount(true, false, Some(temp.path())).as_deref(),
            Some("persisted-store")
        );
    }

    #[test]
    fn doctor_reports_durable_lifecycle_without_overlay_walk() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().to_path_buf();
        let state = doctor_mount_state(&state_dir);
        let identity = local_state::LocalStateIdentity {
            project_id: state.project_id.clone(),
            filesystem: state.repo.clone(),
            workspace_id: state.workspace_id.clone(),
            store_uuid: state.local_state_uuid.clone().unwrap(),
        };
        let store =
            local_state::LocalState::open(state_dir.join(local_state::LOCAL_STATE_FILE), identity)
                .unwrap();
        store
            .set_base_snapshot(Some("snapshot-0".to_string()))
            .unwrap();
        store.record_upsert("dirty.txt", 7).unwrap();
        let generation = store.freeze_current().unwrap().unwrap().generation;
        store
            .mark_prepared(local_state::PreparedGeneration::new(
                generation,
                Some("snapshot-0".to_string()),
                "root-1",
                "source-1",
                vec![1, 2, 3],
            ))
            .unwrap();
        store
            .put_publish_request(local_state::PublishRequest::new(
                "publish-1",
                generation,
                "save",
                false,
                123,
            ))
            .unwrap();
        store.mark_published(generation, "snapshot-1").unwrap();
        let live_lifecycle = local_state_doctor_lifecycle(&store).unwrap();
        assert_eq!(live_lifecycle.active_generation, 2);
        let wire = serde_json::to_value(&live_lifecycle).unwrap();
        let decoded: LocalStateDoctorLifecycle = serde_json::from_value(wire).unwrap();
        assert_eq!(decoded.generations[0].state, "published");
        drop(store);

        let report = inspect_local_state("/mnt/fs".to_string(), state_dir.clone(), &state).unwrap();
        assert_eq!(report.status, "healthy");
        assert_eq!(report.lifecycle.active_generation, 2);
        assert_eq!(report.lifecycle.identity.store_uuid, "store-1");
        let published = report
            .lifecycle
            .generations
            .iter()
            .find(|record| record.generation == generation)
            .unwrap();
        assert_eq!(published.state, "published");
        assert_eq!(published.dirty_paths, 1);
        assert_eq!(published.prepared.as_ref().unwrap().root_id, "root-1");
        assert_eq!(published.publish_requests[0].request_id, "publish-1");
        assert_eq!(published.published_snapshot.as_deref(), Some("snapshot-1"));

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["generations"][0]["dirty_paths"], 1);
    }

    #[test]
    fn doctor_fails_closed_on_missing_or_identity_mismatched_state() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().to_path_buf();
        let state = doctor_mount_state(&state_dir);
        let missing =
            inspect_local_state("/mnt/fs".to_string(), state_dir.clone(), &state).unwrap_err();
        assert!(missing.to_string().contains("is missing"));

        let actual_identity = local_state::LocalStateIdentity {
            project_id: state.project_id.clone(),
            filesystem: state.repo.clone(),
            workspace_id: state.workspace_id.clone(),
            store_uuid: "different-store".to_string(),
        };
        drop(
            local_state::LocalState::open(
                state_dir.join(local_state::LOCAL_STATE_FILE),
                actual_identity,
            )
            .unwrap(),
        );
        let mismatch = inspect_local_state("/mnt/fs".to_string(), state_dir, &state).unwrap_err();
        assert!(mismatch.to_string().contains("different mount"));
    }

    #[test]
    fn doctor_resolves_detached_mountpoint_or_explicit_state_directory() {
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().join("managed-mounts");
        let state_dir = state_root.join("workspace-1");
        let mountpoint = temp.path().join("former-mount");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::create_dir_all(&mountpoint).unwrap();
        let state = doctor_mount_state(&state_dir);
        let state = MountState {
            mountpoint: mountpoint.clone(),
            ..state
        };
        daemon::save_mount_state(&state_dir, &state).unwrap();

        let (resolved_mountpoint, resolved_state) =
            doctor_state_dir_for_in(&mountpoint, &state_root).unwrap();
        assert_eq!(
            resolved_mountpoint,
            canonical_mountpoint(&mountpoint).unwrap()
        );
        assert_eq!(resolved_state, state_dir);

        let (_, explicit_state) = doctor_state_dir_for_in(&resolved_state, &state_root).unwrap();
        assert_eq!(explicit_state, resolved_state.canonicalize().unwrap());

        let duplicate = state_root.join("workspace-2");
        std::fs::create_dir_all(&duplicate).unwrap();
        daemon::save_mount_state(&duplicate, &state).unwrap();
        let ambiguous = doctor_state_dir_for_in(&mountpoint, &state_root).unwrap_err();
        assert!(ambiguous.to_string().contains("multiple detached"));
    }

    #[test]
    fn doctor_repair_exports_original_and_rebuilds_one_all_dirty_generation() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().to_path_buf();
        let mut state = doctor_mount_state(&state_dir);
        state.start_oid = Some("snapshot-attached".to_string());
        std::fs::create_dir_all(&state.mountpoint).unwrap();
        daemon::save_mount_state(&state_dir, &state).unwrap();
        let identity =
            local_state_doctor_identity(state.mountpoint.to_str().unwrap(), &state).unwrap();
        let original = local_state::LocalState::open(
            state_dir.join(local_state::LOCAL_STATE_FILE),
            identity.clone(),
        )
        .unwrap();
        original
            .import_legacy_once(local_state::LegacyImport {
                base_snapshot: Some("snapshot-latest".to_string()),
                mutations: vec![local_state::LegacyMutation::Upsert {
                    path: "old-dirty.txt".to_string(),
                    min_write_offset: 0,
                }],
            })
            .unwrap();
        drop(original);
        let original_database =
            std::fs::read(state_dir.join(local_state::LOCAL_STATE_FILE)).unwrap();

        std::fs::create_dir_all(state_dir.join("upper/empty")).unwrap();
        std::fs::write(state_dir.join("upper/file.txt"), b"new bytes").unwrap();
        std::fs::create_dir_all(state_dir.join("wh")).unwrap();
        std::fs::write(state_dir.join("wh/deleted.txt"), b"").unwrap();
        // Same-path upper recreation wins over a stale whiteout.
        std::fs::write(state_dir.join("wh/file.txt"), b"").unwrap();
        std::fs::write(
            state_dir.join("redirects.json"),
            serde_json::to_vec(&serde_json::json!({"renamed": "original"})).unwrap(),
        )
        .unwrap();
        std::fs::create_dir_all(state_dir.join("staging/generations/9")).unwrap();
        std::fs::write(
            state_dir.join("staging/generations/9/evidence"),
            b"prepared bytes",
        )
        .unwrap();
        std::fs::write(
            state_dir.join(LOCAL_STATE_REPAIR_MARKER),
            b"prior interrupted repair evidence",
        )
        .unwrap();

        let report = repair_local_state_journal(
            state.mountpoint.to_string_lossy().into_owned(),
            state_dir.clone(),
            &state,
            None,
        )
        .unwrap();
        assert_eq!(report.status, "repaired");
        assert_eq!(report.base_snapshot.as_deref(), Some("snapshot-latest"));
        assert_eq!(
            std::fs::read(report.backup_path.join(local_state::LOCAL_STATE_FILE)).unwrap(),
            original_database
        );
        assert_eq!(
            std::fs::read(report.backup_path.join("staging/generations/9/evidence")).unwrap(),
            b"prepared bytes"
        );
        assert_eq!(
            std::fs::read(report.backup_path.join(LOCAL_STATE_REPAIR_MARKER)).unwrap(),
            b"prior interrupted repair evidence"
        );
        assert!(report.backup_path.join("state.json").is_file());
        assert!(report.backup_path.join("redirects.json").is_file());
        assert!(!state_dir.join(LOCAL_STATE_REPAIR_MARKER).exists());
        assert!(!state_dir.join("staging").exists());

        let repaired = local_state::LocalState::open_existing(
            state_dir.join(local_state::LOCAL_STATE_FILE),
            identity,
        )
        .unwrap();
        let generations = repaired.generations().unwrap();
        assert_eq!(generations.len(), 1);
        assert_eq!(generations[0].generation, 1);
        assert_eq!(generations[0].state, local_state::GenerationState::Open);
        assert_eq!(
            generations[0].base_snapshot.as_deref(),
            Some("snapshot-latest")
        );
        let recovery = repaired.recovery_dirty_state().unwrap();
        let paths: std::collections::BTreeMap<_, _> = recovery
            .paths
            .into_iter()
            .map(|path| (path.path, path.kind))
            .collect();
        assert_eq!(paths.get("empty"), Some(&local_state::DirtyKind::Upsert));
        assert_eq!(paths.get("file.txt"), Some(&local_state::DirtyKind::Upsert));
        assert_eq!(
            paths.get("deleted.txt"),
            Some(&local_state::DirtyKind::Delete)
        );
        assert_eq!(paths.get("original"), Some(&local_state::DirtyKind::Delete));
        assert_eq!(paths.get("renamed"), Some(&local_state::DirtyKind::Upsert));
        assert_eq!(recovery.renames.len(), 1);
        assert_eq!(recovery.renames[0].from, "original");
        assert_eq!(recovery.renames[0].to, "renamed");
    }

    #[test]
    fn doctor_repair_recovers_corrupt_database_without_server_state() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().to_path_buf();
        let mut state = doctor_mount_state(&state_dir);
        state.start_oid = Some("snapshot-attach-fallback".to_string());
        std::fs::create_dir_all(&state.mountpoint).unwrap();
        daemon::save_mount_state(&state_dir, &state).unwrap();
        std::fs::create_dir_all(state_dir.join("upper")).unwrap();
        std::fs::write(state_dir.join("upper/survives.txt"), b"local").unwrap();
        std::fs::write(
            state_dir.join(local_state::LOCAL_STATE_FILE),
            b"corrupt-original",
        )
        .unwrap();

        let report = repair_local_state_journal(
            state.mountpoint.to_string_lossy().into_owned(),
            state_dir.clone(),
            &state,
            Some("snapshot-attach-fallback"),
        )
        .unwrap();
        assert_eq!(
            std::fs::read(report.backup_path.join(local_state::LOCAL_STATE_FILE)).unwrap(),
            b"corrupt-original"
        );
        assert_eq!(
            report.base_snapshot.as_deref(),
            Some("snapshot-attach-fallback")
        );
        let repaired = local_state::LocalState::open_existing(
            state_dir.join(local_state::LOCAL_STATE_FILE),
            local_state_doctor_identity(state.mountpoint.to_str().unwrap(), &state).unwrap(),
        )
        .unwrap();
        assert_eq!(
            repaired.recovery_dirty_state().unwrap().paths[0].path,
            "survives.txt"
        );
    }

    #[test]
    fn doctor_repair_refuses_live_writer_and_corrupt_redirects_fail_before_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().to_path_buf();
        let state = doctor_mount_state(&state_dir);
        std::fs::create_dir_all(&state.mountpoint).unwrap();
        daemon::save_mount_state(&state_dir, &state).unwrap();

        let writer = try_local_state_writer_lock(&state_dir).unwrap().unwrap();
        let live_writer = repair_local_state_journal(
            state.mountpoint.to_string_lossy().into_owned(),
            state_dir.clone(),
            &state,
            None,
        )
        .unwrap_err();
        assert!(live_writer.to_string().contains("live writer"));
        assert!(!state_dir.join("repair-backups").exists());
        drop(writer);

        std::fs::create_dir_all(state_dir.join("upper")).unwrap();
        std::fs::write(state_dir.join("redirects.json"), b"{").unwrap();
        let corrupt = repair_local_state_journal(
            state.mountpoint.to_string_lossy().into_owned(),
            state_dir.clone(),
            &state,
            Some("empty"),
        )
        .unwrap_err();
        assert!(corrupt.to_string().contains("redirect state"));
        assert!(!state_dir.join("repair-backups").exists());
        assert!(!state_dir.join(local_state::LOCAL_STATE_FILE).exists());
    }

    #[test]
    fn fleet_item_to_info_carries_real_lease_state() {
        let item: WorkspaceFleetItem = serde_json::from_value(serde_json::json!({
            "id": "aa", "repo": "p1/demo", "status": "detached", "mode": "default",
            "base": "1111111111111111111111111111111111111111",
            "head": "1111111111111111111111111111111111111111",
            "created_at_secs": 1,
            "snapshot_count": 0,
        }))
        .unwrap();
        // Durable workspace (no lease on the wire, the modern norm): pinned.
        let info = fleet_item_to_info(&item);
        assert!(info.pinned);
        assert_eq!(info.lease_due_ms, None);
        assert_eq!(info.ref_name, "refs/workspaces/aa");
        assert_eq!(info.principal, "", "unbound: created_by omitted");
        // Legacy leased workspace: the wire lease surfaces, not a fabricated constant.
        let leased = WorkspaceFleetItem {
            lease_secs: 3600,
            lease_due_ms: Some(4_102_444_800_000),
            ..item
        };
        let info = fleet_item_to_info(&leased);
        assert!(!info.pinned);
        assert_eq!(info.lease_due_ms, Some(4_102_444_800_000));
        assert_eq!(info.lease_secs, 3600);
    }

    #[test]
    fn token_subject_reads_jwt_sub_and_rejects_opaque_tokens() {
        use base64::Engine;
        let enc = |v: &serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string())
        };
        let header = enc(&serde_json::json!({"alg": "ES256", "typ": "JWT"}));
        let claims = enc(&serde_json::json!({"sub": "user:u1", "proj": "p1"}));
        let token = format!("{header}.{claims}.not-a-real-signature");
        assert_eq!(token_subject(&token).as_deref(), Some("user:u1"));
        // Opaque dev tokens (TENSORLAKE_GIT_TOKEN) and malformed payloads yield None — the
        // listing then skips client-side principal filtering, matching open-mode servers.
        assert_eq!(token_subject("just-a-pat"), None);
        assert_eq!(token_subject("a.b.c"), None);
        let no_sub = format!("{header}.{}.sig", enc(&serde_json::json!({"proj": "p1"})));
        assert_eq!(token_subject(&no_sub), None);
    }

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

    #[test]
    fn snapshot_enumeration_has_no_implicit_exclusions() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let upper = state.path().join("upper");
        std::fs::create_dir_all(state.path().join("wh")).unwrap();

        let paths = [
            ".git/HEAD",
            "node_modules/pkg.js",
            "target/debug/build.o",
            "dist/app.js",
            ".cache/entry",
            "__pycache__/module.pyc",
            ".DS_Store",
            "._index",
            ".tlignore",
        ];
        for path in paths {
            let abs = upper.join(path);
            std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
            std::fs::write(abs, "snapshot me").unwrap();
        }
        // `.tlignore` is an ordinary tracked file now and has no exclusion semantics.
        std::fs::write(mount.path().join(".tlignore"), "target\n").unwrap();

        let (upserts, deletes) = enumerate_overlay(state.path(), mount.path()).unwrap();
        let upsert_paths: Vec<_> = upserts.iter().map(|(path, _, _)| path.as_str()).collect();
        assert_eq!(
            upsert_paths,
            vec![
                ".DS_Store",
                "._index",
                ".cache/entry",
                ".git/HEAD",
                ".tlignore",
                "__pycache__/module.pyc",
                "dist/app.js",
                "node_modules/pkg.js",
                "target/debug/build.o",
            ]
        );
        assert!(deletes.is_empty());
    }

    #[test]
    fn snapshot_enumeration_excludes_former_builtins_when_gitignored() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let upper = state.path().join("upper");
        std::fs::create_dir_all(upper.join("target/debug")).unwrap();
        std::fs::create_dir_all(state.path().join("wh")).unwrap();
        std::fs::write(mount.path().join(".gitignore"), "target/\n").unwrap();
        std::fs::write(upper.join("target/debug/build.o"), "ignored by git").unwrap();

        let (upserts, deletes) = enumerate_overlay(state.path(), mount.path()).unwrap();
        assert!(upserts.is_empty());
        assert!(deletes.is_empty());
    }

    /// A fake daemon control endpoint: records the op sequence and replies `{"ok":true}` to
    /// every op except `seal`, which replies like a real sealer that minted a commit — so the
    /// snapshot control flow can be exercised without a mount. `seal` honors the new framing:
    /// each string in `events` is written as an `{"event": ...}` progress line before the
    /// final reply (pass none for the plain single-line reply older tests exercise), and a
    /// `clear:true` request gets a `cleared` list back.
    #[cfg(unix)]
    fn fake_daemon_with_events(
        state_dir: &Path,
        events: Vec<String>,
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
        fake_daemon_with_events(state_dir, Vec::new())
    }

    #[cfg(unix)]
    fn fake_daemon_drops_first_seal_response(
        state_dir: &Path,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<String>>> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let request_ids = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = std::os::unix::net::UnixListener::bind(daemon::control_socket(state_dir))
            .expect("bind control socket");
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::UnixListener::from_std(listener).unwrap();
        let recorded = request_ids.clone();
        tokio::spawn(async move {
            let mut first = true;
            while let Ok((stream, _)) = listener.accept().await {
                let mut reader = tokio::io::BufReader::new(stream);
                let mut line = String::new();
                if reader.read_line(&mut line).await.is_err() {
                    continue;
                }
                let request: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                recorded.lock().unwrap().push(
                    request["request_id"]
                        .as_str()
                        .expect("seal request id")
                        .to_string(),
                );
                if first {
                    first = false;
                    continue;
                }
                let response = serde_json::json!({
                    "ok": true,
                    "clean": false,
                    "commit": "recovered-snapshot",
                    "files": 1,
                    "chunks_uploaded": 0,
                    "chunks_total": 0,
                    "sealed": ["keep.txt"],
                    "push_ms": 0,
                });
                let mut stream = reader.into_inner();
                let _ = stream.write_all(format!("{response}\n").as_bytes()).await;
            }
        });
        request_ids
    }

    /// A fake daemon that answers each op with a canned reply (`{"ok":true}` for ops not in
    /// the map) — for exercising the CLI side of non-streaming ops (`dirty`, `trim`).
    #[cfg(unix)]
    fn fake_daemon_with_replies(
        state_dir: &Path,
        replies: std::collections::HashMap<String, serde_json::Value>,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<String>>> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let ops = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = std::os::unix::net::UnixListener::bind(daemon::control_socket(state_dir))
            .expect("bind control socket");
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::UnixListener::from_std(listener).unwrap();
        let recorded = ops.clone();
        let replies = std::sync::Arc::new(replies);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let recorded = recorded.clone();
                let replies = replies.clone();
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.is_err() {
                        return;
                    }
                    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                    let op = v["op"].as_str().unwrap_or_default().to_string();
                    recorded.lock().unwrap().push(op.clone());
                    let resp = replies
                        .get(&op)
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({ "ok": true }));
                    let mut stream = reader.into_inner();
                    let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
                });
            }
        });
        ops
    }

    /// The truthful-status contract: when the daemon answers the `dirty` op, its answer IS the
    /// dirty set — retained (sealed-and-kept) and ignored upper files are accounted
    /// separately instead of over-reporting as dirt (the #834 regression this replaces).
    #[tokio::test]
    #[cfg(unix)]
    async fn local_changes_prefers_the_daemon_dirty_view() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join(".gitignore"), "*.tmp\n").unwrap();
        for (rel, content) in [
            ("dirty.txt", "unsealed"),
            ("retained.txt", "sealed-and-kept"),
            ("junk.tmp", "ignored"),
        ] {
            let abs = state.path().join("upper").join(rel);
            std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
            std::fs::write(abs, content).unwrap();
        }
        let _ops = fake_daemon_with_replies(
            state.path(),
            [(
                "dirty".to_string(),
                serde_json::json!({
                    "ok": true,
                    "upserts": ["dirty.txt"],
                    "deletes": [],
                    "renames": [],
                    "commit": "cafe0000",
                }),
            )]
            .into_iter()
            .collect(),
        );

        let changes = local_changes(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap();
        assert!(changes.exact, "the daemon answered; the view is exact");
        assert_eq!(changes.upserts, vec!["dirty.txt"]);
        assert_eq!(changes.dirty(), 1);
        assert_eq!(
            changes.retained, 1,
            "the sealed-and-kept file is retained, not dirt"
        );
        assert_eq!(changes.ignored, 1, "the ignored file is counted separately");
    }

    /// Daemon down: the raw walk still answers (labeled inexact) so status never goes dark —
    /// it just can't tell retained content from dirt.
    #[tokio::test]
    #[cfg(unix)]
    async fn local_changes_falls_back_to_the_raw_walk_without_a_daemon() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        for rel in ["a.txt", "b.txt"] {
            let abs = state.path().join("upper").join(rel);
            std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
            std::fs::write(abs, rel).unwrap();
        }
        let changes = local_changes(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap();
        assert!(!changes.exact, "no daemon: the view is the raw walk");
        assert_eq!(changes.upserts, vec!["a.txt", "b.txt"]);
        assert_eq!(changes.retained, 0);
        assert_eq!(changes.ignored, 0);
    }

    fn clean_dirty_reply() -> serde_json::Value {
        serde_json::json!({
            "ok": true, "upserts": [], "deletes": [], "renames": [], "commit": "cafe0000",
        })
    }

    fn upper_file(state: &Path, rel: &str, content: &str) -> std::fs::Metadata {
        let abs = state.join("upper").join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, content).unwrap();
        std::fs::symlink_metadata(&abs).unwrap()
    }

    /// The unmount/restore gate: a retained-only overlay — the DEFAULT state after every
    /// snapshot, since sealing keeps the overlay — is loss-free to drop and must not demand
    /// --discard (the old raw-state gate did, and its own suggested remedy could never
    /// satisfy it). Sealed tombstones and bare directories don't block either: git cannot
    /// represent an empty tree, so no snapshot could ever cover one.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_passes_retained_only_overlay() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let meta = upper_file(state.path(), "kept.txt", "sealed bytes");
        std::fs::create_dir_all(state.path().join("upper/bare-dir")).unwrap();
        let wh = state.path().join("wh/dead.txt");
        std::fs::create_dir_all(wh.parent().unwrap()).unwrap();
        std::fs::write(&wh, b"").unwrap();
        let mut index = daemon::SealedIndex {
            commit: "cafe0000".into(),
            ..Default::default()
        };
        index
            .upserts
            .insert("kept.txt".into(), daemon::SealedStat::of(&meta));
        index.deletes.insert("dead.txt".into());
        index.save(state.path()).unwrap();
        let _ops = fake_daemon_with_replies(
            state.path(),
            [("dirty".to_string(), clean_dirty_reply())]
                .into_iter()
                .collect(),
        );

        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap();
        assert_eq!(losable, None, "retained sealed content is loss-free");
    }

    /// Unsealed changes still refuse (by dirty count from the daemon's authoritative view).
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_refuses_unsealed_changes() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        upper_file(state.path(), "fresh.txt", "unsealed");
        let _ops = fake_daemon_with_replies(
            state.path(),
            [(
                "dirty".to_string(),
                serde_json::json!({
                    "ok": true, "upserts": ["fresh.txt"], "deletes": [], "renames": [],
                    "commit": "cafe0000",
                }),
            )]
            .into_iter()
            .collect(),
        );
        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap()
            .expect("unsealed changes are losable");
        assert!(losable.contains("unsealed"), "{losable}");
    }

    /// Ignored files never enter a snapshot: an ignored-only overlay still refuses.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_refuses_ignored_only_overlay() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join(".gitignore"), "*.tmp\n").unwrap();
        upper_file(state.path(), "junk.tmp", "local only");
        let _ops = fake_daemon_with_replies(
            state.path(),
            [("dirty".to_string(), clean_dirty_reply())]
                .into_iter()
                .collect(),
        );
        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap()
            .expect("ignored files are losable");
        assert!(losable.contains("ignored"), "{losable}");
    }

    /// An overlay entry neither the dirty view nor the sealed index vouches for fails closed:
    /// it might be anything, and this gate guards data destruction.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_refuses_uncovered_entries() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        upper_file(state.path(), "mystery.txt", "whose is this");
        let _ops = fake_daemon_with_replies(
            state.path(),
            [("dirty".to_string(), clean_dirty_reply())]
                .into_iter()
                .collect(),
        );
        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap()
            .expect("uncovered entries are losable");
        assert!(losable.contains("not covered"), "{losable}");
    }

    /// No daemon needed: an EMPTY overlay passes outright, and a populated one classifies
    /// OFFLINE — an entry no seal record vouches for refuses (labeled with the daemon-down
    /// caveat), it does not error demanding a remount just to unmount.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_classifies_offline_without_a_daemon() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        assert_eq!(
            overlay_losable_state(state.path(), &mount.path().to_string_lossy())
                .await
                .unwrap(),
            None,
            "empty overlay needs no daemon"
        );
        upper_file(state.path(), "something.txt", "content");
        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap()
            .expect("an unvouched entry is losable offline");
        assert!(losable.contains("not covered"), "{losable}");
        assert!(
            losable.contains("daemon is not running"),
            "offline answer carries the caveat: {losable}"
        );
    }

    /// The headline case offline: retained-only overlays (stat-verified against sealed.json,
    /// the same identity daemon startup reconciliation trusts) pass with a DEAD daemon — a
    /// reboot no longer forces a remount just to unmount.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_passes_retained_only_without_a_daemon() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let meta = upper_file(state.path(), "kept.txt", "sealed bytes");
        let mut index = daemon::SealedIndex {
            commit: "cafe0000".into(),
            ..Default::default()
        };
        index
            .upserts
            .insert("kept.txt".into(), daemon::SealedStat::of(&meta));
        index.save(state.path()).unwrap();
        assert_eq!(
            overlay_losable_state(state.path(), &mount.path().to_string_lossy())
                .await
                .unwrap(),
            None,
            "stat-verified retained content is loss-free, daemon or not"
        );
    }

    /// Native mounts no longer retain sealed.json. With the daemon down, the destructive gate
    /// must still see durable journal dirt—even when an on-disk stat could happen to match an old
    /// retained baseline—and refuse to discard it.
    #[cfg(unix)]
    #[tokio::test]
    async fn native_losable_gate_reads_durable_dirty_state_offline() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        upper_file(state.path(), "dirty.txt", "local bytes");
        let mount_state = doctor_mount_state(state.path());
        daemon::save_mount_state(state.path(), &mount_state).unwrap();
        let identity = local_state::LocalStateIdentity {
            project_id: mount_state.project_id.clone(),
            filesystem: mount_state.repo.clone(),
            workspace_id: mount_state.workspace_id.clone(),
            store_uuid: mount_state.local_state_uuid.clone().unwrap(),
        };
        let local = local_state::LocalState::open(
            state.path().join(local_state::LOCAL_STATE_FILE),
            identity,
        )
        .unwrap();
        local
            .import_legacy_once(local_state::LegacyImport {
                base_snapshot: Some("base".to_string()),
                mutations: vec![local_state::LegacyMutation::Upsert {
                    path: "dirty.txt".to_string(),
                    min_write_offset: 0,
                }],
            })
            .unwrap();
        drop(local);
        assert!(!state.path().join("sealed.json").exists());

        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap()
            .expect("durable dirty intent must block offline destruction");
        assert!(losable.contains("unsealed"), "{losable}");
    }

    /// A retained native overlay remains loss-free offline using only redb baselines. This is the
    /// counterpart to the dirty test above and guards the removal of the obsolete sealed.json
    /// authority from turning every daemon-less unmount into a false refusal.
    #[cfg(unix)]
    #[tokio::test]
    async fn native_losable_gate_reads_durable_baselines_offline() {
        use std::os::unix::fs::MetadataExt;

        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let meta = upper_file(state.path(), "kept.txt", "sealed bytes");
        let mount_state = doctor_mount_state(state.path());
        daemon::save_mount_state(state.path(), &mount_state).unwrap();
        let identity = local_state::LocalStateIdentity {
            project_id: mount_state.project_id.clone(),
            filesystem: mount_state.repo.clone(),
            workspace_id: mount_state.workspace_id.clone(),
            store_uuid: mount_state.local_state_uuid.clone().unwrap(),
        };
        let local = local_state::LocalState::open(
            state.path().join(local_state::LOCAL_STATE_FILE),
            identity,
        )
        .unwrap();
        local
            .import_legacy_once(local_state::LegacyImport {
                base_snapshot: Some("base".to_string()),
                mutations: vec![local_state::LegacyMutation::Upsert {
                    path: "kept.txt".to_string(),
                    min_write_offset: 0,
                }],
            })
            .unwrap();
        let generation = local.freeze_current().unwrap().unwrap().generation;
        local
            .mark_prepared(local_state::PreparedGeneration::new(
                generation,
                Some("base".to_string()),
                "root",
                "fingerprint",
                Vec::new(),
            ))
            .unwrap();
        local
            .put_publish_request(local_state::PublishRequest::new(
                "request", generation, "snapshot", false, 1,
            ))
            .unwrap();
        local.mark_published(generation, "snapshot").unwrap();
        let baseline = local_state::SealedBaseline::upsert(
            "kept.txt",
            "snapshot",
            local_state::FileIdentity {
                device: meta.dev(),
                inode: meta.ino(),
                size: meta.size(),
                mtime_secs: meta.mtime(),
                mtime_nanos: meta.mtime_nsec(),
                ctime_secs: meta.ctime(),
                ctime_nanos: meta.ctime_nsec(),
                mode: meta.mode(),
            },
            None,
        );
        local
            .retire_published(generation, "snapshot", &[baseline], &[], Vec::new())
            .unwrap();
        drop(local);
        assert!(!state.path().join("sealed.json").exists());

        assert_eq!(
            overlay_losable_state(state.path(), &mount.path().to_string_lossy())
                .await
                .unwrap(),
            None,
            "redb-vouched retained content is loss-free offline"
        );
    }

    /// Path membership alone must not vouch: a sealed path whose CONTENT no longer matches
    /// its seal record (out-of-band write, or a write racing the dirty reply) is losable.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_refuses_modified_sealed_path() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let meta = upper_file(state.path(), "kept.txt", "sealed bytes");
        let mut index = daemon::SealedIndex {
            commit: "cafe0000".into(),
            ..Default::default()
        };
        index
            .upserts
            .insert("kept.txt".into(), daemon::SealedStat::of(&meta));
        index.save(state.path()).unwrap();
        // Rewrite with different content (size changes, so identity breaks even on coarse
        // mtime granularity) — and let the daemon answer CLEAN, simulating the race where
        // the write landed after the dirty reply.
        std::fs::write(state.path().join("upper/kept.txt"), "newer, unsealed bytes").unwrap();
        let _ops = fake_daemon_with_replies(
            state.path(),
            [("dirty".to_string(), clean_dirty_reply())]
                .into_iter()
                .collect(),
        );
        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap()
            .expect("a stat-mismatched sealed path is losable");
        assert!(losable.contains("not covered"), "{losable}");
    }

    /// Coverage outranks ignore rules: a file sealed into a snapshot and LATER matched by a
    /// new ignore rule is retained (loss-free), not "ignored local-only" — the old ordering
    /// refused it with a remedy (`tl fs snapshot`) that could never satisfy the gate.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_passes_sealed_then_ignored_file() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let meta = upper_file(state.path(), "build.log", "sealed before the rule");
        let mut index = daemon::SealedIndex {
            commit: "cafe0000".into(),
            ..Default::default()
        };
        index
            .upserts
            .insert("build.log".into(), daemon::SealedStat::of(&meta));
        index.save(state.path()).unwrap();
        std::fs::write(mount.path().join(".gitignore"), "*.log\n").unwrap();
        let _ops = fake_daemon_with_replies(
            state.path(),
            [("dirty".to_string(), clean_dirty_reply())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            overlay_losable_state(state.path(), &mount.path().to_string_lossy())
                .await
                .unwrap(),
            None,
            "sealed-then-ignored content is retained, not local-only"
        );
    }

    /// An ignored whiteout is a local-only deletion of a committed file: it can never seal
    /// (the sealer's ignore filter skips it), so like an ignored file it refuses and only
    /// --discard clears it. Silently passing it would resurrect the deleted file.
    #[cfg(unix)]
    #[tokio::test]
    async fn losable_gate_refuses_ignored_whiteout() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join(".gitignore"), "*.tmp\n").unwrap();
        let wh = state.path().join("wh/junk.tmp");
        std::fs::create_dir_all(wh.parent().unwrap()).unwrap();
        std::fs::write(&wh, b"").unwrap();
        let _ops = fake_daemon_with_replies(
            state.path(),
            [("dirty".to_string(), clean_dirty_reply())]
                .into_iter()
                .collect(),
        );
        let losable = overlay_losable_state(state.path(), &mount.path().to_string_lossy())
            .await
            .unwrap()
            .expect("an ignored whiteout is losable");
        assert!(losable.contains("ignored"), "{losable}");
    }

    /// The point-of-no-return re-check: the fingerprint is stable over unchanged state and
    /// moves on any write, new file, or whiteout — including ignored files (rule evaluation
    /// deliberately plays no part, so it also works with the daemon dead).
    #[cfg(unix)]
    #[test]
    fn overlay_fingerprint_tracks_any_overlay_change() {
        let state = tempfile::tempdir().unwrap();
        upper_file(state.path(), "a.txt", "one");
        let base = overlay_fingerprint(state.path()).unwrap();
        assert_eq!(
            base,
            overlay_fingerprint(state.path()).unwrap(),
            "stable across identical state"
        );
        upper_file(state.path(), "junk.tmp", "ignored files count too");
        let with_new_file = overlay_fingerprint(state.path()).unwrap();
        assert_ne!(base, with_new_file, "a new file moves the fingerprint");
        let wh = state.path().join("wh/gone.txt");
        std::fs::create_dir_all(wh.parent().unwrap()).unwrap();
        std::fs::write(&wh, b"").unwrap();
        assert_ne!(
            with_new_file,
            overlay_fingerprint(state.path()).unwrap(),
            "a new whiteout moves the fingerprint"
        );
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
        let (sealed, cleared, _) = seal_via_daemon(
            state.path(),
            mount.path().to_str().unwrap(),
            Some("msg"),
            false,
            &bar,
        )
        .await
        .unwrap();
        let DaemonSealOutcome::Sealed(sealed) = sealed else {
            panic!("daemon did not seal a commit");
        };

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

    /// `--clear` rides the seal request itself (ONE control op), and the caller's revalidation set
    /// is exactly the daemon-reported generation-safe `cleared` list.
    #[cfg(unix)]
    #[tokio::test]
    async fn snapshot_clear_rides_the_seal_op_and_reports_dropped_paths() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let ops = fake_daemon(state.path());

        let bar = indicatif::ProgressBar::hidden();
        let (sealed, cleared, _) = seal_via_daemon(
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
        assert!(matches!(sealed, DaemonSealOutcome::Sealed(_)));
        // The fake daemon cleared 3 paths (one sealed, two never-sealed) — the revalidation
        // set came from `cleared`, not the seal delta.
        assert_eq!(cleared, Some(3));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn seal_response_loss_retries_with_the_same_request_id() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let request_ids = fake_daemon_drops_first_seal_response(state.path());
        let bar = indicatif::ProgressBar::hidden();

        let (outcome, cleared, _) = seal_via_daemon(
            state.path(),
            mount.path().to_str().unwrap(),
            Some("response-loss"),
            false,
            &bar,
        )
        .await
        .unwrap();

        let DaemonSealOutcome::Sealed(sealed) = outcome else {
            panic!("retry did not recover the sealed response");
        };
        assert_eq!(sealed.commit, "recovered-snapshot");
        assert_eq!(cleared, None);
        let ids = request_ids.lock().unwrap();
        assert_eq!(ids.len(), 2);
        assert!(!ids[0].is_empty());
        assert_eq!(ids[0], ids[1], "retry must preserve the idempotency key");
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
        );

        let bar = indicatif::ProgressBar::hidden();
        let (sealed, _cleared, _) = seal_via_daemon(
            state.path(),
            mount.path().to_str().unwrap(),
            Some("msg"),
            false,
            &bar,
        )
        .await
        .unwrap();

        let DaemonSealOutcome::Sealed(sealed) = sealed else {
            panic!("daemon did not seal");
        };
        assert_eq!(sealed.commit, "cafe0000");
        assert_eq!(
            bar.message(),
            "uploaded 3 chunks (2 KiB)...",
            "the spinner followed the streamed event lines"
        );
    }

    /// Snapshot requests never fall back to client-side preparation. When the daemon has no
    /// prepared root yet, the pending watermark is a successful, structured outcome and a
    /// destructive clear has not happened.
    #[cfg(unix)]
    #[tokio::test]
    async fn snapshot_seal_returns_pending_without_requiring_sealed_fields() {
        let state = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let ops = fake_daemon_with_replies(
            state.path(),
            [(
                "seal".to_string(),
                serde_json::json!({
                    "ok": true,
                    "clean": false,
                    "pending": true,
                    "pending_watermark": 42,
                    "commit": "current",
                }),
            )]
            .into_iter()
            .collect(),
        );

        let bar = indicatif::ProgressBar::hidden();
        let (outcome, cleared, _) = seal_via_daemon(
            state.path(),
            mount.path().to_str().unwrap(),
            Some("save point"),
            true,
            &bar,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            DaemonSealOutcome::Pending { watermark: 42 }
        ));
        assert_eq!(
            cleared, None,
            "pending publication cannot clear the overlay"
        );
        assert_eq!(*ops.lock().unwrap(), vec!["seal"]);
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
            "Nothing new to snapshot; cleared 4 locally retained path(s) from the overlay."
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
            assert!(msg.contains("--discard"), "names the escape hatch: {msg}");
        }
        // Restore so the tempdir can be cleaned up.
        std::fs::set_permissions(&upper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn cold_push_state_reopens_with_the_same_store_identity() {
        let state_root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        let source = source.path().canonicalize().unwrap();
        let (first_dir, first) =
            open_cold_push_state_at(state_root.path(), "https://api", "project", "fs", &source)
                .unwrap();
        let identity = first.identity().clone();
        assert!(first.needs_legacy_import().unwrap());
        drop(first);

        let (second_dir, second) =
            open_cold_push_state_at(state_root.path(), "https://api", "project", "fs", &source)
                .unwrap();
        assert_eq!(first_dir, second_dir);
        assert_eq!(second.identity(), &identity);
    }

    #[test]
    fn cold_push_state_key_is_component_delimited() {
        let root = Path::new("/tmp/source");
        assert_ne!(
            cold_push_state_key("a", "bc", "d", root),
            cold_push_state_key("ab", "c", "d", root)
        );
        assert_eq!(
            cold_push_state_key("a", "b", "c", root),
            cold_push_state_key("a", "b", "c", root)
        );
    }

    #[test]
    fn tracked_push_delta_does_not_open_identity_proven_files() {
        let source = tempfile::tempdir().unwrap();
        std::fs::create_dir(source.path().join("dir")).unwrap();
        std::fs::write(source.path().join("dir/unchanged"), b"stable").unwrap();
        std::fs::write(source.path().join("changed"), b"before").unwrap();
        std::fs::write(source.path().join("deleted"), b"gone").unwrap();

        let first = scan_cold_push_tree(source.path()).unwrap();
        let baselines: Vec<_> = first
            .paths
            .iter()
            .map(|entry| cold_push_baseline(entry, "snapshot-1"))
            .collect();
        let unchanged = scan_cold_push_tree(source.path()).unwrap();
        let clean = plan_cold_push_delta(source.path(), &unchanged, &baselines).unwrap();
        assert!(clean.upserts.is_empty(), "{:?}", clean.upserts);
        assert!(clean.deletes.is_empty(), "{:?}", clean.deletes);

        std::fs::write(source.path().join("changed"), b"after-and-longer").unwrap();
        std::fs::remove_file(source.path().join("deleted")).unwrap();
        std::fs::write(source.path().join("new"), b"new").unwrap();
        let second = scan_cold_push_tree(source.path()).unwrap();
        let delta = plan_cold_push_delta(source.path(), &second, &baselines).unwrap();
        let upserts: BTreeSet<_> = delta
            .upserts
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        assert!(upserts.contains("changed"));
        assert!(upserts.contains("new"));
        assert!(!upserts.contains("dir/unchanged"));
        assert_eq!(delta.deletes, vec!["deleted"]);
    }

    #[test]
    fn tracked_push_new_ignore_rule_preserves_the_remote_baseline() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("kept-remotely"), b"bytes").unwrap();
        let first = scan_cold_push_tree(source.path()).unwrap();
        let baselines: Vec<_> = first
            .paths
            .iter()
            .map(|entry| cold_push_baseline(entry, "snapshot-1"))
            .collect();

        std::fs::write(source.path().join(".gitignore"), b"kept-remotely\n").unwrap();
        let second = scan_cold_push_tree(source.path()).unwrap();
        let delta = plan_cold_push_delta(source.path(), &second, &baselines).unwrap();
        assert!(
            !delta.deletes.iter().any(|path| path == "kept-remotely"),
            "ignored is not absent: {:?}",
            delta.deletes
        );
        assert!(delta.upserts.iter().any(|entry| entry.path == ".gitignore"));
    }

    #[test]
    fn tracked_push_racy_window_forces_revalidation() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("racy"), b"bytes").unwrap();
        let scan = scan_cold_push_tree(source.path()).unwrap();
        let observed = scan
            .paths
            .iter()
            .find(|entry| entry.path == "racy")
            .unwrap();
        let mut baseline = cold_push_baseline(observed, "snapshot-1");
        baseline.observed_at_secs = Some(observed.identity.ctime_secs);
        baseline.observed_at_nanos = Some(observed.identity.ctime_nanos);
        let delta = plan_cold_push_delta(source.path(), &scan, &[baseline]).unwrap();
        assert_eq!(delta.upserts.len(), 1);
        assert_eq!(delta.upserts[0].path, "racy");
    }

    #[test]
    fn corrupt_cold_push_identity_fails_closed() {
        let state_root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        let source = source.path().canonicalize().unwrap();
        let (state_dir, store) =
            open_cold_push_state_at(state_root.path(), "https://api", "project", "fs", &source)
                .unwrap();
        drop(store);
        std::fs::write(state_dir.join("identity.json"), b"{").unwrap();
        let error =
            open_cold_push_state_at(state_root.path(), "https://api", "project", "fs", &source)
                .err()
                .expect("corrupt identity must fail closed")
                .to_string();
        assert!(error.contains("corrupt"), "{error}");
        assert!(error.contains("refusing to guess"), "{error}");
    }

    #[test]
    fn git_status_json_has_only_repository_workspace_vocabulary() {
        let report = GitStatusReport {
            format_ver: 1,
            state: "workspace_snapshotted_unpromoted".to_string(),
            view_type: "writable_workspace".to_string(),
            repository: "demo".to_string(),
            subtree: Some("services/api".to_string()),
            canonical_source: "refs/heads/main".to_string(),
            resolved_commit: Some("abc".to_string()),
            following: false,
            connected: true,
            workspace: Some(GitStatusWorkspace {
                id: "ws-1".to_string(),
                base: "base".to_string(),
                snapshot: "head".to_string(),
                target: Some("main".to_string()),
                relationship: "ahead".to_string(),
                conflicts: Vec::new(),
                conflict_operation: None,
                retained_chains: vec!["refs/recovery/ws-1/one".to_string()],
                retained_chains_truncated: false,
            }),
            local: GitStatusLocal {
                changed_paths: 0,
                exact: true,
            },
            next_actions: vec!["tl git promote main".to_string()],
        };
        let json = serde_json::to_string(&report).unwrap();
        for forbidden in ["filesystem", "session", "save_id", "current_save"] {
            assert!(!json.contains(forbidden), "{forbidden} leaked into {json}");
        }
        assert!(json.contains("\"workspace\""));
        assert!(json.contains("\"snapshot\""));
        assert!(json.contains("\"base\""));
        assert!(json.contains("\"target\""));
        assert!(json.contains("\"conflicts\""));
    }

    #[test]
    fn git_status_state_machine_only_recommends_valid_next_transitions() {
        assert_eq!(
            classify_git_status(false, true, None, true, true, 0, false, false, false, false,).0,
            "read_only_following"
        );
        assert_eq!(
            classify_git_status(
                false, true, None, true, false, 0, false, false, false, false,
            )
            .0,
            "read_only_pinned"
        );
        let dirty =
            classify_git_status(false, true, None, false, false, 3, false, true, true, true);
        assert_eq!(dirty.0, "workspace_locally_dirty");
        assert_eq!(dirty.1, vec!["tl git snapshot"]);
        assert_eq!(
            classify_git_status(
                false,
                true,
                Some("rebase_conflict"),
                false,
                false,
                2,
                false,
                true,
                true,
                true,
            )
            .0,
            "rebase_conflict"
        );
        assert_eq!(
            classify_git_status(false, true, None, false, false, 0, false, true, true, true,).0,
            "workspace_target_advanced"
        );
        assert_eq!(
            classify_git_status(false, true, None, false, false, 0, false, false, true, true,).0,
            "workspace_snapshotted_unpromoted"
        );
        assert_eq!(
            classify_git_status(
                false, false, None, false, false, 0, false, false, false, false,
            )
            .0,
            "server_unreachable_stale_view"
        );
        let deleted_publish =
            classify_git_status(true, true, None, false, true, 0, true, false, true, true);
        assert_eq!(deleted_publish.0, "source_ref_deleted");
        assert!(!deleted_publish.1[0].contains("rebase"));
    }
}
