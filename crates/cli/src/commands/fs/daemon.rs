//! The `tl fs` mount daemon.
//!
//! One daemon per mount: it owns the mount core (lazy server reads, immutable caches, workspace
//! ref following) and the writable overlay, heartbeats the workspace lease, rotates the minted
//! git credential before it expires (the shared token slot in the vendored
//! [`gsvc_mount::FsClient`] makes this an in-place swap), and answers a tiny line-JSON control
//! protocol on a unix socket in the state directory:
//!
//! ```text
//! {"op":"ping"}        -> {"ok":true,"commit":"<hex>"}
//! {"op":"refresh"}     -> poll the workspace ref now; reply with the (possibly new) commit
//! {"op":"clear_upper"} -> drop all overlay state (post-snapshot / restore)
//! {"op":"shutdown"}    -> unmount and exit
//! ```
//!
//! How the kernel reaches the overlay differs by platform:
//! - **Linux**: an in-process FUSE session over `/dev/fuse` ([`super::fusefs`]).
//! - **macOS**: the TensorLake FSKit extension (`ai.tensorlake.tlfs.fsmodule`, a sandboxed Swift
//!   proxy) speaks the [`super::vfsserver`] protocol to this daemon over localhost TCP; the
//!   daemon invokes `mount -F -t tlfs 'tlfs://127.0.0.1:<port>/<secret>' <dir>` once the server
//!   is listening. No kernel extension, no sudo.

use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

#[cfg(unix)]
use super::overlay::{OverlayFs, OverlayInval};
#[cfg(unix)]
use std::sync::Arc;

/// Pushes kernel-cache invalidations for a batch of overlay inos. On Linux this drives the FUSE
/// session's `Notifier` (which is what makes the binding's long entry/attr TTLs sound); on
/// macOS/FSKit there is no notify channel and the sink is a no-op (FSKit revalidates through
/// its own attribute protocol).
#[cfg(unix)]
type InvalSink = Arc<dyn Fn(Vec<OverlayInval>) + Send + Sync>;

/// Persisted per-mount state (`<state dir>/state.json`). No credentials: the daemon mints its
/// own from the same CLI auth context.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountState {
    pub project_id: String,
    /// Scope for platform token minting with a PAT. Absent in state files written before
    /// path-addressed commands resolved their scope from the mount instead of the CWD.
    #[serde(default)]
    pub organization_id: Option<String>,
    pub repo: String,
    pub workspace_id: String,
    pub ref_name: String,
    pub mountpoint: PathBuf,
    /// Branch-following view: the lower side follows this ref (a real branch) instead of the
    /// workspace ref. Shared-ro *and* shared-rw set it — the mode axiom is that modes vary only
    /// what the view follows plus write policy.
    #[serde(default)]
    pub follow_ref: Option<String>,
    /// Write policy, decoupled from following: shared-ro is the only read-only mode. `None` in
    /// state files written before shared-rw followed the branch; those were read-only exactly
    /// when they followed, which is what the accessor falls back to.
    #[serde(default)]
    pub read_only: Option<bool>,
}

impl MountState {
    pub fn read_only(&self) -> bool {
        self.read_only.unwrap_or(self.follow_ref.is_some())
    }
}

pub fn state_dir_root() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("share")
        .join("tensorlake")
        .join("mounts")
}

pub fn load_mount_state(state_dir: &Path) -> Result<MountState> {
    let raw = std::fs::read(state_dir.join("state.json"))?;
    Ok(serde_json::from_slice(&raw)?)
}

pub fn save_mount_state(state_dir: &Path, state: &MountState) -> Result<()> {
    std::fs::create_dir_all(state_dir)?;
    std::fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec_pretty(state)?,
    )?;
    Ok(())
}

pub fn control_socket(state_dir: &Path) -> PathBuf {
    state_dir.join("control.sock")
}

/// The daemon's pid, written at startup so `unmount` can wait for the process to actually die
/// before tearing down the state dir (a shutdown fired into the socket alone races the exit).
pub fn pid_file(state_dir: &Path) -> PathBuf {
    state_dir.join("daemon.pid")
}

pub fn daemon_pid(state_dir: &Path) -> Option<i32> {
    std::fs::read_to_string(pid_file(state_dir))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// One control round-trip from a CLI command to the daemon. Mounts (and so daemons) exist
/// only on unix; elsewhere every control call reports the daemon as not running.
#[cfg(not(unix))]
pub async fn control(_state_dir: &Path, _op: &str) -> Result<serde_json::Value> {
    Err(CliError::usage(
        "tl fs mounts are supported on Linux (FUSE) and macOS (FSKit) only.",
    ))
}

/// One control round-trip from a CLI command to the daemon.
#[cfg(unix)]
pub async fn control(state_dir: &Path, op: &str) -> Result<serde_json::Value> {
    let sock = control_socket(state_dir);
    let mut stream = tokio::net::UnixStream::connect(&sock).await.map_err(|e| {
        CliError::usage(format!(
            "mount daemon is not running ({}): {e}",
            sock.display()
        ))
    })?;
    stream
        .write_all(format!("{}\n", serde_json::json!({ "op": op })).as_bytes())
        .await?;
    let mut reader = tokio::io::BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let resp: serde_json::Value = serde_json::from_str(line.trim())?;
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(CliError::usage(format!(
            "daemon {op} failed: {}",
            resp.get("error").and_then(|v| v.as_str()).unwrap_or("?")
        )));
    }
    Ok(resp)
}

/// How long before recorded credential expiry the daemon re-mints. Minted tokens live ~1h.
#[cfg(unix)]
const CREDENTIAL_ROTATE_MARGIN: Duration = Duration::from_secs(10 * 60);
#[cfg(unix)]
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20 * 60);

#[cfg(unix)]
fn expires_in(expires_at: &str) -> Duration {
    chrono::DateTime::parse_from_rfc3339(expires_at)
        .map(|t| {
            Duration::from_secs((t.timestamp() - chrono::Utc::now().timestamp()).max(60) as u64)
        })
        .unwrap_or(Duration::from_secs(30 * 60))
}

/// Run the daemon in the foreground of the current process. `tl fs mount` spawns this as a
/// detached child (`tl fs daemon --state-dir ...`).
pub async fn run(ctx: &CliContext, state_dir: &Path) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (ctx, state_dir);
        Err(CliError::usage(
            "tl fs mount is supported on Linux (FUSE) and macOS (FSKit) only.",
        ))
    }
    #[cfg(unix)]
    {
        run_mount(ctx, state_dir).await
    }
}

#[cfg(unix)]
async fn run_mount(ctx: &CliContext, state_dir: &Path) -> Result<()> {
    use crate::commands::git::{artifact_storage_client, project_id};
    use gsvc_mount::{FsClient, MountCore, MountOptions};

    let state = load_mount_state(state_dir)?;
    let sdk = artifact_storage_client(ctx)?;
    let project = project_id(ctx)?;

    // Initial credential: the dev override, or a fresh mint.
    let (token, mut expires_at) = match std::env::var("TENSORLAKE_GIT_TOKEN") {
        Ok(token) => (token, None),
        Err(_) => {
            let cred = sdk
                .mint_token_for_repo(&project, Some(&state.repo))
                .await?
                .into_inner();
            (cred.token, Some(cred.expires_at))
        }
    };

    let client = FsClient::new(
        sdk.git_base_url(),
        &state.project_id,
        &state.repo,
        Some(token),
    )
    .map_err(|e| CliError::usage(format!("mount client: {e}")))?;
    // Keep a handle onto the shared credential slot for rotation.
    let rotating_client = client.clone();

    // Shared-ro sessions follow the branch itself; writable mounts follow their workspace ref.
    let followed = state
        .follow_ref
        .clone()
        .unwrap_or_else(|| state.ref_name.clone());
    let core = MountCore::new(
        client,
        MountOptions {
            reference: followed,
            follow: true,
            poll_interval: Duration::from_secs(5),
            // Manifest-driven cache prefill in the background: first walks serve warm instead
            // of paying a per-directory crawl. Best-effort — a failed warmup just starts cold.
            warmup: true,
            ..Default::default()
        },
    )
    .await
    .map_err(|e| CliError::usage(format!("mount init: {e}")))?;
    let overlay = OverlayFs::new(core.clone(), state_dir, state.read_only())
        .map_err(|e| CliError::usage(format!("overlay init: {e}")))?;

    // Credential rotation: re-mint comfortably before expiry, swap in place.
    if expires_at.is_some() {
        let sdk = sdk.clone();
        let (project, repo) = (project.clone(), state.repo.clone());
        let rotate = rotating_client;
        tokio::spawn(async move {
            loop {
                let due = expires_in(expires_at.as_deref().unwrap_or_default())
                    .saturating_sub(CREDENTIAL_ROTATE_MARGIN);
                tokio::time::sleep(due.max(Duration::from_secs(60))).await;
                match sdk.mint_token_for_repo(&project, Some(&repo)).await {
                    Ok(cred) => {
                        let cred = cred.into_inner();
                        rotate.set_token(Some(cred.token));
                        expires_at = Some(cred.expires_at);
                    }
                    Err(e) => {
                        tracing::warn!("credential rotation failed (will retry): {e}");
                        expires_at = None; // retry on the fallback cadence
                    }
                }
            }
        });
    }

    // Lease heartbeat.
    {
        let sdk = sdk.clone();
        let (project, repo, ws) = (
            project.clone(),
            state.repo.clone(),
            state.workspace_id.clone(),
        );
        let creds = crate::commands::fs::FsSession::open(ctx, Some(&state.repo)).await?;
        tokio::spawn(async move {
            loop {
                let (user, token) = creds.creds();
                if let Err(e) = sdk
                    .workspace_heartbeat(&project, &repo, user, token, &ws)
                    .await
                {
                    tracing::warn!("workspace heartbeat failed: {e}");
                }
                tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            }
        });
    }

    let mountpoint = state.mountpoint.clone();

    // Attach the kernel: platform-specific. Only after this succeeds does the control socket
    // exist — the socket answering is what `tl fs mount` treats as success.
    let (served, invalidate) = attach(overlay.clone(), &mountpoint).await?;

    // Follow the workspace ref, pushing each refresh's exact delta to the kernel. Spawned after
    // attach because the invalidation sink is born with the kernel session.
    {
        let overlay = overlay.clone();
        let invalidate = invalidate.clone();
        gsvc_mount::spawn_ref_watcher(&core, move |delta| {
            invalidate(overlay.translate_delta(&delta));
        });
    }

    // Control socket (mount is live).
    let sock_path = control_socket(state_dir);
    let _ = std::fs::remove_file(&sock_path);
    std::fs::write(pid_file(state_dir), std::process::id().to_string())?;
    let listener = tokio::net::UnixListener::bind(&sock_path)?;
    {
        let overlay = overlay.clone();
        let core = core.clone();
        let mountpoint = mountpoint.clone();
        let invalidate = invalidate.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let overlay = overlay.clone();
                let core = core.clone();
                let mountpoint = mountpoint.clone();
                let invalidate = invalidate.clone();
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.is_err() {
                        return;
                    }
                    let op = serde_json::from_str::<serde_json::Value>(line.trim())
                        .ok()
                        .and_then(|v| v.get("op").and_then(|o| o.as_str()).map(str::to_string))
                        .unwrap_or_default();
                    let resp = match op.as_str() {
                        "ping" => {
                            serde_json::json!({ "ok": true, "commit": core.current_commit() })
                        }
                        "refresh" => match core.poll_ref().await {
                            Ok(delta) => {
                                if let Some(delta) = delta {
                                    invalidate(overlay.translate_delta(&delta));
                                }
                                serde_json::json!({ "ok": true, "commit": core.current_commit() })
                            }
                            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                        },
                        // The upper drop flips interned paths to their lower view with no
                        // kernel-visible operation; push the implied invalidations.
                        "clear_upper" => match overlay.clear_upper() {
                            Ok(affected) => {
                                invalidate(affected);
                                serde_json::json!({ "ok": true })
                            }
                            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                        },
                        "shutdown" => {
                            // Unmount BEFORE replying: the reply is the CLI's signal that the
                            // kernel released the volume (the slow phase on macOS — fskitd
                            // teardown). A busy volume (a shell cd'd inside) keeps the daemon
                            // serving — exiting with the volume still attached is how zombie
                            // mounts are born.
                            if unmount(&mountpoint).await {
                                let resp = serde_json::json!({ "ok": true });
                                let mut stream = reader.into_inner();
                                let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
                                // Exit outright: session-wait is not guaranteed to return after
                                // an external unmount (observed leaked daemons on Linux), and
                                // the daemon's one job is over. The reply above already flushed.
                                std::process::exit(0);
                            }
                            serde_json::json!({
                                "ok": false,
                                "error": "the volume is busy (something is still using it)",
                            })
                        }
                        other => {
                            serde_json::json!({ "ok": false, "error": format!("unknown op {other:?}") })
                        }
                    };
                    let mut stream = reader.into_inner();
                    let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
                });
            }
        });
    }

    // Serve until the kernel lets go of the mountpoint.
    let result = served.wait().await;
    let _ = std::fs::remove_file(&sock_path);
    result
}

/// A live kernel attachment; `wait` returns when the mount ends.
#[cfg(unix)]
enum Attached {
    #[cfg(target_os = "linux")]
    Fuse(tokio::task::JoinHandle<std::io::Result<()>>),
    #[cfg(target_os = "macos")]
    FsKit { mountpoint: PathBuf },
}

#[cfg(unix)]
impl Attached {
    async fn wait(self) -> Result<()> {
        match self {
            #[cfg(target_os = "linux")]
            Attached::Fuse(handle) => match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(CliError::usage(format!("fuse session: {e}"))),
                Err(e) => Err(CliError::usage(format!("fuse thread: {e}"))),
            },
            #[cfg(target_os = "macos")]
            Attached::FsKit { mountpoint } => {
                // FSKit serves through our TCP server; the daemon's job is simply to outlive
                // the mount. Poll the mount table and exit once the kernel lets go.
                loop {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    if !is_mounted(&mountpoint) {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
async fn attach(overlay: Arc<OverlayFs>, mountpoint: &Path) -> Result<(Attached, InvalSink)> {
    let (mounted_tx, mounted_rx) = tokio::sync::oneshot::channel();
    let fuse = super::fusefs::WorkspaceFuse::new(overlay, tokio::runtime::Handle::current());
    let mp = mountpoint.to_path_buf();
    let served = tokio::task::spawn_blocking(move || fuse.run(&mp, mounted_tx));
    let notifier = match mounted_rx.await {
        Ok(notifier) => notifier,
        Err(_) => {
            return match served.await {
                Ok(Ok(())) => Err(CliError::usage("fuse session ended before mounting")),
                Ok(Err(e)) => Err(CliError::usage(format!("fuse mount failed: {e}"))),
                Err(e) => Err(CliError::usage(format!("fuse thread: {e}"))),
            };
        }
    };
    // Notify errors are expected steady-state (ENOENT when the kernel holds no cache for the
    // ino/dentry) and never actionable — the point is only to drop what *is* cached.
    let invalidate: InvalSink = Arc::new(move |items: Vec<OverlayInval>| {
        for item in items {
            if item.staled {
                if let Some(parent) = item.parent_ino {
                    let _ = notifier.inval_entry(parent, std::ffi::OsStr::new(&item.name));
                }
            }
            let _ = notifier.inval_inode(item.ino, 0, 0);
        }
    });
    Ok((Attached::Fuse(served), invalidate))
}

/// macOS: serve the overlay over localhost TCP and ask the kernel to mount through the
/// TensorLake FSKit extension. There is no notify channel in the FSKit protocol, so the
/// invalidation sink is a no-op — FSKit revalidates through its own attribute traffic.
#[cfg(target_os = "macos")]
async fn attach(overlay: Arc<OverlayFs>, mountpoint: &Path) -> Result<(Attached, InvalSink)> {
    let server = super::vfsserver::serve(overlay)
        .await
        .map_err(|e| CliError::usage(format!("vfs server: {e}")))?;
    let url = format!("tlfs://127.0.0.1:{}/{}", server.port, server.secret);
    // nobrowse asks for MNT_DONTBROWSE: no Finder sidebar entry, no mds indexing crawl (the
    // classic macOS unmount-delayer), no .DS_Store turds in workspaces. Advisory for now —
    // fskitd on 26.5 accepts but does not apply it (mount table shows no nobrowse; measured) —
    // kept so the behavior arrives for free when FSKit honors it.
    let status = tokio::process::Command::new("/sbin/mount")
        .arg("-F")
        .arg("-t")
        .arg("tlfs")
        .arg("-o")
        .arg("nobrowse")
        .arg(&url)
        .arg(mountpoint)
        .status()
        .await?;
    if !status.success() {
        return Err(CliError::usage(
            "mount(8) failed: is the TensorLake file-system extension installed and enabled? \
             Run `tl fs setup`, then enable it under System Settings -> General -> Login Items \
             & Extensions -> File System Extensions.",
        ));
    }
    Ok((
        Attached::FsKit {
            mountpoint: mountpoint.to_path_buf(),
        },
        Arc::new(|_| {}),
    ))
}

#[cfg(target_os = "macos")]
fn is_mounted(mountpoint: &Path) -> bool {
    // Read the kernel's mount table with MNT_NOWAIT instead of statfs(mountpoint): statfs
    // calls into the filesystem and BLOCKS (uninterruptibly) while an unmount of it is in
    // flight — the exact moment this question gets asked. Measured: a busy-volume unmount
    // wedged the daemon in statfs for 50 minutes. getfsstat with MNT_NOWAIT only copies
    // cached table entries and never touches the fs.
    use std::os::unix::ffi::OsStrExt;
    let want = mountpoint.as_os_str().as_bytes();
    let count = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
    if count <= 0 {
        return false;
    }
    // Room for a few mounts appearing between the two calls; the kernel truncates to fit.
    let capacity = count as usize + 8;
    let mut stats: Vec<libc::statfs> = Vec::with_capacity(capacity);
    let bufsize = (capacity * std::mem::size_of::<libc::statfs>()) as libc::c_int;
    let written = unsafe { libc::getfsstat(stats.as_mut_ptr(), bufsize, libc::MNT_NOWAIT) };
    if written <= 0 {
        return false;
    }
    unsafe { stats.set_len(written as usize) };
    stats.iter().any(|sfs| {
        let name = unsafe { std::ffi::CStr::from_ptr(sfs.f_mntonname.as_ptr()) };
        let fstype = unsafe { std::ffi::CStr::from_ptr(sfs.f_fstypename.as_ptr()) };
        fstype.to_bytes() == b"tlfs" && name.to_bytes() == want
    })
}

/// Ask the kernel to unmount, bounded. A busy volume (a shell cd'd inside) makes umount(8)
/// fail with EBUSY; on macOS/FSKit a teardown can also wedge in-kernel, so the wait is capped
/// and the mount table (never the filesystem itself — see is_mounted) is the arbiter on every
/// non-success path. Returns whether the volume is actually detached.
#[cfg(unix)]
async fn unmount(mountpoint: &Path) -> bool {
    #[cfg(target_os = "linux")]
    let mut cmd = {
        let mut cmd = tokio::process::Command::new("fusermount");
        cmd.arg("-u");
        cmd
    };
    #[cfg(not(target_os = "linux"))]
    let mut cmd = tokio::process::Command::new("umount");
    cmd.arg(mountpoint)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            tracing::warn!("unmount of {} failed to spawn: {e}", mountpoint.display());
            return !still_mounted(mountpoint);
        }
    };
    match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
        Ok(Ok(status)) if status.success() => true,
        // Fast failure (EBUSY) — or, on timeout, a wedged teardown whose child is deliberately
        // left running (killing it would not abort an in-flight detach anyway).
        _ => !still_mounted(mountpoint),
    }
}

/// Whether the kernel still shows a live mount at `mountpoint`.
#[cfg(unix)]
fn still_mounted(mountpoint: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        is_mounted(mountpoint)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let path = mountpoint.to_string_lossy();
        std::fs::read_to_string("/proc/self/mounts")
            .map(|mounts| {
                mounts
                    .lines()
                    .any(|line| line.split_whitespace().nth(1) == Some(path.as_ref()))
            })
            .unwrap_or(false)
    }
}
