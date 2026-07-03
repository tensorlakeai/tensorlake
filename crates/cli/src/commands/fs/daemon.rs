//! The `tl fs` mount daemon.
//!
//! One daemon per mount: it holds the FUSE session over the overlay, follows the workspace ref
//! (so a snapshot's ref advance swaps the lower layer to the new commit), heartbeats the
//! workspace lease, rotates the minted git credential before it expires (the shared token slot
//! in the vendored [`gsvc_mount::FsClient`] makes this an in-place swap), and answers a tiny
//! line-JSON control protocol on a unix socket in the state directory:
//!
//! ```text
//! {"op":"ping"}        -> {"ok":true,"commit":"<hex>"}
//! {"op":"refresh"}     -> poll the workspace ref now; reply with the (possibly new) commit
//! {"op":"clear_upper"} -> drop all overlay state (post-snapshot / restore)
//! {"op":"shutdown"}    -> unmount and exit
//! ```

use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

#[cfg(target_os = "linux")]
use super::overlay::OverlayFs;
#[cfg(target_os = "linux")]
use std::sync::Arc;

/// Persisted per-mount state (`<state dir>/state.json`). No credentials: the daemon mints its
/// own from the same CLI auth context.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountState {
    pub project_id: String,
    pub repo: String,
    pub workspace_id: String,
    pub ref_name: String,
    pub mountpoint: PathBuf,
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

/// One control round-trip from a CLI command to the daemon.
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
#[cfg(target_os = "linux")]
const CREDENTIAL_ROTATE_MARGIN: Duration = Duration::from_secs(10 * 60);
#[cfg(target_os = "linux")]
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20 * 60);

#[cfg(target_os = "linux")]
fn expires_in(expires_at: &str) -> Duration {
    chrono::DateTime::parse_from_rfc3339(expires_at)
        .map(|t| {
            Duration::from_secs((t.timestamp() - chrono::Utc::now().timestamp()).max(60) as u64)
        })
        .unwrap_or(Duration::from_secs(30 * 60))
}

/// Run the daemon in the foreground of the current process. `tl fs mount` spawns this as a
/// detached child (`tl fs daemon --state-dir ... --mountpoint ...`).
pub async fn run(ctx: &CliContext, state_dir: &Path) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (ctx, state_dir);
        Err(CliError::usage(
            "This platform mounts via the TensorLake FSKit extension, which this build does not \
             ship yet; Linux builds mount via FUSE.",
        ))
    }
    #[cfg(target_os = "linux")]
    {
        run_fuse(ctx, state_dir).await
    }
}

#[cfg(target_os = "linux")]
async fn run_fuse(ctx: &CliContext, state_dir: &Path) -> Result<()> {
    use crate::commands::git::{artifact_storage_client, project_id};
    use gsvc_mount::{FsClient, MountCore, MountOptions};

    eprintln!("daemon: loading state from {}", state_dir.display());
    let state = load_mount_state(state_dir)?;
    eprintln!(
        "daemon: state ok (mountpoint {})",
        state.mountpoint.display()
    );
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

    eprintln!("daemon: client ok; building core");
    let core = MountCore::new(
        client,
        MountOptions {
            reference: state.ref_name.clone(),
            follow: true,
            poll_interval: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| CliError::usage(format!("mount init: {e}")))?;
    gsvc_mount::spawn_ref_watcher(&core, || {});
    let overlay = OverlayFs::new(core.clone(), state_dir)
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

    // The FUSE session must attach before the control socket exists: the socket answering is
    // what `tl fs mount` treats as success.
    eprintln!(
        "daemon: overlay ok; attaching fuse at {}",
        state.mountpoint.display()
    );
    let (mounted_tx, mounted_rx) = tokio::sync::oneshot::channel();
    let fuse =
        super::fusefs::WorkspaceFuse::new(overlay.clone(), tokio::runtime::Handle::current());
    let mountpoint = state.mountpoint.clone();
    let mp = mountpoint.clone();
    let served = tokio::task::spawn_blocking(move || fuse.run(&mp, mounted_tx));
    if mounted_rx.await.is_err() {
        // Session establishment failed; surface the real error.
        return match served.await {
            Ok(Ok(())) => Err(CliError::usage("fuse session ended before mounting")),
            Ok(Err(e)) => Err(CliError::usage(format!("fuse mount failed: {e}"))),
            Err(e) => Err(CliError::usage(format!("fuse thread: {e}"))),
        };
    }

    // Control socket (mount is live).
    let sock_path = control_socket(state_dir);
    let _ = std::fs::remove_file(&sock_path);
    let listener = tokio::net::UnixListener::bind(&sock_path)?;
    {
        let overlay = overlay.clone();
        let core = core.clone();
        let mountpoint = mountpoint.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let overlay = overlay.clone();
                let core = core.clone();
                let mountpoint = mountpoint.clone();
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
                            Ok(_) => {
                                serde_json::json!({ "ok": true, "commit": core.current_commit() })
                            }
                            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                        },
                        "clear_upper" => match overlay.clear_upper() {
                            Ok(()) => serde_json::json!({ "ok": true }),
                            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                        },
                        "shutdown" => {
                            let resp = serde_json::json!({ "ok": true });
                            let mut stream = reader.into_inner();
                            let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
                            unmount(&mountpoint);
                            return;
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

    // Serve until the kernel unmounts us.
    let served = served.await;
    let _ = std::fs::remove_file(&sock_path);
    match served {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(CliError::usage(format!("fuse session: {e}"))),
        Err(e) => Err(CliError::usage(format!("fuse thread: {e}"))),
    }
}

/// Ask the kernel to unmount; the blocked FUSE session then returns and the daemon exits.
#[cfg(target_os = "linux")]
fn unmount(mountpoint: &Path) {
    #[cfg(target_os = "linux")]
    let status = std::process::Command::new("fusermount")
        .arg("-u")
        .arg(mountpoint)
        .status();
    #[cfg(not(target_os = "linux"))]
    let status = std::process::Command::new("umount")
        .arg(mountpoint)
        .status();
    if let Err(e) = status {
        tracing::warn!("unmount of {} failed: {e}", mountpoint.display());
    }
}
