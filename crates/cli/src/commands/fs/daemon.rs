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
//!                         plus every probe expectation banked since the last drain
//!                         ("changed": [{path, present, size?}], "complete": bool) so callers
//!                         without a kernel notify channel (macOS) can converge the kernel
//!                         view themselves. `complete: false` means some refresh since the
//!                         last drain could not enumerate first-appearance names.
//! {"op":"clear_upper"} -> drop all overlay state (post-snapshot / restore)
//! {"op":"reindex"}     -> rebuild the overlay's dirty index from disk (post-restore)
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
use super::overlay::{KernelExpectation, OverlayFs, OverlayInval};
#[cfg(unix)]
use std::sync::Arc;

/// Pushes kernel-cache invalidations for a batch of overlay inos. On Linux this drives the FUSE
/// session's `Notifier` (which is what makes the binding's long entry/attr TTLs sound); on
/// macOS/FSKit there is no notify channel and the sink is a no-op (FSKit revalidates through
/// its own attribute protocol).
#[cfg(unix)]
type InvalSink = Arc<dyn Fn(Vec<OverlayInval>) + Send + Sync>;

/// Probe expectations produced by refreshes but not yet drained to an out-of-process prober.
/// On macOS there is no kernel notify channel; `tl fs sync` converges the kernel view by
/// probing paths from outside the mount, fed by the `refresh` control reply. Every poll site
/// deposits here — the background ref watcher or the auto-commit post-seal poll can consume
/// the very ref advance a concurrent `tl fs sync` triggered, and the expectations must not be
/// lost with it — and the control op drains the whole backlog into its reply.
#[cfg(unix)]
#[derive(Default)]
struct PendingProbe {
    /// Path → latest expectation (last write wins across deltas).
    expect: std::collections::BTreeMap<String, KernelExpectation>,
    /// Cleared when any absorbed delta had unknown appearance info (the stat-walk refresh
    /// fallback cannot see first-appearance names); reset to complete on drain.
    incomplete: bool,
}

/// Absorb one refresh delta: push kernel invalidations (Linux notify) and bank the probe
/// expectations for the next `refresh` control drain (macOS convergence).
#[cfg(unix)]
fn absorb_refresh(
    overlay: &OverlayFs,
    invalidate: &InvalSink,
    pending: &std::sync::Mutex<PendingProbe>,
    delta: &gsvc_mount::RefreshDelta,
) {
    let outputs = overlay.refresh_outputs(delta);
    {
        let mut p = pending.lock().expect("pending probe lock");
        if delta.appeared.is_none() {
            p.incomplete = true;
        }
        for e in outputs.expectations {
            p.expect.insert(e.path.clone(), e);
        }
        // A mount nobody syncs must not grow this without bound; dropping the backlog is
        // honest as long as the drain reports it was incomplete.
        if p.expect.len() > 65_536 {
            p.expect.clear();
            p.incomplete = true;
        }
    }
    invalidate(outputs.invals);
}

/// Persisted per-mount state (`<state dir>/state.json`). No credentials: the daemon mints its
/// own from the same CLI auth context.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountState {
    pub project_id: String,
    /// Scope for platform token minting with a PAT. Absent in state files written before
    /// path-addressed commands resolved their scope from the mount instead of the CWD.
    #[serde(default)]
    pub organization_id: Option<String>,
    /// Who the mount belongs to: every file is presented as owned by this uid/gid. Differs
    /// from the daemon's identity under `sudo tl fs mount` (daemon root, owner the invoking
    /// user) — the escape hatch for environments without unprivileged FUSE. Absent in state
    /// files from before that; the daemon then presents its own identity.
    #[serde(default)]
    pub owner_uid: Option<u32>,
    #[serde(default)]
    pub owner_gid: Option<u32>,
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
    /// Periodic auto-commit: the daemon seals the overlay's dirty set into a snapshot commit
    /// every this many seconds. The overlay is kept (only `tl fs snapshot` seals-and-clears),
    /// so writes racing an auto-commit are never dropped — they ride the next one. Absent on
    /// mounts that didn't opt in and in state files from before the feature.
    #[serde(default)]
    pub auto_commit_interval_secs: Option<u64>,
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
/// detached child (`tl fs daemon --state-dir ...`) with stderr pointed at the state dir's
/// `daemon.log`.
pub async fn run(ctx: &CliContext, state_dir: &Path, log_level: &str) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (ctx, state_dir, log_level);
        Err(CliError::usage(
            "tl fs mount is supported on Linux (FUSE) and macOS (FSKit) only.",
        ))
    }
    #[cfg(unix)]
    {
        init_logging(log_level)?;
        run_mount(ctx, state_dir).await
    }
}

/// Install the daemon's tracing subscriber, writing to stderr — which the detached spawn
/// redirects to the state dir's `daemon.log` (foreground runs log to the terminal). Without
/// this every `tracing::warn!` in the daemon is silently discarded.
#[cfg(unix)]
fn init_logging(level: &str) -> Result<()> {
    use std::str::FromStr;
    let level = tracing_subscriber::filter::LevelFilter::from_str(level).map_err(|_| {
        CliError::usage(format!(
            "invalid --log-level {level:?} (use off, error, warn, info, debug, or trace)"
        ))
    })?;
    // try_init: the foreground path may run inside a process that already installed a
    // subscriber; keep whatever is there rather than panic.
    let _ = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
    Ok(())
}

#[cfg(unix)]
async fn run_mount(ctx: &CliContext, state_dir: &Path) -> Result<()> {
    use crate::commands::git::{artifact_storage_client, project_id};
    use gsvc_mount::{FsClient, MountCore, MountOptions};

    let state = load_mount_state(state_dir)?;
    let sdk = artifact_storage_client(ctx)?;
    let project = project_id(ctx)?;

    // Initial credential: the dev override, or a fresh mint.
    let (git_username, token, mut expires_at) = match std::env::var("TENSORLAKE_GIT_TOKEN") {
        Ok(token) => (
            std::env::var("TENSORLAKE_GIT_USERNAME").unwrap_or_else(|_| "t".to_string()),
            token,
            None,
        ),
        Err(_) => {
            let cred = sdk
                .mint_token_for_repo(&project, Some(&state.repo))
                .await?
                .into_inner();
            (cred.git_username, cred.token, Some(cred.expires_at))
        }
    };
    // The daemon's long-lived `(user, token)` credential: heartbeats and auto-commits read it,
    // and the rotation task below swaps it in place before expiry — a static copy would start
    // failing an hour into the mount's life.
    let api_creds = Arc::new(std::sync::Mutex::new((git_username, token.clone())));

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
        let creds = api_creds.clone();
        tokio::spawn(async move {
            loop {
                let due = expires_in(expires_at.as_deref().unwrap_or_default())
                    .saturating_sub(CREDENTIAL_ROTATE_MARGIN);
                tokio::time::sleep(due.max(Duration::from_secs(60))).await;
                match sdk.mint_token_for_repo(&project, Some(&repo)).await {
                    Ok(cred) => {
                        let cred = cred.into_inner();
                        rotate.set_token(Some(cred.token.clone()));
                        *creds.lock().expect("creds lock") = (cred.git_username, cred.token);
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
        let creds = api_creds.clone();
        tokio::spawn(async move {
            loop {
                let (user, token) = creds.lock().expect("creds lock").clone();
                if let Err(e) = sdk
                    .workspace_heartbeat(&project, &repo, &user, &token, &ws)
                    .await
                {
                    tracing::warn!("workspace heartbeat failed: {e}");
                }
                tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            }
        });
    }

    let mountpoint = state.mountpoint.clone();

    let owner = (
        state.owner_uid.unwrap_or_else(|| unsafe { libc::getuid() }),
        state.owner_gid.unwrap_or_else(|| unsafe { libc::getgid() }),
    );

    // Attach the kernel: platform-specific. Only after this succeeds does the control socket
    // exist — the socket answering is what `tl fs mount` treats as success.
    let (served, invalidate) = attach(overlay.clone(), &mountpoint, owner).await?;
    tracing::info!(
        repo = %state.repo,
        workspace = %state.workspace_id,
        mountpoint = %mountpoint.display(),
        commit = %core.current_commit(),
        "mount daemon serving"
    );

    // Expectations banked by every poll site below, drained by the `refresh` control op.
    let pending = Arc::new(std::sync::Mutex::new(PendingProbe::default()));

    // Follow the workspace ref, pushing each refresh's exact delta to the kernel. Spawned after
    // attach because the invalidation sink is born with the kernel session.
    {
        let overlay = overlay.clone();
        let invalidate = invalidate.clone();
        let pending = pending.clone();
        gsvc_mount::spawn_ref_watcher(&core, move |delta| {
            absorb_refresh(&overlay, &invalidate, &pending, &delta);
        });
    }

    // Auto-commit: seal dirty paths into snapshot commits every interval, event-driven. The
    // overlay records every mutation in its dirty index, so nothing is ever scanned — an idle
    // tick is one atomic load. Each seal pushes only paths touched since the last sealed
    // generation: everything sealed earlier is already served by the lower (the workspace ref
    // advances with each snapshot), so commits are incremental deltas, and unchanged dirty
    // files are never re-hashed or re-sent. The overlay is deliberately NOT cleared — the
    // on-demand snapshot's clear-after-push is only safe with writes quiesced; here the upper
    // keeps shadowing the byte-identical sealed content.
    if let Some(secs) = state.auto_commit_interval_secs
        && !state.read_only()
    {
        use tensorlake::artifact_storage::ingest::{PushOptions, PushSource};
        let sdk = sdk.clone();
        let creds = api_creds.clone();
        let (project, repo, ws) = (
            project.clone(),
            state.repo.clone(),
            state.workspace_id.clone(),
        );
        let state_dir = state_dir.to_path_buf();
        let mountpoint = mountpoint.clone();
        let overlay = overlay.clone();
        let core = core.clone();
        let invalidate = invalidate.clone();
        let pending = pending.clone();
        tokio::spawn(async move {
            let mut sealed_gen = 0u64;
            // The overlay epoch this sealer's caches describe. clear_upper (manual snapshot,
            // restore) and rebuild_dirty_index rewrite the overlay's world out-of-band; every
            // cache below is a claim about the old world and dies with it.
            let mut seen_epoch = overlay.epoch();
            // Upserts of not-yet-confirmed seals, in seal order, each tagged with its commit:
            // the guard set for deletes racing the lower's advance past their seal (see
            // resolve_seal's tombstone arm). Confirmation-based — a set is only dropped once
            // the lower is observed at (or past) its seal — because eviction-by-count expires
            // the guard exactly when index materialization lags behind hot pushes. Memory is
            // bounded by the unconfirmed window, not a fixed depth.
            let mut recent_seals: Vec<(String, std::collections::HashSet<String>)> = Vec::new();
            // Chunk lists from previous seals (path -> the pushed CDC chunk list): the append
            // fast path's memory. A re-touched file whose writes never went below a cached
            // boundary seals as a `StablePrefix` — only bytes past that boundary are re-read.
            // Daemon-local; a restart just means one full-cost seal per file to re-learn.
            let mut chunk_cache: std::collections::HashMap<String, ChunkList> =
                std::collections::HashMap::new();
            loop {
                tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
                let epoch = overlay.epoch();
                if epoch != seen_epoch {
                    seen_epoch = epoch;
                    chunk_cache.clear();
                    recent_seals.clear();
                }
                // Drop guard sets the lower has caught up with: the followed ref only moves
                // along this workspace's snapshots, so matching the current lower commit
                // confirms it and everything sealed before it.
                let lower = core.current_commit();
                if let Some(i) = recent_seals
                    .iter()
                    .rposition(|(commit, _)| *commit == lower)
                {
                    recent_seals.drain(..=i);
                }
                // Renames a previous seal published but never consumed (crash, or the lower
                // lagged past our post-seal check) dangle once the lower advances; reap them
                // before anything resolves through the table.
                if overlay.has_redirects() {
                    match overlay.reap_sealed_redirects().await {
                        Ok(consumed) if !consumed.is_empty() => {
                            eprintln!("auto-commit: reaped {} sealed rename(s)", consumed.len());
                        }
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("auto-commit: reaping sealed renames failed: {e}");
                            continue;
                        }
                    }
                }
                let delta = overlay.dirty_since(sealed_gen);
                let watermark = delta.watermark;
                if delta.is_empty() && !overlay.has_redirects() {
                    sealed_gen = watermark;
                    continue;
                }
                // Resolution reads ignore files through the mountpoint — FUSE round-trips
                // served by this very process. Run it on the blocking pool so it can never
                // starve the runtime workers serving it.
                let recently: std::collections::HashSet<String> = recent_seals
                    .iter()
                    .flat_map(|(_, set)| set)
                    .cloned()
                    .collect();
                let cached: std::collections::HashMap<String, ChunkList> = delta
                    .upserts
                    .iter()
                    .filter_map(|(path, _)| {
                        chunk_cache
                            .get(path)
                            .map(|chunks| (path.clone(), chunks.clone()))
                    })
                    .collect();
                let (sd, mp) = (state_dir.clone(), mountpoint.clone());
                let resolved = tokio::task::spawn_blocking(move || {
                    resolve_seal(&sd, &mp, &delta, &recently, &cached)
                })
                .await;
                // eprintln, not tracing: the daemon installs no subscriber, and its stderr is
                // the state dir's daemon.log — the one place a user can see an async flush fail.
                let mut resolved = match resolved {
                    Ok(Ok(resolved)) => resolved,
                    Ok(Err(e)) => {
                        eprintln!("auto-commit: resolving the dirty delta failed: {e}");
                        continue;
                    }
                    Err(e) => {
                        eprintln!("auto-commit: resolution task failed: {e}");
                        continue;
                    }
                };
                if overlay.epoch() != seen_epoch {
                    // clear_upper/restore raced this tick: the resolution described a world
                    // that no longer exists — publishing it would delete files a manual
                    // snapshot just sealed. Undo the whiteouts the tombstone arm wrote and
                    // start over next tick (the epoch check up top clears the caches).
                    for path in &resolved.tombstoned {
                        let _ = std::fs::remove_file(state_dir.join("wh").join(path));
                    }
                    if !resolved.tombstoned.is_empty() {
                        invalidate(overlay.invals_for(&resolved.tombstoned));
                    }
                    continue;
                }
                if !resolved.tombstoned.is_empty() {
                    // The on-disk merged view already flipped when resolve wrote the
                    // whiteouts; tell the kernel now — deferring to push success would leave
                    // stale positive dentries until TTL if the push fails (the retry routes
                    // through the plain-deletes arm and never re-lists these).
                    invalidate(overlay.invals_for(&resolved.tombstoned));
                }
                // Pending directory renames seal as by-oid references: every file the
                // destination serves from the lower commits by blob oid (nothing uploads),
                // alongside the source delete the whiteout already produced. Expansion
                // failing leaves the whole delta pending — publishing the source delete
                // without the destination would lose the subtree.
                let redirect_seals = if overlay.has_redirects() {
                    match overlay.expand_redirects().await {
                        Ok(seals) => seals,
                        Err(e) => {
                            eprintln!(
                                "auto-commit: expanding pending renames failed (will retry): {e}"
                            );
                            continue;
                        }
                    }
                } else {
                    Vec::new()
                };
                if resolved.files.is_empty() && redirect_seals.is_empty() {
                    // The whole delta was ignored paths, bare directories, or files that were
                    // born and died between seals: sealed through, nothing to publish.
                    sealed_gen = watermark;
                    overlay.prune_dirty(watermark);
                    continue;
                }
                {
                    // An upper copy-up under a renamed tree shadows the lower file; the
                    // resolved walk already carries it.
                    let have: std::collections::HashSet<String> =
                        resolved.files.iter().map(|f| f.repo_path.clone()).collect();
                    for seal in &redirect_seals {
                        for file in &seal.files {
                            if !have.contains(&file.path) {
                                resolved.files.push(
                                    tensorlake::artifact_storage::ingest::PushFile {
                                        repo_path: file.path.clone(),
                                        source: PushSource::KnownOid(file.oid.clone()),
                                        mode: Some(file.mode),
                                        delete: false,
                                    },
                                );
                            }
                        }
                    }
                }
                // Final validity check on every stable prefix: a write below the boundary
                // that landed after the delta snapshot voids the stability claim — sealing
                // it would publish a prefix+tail chimera that never existed on disk. Demote
                // to a full read; the racing write's entry stays pending for the next tick.
                for file in &mut resolved.files {
                    let PushSource::StablePrefix {
                        path,
                        stable_chunks,
                    } = &file.source
                    else {
                        continue;
                    };
                    let stable_len: u64 = stable_chunks.iter().map(|(_, s)| *s as u64).sum();
                    if overlay.min_write_offset(&file.repo_path).unwrap_or(0) < stable_len {
                        file.source = PushSource::Path(path.clone());
                    }
                }
                let (user, token) = creds.lock().expect("creds lock").clone();
                let delete_paths: Vec<String> = resolved
                    .files
                    .iter()
                    .filter(|f| f.delete)
                    .map(|f| f.repo_path.clone())
                    .collect();
                match sdk
                    .push_files(
                        &project,
                        &repo,
                        &user,
                        &token,
                        resolved.files,
                        PushOptions {
                            message: "tl fs auto-commit".to_string(),
                            workspace_snapshot: Some(ws.clone()),
                            collect_file_chunks: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(report) => {
                        sealed_gen = watermark;
                        overlay.prune_dirty(watermark);
                        let report = report.into_inner();
                        recent_seals.push((report.commit.clone(), resolved.sealed_upserts));
                        // Remember what each file's content chunked to; a blunt cap bounds
                        // daemon memory (a full re-learn is just one full-cost seal per file).
                        for (path, chunks) in &report.file_chunks {
                            chunk_cache.insert(path.clone(), chunks.clone());
                        }
                        for path in &delete_paths {
                            chunk_cache.remove(path);
                        }
                        if chunk_cache.len() > 8192 {
                            chunk_cache.clear();
                        }
                        // Advance the lower to the sealed commit now instead of waiting out
                        // the follow poll: from here on, a delete of a just-sealed path sees
                        // lower presence and whiteouts normally. Best-effort — the guard
                        // above holds every unconfirmed seal, however long the lower lags.
                        match core.poll_ref().await {
                            Ok(Some(refresh)) => {
                                absorb_refresh(&overlay, &invalidate, &pending, &refresh)
                            }
                            Ok(None) => {}
                            Err(e) => eprintln!(
                                "auto-commit: post-seal refresh failed (follow poll catches \
                                 up): {e}"
                            ),
                        }
                        // Published renames are consumed only once the lower serves the
                        // sealed commit: the new tree carries their destinations directly
                        // and drops their sources, so remapping through the entry would
                        // dangle from here on — and conversely, consuming against an older
                        // commit would make the destinations unreachable.
                        if !redirect_seals.is_empty() {
                            if core.current_commit() == report.commit {
                                let dsts: Vec<String> =
                                    redirect_seals.iter().map(|s| s.dst.clone()).collect();
                                if let Err(e) = overlay.consume_redirects(&dsts) {
                                    eprintln!("auto-commit: consuming sealed renames failed: {e}");
                                }
                            } else {
                                eprintln!(
                                    "auto-commit: lower has not reached sealed snapshot {}; \
                                     pending renames stay recorded (next seal republishes \
                                     them idempotently)",
                                    report.commit
                                );
                            }
                        }
                        eprintln!("auto-commit sealed snapshot {}", report.commit);
                    }
                    Err(e) => eprintln!("auto-commit push failed (will retry): {e}"),
                }
            }
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
        let pending = pending.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let overlay = overlay.clone();
                let core = core.clone();
                let mountpoint = mountpoint.clone();
                let invalidate = invalidate.clone();
                let pending = pending.clone();
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
                                    absorb_refresh(&overlay, &invalidate, &pending, &delta);
                                }
                                // The advance may be the seal that published pending renames
                                // (`tl fs snapshot` refreshes before clearing the overlay);
                                // reap so nothing remaps through consumed entries.
                                if overlay.has_redirects()
                                    && let Err(e) = overlay.reap_sealed_redirects().await
                                {
                                    eprintln!("refresh: reaping sealed renames failed: {e}");
                                }
                                // Drain every expectation banked since the last drain — not
                                // just this poll's. A background poll may have consumed the
                                // very ref advance this caller triggered; its probe list must
                                // ride out on this reply or macOS never converges it.
                                let (changed, complete) = {
                                    let mut p = pending.lock().expect("pending probe lock");
                                    let changed: Vec<KernelExpectation> =
                                        std::mem::take(&mut p.expect).into_values().collect();
                                    (changed, !std::mem::replace(&mut p.incomplete, false))
                                };
                                serde_json::json!({
                                    "ok": true,
                                    "commit": core.current_commit(),
                                    "changed": changed,
                                    "complete": complete,
                                })
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
                        // The upper was mutated out-of-band (restore writes into the state dir
                        // from the CLI process); rebuild the dirty index from disk so an
                        // auto-commit mount seals the new state.
                        "reindex" => match overlay.rebuild_dirty_index() {
                            Ok(()) => serde_json::json!({ "ok": true }),
                            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                        },
                        // Pending directory renames, expanded to the by-oid upserts a seal
                        // must publish (`tl fs snapshot` merges these into its push).
                        "expand_redirects" => match overlay.expand_redirects().await {
                            Ok(seals) => serde_json::json!({ "ok": true, "seals": seals }),
                            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                        },
                        "shutdown" => {
                            // Unmount BEFORE replying: the reply is the CLI's signal that the
                            // kernel released the volume (the slow phase on macOS — fskitd
                            // teardown). A busy volume (a shell cd'd inside) keeps the daemon
                            // serving — exiting with the volume still attached is how zombie
                            // mounts are born.
                            tracing::info!(mountpoint = %mountpoint.display(), "shutdown requested; unmounting");
                            if unmount(&mountpoint).await {
                                let resp = serde_json::json!({ "ok": true });
                                let mut stream = reader.into_inner();
                                let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
                                // Orderly FIN before exiting: dying with the reply still in
                                // flight can surface client-side as a lost reply (measured on
                                // Linux; the CLI then double-checks the mount table).
                                let _ = stream.shutdown().await;
                                // Exit outright: session-wait is not guaranteed to return after
                                // an external unmount (observed leaked daemons on Linux), and
                                // the daemon's one job is over.
                                std::process::exit(0);
                            }
                            tracing::warn!(mountpoint = %mountpoint.display(), "unmount refused: volume busy");
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

    // ^C / SIGTERM: detach before dying. Without this the volume outlives the process — on
    // macOS the FSKit extension proxies to this daemon over TCP, so the kernel keeps serving
    // the mountpoint as ECONNREFUSED forever (and a killed FUSE daemon leaves an ENOTCONN
    // mount on Linux). Mirrors the `shutdown` op: unmount first, refuse to die on a busy
    // volume — a second signal force-exits, accepting the zombie (`tl fs unmount` clears it).
    {
        let mountpoint = mountpoint.clone();
        let sock_path = sock_path.clone();
        let pid_path = pid_file(state_dir);
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            let (Ok(mut int), Ok(mut term)) = (
                signal(SignalKind::interrupt()),
                signal(SignalKind::terminate()),
            ) else {
                return;
            };
            tokio::select! { _ = int.recv() => {}, _ = term.recv() => {} }
            eprintln!("unmounting {} ...", mountpoint.display());
            if unmount(&mountpoint).await {
                // The pid file dies with us: left behind, a recycled pid would make a later
                // `tl fs unmount` wait on (and then SIGKILL) an unrelated process.
                let _ = std::fs::remove_file(&sock_path);
                let _ = std::fs::remove_file(&pid_path);
                std::process::exit(0);
            }
            eprintln!(
                "the volume is busy (something is still using it); close shells and editors \
                 inside it, or send the signal again to exit anyway (the volume then stays \
                 attached until `tl fs unmount {}`)",
                mountpoint.display()
            );
            tokio::select! { _ = int.recv() => {}, _ = term.recv() => {} }
            std::process::exit(1);
        });
    }

    // Serve until the kernel lets go of the mountpoint.
    let result = served.wait().await;
    if let Err(e) = &result {
        tracing::error!("mount session ended with error: {e}");
    } else {
        tracing::info!("mount session ended");
    }
    let _ = std::fs::remove_file(&sock_path);
    result
}

/// fskit_agent's "Module … is disabled!" answer, matched in mount(8) output here and by the
/// setup probe in fs.rs — one marker so the two verdicts can never drift apart.
#[cfg(target_os = "macos")]
pub(crate) const MODULE_DISABLED_MARKER: &str = "is disabled";

/// A pushed file's CDC chunk list, as returned in `PushReport::file_chunks`.
#[cfg(unix)]
type ChunkList = Vec<([u8; 32], u32)>;

/// One tick's seal work, resolved from the overlay's event delta against the on-disk overlay
/// state. Produced by [`resolve_seal`].
#[cfg(unix)]
struct ResolvedSeal {
    files: Vec<tensorlake::artifact_storage::ingest::PushFile>,
    /// Paths whose content this seal publishes — the next ticks' resurrection guard.
    sealed_upserts: std::collections::HashSet<String>,
    /// Vanished-but-recently-sealed paths that got a whiteout written here; their merged view
    /// flipped without a kernel-visible operation, so the kernel needs invalidations.
    tombstoned: Vec<String>,
}

/// Resolve an event delta into upload-ready push files. The dirty index's kinds are routing
/// hints; the on-disk overlay is the authority — a path is an upsert if the upper serves it,
/// a delete if a whiteout covers it, and skipped when it is a bare directory or ignored.
///
/// The subtle arm is the tombstone: a path sealed by a recent commit, then deleted before the
/// lower advanced to that commit. The unlink saw no lower presence, so no whiteout was written
/// — once the lower catches up the path would silently resurrect. Recognize it by membership
/// in the recent seals, write the whiteout the unlink would have, and publish the delete.
///
/// Runs on the blocking pool: the ignore rules read `.gitignore` files through the mountpoint,
/// which this very daemon serves.
#[cfg(unix)]
fn resolve_seal(
    state_dir: &Path,
    mount_root: &Path,
    delta: &super::overlay::DirtyDelta,
    recently_sealed: &std::collections::HashSet<String>,
    chunk_cache: &std::collections::HashMap<String, ChunkList>,
) -> crate::error::Result<ResolvedSeal> {
    let mut ignore = super::SnapshotIgnore::new(mount_root);
    let upper = state_dir.join("upper");
    let wh = state_dir.join("wh");
    let mut upserts: super::OverlayUpserts = Vec::new();
    let mut deletes: Vec<String> = Vec::new();
    let mut tombstoned: Vec<String> = Vec::new();
    let mut vanished: Vec<String> = Vec::new();

    for (path, _) in &delta.upserts {
        let abs = upper.join(path);
        let Ok(meta) = std::fs::symlink_metadata(&abs) else {
            // Gone from the upper with no delete event in this delta (a rename or unlink
            // racing the tick): route through the delete resolution below.
            vanished.push(path.clone());
            continue;
        };
        if meta.is_dir() && !meta.file_type().is_symlink() {
            // A directory upsert names a subtree (a directory rename lands one alongside its
            // per-child events; future bulk ops may not): publish its files so nothing under
            // it can be missed. The sort+dedup below collapses overlap with child events.
            // Empty directories still publish nothing — git has no empty trees.
            collect_dir_upserts(&upper, path, &mut ignore, &mut upserts)?;
            continue;
        }
        if ignore.is_ignored(path, false)? {
            continue;
        }
        upserts.push((path.clone(), abs, git_mode(&meta)));
    }
    for path in delta.deletes.iter().chain(vanished.iter()) {
        if upper.join(path).symlink_metadata().is_ok() {
            // Re-created since the event; its own upsert event covers it.
            continue;
        }
        if ignore.is_ignored(path, false)? {
            continue;
        }
        if whited_out_on_disk(&wh, path) {
            deletes.push(path.clone());
        } else if recently_sealed.contains(path) {
            super::write_whiteout(&wh, path)?;
            deletes.push(path.clone());
            tombstoned.push(path.clone());
        }
        // Neither: born and died locally between seals — nothing was ever published.
    }
    upserts.sort_by(|a, b| a.0.cmp(&b.0));
    upserts.dedup_by(|a, b| a.0 == b.0);
    deletes.sort();
    deletes.dedup();
    let mut files = super::overlay_push_files(&upserts, &deletes)?;

    // Append fast path: a file with a cached chunk list from its previous seal, whose writes
    // since then never went below a cached boundary, seals as a `StablePrefix` — the push
    // reads only bytes past that boundary. The cached FINAL chunk is never reused (it was cut
    // at the old EOF, not at a content-chosen boundary), and neither is anything at or past
    // the lowest written offset.
    let min_write: std::collections::HashMap<&str, u64> = delta
        .upserts
        .iter()
        .map(|(path, min)| (path.as_str(), *min))
        .collect();
    use tensorlake::artifact_storage::ingest::PushSource;
    for file in &mut files {
        if file.delete || file.mode == Some(0o120000) {
            continue;
        }
        let (Some(min_offset), Some(cached)) = (
            min_write.get(file.repo_path.as_str()),
            chunk_cache.get(&file.repo_path),
        ) else {
            continue;
        };
        let usable = &cached[..cached.len().saturating_sub(1)];
        let mut stable: ChunkList = Vec::new();
        let mut end = 0u64;
        for (hash, size) in usable {
            if end + *size as u64 > *min_offset {
                break;
            }
            end += *size as u64;
            stable.push((*hash, *size));
        }
        if stable.is_empty() {
            continue;
        }
        if let PushSource::Path(path) = &file.source {
            file.source = PushSource::StablePrefix {
                path: path.clone(),
                stable_chunks: stable,
            };
        }
    }

    Ok(ResolvedSeal {
        sealed_upserts: upserts.iter().map(|(p, _, _)| p.clone()).collect(),
        files,
        tombstoned,
    })
}

/// The git mode a local file publishes as (same policy as `enumerate_overlay`'s walk).
#[cfg(unix)]
fn git_mode(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    if meta.file_type().is_symlink() {
        0o120000
    } else if meta.permissions().mode() & 0o111 != 0 {
        0o100755
    } else {
        0o100644
    }
}

/// Recursively enqueue every non-ignored file/symlink under an upper directory as an upsert —
/// the resolution for directory-level events (renames especially), whose per-child events may
/// or may not exist.
#[cfg(unix)]
fn collect_dir_upserts(
    upper: &Path,
    dir_rel: &str,
    ignore: &mut super::SnapshotIgnore,
    upserts: &mut super::OverlayUpserts,
) -> crate::error::Result<()> {
    let abs_dir = upper.join(dir_rel);
    let Ok(read) = std::fs::read_dir(&abs_dir) else {
        return Ok(());
    };
    for entry in read.flatten() {
        let abs = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&abs) else {
            continue;
        };
        let rel = format!("{dir_rel}/{}", entry.file_name().to_string_lossy());
        if meta.is_dir() && !meta.file_type().is_symlink() {
            if !ignore.is_ignored(&rel, true)? {
                collect_dir_upserts(upper, &rel, ignore, upserts)?;
            }
        } else if !ignore.is_ignored(&rel, false)? {
            upserts.push((rel, abs, git_mode(&meta)));
        }
    }
    Ok(())
}

/// Whether a whiteout marker covers `path` (at the path or any ancestor) — the on-disk mirror
/// of the overlay's whiteout rule, so resolution can run without the overlay.
#[cfg(unix)]
fn whited_out_on_disk(wh: &Path, path: &str) -> bool {
    let mut probe = String::with_capacity(path.len());
    for component in path.split('/') {
        if !probe.is_empty() {
            probe.push('/');
        }
        probe.push_str(component);
        let marker_is_file = wh
            .join(&probe)
            .symlink_metadata()
            .map(|m| m.is_file())
            .unwrap_or(false);
        if marker_is_file {
            return true;
        }
    }
    false
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
async fn attach(
    overlay: Arc<OverlayFs>,
    mountpoint: &Path,
    owner: (u32, u32),
) -> Result<(Attached, InvalSink)> {
    let (mounted_tx, mounted_rx) = tokio::sync::oneshot::channel();
    let fuse = super::fusefs::WorkspaceFuse::new(overlay, tokio::runtime::Handle::current(), owner);
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
async fn attach(
    overlay: Arc<OverlayFs>,
    mountpoint: &Path,
    _owner: (u32, u32),
) -> Result<(Attached, InvalSink)> {
    // Ownership presentation is a Linux concern: FSKit user mounts never need sudo, so the
    // daemon and the human are the same identity.
    let server = super::vfsserver::serve(overlay)
        .await
        .map_err(|e| CliError::usage(format!("vfs server: {e}")))?;
    let url = format!("tlfs://127.0.0.1:{}/{}", server.port, server.secret);
    // nobrowse asks for MNT_DONTBROWSE: no Finder sidebar entry, no mds indexing crawl (the
    // classic macOS unmount-delayer), no .DS_Store turds in workspaces. Advisory for now —
    // fskitd on 26.5 accepts but does not apply it (mount table shows no nobrowse; measured) —
    // kept so the behavior arrives for free when FSKit honors it.
    let out = tokio::process::Command::new("/sbin/mount")
        .arg("-F")
        .arg("-t")
        .arg("tlfs")
        .arg("-o")
        .arg("nobrowse")
        .arg(&url)
        .arg(mountpoint)
        .output()
        .await?;
    if !out.status.success() {
        // mount's own words first (output() swallowed the inherited stderr), then guidance
        // matched to the failure: "Module … is disabled!" is fskit_agent answering from the
        // allowlist snapshot it took at launch — enablement written after that launch is
        // invisible to it until it restarts, which `tl fs setup` now does.
        let err = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        eprint!("{err}");
        return Err(CliError::usage(if err.contains(MODULE_DISABLED_MARKER) {
            "mount(8) failed: the extension is enabled on disk, but the running fskit_agent \
             predates the enablement. Re-run `tl fs setup` (it restarts the agent), or reboot."
        } else {
            "mount(8) failed: is the TensorLake file-system extension installed and enabled? \
             Run `tl fs setup`, then enable it under System Settings -> General -> Login Items \
             & Extensions -> File System Extensions."
        }));
    }
    Ok((
        Attached::FsKit {
            mountpoint: mountpoint.to_path_buf(),
        },
        Arc::new(|_| {}),
    ))
}

/// The kernel mount table, copied with MNT_NOWAIT instead of statfs(mountpoint): statfs
/// calls into the filesystem and BLOCKS (uninterruptibly) while an unmount of it is in
/// flight — the exact moment these questions get asked. Measured: a busy-volume unmount
/// wedged the daemon in statfs for 50 minutes. getfsstat with MNT_NOWAIT only copies
/// cached table entries and never touches any fs. The one copy of this unsafe buffer dance:
/// every mount-table question (is our volume attached? is anything attached here? is
/// fskit_agent serving something?) filters this snapshot.
#[cfg(target_os = "macos")]
pub(crate) fn mount_table() -> Vec<libc::statfs> {
    let count = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
    if count <= 0 {
        return Vec::new();
    }
    // Room for a few mounts appearing between the two calls; the kernel truncates to fit.
    let capacity = count as usize + 8;
    let mut stats: Vec<libc::statfs> = Vec::with_capacity(capacity);
    let bufsize = (capacity * std::mem::size_of::<libc::statfs>()) as libc::c_int;
    let written = unsafe { libc::getfsstat(stats.as_mut_ptr(), bufsize, libc::MNT_NOWAIT) };
    if written <= 0 {
        return Vec::new();
    }
    unsafe { stats.set_len(written as usize) };
    stats
}

/// Whether any volume (any filesystem type) is attached at `mountpoint`.
#[cfg(target_os = "macos")]
pub(crate) fn mounted_at(mountpoint: &str) -> bool {
    mount_table().iter().any(|sfs| {
        let name = unsafe { std::ffi::CStr::from_ptr(sfs.f_mntonname.as_ptr()) };
        name.to_bytes() == mountpoint.as_bytes()
    })
}

#[cfg(target_os = "macos")]
fn is_mounted(mountpoint: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let want = mountpoint.as_os_str().as_bytes();
    mount_table().iter().any(|sfs| {
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
pub(crate) async fn unmount(mountpoint: &Path) -> bool {
    // fuse3 systems ship only `fusermount3`, fuse2 systems only `fusermount` — try in that
    // order (measured: Ubuntu 24.04's fuse3 has no `fusermount` compat name, and the old
    // single-name spawn failed instantly, misreporting a free volume as busy). A root daemon
    // (`sudo tl fs mount`) unmounts directly with umount(8): the sudo path serves exactly the
    // environments where no fusermount helper exists at all.
    #[cfg(target_os = "linux")]
    let unmounters: &[(&str, &[&str])] = if unsafe { libc::geteuid() } == 0 {
        &[("umount", &[])]
    } else {
        &[("fusermount3", &["-u"]), ("fusermount", &["-u"])]
    };
    #[cfg(not(target_os = "linux"))]
    let unmounters: &[(&str, &[&str])] = &[("umount", &[])];
    let mut child = None;
    for (helper, args) in unmounters {
        let mut cmd = tokio::process::Command::new(helper);
        cmd.args(*args)
            .arg(mountpoint)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        match cmd.spawn() {
            Ok(spawned) => {
                child = Some(spawned);
                break;
            }
            Err(e) => tracing::warn!("could not spawn {helper}: {e}"),
        }
    }
    let Some(mut child) = child else {
        return !still_mounted(mountpoint);
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
pub(crate) fn still_mounted(mountpoint: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        is_mounted(mountpoint)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let path = mountpoint.to_string_lossy();
        std::fs::read_to_string("/proc/self/mounts")
            .map(|mounts| {
                mounts.lines().any(|line| {
                    // <source> <mountpoint> <fstype> …; ours are `tlfs <path> fuse …`. The
                    // source check is what keeps `tl fs unmount` from ever treating someone
                    // else's sshfs/rclone mount at the path as one of ours.
                    let mut fields = line.split_whitespace();
                    fields.next() == Some("tlfs") && fields.next() == Some(path.as_ref())
                })
            })
            .unwrap_or(false)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::super::overlay::DirtyDelta;
    use super::*;

    pub(super) fn state_with(upper: &[(&str, &str)], wh: &[&str]) -> tempfile::TempDir {
        let state = tempfile::tempdir().unwrap();
        for (path, content) in upper {
            let abs = state.path().join("upper").join(path);
            std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
            std::fs::write(abs, content).unwrap();
        }
        for path in wh {
            let abs = state.path().join("wh").join(path);
            std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
            std::fs::write(abs, b"").unwrap();
        }
        state
    }

    fn delta(upserts: &[&str], deletes: &[&str]) -> DirtyDelta {
        // Structural events pin min_write_offset to 0; extent-carrying cases build their own.
        DirtyDelta {
            upserts: upserts.iter().map(|s| (s.to_string(), 0)).collect(),
            deletes: deletes.iter().map(|s| s.to_string()).collect(),
            watermark: 1,
        }
    }

    fn no_cache() -> std::collections::HashMap<String, ChunkList> {
        std::collections::HashMap::new()
    }

    #[test]
    fn resolve_seal_walks_directory_upserts() {
        // A directory rename records dir-level events (plus per-child events); even with only
        // the dir event, the seal must publish every file under it — the pre-review bug lost
        // a renamed directory's entire contents from the snapshot lineage.
        let state = state_with(
            &[("moved/a.txt", "alpha"), ("moved/sub/b.txt", "beta")],
            &[],
        );
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join(".gitignore"), "*.tmp\n").unwrap();
        std::fs::write(state.path().join("upper/moved/junk.tmp"), "x").unwrap();

        let resolved = resolve_seal(
            state.path(),
            mount.path(),
            &delta(&["moved"], &[]),
            &std::collections::HashSet::new(),
            &no_cache(),
        )
        .unwrap();

        let mut published: Vec<&str> = resolved
            .files
            .iter()
            .map(|f| f.repo_path.as_str())
            .collect();
        published.sort();
        assert_eq!(published, vec!["moved/a.txt", "moved/sub/b.txt"]);
    }

    #[test]
    fn resolve_seal_routes_upserts_deletes_and_skips() {
        let state = state_with(
            &[("kept.txt", "hi"), ("dir/nested.txt", "deep")],
            &["gone.txt"],
        );
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join(".gitignore"), "*.tmp\n").unwrap();
        std::fs::write(state.path().join("upper/junk.tmp"), "x").unwrap();
        std::fs::create_dir_all(state.path().join("upper/empty-dir")).unwrap();

        let resolved = resolve_seal(
            state.path(),
            mount.path(),
            &delta(
                &["dir", "dir/nested.txt", "empty-dir", "junk.tmp", "kept.txt"],
                &["gone.txt"],
            ),
            &std::collections::HashSet::new(),
            &no_cache(),
        )
        .unwrap();

        let mut published: Vec<(&str, bool)> = resolved
            .files
            .iter()
            .map(|f| (f.repo_path.as_str(), f.delete))
            .collect();
        published.sort();
        // Directories and gitignored paths never publish; whiteouts publish as deletes.
        assert_eq!(
            published,
            vec![
                ("dir/nested.txt", false),
                ("gone.txt", true),
                ("kept.txt", false),
            ]
        );
        assert!(resolved.sealed_upserts.contains("kept.txt"));
        assert!(resolved.tombstoned.is_empty());
    }

    #[test]
    fn resolve_seal_tombstones_vanished_recently_sealed_paths() {
        // The resurrection race: a path sealed by the previous commit, then deleted before the
        // lower advanced — no upper file, no whiteout. The delete must still publish, and a
        // whiteout must be written so the local view stays deleted once the lower catches up.
        let state = state_with(&[], &[]);
        let mount = tempfile::tempdir().unwrap();
        let recently: std::collections::HashSet<String> = ["sealed-then-deleted.txt".to_string()]
            .into_iter()
            .collect();

        let resolved = resolve_seal(
            state.path(),
            mount.path(),
            &delta(&[], &["sealed-then-deleted.txt", "never-sealed.txt"]),
            &recently,
            &no_cache(),
        )
        .unwrap();

        let published: Vec<(&str, bool)> = resolved
            .files
            .iter()
            .map(|f| (f.repo_path.as_str(), f.delete))
            .collect();
        // The never-sealed path was born and died locally: nothing to publish for it.
        assert_eq!(published, vec![("sealed-then-deleted.txt", true)]);
        assert_eq!(resolved.tombstoned, vec!["sealed-then-deleted.txt"]);
        assert!(
            state.path().join("wh/sealed-then-deleted.txt").is_file(),
            "the whiteout the unlink would have written"
        );
    }

    #[test]
    fn resolve_seal_skips_recreated_paths_and_honors_ancestor_whiteouts() {
        let state = state_with(&[("back.txt", "again")], &["dead-dir"]);
        let mount = tempfile::tempdir().unwrap();

        let resolved = resolve_seal(
            state.path(),
            mount.path(),
            // back.txt carries a stale delete event but the upper serves it again; a child of
            // a whiteouted directory is covered by the ancestor marker.
            &delta(&[], &["back.txt", "dead-dir/child.txt"]),
            &std::collections::HashSet::new(),
            &no_cache(),
        )
        .unwrap();

        let published: Vec<(&str, bool)> = resolved
            .files
            .iter()
            .map(|f| (f.repo_path.as_str(), f.delete))
            .collect();
        assert_eq!(published, vec![("dead-dir/child.txt", true)]);
    }
}

#[cfg(all(test, unix))]
mod stable_prefix_tests {
    use tensorlake::artifact_storage::ingest::PushSource;

    use super::super::overlay::DirtyDelta;
    use super::tests::state_with;
    use super::*;

    fn resolve_with_cache(min_offset: u64) -> ResolvedSeal {
        let state = state_with(&[("log.bin", "0123456789")], &[]);
        let mount = tempfile::tempdir().unwrap();
        // The previous seal chunked the 10-byte file as 4+4+2; the trailing 2-byte chunk was
        // cut at the old EOF and must never be reused as a stable boundary.
        let cache: std::collections::HashMap<String, ChunkList> = [(
            "log.bin".to_string(),
            vec![([1u8; 32], 4), ([2u8; 32], 4), ([3u8; 32], 2)],
        )]
        .into_iter()
        .collect();
        resolve_seal(
            state.path(),
            mount.path(),
            &DirtyDelta {
                upserts: vec![("log.bin".to_string(), min_offset)],
                deletes: Vec::new(),
                watermark: 1,
            },
            &std::collections::HashSet::new(),
            &cache,
        )
        .unwrap()
    }

    fn stable_of(resolved: &ResolvedSeal) -> Option<Vec<u32>> {
        match &resolved.files[0].source {
            PushSource::StablePrefix { stable_chunks, .. } => {
                Some(stable_chunks.iter().map(|(_, s)| *s).collect())
            }
            _ => None,
        }
    }

    #[test]
    fn append_reuses_all_but_the_eof_cut_chunk() {
        // Writes started at the old EOF (10): both content-boundary chunks are stable.
        let resolved = resolve_with_cache(10);
        assert_eq!(stable_of(&resolved), Some(vec![4, 4]));
    }

    #[test]
    fn mid_file_write_keeps_only_chunks_fully_before_it() {
        // A write at offset 5 lands inside the second chunk: only the first survives.
        let resolved = resolve_with_cache(5);
        assert_eq!(stable_of(&resolved), Some(vec![4]));
    }

    #[test]
    fn structural_change_falls_back_to_a_full_read() {
        let resolved = resolve_with_cache(0);
        assert_eq!(stable_of(&resolved), None);
        assert!(matches!(resolved.files[0].source, PushSource::Path(_)));
    }
}
