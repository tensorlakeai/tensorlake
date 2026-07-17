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
//! {"op":"seal","message":?,"clear":?} -> publish the dirty generation only when the daemon's
//!                         background journal worker has already resolved, hashed,
//!                         compressed, uploaded, and validated it. Otherwise wake that
//!                         worker and return `{ok:true,pending:true,pending_watermark:N}`
//!                         immediately; the durable request publishes after preparation.
//!                         A ready generation performs only metadata publication and lower
//!                         advancement. The final [`SealReply`] (+`ok`) is
//!                         `{ok,clean,commit}` for a clean workspace,
//!                         `{ok,pending,pending_watermark,commit}` while preparing, or
//!                         `{ok,clean:false,commit,files,chunks_uploaded,chunks_total,sealed,
//!                         push_ms}` after publication — plus the same drained
//!                         "changed"/"complete" probe list as `refresh`. For native filesystems,
//!                         `clear:true` generation-safely trims only retained paths owned by the
//!                         published generation; later writes and ignored/local-only content
//!                         survive. Repository mounts retain the legacy whole-overlay clear.
//!                         This is what `tl fs snapshot` calls — manual snapshots and auto-commits
//!                         share one dirty watermark, resurrection guard, and chunk cache.
//! {"op":"clear_upper"} -> drop all overlay state without sealing (restore's reset; `tl fs
//!                         snapshot --clear` instead rides the seal op for coherence)
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
use super::overlay::{
    KernelExpectation, OverlayDirtySeed, OverlayFs, OverlayInval, OverlayMutationIntent,
    OverlayMutationJournal, TrimBoundaries,
};
#[cfg(unix)]
use std::sync::Arc;

#[cfg(unix)]
struct LocalStateMutationJournal(super::local_state::LocalState);

#[cfg(unix)]
impl OverlayMutationJournal for LocalStateMutationJournal {
    fn record_intents(
        &self,
        intents: &[OverlayMutationIntent],
    ) -> std::result::Result<(), gsvc_mount::MountError> {
        let intents = intents
            .iter()
            .map(|intent| match intent {
                OverlayMutationIntent::Upsert {
                    path,
                    min_write_offset,
                } => super::local_state::MutationIntent::Upsert {
                    path: path.clone(),
                    min_write_offset: *min_write_offset,
                },
                OverlayMutationIntent::Delete { path } => {
                    super::local_state::MutationIntent::Delete { path: path.clone() }
                }
                OverlayMutationIntent::Rename {
                    source,
                    destination,
                } => super::local_state::MutationIntent::Rename {
                    source: source.clone(),
                    destination: destination.clone(),
                },
            })
            .collect::<Vec<_>>();
        let result = self.0.record_mutations(&intents).map(|_| ());
        result.map_err(|error| gsvc_mount::MountError::Protocol(error.to_string()))
    }

    fn mark_rename_applied(
        &self,
        source: &str,
        destination: &str,
    ) -> std::result::Result<(), gsvc_mount::MountError> {
        self.0
            .mark_rename_applied(source, destination)
            .map_err(|error| gsvc_mount::MountError::Protocol(error.to_string()))
    }
}

#[cfg(unix)]
fn overlay_recovery_seed(
    recovery: &super::local_state::RecoveryDirtyState,
) -> Vec<OverlayDirtySeed> {
    recovery
        .paths
        .iter()
        .map(|path| OverlayDirtySeed {
            path: path.path.clone(),
            kind: match path.kind {
                super::local_state::DirtyKind::Upsert => super::overlay::DirtyKind::Upsert,
                super::local_state::DirtyKind::Delete => super::overlay::DirtyKind::Delete,
            },
            min_write_offset: path.min_write_offset,
            generation: path.sequence,
        })
        .collect()
}

#[cfg(unix)]
fn overlay_recovery_renames(
    recovery: &super::local_state::RecoveryDirtyState,
) -> Vec<(String, String, String, u64)> {
    let mut ordered = recovery
        .renames
        .iter()
        .filter(|rename| rename.applied)
        .cloned()
        .collect::<Vec<_>>();
    ordered.sort_by_key(|rename| rename.sequence);
    let mut live: std::collections::HashMap<String, (String, String, u64)> =
        std::collections::HashMap::new();
    for rename in ordered {
        let true_source = live
            .remove(&rename.local_from)
            .map(|(source, _, _)| source)
            .unwrap_or_else(|| rename.from.clone());
        live.remove(&rename.to);
        live.insert(rename.to, (true_source, rename.local_from, rename.sequence));
    }
    live.into_iter()
        .map(|(destination, (true_source, local_source, sequence))| {
            (destination, true_source, local_source, sequence)
        })
        .collect()
}

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
    // Divergence first: retained upper copies the branch moved past stop shadowing, so the
    // shadow filter inside refresh_outputs sees them gone and their invalidations flow.
    let divergence = overlay.absorb_divergence(delta);
    if !divergence.invals.is_empty() {
        invalidate(divergence.invals);
    }
    let mut outputs = overlay.refresh_outputs(delta);
    outputs.expectations.extend(divergence.expectations);
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
    /// Canonical repository-relative directory exposed as this Git mount's root.
    #[serde(default)]
    pub subtree: Option<String>,
    /// Server-resolved source for a stateless read-only Git view. Its presence record is
    /// observational only: it expires and never roots history.
    #[serde(default)]
    pub git_mount_source: Option<tensorlake::artifact_storage::workspaces::GitMountSource>,
    #[serde(default)]
    pub mount_presence_id: Option<String>,
    /// Native filesystem snapshots rather than Git commits and refs.
    #[serde(default)]
    pub native_filesystem: bool,
    /// A retained native snapshot mounted as an immutable point-in-time view. Unlike a normal
    /// read-only filesystem mount, this view never follows `fs/head`.
    #[serde(default)]
    pub pinned_snapshot: Option<String>,
    pub workspace_id: String,
    /// Identity of the crash-safe local mutation journal. Writable native filesystem mounts
    /// persist one UUID and reuse it when resuming the same state directory; copying or mixing a
    /// database with another mount then fails closed.
    #[serde(default)]
    pub local_state_uuid: Option<String>,
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
    /// every this many seconds. The overlay is kept (only `tl fs snapshot --clear` drops it),
    /// so writes racing an auto-commit are never dropped — they ride the next one. Absent on
    /// mounts that didn't opt in and in state files from before the feature.
    #[serde(default)]
    pub auto_commit_interval_secs: Option<u64>,
    /// The commit the followed ref resolved to in the create/attach response that produced
    /// this state file — a latency hint (`MountOptions::start_oid`) that lets the mount core
    /// overlap its serve probe with ref resolution. The ref answer stays authoritative, so an
    /// aged value (a hand-rerun `tl fs daemon` on an old state dir) is superseded at mount.
    /// Absent in older state files: the core resolves serially, one extra round trip.
    #[serde(default)]
    pub start_oid: Option<String>,
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
    let bytes = serde_json::to_vec_pretty(state)?;
    let pending = state_dir.join("state.json.next");
    let final_path = state_dir.join("state.json");
    {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&pending, &final_path)?;
    if let Ok(directory) = std::fs::File::open(state_dir) {
        let _ = directory.sync_all();
    }
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

/// One retained upper file's identity at the moment its content was resolved for a seal.
/// Capturing the stat at resolve time (not after the push) is what makes the record safe: a
/// write racing the push changes the file's mtime relative to this, so a mismatch always
/// classifies the racier state as dirty. The residual exposure is a same-size write inside
/// the filesystem's timestamp granularity — the classic racy-git window, accepted here for
/// the same reason git accepts it.
#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SealedStat {
    pub size: u64,
    pub mtime_secs: i64,
    pub mtime_nanos: u32,
    /// Git mode (040000/100644/100755/120000) — a chmod flips it without touching size or mtime.
    pub mode: u32,
}

#[cfg(unix)]
impl SealedStat {
    pub(crate) fn of(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        SealedStat {
            size: meta.size(),
            mtime_secs: meta.mtime(),
            mtime_nanos: meta.mtime_nsec() as u32,
            mode: git_mode(meta),
        }
    }
}

#[cfg(unix)]
fn sealed_index_from_local_state(
    local: &super::local_state::LocalState,
) -> std::result::Result<SealedIndex, super::local_state::LocalStateError> {
    let active_generation = local.active_generation()?;
    sealed_index_from_records(
        active_generation,
        local.generation(active_generation)?,
        local.sealed_baselines()?,
    )
}

#[cfg(unix)]
pub(crate) fn sealed_index_from_local_state_reader(
    local: &super::local_state::LocalStateReader,
) -> std::result::Result<SealedIndex, super::local_state::LocalStateError> {
    let active_generation = local.active_generation()?;
    sealed_index_from_records(
        active_generation,
        local.generation(active_generation)?,
        local.sealed_baselines()?,
    )
}

#[cfg(unix)]
fn sealed_index_from_records(
    active_generation: u64,
    active: Option<super::local_state::GenerationRecord>,
    baselines: Vec<super::local_state::SealedBaseline>,
) -> std::result::Result<SealedIndex, super::local_state::LocalStateError> {
    let active = active.ok_or_else(|| {
        super::local_state::LocalStateError::Corrupt(format!(
            "active generation {active_generation} is missing"
        ))
    })?;
    let mut index = SealedIndex {
        commit: active.base_snapshot.unwrap_or_default(),
        ..Default::default()
    };
    for baseline in baselines {
        match baseline.state {
            super::local_state::SealedPathState::Upsert { identity, .. } => {
                index.upserts.insert(
                    baseline.path,
                    SealedStat {
                        size: identity.size,
                        mtime_secs: identity.mtime_secs,
                        mtime_nanos: identity.mtime_nanos as u32,
                        mode: git_mode_from_raw(identity.mode),
                    },
                );
            }
            super::local_state::SealedPathState::Delete => {
                index.deletes.insert(baseline.path);
            }
        }
    }
    Ok(index)
}

/// The persisted seal record (`<state dir>/sealed.json`): every overlay path whose current
/// on-disk state a snapshot has published — upserts with the [`SealedStat`] identity of the
/// sealed content, deletes as inert-whiteout markers — plus the last sealed commit. The
/// sealer owns it (written after each successful push, under the sealer state lock); daemon
/// startup and the `reindex` op read it to absolve rebuild-marked dirt whose identity still
/// matches. Losing or corrupting the file is safe: everything degrades to the rebuild's
/// pessimistic all-dirty answer, and the next seal re-records (content dedup makes the
/// re-push free).
///
/// The save-after-every-seal timing is a CROSS-PROCESS CONTRACT, not an implementation
/// detail: the CLI's unmount/restore gate (`fs::overlay_losable_state`) stat-verifies the
/// overlay against this file — possibly with the daemon dead — to decide what is loss-free
/// to destroy. Batching or deferring saves would make retained content classify as
/// "uncovered" (spurious refusals at best, and it weakens a data-destruction gate).
#[cfg(unix)]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct SealedIndex {
    #[serde(default)]
    pub commit: String,
    #[serde(default)]
    pub upserts: std::collections::BTreeMap<String, SealedStat>,
    #[serde(default)]
    pub deletes: std::collections::BTreeSet<String>,
}

#[cfg(unix)]
impl SealedIndex {
    fn file(state_dir: &Path) -> PathBuf {
        state_dir.join("sealed.json")
    }

    pub(crate) fn load(state_dir: &Path) -> SealedIndex {
        std::fs::read(Self::file(state_dir))
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok())
            .unwrap_or_default()
    }

    pub(crate) fn save(&self, state_dir: &Path) -> std::io::Result<()> {
        let tmp = state_dir.join("sealed.json.tmp");
        std::fs::write(
            &tmp,
            serde_json::to_vec(self).expect("plain data serializes"),
        )?;
        std::fs::rename(&tmp, Self::file(state_dir))
    }

    fn reset(state_dir: &Path) {
        let _ = std::fs::remove_file(Self::file(state_dir));
    }
}

/// Remove metadata files superseded by the durable local-state database.
///
/// This runs only after the database's atomic legacy-import transaction has committed. Keeping
/// the cleanup outside that transaction is safe because none of these files is authoritative for
/// a native mount once `legacy_import_completed` is durable; retrying cleanup on the next startup
/// also closes a crash between commit and unlink.
#[cfg(unix)]
fn remove_legacy_native_snapshot_state(state_dir: &Path) -> std::io::Result<()> {
    for name in [
        "sealed.json",
        "sealed.json.tmp",
        "native-prepared.json",
        "native-prepared.json.tmp",
        "native-seal-request.json",
        "native-seal-request.json.tmp",
        "redirects.json",
        "redirects.json.tmp",
    ] {
        match std::fs::remove_file(state_dir.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn install_local_state_format_marker(state_dir: &Path) -> Result<()> {
    use std::io::Write as _;

    let marker = state_dir.join(super::LOCAL_STATE_FORMAT_MARKER);
    let database = state_dir.join(super::local_state::LOCAL_STATE_FILE);
    if marker.exists() {
        if !database.exists() {
            return Err(CliError::usage(format!(
                "durable local snapshot database {} is missing after this mount was converted; \
                 refusing to reinterpret retained upper bytes against a newer server head. Run \
                 `tl fs doctor {} --repair-journal` to rebuild a conservative generation.",
                database.display(),
                state_dir.display(),
            )));
        }
        return Ok(());
    }

    match std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&marker)
    {
        Ok(mut file) => {
            file.write_all(b"tensorlake-native-local-state-v3\n")?;
            file.sync_all()?;
            std::fs::File::open(state_dir)?.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Startup/reindex reconciliation. [`OverlayFs::rebuild_dirty_index`] pessimistically marks
/// every upper file an upsert and every whiteout a delete; absolve the ones whose on-disk
/// identity still matches their persisted seal record — they are retained sealed content, not
/// dirt, and without this every daemon restart re-reports (and the next seal re-hashes) the
/// whole ever-written set. Returns (absolved upserts, absolved deletes) for the startup log.
#[cfg(unix)]
fn reconcile_sealed(state_dir: &Path, overlay: &OverlayFs) -> (usize, usize) {
    let index = SealedIndex::load(state_dir);
    if index.upserts.is_empty() && index.deletes.is_empty() {
        return (0, 0);
    }
    // Clock snapshot BEFORE the stat pass: anything mutated after this proves nothing.
    let upto = overlay.current_generation();
    let (clean_upserts, clean_deletes) = sealed_survivors(state_dir, &index);
    overlay.absolve_clean(&clean_upserts, &clean_deletes, upto);
    (clean_upserts.len(), clean_deletes.len())
}

/// The sealed-index entries whose on-disk overlay state still matches the seal record: upserts
/// by exact [`SealedStat`] identity, deletes by the whiteout marker still being present.
#[cfg(unix)]
fn sealed_survivors(state_dir: &Path, index: &SealedIndex) -> (Vec<String>, Vec<String>) {
    let upper = state_dir.join("upper");
    let wh = state_dir.join("wh");
    let clean_upserts: Vec<String> = index
        .upserts
        .iter()
        .filter(|(path, sealed)| {
            std::fs::symlink_metadata(upper.join(path))
                .is_ok_and(|meta| SealedStat::of(&meta) == **sealed)
        })
        .map(|(path, _)| path.clone())
        .collect();
    let clean_deletes: Vec<String> = index
        .deletes
        .iter()
        .filter(|path| {
            wh.join(path)
                .symlink_metadata()
                .is_ok_and(|meta| meta.is_file())
        })
        .cloned()
        .collect();
    (clean_upserts, clean_deletes)
}

/// Request body of the `seal` control op — shared by the daemon's handler and the CLI's
/// client path (`fs::seal_via_daemon`) so the wire shape cannot drift between them.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct SealRequest {
    /// Stable client request id. Native snapshot publication persists this as the server
    /// idempotency key and retains a bounded success receipt after generation retirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Snapshot commit message; the daemon defaults it when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// After a successful seal, trim only retained paths owned by that published generation.
    /// Writes in a later generation and ignored/local-only content are never removed.
    #[serde(default)]
    pub clear: bool,
}

/// Final reply of the `seal` control op (the line after any `{"event": ...}` progress lines).
/// The daemon serializes exactly this (plus the `ok`/`changed`/`complete` envelope fields);
/// the CLI deserializes it with serde and treats missing or mistyped fields as an error —
/// never as defaults.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SealReply {
    /// Nothing was dirty: no commit was minted (`commit` is the current lower).
    pub clean: bool,
    /// The dirty generation is being prepared by the daemon's background journal worker.
    /// No snapshot was published and `commit` remains the currently served lower.
    #[serde(default)]
    pub pending: bool,
    /// Dirty-index watermark requested for background preparation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_watermark: Option<u64>,
    /// Durable request whose exact success this reply delivers. The CLI acknowledges it only
    /// after rendering the result to the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_request_id: Option<String>,
    pub commit: String,
    /// Sealed-only fields (absent on clean replies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunks_uploaded: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunks_total: Option<u64>,
    /// Every repo path the seal published (upserts and deletes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_ms: Option<u64>,
    /// Present iff the request set `clear`: the generation-owned retained paths and tombstones
    /// actually removed. Ignored/local-only files and later-generation writes are excluded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleared: Option<Vec<String>>,
}

/// Final reply of the `dirty` control op: the sealer's truthful dirty view — exactly what the
/// next `seal` would publish, resolved by the same dry-run walk (ignore rules applied,
/// directory events expanded, no side effects). Shared by the daemon's handler and every CLI
/// consumer (`status`, `promote`, `sync`, `diff`) so there is ONE definition of dirty.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct DirtyReply {
    /// Paths the next seal would upsert.
    #[serde(default)]
    pub upserts: Vec<String>,
    /// Paths the next seal would delete.
    #[serde(default)]
    pub deletes: Vec<String>,
    /// Pending committed-directory renames, `(from, to)`.
    #[serde(default)]
    pub renames: Vec<(String, String)>,
    /// The lower commit currently served.
    #[serde(default)]
    pub commit: String,
}

/// Destructive-gate view served by a live daemon. `sealed` comes from the same in-memory mirror
/// the native sealer rebuilt from redb at startup, avoiding both the obsolete sealed.json
/// authority and redb's intentional refusal to open a second read-only process while the daemon
/// owns the writable database lock.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct OverlaySafetyReply {
    pub dirty: DirtyReply,
    pub sealed: SealedIndex,
}

/// Final reply of the `trim` control op: retained (sealed-and-kept) overlay state dropped in
/// place, the non-destructive alternative to `clear_upper` — dirty and ignored files are never
/// touched. `held_open` lists sealed paths that could not be dropped and still shadow the
/// lower (a live writer's descriptor, or an unlink failure); the caller decides whether that
/// blocks (sync does).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct TrimReply {
    #[serde(default)]
    pub trimmed: u64,
    #[serde(default)]
    pub held_open: Vec<String>,
}

/// One control round-trip from a CLI command to the daemon. Mounts (and so daemons) exist
/// only on unix; elsewhere every control call reports the daemon as not running.
#[cfg(not(unix))]
pub async fn control(_state_dir: &Path, _op: &str) -> Result<serde_json::Value> {
    Err(CliError::usage(
        "tl fs mounts are supported on Linux (FUSE) and macOS (FSKit) only.",
    ))
}

/// [`control`] with an op payload (extra request fields alongside `"op"`).
#[cfg(not(unix))]
pub async fn control_with(
    _state_dir: &Path,
    _op: &str,
    _args: serde_json::Value,
) -> Result<serde_json::Value> {
    Err(CliError::usage(
        "tl fs mounts are supported on Linux (FUSE) and macOS (FSKit) only.",
    ))
}

/// [`control_with`] for line-streaming ops (`seal`).
#[cfg(not(unix))]
pub async fn control_streaming(
    _state_dir: &Path,
    _op: &str,
    _args: serde_json::Value,
    _on_event: impl FnMut(&str),
) -> Result<serde_json::Value> {
    Err(CliError::usage(
        "tl fs mounts are supported on Linux (FUSE) and macOS (FSKit) only.",
    ))
}

/// One control round-trip from a CLI command to the daemon.
#[cfg(unix)]
pub async fn control(state_dir: &Path, op: &str) -> Result<serde_json::Value> {
    control_with(state_dir, op, serde_json::Value::Null).await
}

/// One control round-trip carrying an op payload: `args` must be a JSON object (or null); its
/// fields ride in the request line alongside `"op"`. Plain-string ops (`control`) stay the
/// common case — older daemons ignore fields they don't know.
#[cfg(unix)]
pub async fn control_with(
    state_dir: &Path,
    op: &str,
    args: serde_json::Value,
) -> Result<serde_json::Value> {
    let sock = control_socket(state_dir);
    let mut stream = tokio::net::UnixStream::connect(&sock).await.map_err(|e| {
        CliError::usage(format!(
            "mount daemon is not running ({}): {e}",
            sock.display()
        ))
    })?;
    let mut request = serde_json::json!({ "op": op });
    if let serde_json::Value::Object(fields) = args {
        let obj = request.as_object_mut().expect("request is an object");
        for (k, v) in fields {
            obj.insert(k, v);
        }
    }
    stream.write_all(format!("{request}\n").as_bytes()).await?;
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

/// How long the streaming client waits without ANY line (event or reply) before declaring the
/// daemon wedged. The daemon emits push progress at least every ~100ms while hashing/uploading
/// and a keepalive event every ~15s through the sparse server-side commit phases, so a full
/// minute of silence means the seal is not making progress.
#[cfg(unix)]
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// One control round-trip for a line-streaming op (`seal`): the daemon writes zero or more
/// `{"event": "<message>"}` progress lines followed by the single final reply line. Each event
/// line invokes `on_event`; the final line is returned RAW — including `ok:false` failures —
/// so the caller can inspect structured fields (`code`) that a flattened error string would
/// lose. Plain-string ops keep the single-line [`control`]/[`control_with`] path.
#[cfg(unix)]
pub async fn control_streaming(
    state_dir: &Path,
    op: &str,
    args: serde_json::Value,
    mut on_event: impl FnMut(&str),
) -> Result<serde_json::Value> {
    let sock = control_socket(state_dir);
    let mut stream = tokio::net::UnixStream::connect(&sock).await.map_err(|e| {
        CliError::usage(format!(
            "mount daemon is not running ({}): {e}",
            sock.display()
        ))
    })?;
    let mut request = serde_json::json!({ "op": op });
    if let serde_json::Value::Object(fields) = args {
        let obj = request.as_object_mut().expect("request is an object");
        for (k, v) in fields {
            obj.insert(k, v);
        }
    }
    stream.write_all(format!("{request}\n").as_bytes()).await?;
    let mut reader = tokio::io::BufReader::new(stream);
    loop {
        let mut line = String::new();
        let read = tokio::time::timeout(STREAM_IDLE_TIMEOUT, reader.read_line(&mut line)).await;
        let n = match read {
            Ok(res) => res?,
            Err(_) => {
                return Err(CliError::usage(
                    "mount daemon stopped responding mid-seal; check `tl fs status`",
                ));
            }
        };
        if n == 0 {
            return Err(CliError::usage(format!(
                "mount daemon closed the connection before replying to {op}; check `tl fs status`"
            )));
        }
        let v: serde_json::Value = serde_json::from_str(line.trim())?;
        if let Some(event) = v.get("event").and_then(|e| e.as_str()) {
            on_event(event);
            continue;
        }
        return Ok(v);
    }
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

/// Mint a repo-scoped git credential and write it through to the CLI's on-disk cache, so a
/// daemon-side mint (startup fallback, rotation, auth recovery) also warms the cache the next
/// `tl fs` command reads — the same save `FsSession::open` does for CLI-side mints.
#[cfg(unix)]
async fn mint_and_cache(
    sdk: &tensorlake::artifact_storage::ArtifactStorageClient,
    api_url: &str,
    project: &str,
    repo: &str,
) -> Result<(String, String, String)> {
    let cred = sdk
        .mint_token_for_repo(project, Some(repo))
        .await?
        .into_inner();
    if let Err(e) = crate::config::files::save_git_credential(
        api_url,
        project,
        repo,
        &cred.git_username,
        &cred.token,
        &cred.expires_at,
    ) {
        tracing::warn!("could not cache minted git credential: {e}");
    }
    Ok((cred.git_username, cred.token, cred.expires_at))
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
/// this every `tracing::warn!` in the daemon is silently discarded. `tl fs mount` installs
/// the same subscriber in the CLI process, which is what surfaces its phase-timing lines.
#[cfg(not(unix))]
pub(crate) fn init_logging(_level: &str) -> Result<()> {
    Ok(())
}

/// Install the daemon's tracing subscriber, writing to stderr — which the detached spawn
/// redirects to the state dir's `daemon.log` (foreground runs log to the terminal). Without
/// this every `tracing::warn!` in the daemon is silently discarded.
#[cfg(unix)]
pub(crate) fn init_logging(level: &str) -> Result<()> {
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

    let started = std::time::Instant::now();
    let mut state = load_mount_state(state_dir)?;
    // Own the complete native local-state artifact set for the daemon's lifetime. Journal repair
    // replaces the database and retires old capture directories, so the database's own lock is
    // too narrow to serialize those operations. The offline doctor takes the same non-blocking
    // flock and refuses while this guard is live.
    let _local_state_writer_guard = if state.native_filesystem && !state.read_only() {
        Some(
            super::try_local_state_writer_lock(state_dir)?.ok_or_else(|| {
                CliError::usage(format!(
                    "native local snapshot state at {} is owned by another daemon or an \
                         offline journal repair",
                    state_dir.display()
                ))
            })?,
        )
    } else {
        None
    };
    if state_dir.join(super::LOCAL_STATE_REPAIR_MARKER).exists() {
        return Err(CliError::usage(format!(
            "an interrupted journal repair marker exists at {}; run `tl fs doctor {} \
             --repair-journal` before mounting",
            state_dir.join(super::LOCAL_STATE_REPAIR_MARKER).display(),
            state.mountpoint.display()
        )));
    }
    if state.native_filesystem && !state.read_only() && state.local_state_uuid.is_none() {
        // Local-only cutover for development mounts created before the durable journal. The
        // server-side filesystem is pre-production and needs no migration, but unsnapshotted
        // bytes under upper/ and wh/ must remain recoverable.
        state.local_state_uuid = Some(uuid::Uuid::new_v4().to_string());
        save_mount_state(state_dir, &state)?;
    }
    let sdk = artifact_storage_client(ctx)?;
    let project = project_id(ctx)?;

    // Initial credential: the dev override, the cache the mounting CLI just wrote (the mint
    // round trip through the platform ingress is the slowest single call in daemon startup),
    // or a fresh mint. A cached credential is adopted only with comfortable runway — anything
    // the rotation task would replace within minutes is minted fresh instead, so the
    // rotation schedule never starts inside its own margin. The rotation task re-mints
    // before whichever credential this is expires.
    let (git_username, token, mut expires_at, credential_source) =
        match tensorlake::artifact_storage::ArtifactStorageClient::git_credential_from_env() {
            Some(cred) => (cred.git_username, cred.token, None, "env"),
            None => {
                // Freshest cached entry wins across scopes: a stale repo-scoped token from an
                // earlier credential-helper use must not shadow the "*" token the mounting
                // CLI just wrote.
                let cached = [state.repo.as_str(), "*"]
                    .iter()
                    .filter_map(|scope| {
                        crate::config::files::load_git_credential(&ctx.api_url, &project, scope)
                    })
                    .max_by_key(|(_, _, expires_at)| expires_in(expires_at));
                match cached.filter(|(_, _, expires_at)| {
                    expires_in(expires_at) > CREDENTIAL_ROTATE_MARGIN + Duration::from_secs(5 * 60)
                }) {
                    Some((username, token, expires_at)) => {
                        (username, token, Some(expires_at), "cache")
                    }
                    None => {
                        let (username, token, expires_at) =
                            mint_and_cache(&sdk, &ctx.api_url, &project, &state.repo).await?;
                        (username, token, Some(expires_at), "mint")
                    }
                }
            }
        };
    tracing::info!(
        source = credential_source,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "mount daemon: credential ready"
    );
    // The daemon's long-lived `(user, token)` credential: heartbeats and auto-commits read it,
    // and the rotation task below swaps it in place before expiry — a static copy would start
    // failing an hour into the mount's life.
    let api_creds = Arc::new(std::sync::Mutex::new((git_username, token.clone())));

    let client = if state.native_filesystem {
        FsClient::new_native(
            sdk.git_base_url(),
            &state.project_id,
            &state.repo,
            Some(token),
        )
    } else {
        FsClient::new(
            sdk.git_base_url(),
            &state.project_id,
            &state.repo,
            Some(token),
        )
    }
    .map_err(|e| CliError::usage(format!("mount client: {e}")))?;
    // Keep a handle onto the shared credential slot for rotation.
    let rotating_client = client.clone();

    // Shared-ro sessions follow the branch itself; writable mounts follow their workspace ref.
    // Historical native views are different: their immutable snapshot id is the reference and
    // the watcher stays disabled, so a later head promotion cannot move the mount.
    let followed = state.pinned_snapshot.clone().unwrap_or_else(|| {
        state
            .follow_ref
            .clone()
            .unwrap_or_else(|| state.ref_name.clone())
    });
    let follows = if state.mount_presence_id.is_some() {
        // Branch/tag presences follow their exact canonical ref; raw commits are pinned.
        state.follow_ref.is_some()
    } else {
        state.pinned_snapshot.is_none()
    };
    let mount_options = MountOptions {
        reference: followed,
        subtree: state.subtree.clone(),
        follow: follows,
        poll_interval: Duration::from_secs(5),
        // Manifest-driven cache prefill in the background: first walks serve warm instead
        // of paying a per-directory crawl. Best-effort — a failed warmup just starts cold.
        warmup: !state.native_filesystem,
        // The create/attach response's commit: lets the core overlap its serve probe with
        // ref resolution (one startup round trip instead of two chained). The ref answer
        // stays authoritative, so a stale value is superseded at mount.
        start_oid: state.start_oid.clone(),
        ..Default::default()
    };
    // MountCore::new is also the adopted credential's first live use: a cached token can be
    // revoked before its recorded expiry (project auth-epoch rotation), which its expires_at
    // cannot reveal. On an auth failure, purge the poisoned cache, mint fresh, and retry once.
    let core = match MountCore::new(client.clone(), mount_options.clone()).await {
        Err(gsvc_mount::MountError::Status { status: 401, .. }) if credential_source == "cache" => {
            tracing::warn!("cached git credential rejected (revoked?); re-minting");
            crate::config::files::purge_git_credentials();
            let (username, token, fresh_expires) =
                mint_and_cache(&sdk, &ctx.api_url, &project, &state.repo).await?;
            rotating_client.set_token(Some(token.clone()));
            *api_creds.lock().expect("creds lock") = (username, token);
            expires_at = Some(fresh_expires);
            MountCore::new(client, mount_options).await
        }
        other => other,
    }
    .map_err(|e| CliError::usage(format!("mount init: {e}")))?;
    let local_state = if state.native_filesystem && !state.read_only() {
        install_local_state_format_marker(state_dir)?;
        let identity = super::local_state::LocalStateIdentity {
            project_id: state.project_id.clone(),
            filesystem: state.repo.clone(),
            workspace_id: state.workspace_id.clone(),
            store_uuid: state
                .local_state_uuid
                .clone()
                .expect("writable native mount has a local state UUID"),
        };
        let local = super::local_state::LocalState::open(
            state_dir.join(super::local_state::LOCAL_STATE_FILE),
            identity,
        )
        .map_err(|error| {
            CliError::usage(format!(
                "opening durable local snapshot state failed closed: {error}; \
                 run `tl fs doctor {}` before retrying",
                state.mountpoint.display()
            ))
        })?;
        if local.needs_legacy_import().map_err(|error| {
            CliError::usage(format!(
                "checking durable local snapshot-state cutover failed closed: {error}"
            ))
        })? {
            // This is the only automatic overlay walk in the durable format: conservatively
            // import a pre-database mount's unsnapshotted local state before the kernel attaches.
            let legacy = OverlayFs::new(core.clone(), state_dir, false)
                .map_err(|error| CliError::usage(format!("legacy overlay import: {error}")))?;
            // Do not reconcile against sealed.json here. The old stat-based baseline cannot
            // distinguish a same-size/same-timestamp edit in its racy window, and the durable
            // database has not imported those baselines. Treat every upper entry and whiteout as
            // dirty once during cutover: an extra deduplicated upload is safe; dropping a local
            // byte because a legacy identity happened to match is not.
            let delta = legacy.dirty_since(0);
            let mut mutations = Vec::with_capacity(
                delta.upserts.len() + delta.deletes.len() + legacy.redirect_entries().len(),
            );
            mutations.extend(delta.upserts.into_iter().map(|(path, min_write_offset)| {
                super::local_state::LegacyMutation::Upsert {
                    path,
                    min_write_offset,
                }
            }));
            mutations.extend(
                delta
                    .deletes
                    .into_iter()
                    .map(|path| super::local_state::LegacyMutation::Delete { path }),
            );
            mutations.extend(
                legacy
                    .redirect_entries()
                    .into_iter()
                    .map(|(to, from)| super::local_state::LegacyMutation::Rename { from, to }),
            );
            let imported_dirty_paths = mutations.len();
            let imported = local
                .import_legacy_once(super::local_state::LegacyImport {
                    base_snapshot: Some(core.current_commit()),
                    mutations,
                })
                .map_err(|error| {
                    CliError::usage(format!(
                        "importing pre-database local snapshot state failed closed: {error}"
                    ))
                })?;
            tracing::info!(
                imported,
                imported_dirty_paths,
                "mount: durable local snapshot state initialized"
            );
        }
        remove_legacy_native_snapshot_state(state_dir).map_err(|error| {
            CliError::usage(format!(
                "removing obsolete pre-database snapshot metadata failed: {error}"
            ))
        })?;
        let owned_generations: std::collections::BTreeSet<u64> = local
            .artifacts()
            .map_err(|error| {
                CliError::usage(format!(
                    "reading capture ownership for recovery failed closed: {error}"
                ))
            })?
            .into_iter()
            .map(|artifact| artifact.generation)
            .collect();
        let (reclaimed_captures, reclaimed_bytes) =
            super::generation_capture::reclaim_orphan_generation_captures(
                state_dir,
                &owned_generations,
            )
            .map_err(|error| {
                CliError::usage(format!(
                    "reclaiming orphaned generation captures failed closed: {error}"
                ))
            })?;
        if reclaimed_captures > 0 {
            tracing::info!(
                reclaimed_captures,
                reclaimed_bytes,
                "mount: reclaimed orphaned generation captures"
            );
        }
        Some(local)
    } else {
        None
    };
    let overlay = if let Some(local) = &local_state {
        let recovery = local.recovery_dirty_state().map_err(|error| {
            CliError::usage(format!(
                "recovering durable local snapshot state failed closed: {error}"
            ))
        })?;
        tracing::info!(
            engine = "redb",
            active_generation = recovery.active_generation,
            dirty_paths = recovery.paths.len(),
            rename_intents = recovery.renames.len(),
            "mount: recovered durable local snapshot state without overlay walk"
        );
        OverlayFs::new_with_journal(
            core.clone(),
            state_dir,
            false,
            Arc::new(LocalStateMutationJournal(local.clone())),
            overlay_recovery_seed(&recovery),
            overlay_recovery_renames(&recovery),
        )
    } else {
        OverlayFs::new(core.clone(), state_dir, state.read_only())
    }
    .map_err(|error| CliError::usage(format!("overlay init: {error}")))?;
    // Git-backed and read-only legacy mounts retain their old reconstruction path. Writable native
    // mounts were either seeded from the durable DB above or imported exactly once before attach.
    if !state.read_only() && local_state.is_none() {
        let (upserts, deletes) = reconcile_sealed(state_dir, &overlay);
        if upserts + deletes > 0 {
            tracing::info!(
                retained_files = upserts,
                inert_whiteouts = deletes,
                "mount: reconciled retained sealed state; not dirty"
            );
        }
    }

    // Credential rotation: re-mint comfortably before expiry (or on demand — the heartbeat
    // task nudges `remint` when the server rejects the current token), swap in place, and
    // keep the on-disk cache warm for the next CLI command. A failed mint retries on a short
    // fixed cadence: `expires_at` is left untouched, so `due` collapses toward the 60s floor
    // instead of the old 30-minute parse-fallback sleep that could strand a near-expiry
    // token unrotated.
    let remint = Arc::new(tokio::sync::Notify::new());
    let rotates = expires_at.is_some();
    if rotates {
        let sdk = sdk.clone();
        let (api_url, project, repo) = (ctx.api_url.clone(), project.clone(), state.repo.clone());
        let rotate = rotating_client;
        let creds = api_creds.clone();
        let remint = remint.clone();
        tokio::spawn(async move {
            loop {
                let due = expires_in(expires_at.as_deref().unwrap_or_default())
                    .saturating_sub(CREDENTIAL_ROTATE_MARGIN);
                tokio::select! {
                    _ = tokio::time::sleep(due.max(Duration::from_secs(60))) => {}
                    _ = remint.notified() => {}
                }
                match mint_and_cache(&sdk, &api_url, &project, &repo).await {
                    Ok((username, token, fresh_expires)) => {
                        rotate.set_token(Some(token.clone()));
                        *creds.lock().expect("creds lock") = (username, token);
                        expires_at = Some(fresh_expires);
                    }
                    Err(e) => {
                        tracing::warn!("credential rotation failed (retrying in ~60s): {e}");
                    }
                }
            }
        });
    }

    // Lease heartbeat. An auth failure here is the running daemon's signal that its token
    // died early (revocation, epoch rotation): nudge the rotation task instead of waiting
    // out the scheduled re-mint.
    let heartbeat_sdk = sdk.clone();
    let (heartbeat_project, heartbeat_repo, heartbeat_ws) = (
        project.clone(),
        state.repo.clone(),
        state.workspace_id.clone(),
    );
    let mount_presence = Arc::new(std::sync::RwLock::new(
        state
            .mount_presence_id
            .clone()
            .zip(state.git_mount_source.clone()),
    ));
    let heartbeat_mountpoint = state.mountpoint.to_string_lossy().into_owned();
    let native_filesystem = state.native_filesystem;
    let creds = api_creds.clone();
    let remint = rotates.then(|| remint.clone());
    let heartbeat_presence = mount_presence.clone();
    tokio::spawn(async move {
        loop {
            let (user, token) = creds.lock().expect("creds lock").clone();
            let presence = heartbeat_presence
                .read()
                .expect("mount presence lock")
                .clone();
            let result = if let Some((session_id, source)) = presence {
                heartbeat_sdk
                    .record_git_mount_presence(
                        &heartbeat_project,
                        &heartbeat_repo,
                        &user,
                        &token,
                        &session_id,
                        &tensorlake::artifact_storage::workspaces::RecordGitMountPresenceRequest {
                            source: &source,
                            mounted_on: &heartbeat_mountpoint,
                            ttl_seconds: None,
                        },
                    )
                    .await
                    .map(|_| ())
            } else if native_filesystem {
                heartbeat_sdk
                    .native_workspace_heartbeat_with_credential(
                        &heartbeat_project,
                        &heartbeat_repo,
                        &heartbeat_ws,
                        &user,
                        &token,
                    )
                    .await
                    .map(|_| ())
            } else {
                heartbeat_sdk
                    .workspace_heartbeat(
                        &heartbeat_project,
                        &heartbeat_repo,
                        &user,
                        &token,
                        &heartbeat_ws,
                    )
                    .await
                    .map(|_| ())
            };
            if let Err(e) = result {
                tracing::warn!("mount heartbeat failed: {e}");
                if let (
                    Some(remint),
                    tensorlake::error::SdkError::Authentication(_)
                    | tensorlake::error::SdkError::Authorization(_),
                ) = (&remint, &e)
                {
                    remint.notify_one();
                }
            }
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
        }
    });

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
        startup_ms = started.elapsed().as_millis() as u64,
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

    let initial_sealed = if let Some(local) = &local_state {
        sealed_index_from_local_state(local).map_err(|error| {
            CliError::usage(format!(
                "reading durable sealed-path baselines failed closed: {error}"
            ))
        })?
    } else {
        SealedIndex::load(state_dir)
    };

    // The sealer: one cycle turns the overlay's dirty delta into a snapshot commit. Built for
    // EVERY writable mount, whether or not auto-commit is on — the `seal` control op (what
    // `tl fs snapshot` calls) runs a cycle on demand, and the auto-commit task below ticks the
    // same instance — so every seal, manual or automatic, advances the same dirty watermark,
    // registers in the same resurrection guard, and shares the same chunk caches. (The
    // CLI-side snapshot used to enumerate and push the whole upper itself: it never advanced
    // the daemon's watermark, so auto-commit re-published the same paths next tick, it
    // re-pushed the entire ever-dirty set on every run, and its deletes bypassed the
    // recent-seals tombstone guard.)
    let sealer: Option<Arc<Sealer>> = (!state.read_only()).then(|| {
        let native_prepare_notify = Arc::new(tokio::sync::Notify::new());
        Arc::new(Sealer {
            sdk: sdk.clone(),
            creds: api_creds.clone(),
            project: project.clone(),
            repo: state.repo.clone(),
            subtree: state.subtree.clone(),
            workspace: state.workspace_id.clone(),
            native_filesystem: state.native_filesystem,
            local_state: local_state.clone(),
            state_dir: state_dir.to_path_buf(),
            mountpoint: mountpoint.clone(),
            overlay: overlay.clone(),
            core: core.clone(),
            invalidate: invalidate.clone(),
            pending: pending.clone(),
            native_prepare_notify,
            state: tokio::sync::Mutex::new(SealerState {
                sealed_gen: 0,
                seen_epoch: overlay.epoch(),
                recent_seals: Vec::new(),
                chunk_cache: std::collections::HashMap::new(),
                sealed: {
                    let index = initial_sealed.clone();
                    // Retained upper files AND sealed-delete whiteouts from before this
                    // restart have seal records but no recorded oids: seed both so
                    // divergence eviction still covers them (unknown oid = divergent
                    // whenever the branch touches the path — for a whiteout, that is what
                    // lets a remote re-add surface instead of staying hidden forever).
                    overlay
                        .seed_retained(index.upserts.keys().chain(index.deletes.iter()).cloned());
                    index
                },
                reindex_pending: false,
            }),
            mirror: std::sync::Mutex::new(SealerMirror::default()),
        })
    });

    // Native write-ahead preparation: once mutations have been quiet briefly, resolve the exact
    // journal generation and move its bytes through hash/compress/upload. Publication remains the
    // sealer's job, but it consumes this durable prepared record instead of reopening files.
    if state.native_filesystem
        && let Some(sealer) = sealer.clone()
    {
        let notify = sealer.native_prepare_notify.clone();
        let worker = sealer.clone();
        tokio::spawn(async move {
            const PREPARE_POLL: Duration = Duration::from_millis(500);
            const PREPARE_QUIET_MS: u64 = 750;
            loop {
                let explicitly_requested = tokio::select! {
                    _ = notify.notified() => true,
                    _ = tokio::time::sleep(PREPARE_POLL) => false,
                };
                if !explicitly_requested {
                    let Some((_, last)) = worker.overlay.dirty_clock() else {
                        continue;
                    };
                    if worker.overlay.clock_ms().saturating_sub(last) < PREPARE_QUIET_MS {
                        continue;
                    }
                }
                if let Err(error) = worker.prepare_native_dirty().await {
                    eprintln!("autosave: background prepare failed (will retry): {error}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                if let Err(error) = worker.publish_pending_native_seal().await {
                    eprintln!("autosave: pending native seal failed (will retry): {error}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        });
        // Frozen/prepared/requested state persisted before a crash should not wait for the first
        // polling interval.
        if local_state.as_ref().is_some_and(|local| {
            local.generations().is_ok_and(|generations| {
                generations
                    .iter()
                    .any(|generation| generation.state != super::local_state::GenerationState::Open)
            })
        }) {
            sealer.native_prepare_notify.notify_one();
        }
    }

    // Auto-commit: seal dirty paths into snapshot commits every interval, event-driven. The
    // overlay records every mutation in its dirty index, so nothing is ever scanned — an idle
    // tick is one atomic load. Each seal pushes only paths touched since the last sealed
    // generation: everything sealed earlier is already served by the lower (the workspace ref
    // advances with each snapshot), so commits are incremental deltas, and unchanged dirty
    // files are never re-hashed or re-sent. The overlay is NOT cleared — the upper keeps
    // shadowing the byte-identical sealed content (only `tl fs snapshot --clear` drops it,
    // an explicitly destructive opt-in that requires quiesced writers).
    if let Some(secs) = state.auto_commit_interval_secs
        && let Some(sealer) = sealer.clone()
    {
        // Event-driven autosave (issue #103 step 5): seal when the overlay has been dirty AND
        // quiet for `QUIET_SECS`, or dirty for `secs` regardless — bounded time-to-published
        // instead of "some tick after". Quiet is measured from the dirty files' real mtimes
        // in the upper (no write hooks needed); very large dirty sets skip the stat pass and
        // ride the max-latency bound alone. The old behavior was one blind seal every `secs`,
        // which let a write land right after a tick and wait the whole interval.
        const POLL: Duration = Duration::from_secs(2);
        const QUIET_SECS: u64 = 10;
        let overlay_clock = sealer.overlay.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(POLL).await;
                // The idle tick is a pair of atomic loads — no dirty-set resolution, no
                // file stats. The write hooks stamp the clock at mutation time, which is
                // strictly more accurate than statting: deletes and renames have no mtime,
                // and same-second writes hide inside filesystem timestamp granularity.
                let Some((first, last)) = overlay_clock.dirty_clock() else {
                    continue;
                };
                let now = overlay_clock.clock_ms();
                let quiet = now.saturating_sub(last) >= QUIET_SECS * 1000;
                let dirty_for_s = now.saturating_sub(first) / 1000;
                if !quiet && dirty_for_s < secs.max(1) {
                    continue;
                }
                // eprintln, not tracing: the daemon's stderr is the state dir's daemon.log —
                // the one place a user can see an async flush fail. One structured line per
                // stage so a slow save localizes to seal vs publish (server landing times
                // come from the ops log).
                let reason = if quiet { "quiet" } else { "max-latency" };
                eprintln!("autosave: sealing reason={reason} dirty_for_s={dirty_for_s}");
                let seal_started = std::time::Instant::now();
                match sealer
                    .seal_once("tl fs auto-commit", false, None, None)
                    .await
                {
                    Ok(SealOutcome::Sealed(report)) => {
                        eprintln!(
                            "autosave: sealed snapshot {} seal_ms={} push_ms={:?}",
                            report.commit,
                            seal_started.elapsed().as_millis(),
                            report.push_ms,
                        );
                    }
                    Ok(SealOutcome::Pending { watermark }) => {
                        eprintln!(
                            "autosave: preparation queued watermark={watermark}; publish will resume in background"
                        );
                    }
                    Ok(SealOutcome::Clean { .. }) => {}
                    Err(e) => eprintln!("autosave: {e}"),
                }
            }
        });
    }

    // Control socket (mount is live).
    // Source switching spans durable state, the mount core, and presence identity; serialize the
    // whole transition so concurrent control clients cannot roll one another's persisted source
    // back after a failed probe.
    let source_switch = Arc::new(tokio::sync::Mutex::new(()));
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
        let sealer = sealer.clone();
        let local_state = local_state.clone();
        let mount_presence = mount_presence.clone();
        let source_switch = source_switch.clone();
        let control_state_dir = state_dir.to_path_buf();
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
                let sealer = sealer.clone();
                let local_state = local_state.clone();
                let mount_presence = mount_presence.clone();
                let source_switch = source_switch.clone();
                let control_state_dir = control_state_dir.clone();
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.is_err() {
                        return;
                    }
                    let request = serde_json::from_str::<serde_json::Value>(line.trim())
                        .unwrap_or(serde_json::Value::Null);
                    let op = request
                        .get("op")
                        .and_then(|o| o.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let resp = match op.as_str() {
                        "ping" => {
                            serde_json::json!({ "ok": true, "commit": core.current_commit() })
                        }
                        "doctor-local-state" => match local_state.as_ref() {
                            Some(store) => match super::local_state_doctor_lifecycle(store) {
                                Ok(report) => match serde_json::to_value(report) {
                                    Ok(mut value) => {
                                        value["ok"] = serde_json::Value::Bool(true);
                                        value
                                    }
                                    Err(error) => serde_json::json!({
                                        "ok": false,
                                        "error": error.to_string(),
                                    }),
                                },
                                Err(error) => serde_json::json!({
                                    "ok": false,
                                    "error": format!(
                                        "durable local snapshot state failed validation: {error}"
                                    ),
                                }),
                            },
                            None => serde_json::json!({
                                "ok": false,
                                "error": "this mount has no durable local snapshot state",
                            }),
                        },
                        // The truthful dirty view: exactly what the next seal would publish
                        // (`tl fs status`/`promote`/`sync`/`diff` all read this — one
                        // definition of dirty). A read-only mount has no sealer and can hold
                        // no dirt.
                        "dirty" => match sealer.as_ref() {
                            None => {
                                let reply = DirtyReply {
                                    commit: core.current_commit(),
                                    ..Default::default()
                                };
                                match serde_json::to_value(&reply) {
                                    Ok(mut v) => {
                                        v["ok"] = serde_json::Value::Bool(true);
                                        v
                                    }
                                    Err(e) => {
                                        serde_json::json!({ "ok": false, "error": e.to_string() })
                                    }
                                }
                            }
                            Some(sealer) => match sealer.dirty_view().await {
                                Ok(reply) => match serde_json::to_value(&reply) {
                                    Ok(mut v) => {
                                        v["ok"] = serde_json::Value::Bool(true);
                                        v
                                    }
                                    Err(e) => {
                                        serde_json::json!({ "ok": false, "error": e.to_string() })
                                    }
                                },
                                Err(e) => {
                                    serde_json::json!({ "ok": false, "error": e.to_string() })
                                }
                            },
                        },
                        // Destructive overlay gates also need the retained-path baselines. Native
                        // mounts keep those in redb and the live daemon owns its writable lock, so
                        // serve the in-memory mirror alongside the truthful dirty view instead of
                        // reopening the database or reviving sealed.json.
                        "overlay-safety" => match sealer.as_ref() {
                            None => serde_json::json!({
                                "ok": false,
                                "error": "read-only mount: no overlay safety state",
                            }),
                            Some(sealer) => match sealer.overlay_safety_view().await {
                                Ok(reply) => match serde_json::to_value(&reply) {
                                    Ok(mut value) => {
                                        value["ok"] = serde_json::Value::Bool(true);
                                        value
                                    }
                                    Err(error) => serde_json::json!({
                                        "ok": false,
                                        "error": error.to_string(),
                                    }),
                                },
                                Err(error) => serde_json::json!({
                                    "ok": false,
                                    "error": error.to_string(),
                                }),
                            },
                        },
                        // The still-unresolved collisions on the followed branch (issue
                        // #103 step 4b): fed by refresh deltas inside the mount core,
                        // cleared when a later save changes the path again.
                        "conflicts" => {
                            let list: Vec<serde_json::Value> = core
                                .open_conflicts()
                                .into_iter()
                                .map(|(path, c)| {
                                    serde_json::json!({
                                        "path": path,
                                        "commit": c.commit,
                                        "kind": c.kind,
                                    })
                                })
                                .collect();
                            serde_json::json!({ "ok": true, "conflicts": list })
                        }
                        // Drop retained (sealed-and-kept) overlay state — sync's pre-flight.
                        // Unlike `clear_upper`, dirty and ignored files survive.
                        "trim" => match sealer.as_ref() {
                            None => serde_json::json!({
                                "ok": false,
                                "error": "read-only mount: nothing is retained",
                            }),
                            Some(sealer) => match sealer.trim_all().await {
                                Ok(reply) => match serde_json::to_value(&reply) {
                                    Ok(mut v) => {
                                        v["ok"] = serde_json::Value::Bool(true);
                                        v
                                    }
                                    Err(e) => {
                                        serde_json::json!({ "ok": false, "error": e.to_string() })
                                    }
                                },
                                Err(e) => {
                                    serde_json::json!({ "ok": false, "error": e.to_string() })
                                }
                            },
                        },
                        "ack-snapshot" => {
                            let request_id = request
                                .get("request_id")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default();
                            match sealer.as_ref() {
                                Some(sealer) if !request_id.is_empty() => {
                                    match sealer.acknowledge_snapshot_response(request_id) {
                                        Ok(()) => serde_json::json!({ "ok": true }),
                                        Err(error) => serde_json::json!({
                                            "ok": false,
                                            "error": error.to_string(),
                                        }),
                                    }
                                }
                                Some(_) => serde_json::json!({
                                    "ok": false,
                                    "error": "ack-snapshot requires request_id",
                                }),
                                None => serde_json::json!({
                                    "ok": false,
                                    "error": "read-only mount has no snapshot receipts",
                                }),
                            }
                        }
                        // Seal on demand: `tl fs snapshot` runs exactly one cycle of the same
                        // sealer auto-commit ticks, with the caller's message (and optional
                        // overlay clear). Streaming op — the handler writes its own event
                        // lines and final reply, so it owns the stream from here.
                        "seal" => {
                            handle_seal(reader, request, sealer.clone(), pending.clone(), core)
                                .await;
                            return;
                        }
                        "refresh" => match core.poll_ref().await {
                            Ok(delta) => {
                                if let Some(delta) = delta {
                                    absorb_refresh(&overlay, &invalidate, &pending, &delta);
                                }
                                // The advance may be a seal that published pending renames
                                // (this daemon's own, or a peer writer's on a shared ref);
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
                        "switch-source" => {
                            let _switch = source_switch.lock().await;
                            let source = request
                                .get("source")
                                .cloned()
                                .ok_or_else(|| "switch-source requires source".to_string())
                                .and_then(|value| {
                                    serde_json::from_value::<
                                        tensorlake::artifact_storage::workspaces::GitMountSource,
                                    >(value)
                                    .map_err(|error| format!("invalid switch source: {error}"))
                                });
                            let presence_id = request
                                .get("presence_id")
                                .and_then(serde_json::Value::as_str)
                                .filter(|id| !id.is_empty())
                                .map(str::to_string)
                                .ok_or_else(|| "switch-source requires presence_id".to_string());
                            let prepared = source.and_then(|source| {
                                let presence_id = presence_id?;
                                let previous = load_mount_state(&control_state_dir)
                                    .map_err(|error| format!("loading mount state: {error}"))?;
                                if !previous.read_only()
                                    || previous.native_filesystem
                                    || previous.mount_presence_id.is_none()
                                {
                                    return Err(
                                        "switch-source is only valid for a stateless read-only Git mount"
                                            .to_string(),
                                    );
                                }
                                if source.subtree != previous.subtree {
                                    return Err(
                                        "sync cannot change the mounted subtree; unmount and mount the new subtree"
                                            .to_string(),
                                    );
                                }
                                // Canonical branch and tag refs follow their exact namespace;
                                // a raw commit has no canonical ref and remains pinned.
                                let follow_ref = super::git_mount_follow_ref(&source)
                                    .map_err(|error| error.to_string())?;
                                let follow = follow_ref.is_some();
                                let reference = if follow {
                                    follow_ref.expect("follow policy returned a canonical ref")
                                } else {
                                    source.resolved_commit.clone()
                                };
                                let mut next = previous.clone();
                                next.ref_name = reference.clone();
                                next.follow_ref = follow.then(|| reference.clone());
                                next.start_oid = Some(source.resolved_commit.clone());
                                next.mount_presence_id = Some(presence_id.clone());
                                next.git_mount_source = Some(source.clone());
                                save_mount_state(&control_state_dir, &next)
                                    .map_err(|error| format!("persisting switched source: {error}"))?;
                                Ok((previous, source, presence_id, reference, follow))
                            });
                            match prepared {
                                Err(error) => serde_json::json!({ "ok": false, "error": error }),
                                Ok((previous, source, presence_id, reference, follow)) => {
                                    match core.switch_source(&reference, follow).await {
                                        Ok(delta) => {
                                            *mount_presence.write().expect("mount presence lock") =
                                                Some((presence_id.clone(), source));
                                            absorb_refresh(&overlay, &invalidate, &pending, &delta);
                                            let (changed, complete) = {
                                                let mut p =
                                                    pending.lock().expect("pending probe lock");
                                                let changed: Vec<KernelExpectation> =
                                                    std::mem::take(&mut p.expect)
                                                        .into_values()
                                                        .collect();
                                                (
                                                    changed,
                                                    !std::mem::replace(&mut p.incomplete, false),
                                                )
                                            };
                                            serde_json::json!({
                                                "ok": true,
                                                "commit": core.current_commit(),
                                                "reference": reference,
                                                "follow": follow,
                                                "presence_id": presence_id,
                                                "changed": changed,
                                                "complete": complete,
                                            })
                                        }
                                        Err(error) => {
                                            let rollback =
                                                save_mount_state(&control_state_dir, &previous);
                                            serde_json::json!({
                                                "ok": false,
                                                "error": match rollback {
                                                    Ok(()) => error.to_string(),
                                                    Err(rollback) => format!(
                                                        "{error}; restoring the prior persisted source also failed: {rollback}"
                                                    ),
                                                },
                                            })
                                        }
                                    }
                                }
                            }
                        }
                        // The upper drop flips interned paths to their lower view with no
                        // kernel-visible operation; push the implied invalidations. Routed
                        // through the sealer (state-lock serialized against in-flight seals,
                        // arms the reindex-pending fail-closed guard) whenever one exists;
                        // read-only mounts fall back to the bare drop.
                        "clear_upper" => {
                            let cleared = match sealer.as_ref() {
                                Some(sealer) => sealer.clear_upper_control().await,
                                None => {
                                    SealedIndex::reset(&control_state_dir);
                                    overlay.clear_upper().map_err(|e| {
                                        CliError::usage(format!("clearing the overlay failed: {e}"))
                                    })
                                }
                            };
                            match cleared {
                                Ok(affected) => {
                                    invalidate(affected);
                                    serde_json::json!({ "ok": true })
                                }
                                Err(e) => {
                                    serde_json::json!({ "ok": false, "error": e.to_string() })
                                }
                            }
                        }
                        // Native restore already advanced the server-side workspace/head. Adopt
                        // that exact save locally in one daemon-serialized operation: clear the
                        // old overlay first, then reset the durable generation engine to the new
                        // base. No refill or repair walk is involved.
                        "begin_native_restore" => {
                            let target = request
                                .get("target")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default();
                            let discard_local = request
                                .get("discard_local")
                                .and_then(|value| value.as_bool())
                                .unwrap_or(false);
                            match sealer.as_ref() {
                                Some(sealer) if !target.is_empty() => {
                                    match sealer.begin_native_restore(target, discard_local) {
                                        Ok(operation) => match serde_json::to_value(operation) {
                                            Ok(mut value) => {
                                                value["ok"] = serde_json::Value::Bool(true);
                                                value
                                            }
                                            Err(error) => serde_json::json!({
                                                "ok": false,
                                                "error": error.to_string(),
                                            }),
                                        },
                                        Err(error) => serde_json::json!({
                                            "ok": false,
                                            "error": error.to_string(),
                                        }),
                                    }
                                }
                                Some(_) => serde_json::json!({
                                    "ok": false,
                                    "error": "begin_native_restore requires a target snapshot",
                                }),
                                None => serde_json::json!({
                                    "ok": false,
                                    "error": "read-only mounts cannot restore",
                                }),
                            }
                        }
                        "adopt_native_restore" => {
                            let snapshot = request
                                .get("snapshot")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default();
                            let request_id = request
                                .get("request_id")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default();
                            let adopted = match sealer.as_ref() {
                                Some(sealer) if !snapshot.is_empty() && !request_id.is_empty() => {
                                    sealer.adopt_native_restore(request_id, snapshot).await
                                }
                                Some(_) => Err(CliError::usage(
                                    "adopt_native_restore requires request_id and snapshot",
                                )),
                                None => Err(CliError::usage(
                                    "read-only mounts cannot adopt a restored save",
                                )),
                            };
                            match adopted {
                                Ok(affected) => {
                                    invalidate(affected);
                                    serde_json::json!({ "ok": true })
                                }
                                Err(error) => {
                                    serde_json::json!({ "ok": false, "error": error.to_string() })
                                }
                            }
                        }
                        "ack-restore" => {
                            let request_id = request
                                .get("request_id")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default();
                            match sealer.as_ref() {
                                Some(sealer) if !request_id.is_empty() => {
                                    match sealer.acknowledge_restore_response(request_id) {
                                        Ok(()) => serde_json::json!({ "ok": true }),
                                        Err(error) => serde_json::json!({
                                            "ok": false,
                                            "error": error.to_string(),
                                        }),
                                    }
                                }
                                Some(_) => serde_json::json!({
                                    "ok": false,
                                    "error": "ack-restore requires request_id",
                                }),
                                None => serde_json::json!({
                                    "ok": false,
                                    "error": "read-only mount has no restore receipts",
                                }),
                            }
                        }
                        "fail_native_restore" => {
                            let request_id = request
                                .get("request_id")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default();
                            let reason = request
                                .get("reason")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default();
                            match sealer.as_ref() {
                                Some(sealer) if !request_id.is_empty() && !reason.is_empty() => {
                                    match sealer.fail_native_restore(request_id, reason) {
                                        Ok(()) => serde_json::json!({ "ok": true }),
                                        Err(error) => serde_json::json!({
                                            "ok": false,
                                            "error": error.to_string(),
                                        }),
                                    }
                                }
                                Some(_) => serde_json::json!({
                                    "ok": false,
                                    "error": "fail_native_restore requires request_id and reason",
                                }),
                                None => serde_json::json!({
                                    "ok": false,
                                    "error": "read-only mount has no restore lifecycle",
                                }),
                            }
                        }
                        "restore-status" => match sealer.as_ref() {
                            Some(sealer) => match sealer.active_restore() {
                                Ok(operation) => serde_json::json!({
                                    "ok": true,
                                    "operation": operation,
                                }),
                                Err(error) => serde_json::json!({
                                    "ok": false,
                                    "error": error.to_string(),
                                }),
                            },
                            None => serde_json::json!({
                                "ok": true,
                                "operation": serde_json::Value::Null,
                            }),
                        },
                        // The upper was mutated out-of-band (restore writes into the state dir
                        // from the CLI process); rebuild the dirty index from disk so an
                        // auto-commit mount seals the new state, and disarm the fail-closed
                        // reindex-pending guard. The reconcile is a correctness backstop, not
                        // an optimization: in the restore flow it absolves nothing
                        // (clear_upper reset sealed.json, and refilled files carry fresh
                        // mtimes), but any future out-of-band flow that preserves seal
                        // records must not re-dirty retained content.
                        "reindex" => {
                            let reindexed = match sealer.as_ref() {
                                Some(sealer) => sealer.reindex_control().await,
                                None => overlay
                                    .rebuild_dirty_index()
                                    .map_err(|e| {
                                        CliError::usage(format!(
                                            "rebuilding the dirty index failed: {e}"
                                        ))
                                    })
                                    .map(|()| {
                                        reconcile_sealed(&control_state_dir, &overlay);
                                    }),
                            };
                            match reindexed {
                                Ok(()) => serde_json::json!({ "ok": true }),
                                Err(e) => {
                                    serde_json::json!({ "ok": false, "error": e.to_string() })
                                }
                            }
                        }
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
                        // "code" is the machine-readable half: the CLI's remount-to-upgrade
                        // detection keys on it (matching the prose is only a legacy fallback
                        // for daemons that predate the field).
                        other => {
                            serde_json::json!({
                                "ok": false,
                                "code": "unknown_op",
                                "error": format!("unknown op {other:?}"),
                            })
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

/// Serve one `seal` control request, streaming progress. Writes zero or more
/// `{"event":"<message>"}` lines — the sealer's push progress mapped to the shared
/// [`crate::commands::push_event_message`] strings, throttled to ~2/s, plus a keepalive
/// whenever the seal goes 15s without an event (server-side commit polling emits sparsely) —
/// followed by the single final reply line ([`SealReply`] + the ok/changed/complete envelope).
#[cfg(unix)]
async fn handle_seal(
    reader: tokio::io::BufReader<tokio::net::UnixStream>,
    request: serde_json::Value,
    sealer: Option<Arc<Sealer>>,
    pending: Arc<std::sync::Mutex<PendingProbe>>,
    core: Arc<gsvc_mount::MountCore>,
) {
    let mut stream = reader.into_inner();
    let Some(sealer) = sealer else {
        let resp = serde_json::json!({
            "ok": false,
            "error": "this mount is read-only; there is nothing to seal",
        });
        let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
        return;
    };
    let req: SealRequest = serde_json::from_value(request).unwrap_or_default();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let progress: tensorlake::artifact_storage::ingest::PushProgress =
        Arc::new(move |ev| drop(tx.send(crate::commands::push_event_message(&ev))));
    let message = req.message.unwrap_or_else(|| "tl fs snapshot".to_string());
    let clear = req.clear;
    let request_id = req.request_id;
    let mut seal = tokio::spawn(async move {
        sealer
            .seal_once(&message, clear, Some(progress), request_id)
            .await
    });
    // Forward events as they come, throttled — a spinner can't render more than ~2/s anyway.
    // Write failures are ignored: the seal is not cancellable mid-push without stranding the
    // commit, so a vanished client just stops seeing progress.
    let min_gap = Duration::from_millis(500);
    let mut last_sent = std::time::Instant::now() - min_gap;
    let mut rx_open = true;
    let outcome = loop {
        tokio::select! {
            res = &mut seal => break res,
            ev = rx.recv(), if rx_open => match ev {
                Some(message) if last_sent.elapsed() >= min_gap => {
                    let line = serde_json::json!({ "event": message });
                    let _ = stream.write_all(format!("{line}\n").as_bytes()).await;
                    last_sent = std::time::Instant::now();
                }
                Some(_) => {}
                None => rx_open = false,
            },
            _ = tokio::time::sleep(Duration::from_secs(15)) => {
                let line = serde_json::json!({ "event": "still working (waiting on the server)..." });
                let _ = stream.write_all(format!("{line}\n").as_bytes()).await;
                last_sent = std::time::Instant::now();
            }
        }
    };
    let resp = match outcome {
        Err(join) => serde_json::json!({
            "ok": false,
            "error": format!("seal task failed: {join}"),
        }),
        Ok(Err(e)) => serde_json::json!({ "ok": false, "error": e.to_string() }),
        Ok(Ok(outcome)) => {
            let (changed, complete) = {
                let mut p = pending.lock().expect("pending probe lock");
                let changed: Vec<KernelExpectation> =
                    std::mem::take(&mut p.expect).into_values().collect();
                (changed, !std::mem::replace(&mut p.incomplete, false))
            };
            let reply = match outcome {
                SealOutcome::Clean { cleared } => SealReply {
                    clean: true,
                    pending: false,
                    pending_watermark: None,
                    completed_request_id: None,
                    commit: core.current_commit(),
                    files: None,
                    chunks_uploaded: None,
                    chunks_total: None,
                    sealed: None,
                    push_ms: None,
                    cleared,
                },
                SealOutcome::Pending { watermark } => SealReply {
                    clean: false,
                    pending: true,
                    pending_watermark: Some(watermark),
                    completed_request_id: None,
                    commit: core.current_commit(),
                    files: None,
                    chunks_uploaded: None,
                    chunks_total: None,
                    sealed: None,
                    push_ms: None,
                    cleared: None,
                },
                SealOutcome::Sealed(r) => SealReply {
                    clean: false,
                    pending: false,
                    pending_watermark: None,
                    completed_request_id: r.completed_request_id,
                    commit: r.commit,
                    files: Some(r.files as u64),
                    chunks_uploaded: Some(r.chunks_uploaded as u64),
                    chunks_total: Some(r.chunks_total as u64),
                    sealed: Some(r.sealed_paths),
                    push_ms: Some(r.push_ms),
                    cleared: r.cleared,
                },
            };
            let mut resp = serde_json::to_value(&reply).expect("seal reply serializes");
            let obj = resp.as_object_mut().expect("seal reply is an object");
            obj.insert("ok".to_string(), true.into());
            obj.insert(
                "changed".to_string(),
                serde_json::to_value(&changed).expect("probe list serializes"),
            );
            obj.insert("complete".to_string(), complete.into());
            resp
        }
    };
    let _ = stream.write_all(format!("{resp}\n").as_bytes()).await;
}

/// A pushed file's CDC chunk list, as returned in `PushReport::file_chunks`.
#[cfg(unix)]
type ChunkList = Vec<([u8; 32], u32)>;

/// One seal cycle's outcome.
#[cfg(unix)]
enum SealOutcome {
    /// Nothing dirty since the watermark and no pending renames: no commit was minted.
    /// `cleared` reports a requested generation-safe retained-byte trim all the same.
    Clean {
        cleared: Option<Vec<String>>,
    },
    /// Preparation was requested without doing content work on the seal request path.
    Pending {
        watermark: u64,
    },
    Sealed(SealReport),
}

/// What a completed seal knows — the `seal` control op's reply body.
#[cfg(unix)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SealReport {
    #[serde(default)]
    completed_request_id: Option<String>,
    commit: String,
    files: usize,
    chunks_uploaded: usize,
    chunks_total: usize,
    /// Every repo path the seal published (upserts and deletes).
    sealed_paths: Vec<String>,
    /// Wall time of the push (chunk/hash + upload + server commit), for the CLI timing line.
    push_ms: u64,
    /// Every generation-owned retained path a requested clear dropped (`None` when no clear was
    /// asked for). Later-generation writes and ignored/local-only content are excluded.
    cleared: Option<Vec<String>>,
}

/// The mount's sealer: the mutable state one seal cycle reads and advances, plus everything a
/// cycle needs to resolve, push, and converge. The auto-commit tick task and the `seal`
/// control op both run [`Sealer::seal_once`]; the state lock serializes them.
#[cfg(unix)]
struct Sealer {
    sdk: tensorlake::artifact_storage::ArtifactStorageClient,
    creds: Arc<std::sync::Mutex<(String, String)>>,
    project: String,
    repo: String,
    /// Full-repository prefix restored onto subtree-relative overlay paths at publish time.
    subtree: Option<String>,
    workspace: String,
    native_filesystem: bool,
    /// Authoritative mutation/generation state for writable native filesystems. Git-backed
    /// mounts keep their existing in-memory generation machinery.
    local_state: Option<super::local_state::LocalState>,
    state_dir: PathBuf,
    mountpoint: PathBuf,
    overlay: Arc<OverlayFs>,
    core: Arc<gsvc_mount::MountCore>,
    invalidate: InvalSink,
    pending: Arc<std::sync::Mutex<PendingProbe>>,
    /// Single daemon-owned preparation worker. Explicit snapshots wake the same worker used by
    /// quiet-time preparation; the control request never scans, hashes, compresses, or uploads.
    native_prepare_notify: Arc<tokio::sync::Notify>,
    state: tokio::sync::Mutex<SealerState>,
    /// A lock-cheap copy of the dirty-relevant sealer state (`sealed_gen`, the resurrection
    /// guard set), republished by [`Sealer::publish_mirror`] whenever the real state changes.
    /// The `dirty` control op reads THIS: taking the state mutex would park status behind an
    /// in-flight push for however long the upload takes.
    mirror: std::sync::Mutex<SealerMirror>,
}

/// See [`Sealer::mirror`].
#[cfg(unix)]
#[derive(Default)]
struct SealerMirror {
    sealed_gen: u64,
    recently: std::collections::HashSet<String>,
    reindex_pending: bool,
}

#[cfg(unix)]
struct SealerState {
    /// The dirty-index generation everything at or below which has been sealed.
    sealed_gen: u64,
    /// The overlay epoch this sealer's caches describe. clear_upper (snapshot --clear,
    /// restore) and rebuild_dirty_index rewrite the overlay's world out-of-band; every cache
    /// below is a claim about the old world and dies with it.
    seen_epoch: u64,
    /// Upserts of not-yet-confirmed seals, in seal order, each tagged with its commit: the
    /// guard set for deletes racing the lower's advance past their seal (see resolve_seal's
    /// tombstone arm). Confirmation-based — a set is only dropped once the lower is observed
    /// at (or past) its seal — because eviction-by-count expires the guard exactly when index
    /// materialization lags behind hot pushes. Memory is bounded by the unconfirmed window,
    /// not a fixed depth.
    recent_seals: Vec<(String, std::collections::HashSet<String>)>,
    /// Chunk lists from previous seals (path -> the pushed CDC chunk list): the append fast
    /// path's memory. A re-touched file whose writes never went below a cached boundary seals
    /// as a `StablePrefix` — only bytes past that boundary are re-read. Daemon-local; a
    /// restart just means one full-cost seal per file to re-learn.
    chunk_cache: std::collections::HashMap<String, ChunkList>,
    /// The persisted seal record — see [`SealedIndex`]. Updated and saved after each
    /// successful push; the durable half of what `recent_seals`/`sealed_gen` know in memory.
    sealed: SealedIndex,
    /// Restore has cleared the overlay and is refilling it out-of-band; until its follow-up
    /// `reindex` rebuilds the dirty index, that index is EMPTY while the upper is in flux —
    /// an unguarded dirty view would read as false-clean mid-refill (and a sync could pass
    /// its dirty gate against a half-restored workspace). Seals, trims, and the dirty view
    /// all fail closed while this is set.
    reindex_pending: bool,
}

#[cfg(unix)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PreparedNativeJournal {
    format_ver: u16,
    /// Durable local generation this candidate was prepared from. Overlay watermarks are only
    /// cache-pruning coordinates and must not identify publication/recovery state.
    local_generation: u64,
    watermark: u64,
    base_snapshot: String,
    signature: Vec<String>,
    stats: std::collections::BTreeMap<String, SealedStat>,
    path_stats: std::collections::BTreeMap<String, PreparedNativePathStat>,
    sealed_upserts: Vec<String>,
    sealed_paths: Vec<String>,
    delete_paths: Vec<String>,
    redirects: Vec<(String, String)>,
    tombstoned: Vec<String>,
    candidate: tensorlake::artifact_storage::native_fs::NativePreparedSnapshotCandidate,
}

/// Exact immutable local input captured while the overlay generation fence is held. This record is
/// committed in the same redb transaction that changes Open(N) to Frozen(N); preparation after the
/// fence never re-resolves the live overlay and therefore cannot absorb writes from N+1.
#[cfg(unix)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct FrozenNativeGeneration {
    format_ver: u16,
    local_generation: u64,
    preparation_operation_id: String,
    watermark: u64,
    captured_upserts: Vec<(String, PathBuf)>,
    deletes: Vec<String>,
    redirects: Vec<(String, String)>,
    signature: Vec<String>,
    stats: std::collections::BTreeMap<String, SealedStat>,
    path_stats: std::collections::BTreeMap<String, PreparedNativePathStat>,
    sealed_upserts: Vec<String>,
    sealed_paths: Vec<String>,
    tombstoned: Vec<String>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PreparedNativePathStat {
    dev: u64,
    ino: u64,
    size: u64,
    mtime_secs: i64,
    mtime_nanos: i64,
    ctime_secs: i64,
    ctime_nanos: i64,
    mode: u32,
}

#[cfg(unix)]
impl PreparedNativePathStat {
    fn of(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            size: meta.size(),
            mtime_secs: meta.mtime(),
            mtime_nanos: meta.mtime_nsec(),
            ctime_secs: meta.ctime(),
            ctime_nanos: meta.ctime_nsec(),
            mode: meta.mode(),
        }
    }
}

#[cfg(unix)]
fn build_frozen_native_capture(
    local: &super::local_state::LocalState,
    state_dir: &Path,
    mountpoint: &Path,
    local_generation: u64,
    delta: &super::overlay::DirtyDelta,
    recently: &std::collections::HashSet<String>,
    mut current_redirects: Vec<(String, String)>,
) -> Result<FrozenNativeGeneration> {
    use tensorlake::artifact_storage::native_fs::{
        NativeChangeSet, NativeLocalUpsert, NativeRename,
    };

    let resolved = resolve_seal(state_dir, mountpoint, delta, recently, &Default::default())
        .map_err(|error| CliError::usage(format!("native freeze resolution failed: {error}")))?;
    let mut upserts: std::collections::BTreeMap<String, PathBuf> = resolved
        .directories
        .iter()
        .map(|(path, source)| (path.clone(), source.clone()))
        .collect();
    for file in &resolved.files {
        if file.delete
            || matches!(
                &file.source,
                tensorlake::artifact_storage::ingest::PushSource::KnownOid(_)
            )
        {
            continue;
        }
        upserts.insert(
            file.repo_path.clone(),
            state_dir.join("upper").join(&file.repo_path),
        );
    }
    let mut deletes: Vec<String> = resolved
        .files
        .iter()
        .filter(|file| file.delete)
        .map(|file| file.repo_path.clone())
        .collect();
    deletes.sort();
    deletes.dedup();
    current_redirects.sort();
    let mut renames: Vec<NativeRename> = current_redirects
        .iter()
        .cloned()
        .map(|(to, from)| NativeRename { from, to })
        .collect();
    renames.sort_by(|a, b| (&a.from, &a.to).cmp(&(&b.from, &b.to)));
    let unresolved_changes = NativeChangeSet {
        upserts: upserts
            .iter()
            .map(|(path, source)| NativeLocalUpsert {
                path: path.clone(),
                source: source.clone(),
            })
            .collect(),
        deletes: deletes.clone(),
        renames,
    };
    let signature = native_change_signature(&unresolved_changes);
    let stats = resolved
        .stats
        .iter()
        .map(|(path, stat)| (path.clone(), *stat))
        .collect();
    let path_stats = unresolved_changes
        .upserts
        .iter()
        .map(|upsert| {
            let metadata = std::fs::symlink_metadata(&upsert.source).map_err(|error| {
                CliError::usage(format!(
                    "statting frozen native path {} failed: {error}",
                    upsert.path
                ))
            })?;
            Ok((upsert.path.clone(), PreparedNativePathStat::of(&metadata)))
        })
        .collect::<Result<_>>()?;
    let mut sealed_upserts: Vec<String> = resolved.sealed_upserts.iter().cloned().collect();
    sealed_upserts.sort();
    let mut sealed_paths: Vec<String> = resolved
        .files
        .iter()
        .map(|file| file.repo_path.clone())
        .chain(resolved.directories.iter().map(|(path, _)| path.clone()))
        .collect();
    sealed_paths.sort();
    sealed_paths.dedup();
    let tombstoned = resolved.tombstoned;
    local
        .claim_generation_capture(local_generation)
        .map_err(|error| {
            CliError::usage(format!(
                "claiming generation capture before creation failed: {error}"
            ))
        })?;
    let captured_upserts = super::generation_capture::capture_managed_generation_upserts(
        state_dir,
        local_generation,
        upserts,
    )?
    .into_iter()
    .map(|upsert| (upsert.path, upsert.source))
    .collect();
    let capture_bytes =
        super::generation_capture::generation_capture_bytes(state_dir, local_generation)?;
    local
        .set_generation_capture_bytes(local_generation, capture_bytes)
        .map_err(|error| {
            CliError::usage(format!(
                "recording generation capture bytes failed: {error}"
            ))
        })?;
    Ok(FrozenNativeGeneration {
        format_ver: 2,
        local_generation,
        preparation_operation_id: uuid::Uuid::new_v4().to_string(),
        watermark: delta.watermark,
        captured_upserts,
        deletes,
        redirects: current_redirects,
        signature,
        stats,
        path_stats,
        sealed_upserts,
        sealed_paths,
        tombstoned,
    })
}

#[cfg(unix)]
impl Sealer {
    fn mount_relative_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        match &self.subtree {
            Some(prefix) => path
                .strip_prefix(prefix)
                .and_then(|path| path.strip_prefix('/'))
                .ok_or_else(|| {
                    CliError::usage(format!(
                        "snapshot returned path {path:?} outside mounted subtree {prefix:?}"
                    ))
                }),
            None => Ok(path),
        }
    }

    fn acknowledge_snapshot_response(&self, request_id: &str) -> Result<()> {
        let local = self.local_state.as_ref().ok_or_else(|| {
            CliError::usage("legacy repository mounts do not use native snapshot receipts")
        })?;
        local
            .acknowledge_completed_publish_request(request_id)
            .map_err(|error| {
                CliError::usage(format!(
                    "acknowledging completed native snapshot request {request_id}: {error}"
                ))
            })
    }

    /// Prepare the current native dirty generation without publishing it. This is the write-path
    /// half of snapshotting: local reads, hashing, compression, segment upload, Merkle composition,
    /// and declaration verification run after files become quiet. `seal_once` only freezes and
    /// publishes the matching journal entry.
    async fn prepare_native_dirty(&self) -> Result<()> {
        use tensorlake::artifact_storage::native_fs::{
            NativeChangeSet, NativeLocalUpsert, NativeRename,
        };

        if !self.native_filesystem {
            return Ok(());
        }
        let local = self
            .local_state
            .as_ref()
            .expect("writable native sealer has durable local state");
        if self.overlay.has_redirects() {
            let consumed = self
                .overlay
                .reap_sealed_redirects()
                .await
                .map_err(|error| {
                    CliError::usage(format!("reaping sealed native renames failed: {error}"))
                })?;
            if !consumed.is_empty() {
                eprintln!("autosave: reaped {} sealed rename(s)", consumed.len());
            }
        }
        let (epoch, sealed_gen, recently) = {
            let mut st = self.state.lock().await;
            if st.reindex_pending {
                return Ok(());
            }
            let epoch = self.overlay.epoch();
            if epoch != st.seen_epoch {
                st.seen_epoch = epoch;
            }
            let recently: std::collections::HashSet<String> = st
                .recent_seals
                .iter()
                .flat_map(|(_, set)| set)
                .cloned()
                .collect();
            (epoch, st.sealed_gen, recently)
        };

        // Resume the oldest frozen generation first, while retaining already-prepared older
        // generations for the serialized publisher. The capture watermark (not merely the last
        // published watermark) prevents later generations from recapturing earlier dirty rows.
        let mut generations = local.generations().map_err(|error| {
            CliError::usage(format!(
                "reading durable native generations failed: {error}"
            ))
        })?;
        generations.sort_by_key(|generation| generation.generation);
        let mut capture_since = sealed_gen;
        let mut earlier_capture_has_rename = false;
        for generation in generations
            .iter()
            .filter(|generation| generation.state != super::local_state::GenerationState::Open)
        {
            if matches!(
                generation.state,
                super::local_state::GenerationState::Prepared
                    | super::local_state::GenerationState::PublishRequested
                    | super::local_state::GenerationState::Published
            ) {
                let prepared = local
                    .prepared(generation.generation)
                    .map_err(|error| {
                        CliError::usage(format!("reading durable native candidate failed: {error}"))
                    })?
                    .ok_or_else(|| {
                        CliError::usage(format!(
                            "durable native generation {} has no prepared candidate",
                            generation.generation
                        ))
                    })?;
                let _: PreparedNativeJournal = serde_json::from_slice(&prepared.candidate)
                    .map_err(|error| {
                        CliError::usage(format!(
                            "durable native candidate {} is corrupt: {error}",
                            generation.generation
                        ))
                    })?;
            }
            if let Some(bytes) = local
                .frozen_capture(generation.generation)
                .map_err(|error| {
                    CliError::usage(format!(
                        "reading frozen native capture {} failed: {error}",
                        generation.generation
                    ))
                })?
            {
                let capture: FrozenNativeGeneration =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        CliError::usage(format!(
                            "frozen native capture {} is corrupt: {error}",
                            generation.generation
                        ))
                    })?;
                if capture.format_ver != 2 || capture.local_generation != generation.generation {
                    return Err(CliError::usage(format!(
                        "frozen native generation {} has an incompatible capture",
                        generation.generation
                    )));
                }
                capture_since = capture_since.max(capture.watermark);
                earlier_capture_has_rename |= !capture.redirects.is_empty();
            }
        }

        let frozen = generations
            .iter()
            .find(|generation| generation.state == super::local_state::GenerationState::Frozen)
            .map(|generation| generation.generation);

        // Heal while the generation is still open so the whiteout is journaled into the same
        // generation. Recovery of an already-frozen generation skips this mutation and resolves
        // only its durable path set.
        let mut live_delta = self.overlay.dirty_since(capture_since);
        if frozen.is_none() {
            let in_flight = generations
                .iter()
                .filter(|generation| generation.state != super::local_state::GenerationState::Open)
                .count();
            if in_flight >= super::local_state::MAX_INFLIGHT_GENERATIONS {
                return Ok(());
            }
            // A lower-backed rename in N changes the coordinate system for N+1. Wait until N is
            // adopted and the lower refresh rebases the surviving redirect before freezing a
            // later generation; ordinary content-only generations remain fully pipelined.
            if earlier_capture_has_rename {
                return Ok(());
            }
            if live_delta.is_empty() && !self.overlay.has_redirects() {
                return Ok(());
            }
            let healed = self
                .overlay
                .heal_replaced_files(live_delta.upserts.iter().map(|path| path.0.as_str()))
                .await
                .map_err(|error| {
                    CliError::usage(format!(
                        "healing native dir-over-file state failed: {error}"
                    ))
                })?;
            if !healed.is_empty() {
                live_delta = self.overlay.dirty_since(capture_since);
                (self.invalidate)(self.overlay.invals_for(&healed));
            }
        }

        let local_generation = if let Some(generation) = frozen {
            generation
        } else {
            let frozen_cell = std::sync::Mutex::new(None);
            let active_generation = local.active_generation().map_err(|error| {
                CliError::usage(format!("reading active native generation failed: {error}"))
            })?;
            let state_dir = self.state_dir.clone();
            let mountpoint = self.mountpoint.clone();
            let recently = recently.clone();
            let overlay = self.overlay.clone();
            self.overlay
                .freeze_dirty_since_with(capture_since, |delta| {
                    let capture = tokio::task::block_in_place(|| {
                        build_frozen_native_capture(
                            local,
                            &state_dir,
                            &mountpoint,
                            active_generation,
                            delta,
                            &recently,
                            overlay.redirect_entries(),
                        )
                    })
                    .map_err(|error| gsvc_mount::MountError::Protocol(error.to_string()))?;
                    let encoded = serde_json::to_vec(&capture).map_err(|error| {
                        gsvc_mount::MountError::Protocol(format!(
                            "serializing frozen native generation failed: {error}"
                        ))
                    })?;
                    let frozen = local
                        .freeze_current_with_capture(encoded)
                        .map_err(|error| {
                            gsvc_mount::MountError::Protocol(format!(
                                "freezing durable native generation failed closed: {error}"
                            ))
                        })?;
                    *frozen_cell.lock().expect("frozen generation cell") = frozen;
                    Ok(())
                })
                .await
                .map_err(|error| CliError::usage(format!("native generation freeze: {error}")))?;
            let Some(frozen) = frozen_cell
                .into_inner()
                .expect("frozen generation cell is not poisoned")
            else {
                if live_delta.is_empty() {
                    return Ok(());
                }
                return Err(CliError::usage(
                    "durable journal reported a clean generation while the overlay was dirty; \
                     refusing to prepare an incomplete snapshot",
                ));
            };
            frozen.generation
        };

        let dirty = local.dirty_generation(local_generation).map_err(|error| {
            CliError::usage(format!(
                "reading frozen native generation {local_generation} failed: {error}"
            ))
        })?;
        let capture_bytes = local
            .frozen_capture(local_generation)
            .map_err(|error| {
                CliError::usage(format!(
                    "reading frozen native capture {local_generation} failed: {error}"
                ))
            })?
            .ok_or_else(|| {
                CliError::usage(format!(
                    "frozen native generation {local_generation} has no immutable capture; \
                     refusing to resolve it from a later live overlay"
                ))
            })?;
        let capture: FrozenNativeGeneration =
            serde_json::from_slice(&capture_bytes).map_err(|error| {
                CliError::usage(format!(
                    "frozen native capture {local_generation} is corrupt: {error}"
                ))
            })?;
        if capture.format_ver != 2 || capture.local_generation != local_generation {
            return Err(CliError::usage(format!(
                "frozen native generation {local_generation} has an incompatible capture"
            )));
        }
        let watermark = capture.watermark;
        if capture.captured_upserts.is_empty()
            && capture.deletes.is_empty()
            && capture.redirects.is_empty()
        {
            let mut st = self.state.lock().await;
            if self.overlay.epoch() != epoch || st.sealed_gen != sealed_gen {
                return Ok(());
            }
            local
                .abort_unpublished_generation(local_generation)
                .map_err(|error| {
                    CliError::usage(format!(
                        "retiring empty durable native generation failed: {error}"
                    ))
                })?;
            st.sealed_gen = watermark;
            self.overlay.prune_dirty(watermark);
            self.overlay.close_dirty_base_if_clean();
            self.publish_mirror(&st);
            return Ok(());
        }
        let renames: Vec<NativeRename> = capture
            .redirects
            .iter()
            .map(|(to, from)| NativeRename {
                from: from.clone(),
                to: to.clone(),
            })
            .collect();
        let changes = NativeChangeSet {
            upserts: capture
                .captured_upserts
                .iter()
                .map(|(path, source)| NativeLocalUpsert {
                    path: path.clone(),
                    source: source.clone(),
                })
                .collect(),
            deletes: capture.deletes.clone(),
            renames,
        };
        let signature = capture.signature;
        let stats = capture.stats;
        let path_stats = capture.path_stats;
        let sealed_upserts = capture.sealed_upserts;
        let sealed_paths = capture.sealed_paths;
        let delete_paths = capture.deletes;
        let redirects = capture.redirects;
        let tombstoned = capture.tombstoned;
        if !tombstoned.is_empty() {
            (self.invalidate)(self.overlay.invals_for(&tombstoned));
        }
        let base_snapshot = dirty.base_snapshot.clone().ok_or_else(|| {
            CliError::usage(format!(
                "frozen durable native generation {local_generation} has no base snapshot"
            ))
        })?;
        let (user, token) = self.creds.lock().expect("creds lock").clone();
        let segment_staging = self
            .state_dir
            .join("staging")
            .join("generations")
            .join(local_generation.to_string())
            .join("segments");
        match tokio::fs::remove_dir_all(&segment_staging).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CliError::usage(format!(
                    "cleaning generation-owned segment staging failed: {error}"
                )));
            }
        }
        tokio::fs::create_dir_all(&segment_staging).await?;
        let candidate = self
            .sdk
            .prepare_native_snapshot_candidate_with_operation_and_staging_directory(
                &self.project,
                &self.repo,
                &base_snapshot,
                changes,
                &user,
                &token,
                capture.preparation_operation_id.clone(),
                Some(segment_staging),
            )
            .await
            .map_err(|error| {
                CliError::usage(format!("native background upload failed: {error}"))
            })?;
        let owned_bytes =
            super::generation_capture::generation_capture_bytes(&self.state_dir, local_generation)?;
        local
            .set_generation_capture_bytes(local_generation, owned_bytes)
            .map_err(|error| {
                CliError::usage(format!(
                    "recording final generation-owned staging bytes failed: {error}"
                ))
            })?;
        let journal = PreparedNativeJournal {
            format_ver: 4,
            local_generation,
            watermark,
            base_snapshot: base_snapshot.clone(),
            signature: signature.clone(),
            stats,
            path_stats,
            sealed_upserts,
            sealed_paths,
            delete_paths,
            redirects,
            tombstoned,
            candidate,
        };
        let candidate_bytes = serde_json::to_vec(&journal).map_err(|error| {
            CliError::usage(format!(
                "serializing durable native prepared generation failed: {error}"
            ))
        })?;
        use sha2::{Digest, Sha256};
        let source_fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&signature).expect("native signature serializes"),
        ));
        local
            .mark_prepared(super::local_state::PreparedGeneration::new(
                local_generation,
                Some(base_snapshot.clone()),
                journal.candidate.root_id(),
                source_fingerprint,
                candidate_bytes,
            ))
            .map_err(|error| {
                CliError::usage(format!(
                    "persisting durable native prepared generation failed: {error}"
                ))
            })?;
        let st = self.state.lock().await;
        if self.overlay.epoch() != epoch || st.sealed_gen != sealed_gen {
            // The prepared generation remains durable and recovery will either publish or expose
            // it for explicit repair. Never delete immutable evidence because the local overlay
            // epoch changed after upload.
            return Ok(());
        }
        eprintln!(
            "autosave: prepared watermark={} files={} logical_bytes={} prepare_ms={} uploaded_bytes={} uploaded_segments={}/{}",
            journal.watermark,
            journal.candidate.files,
            journal.candidate.logical_bytes,
            journal.candidate.preparation_ms,
            journal.candidate.uploaded_bytes,
            journal.candidate.uploaded_segments,
            journal.candidate.total_segments,
        );
        tracing::info!(
            operation_id = %journal.candidate.preparation_operation_id,
            watermark = journal.watermark,
            files = journal.candidate.files,
            directories = journal.candidate.directories,
            logical_bytes = journal.candidate.logical_bytes,
            stored_bytes = journal.candidate.stored_bytes,
            uploaded_bytes = journal.candidate.uploaded_bytes,
            uploaded_segments = journal.candidate.uploaded_segments,
            total_segments = journal.candidate.total_segments,
            prepare_ms = journal.candidate.preparation_ms,
            "native journal generation prepared"
        );
        Ok(())
    }

    /// Resume a manual/autosave publication request after the background journal worker finishes.
    /// Requests and their server idempotency keys live in the durable store, so a daemon crash or
    /// lost HTTP response resumes the exact same publication rather than manufacturing a new one.
    async fn publish_pending_native_seal(&self) -> Result<()> {
        let local = self
            .local_state
            .as_ref()
            .expect("writable native sealer has durable local state");
        let request = local
            .publish_requests()
            .map_err(|error| {
                CliError::usage(format!("reading durable native snapshot requests: {error}"))
            })?
            .into_iter()
            .filter(|request| request.failure.is_none())
            .min_by_key(|request| (request.generation, request.created_at_ms));
        let Some(request) = request else {
            return Ok(());
        };
        match self
            .seal_native_once(
                &request.message,
                request.clear_after_publish,
                Some(request.request_id.clone()),
            )
            .await?
        {
            SealOutcome::Sealed(report) => {
                eprintln!(
                    "autosave: published requested native snapshot {} push_ms={}",
                    report.commit, report.push_ms
                );
            }
            SealOutcome::Clean { .. } => {}
            SealOutcome::Pending { .. } => {}
        }
        Ok(())
    }

    /// Publish one fully prepared native journal generation. The preparation worker owns every
    /// content-bearing operation; this path only freezes the prepared watermark, records the
    /// immutable snapshot, advances the workspace, and retires that journal prefix.
    async fn seal_native_once(
        &self,
        message: &str,
        clear: bool,
        request_id: Option<String>,
    ) -> Result<SealOutcome> {
        let local = self
            .local_state
            .as_ref()
            .expect("writable native sealer has durable local state");
        if let Some(request_id) = request_id.as_deref() {
            if let Some(completed) =
                local
                    .completed_publish_request(request_id)
                    .map_err(|error| {
                        CliError::usage(format!(
                            "reading completed native snapshot request {request_id}: {error}"
                        ))
                    })?
            {
                if completed.request.message != message
                    || completed.request.clear_after_publish != clear
                {
                    return Err(CliError::usage(format!(
                        "native snapshot request id {request_id} was already completed with \
                         different request parameters"
                    )));
                }
                let report =
                    serde_json::from_slice::<SealReport>(&completed.response).map_err(|error| {
                        CliError::usage(format!(
                            "completed native snapshot request {request_id} has a corrupt durable \
                             response: {error}"
                        ))
                    })?;
                if report.commit != completed.snapshot_id {
                    return Err(CliError::usage(format!(
                        "completed native snapshot request {request_id} response names {}, \
                         expected {}",
                        report.commit, completed.snapshot_id
                    )));
                }
                return Ok(SealOutcome::Sealed(report));
            }
        }
        let mut st = self.state.lock().await;
        if st.reindex_pending {
            return Err(CliError::usage(
                "the overlay is being restored (reindex pending); nothing was sealed",
            ));
        }
        if self.overlay.epoch() != st.seen_epoch {
            return Err(CliError::usage(
                "the overlay epoch changed while durable native work is in flight; run `tl fs \
                 doctor` before retrying",
            ));
        }

        let mut generations = local.generations().map_err(|error| {
            CliError::usage(format!(
                "reading durable native generations failed: {error}"
            ))
        })?;
        generations.sort_by_key(|generation| generation.generation);
        let local_generation = if let Some(generation) = generations
            .iter()
            .find(|generation| generation.state != super::local_state::GenerationState::Open)
        {
            generation.generation
        } else {
            let delta = self.overlay.dirty_since(st.sealed_gen);
            if delta.is_empty() && !self.overlay.has_redirects() {
                let cleared = if clear {
                    let baselines = local.sealed_baselines().map_err(|error| {
                        CliError::usage(format!(
                            "reading retained native snapshot baselines: {error}"
                        ))
                    })?;
                    let mut candidates = Vec::new();
                    let mut tombstones = Vec::new();
                    for baseline in baselines {
                        match baseline.state {
                            super::local_state::SealedPathState::Upsert { .. } => {
                                candidates.push(baseline.path)
                            }
                            super::local_state::SealedPathState::Delete => {
                                tombstones.push(baseline.path)
                            }
                        }
                    }
                    let outcome = self.overlay.trim_retained(&candidates, &tombstones).await;
                    (self.invalidate)(outcome.invals);
                    let cleared: Vec<String> = outcome
                        .trimmed
                        .into_iter()
                        .chain(outcome.tombstones_removed)
                        .collect();
                    local.remove_sealed_baselines(&cleared).map_err(|error| {
                        CliError::usage(format!(
                            "recording generation-safe native cleanup: {error}"
                        ))
                    })?;
                    for path in &cleared {
                        st.sealed.upserts.remove(path);
                        st.sealed.deletes.remove(path);
                    }
                    self.publish_mirror(&st);
                    Some(cleared)
                } else {
                    None
                };
                return Ok(SealOutcome::Clean { cleared });
            }
            let frozen_cell = std::sync::Mutex::new(None);
            let active_generation = local.active_generation().map_err(|error| {
                CliError::usage(format!("reading active native generation failed: {error}"))
            })?;
            let state_dir = self.state_dir.clone();
            let mountpoint = self.mountpoint.clone();
            let recently: std::collections::HashSet<String> = st
                .recent_seals
                .iter()
                .flat_map(|(_, paths)| paths)
                .cloned()
                .collect();
            let overlay = self.overlay.clone();
            self.overlay
                .freeze_dirty_since_with(st.sealed_gen, |delta| {
                    let capture = tokio::task::block_in_place(|| {
                        build_frozen_native_capture(
                            local,
                            &state_dir,
                            &mountpoint,
                            active_generation,
                            delta,
                            &recently,
                            overlay.redirect_entries(),
                        )
                    })
                    .map_err(|error| gsvc_mount::MountError::Protocol(error.to_string()))?;
                    let encoded = serde_json::to_vec(&capture).map_err(|error| {
                        gsvc_mount::MountError::Protocol(format!(
                            "serializing frozen native generation failed: {error}"
                        ))
                    })?;
                    let frozen = local
                        .freeze_current_with_capture(encoded)
                        .map_err(|error| {
                            gsvc_mount::MountError::Protocol(format!(
                                "freezing durable native generation failed closed: {error}"
                            ))
                        })?;
                    *frozen_cell.lock().expect("frozen generation cell") = frozen;
                    Ok(())
                })
                .await
                .map_err(|error| CliError::usage(format!("native generation freeze: {error}")))?;
            frozen_cell
                .into_inner()
                .expect("frozen generation cell is not poisoned")
                .ok_or_else(|| {
                    CliError::usage(
                        "durable journal reported clean while the overlay was dirty; refusing \
                         snapshot publication",
                    )
                })?
                .generation
        };

        let existing_request = local
            .publish_requests()
            .map_err(|error| {
                CliError::usage(format!("reading durable native snapshot requests: {error}"))
            })?
            .into_iter()
            .find(|request| request.generation == local_generation);
        if let (Some(existing), Some(incoming_request_id)) =
            (existing_request.as_ref(), request_id.as_deref())
            && existing.request_id != incoming_request_id
        {
            // Explicit coalescing rule: one frozen generation publishes once under the first
            // durable idempotency key/message. Later manual/autosave callers join that request;
            // `--clear` may only strengthen its cleanup policy below. Rejecting the fresh
            // per-invocation CLI id made a Pending snapshot impossible to retry.
            tracing::info!(
                local_generation,
                canonical_request_id = %existing.request_id,
                joined_request_id = %incoming_request_id,
                "coalesced native snapshot request onto the generation's durable publisher"
            );
        }
        let mut request = match existing_request {
            Some(request) if clear && !request.clear_after_publish => local
                .require_clear_after_publish(local_generation)
                .map_err(|error| {
                    CliError::usage(format!(
                        "upgrading durable native snapshot cleanup request failed: {error}"
                    ))
                })?,
            Some(request) => request,
            None => {
                let external = request_id.is_some();
                let request = super::local_state::PublishRequest::new(
                    request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                    local_generation,
                    message,
                    clear,
                    chrono::Utc::now().timestamp_millis().max(0) as u64,
                );
                if external {
                    request
                } else {
                    request.background()
                }
            }
        };
        if let Some(failure) = request.failure.as_deref() {
            return Err(CliError::usage(format!(
                "native snapshot request {} failed permanently: {failure}. Evidence remains in \
                 `tl fs doctor`; automatic retries are disabled",
                request.request_id
            )));
        }
        local
            .put_publish_request(request.clone())
            .map_err(|error| {
                CliError::usage(format!(
                    "persisting native snapshot request before publication failed closed: {error}"
                ))
            })?;

        let Some(prepared_record) = local.prepared(local_generation).map_err(|error| {
            CliError::usage(format!(
                "reading durable native prepared candidate: {error}"
            ))
        })?
        else {
            let watermark = local
                .dirty_generation(local_generation)
                .map_err(|error| {
                    CliError::usage(format!("reading pending native generation: {error}"))
                })?
                .paths
                .into_iter()
                .map(|path| path.sequence)
                .max()
                .unwrap_or(st.sealed_gen);
            drop(st);
            self.native_prepare_notify.notify_one();
            return Ok(SealOutcome::Pending { watermark });
        };
        let mut prepared: PreparedNativeJournal =
            serde_json::from_slice(&prepared_record.candidate).map_err(|error| {
                CliError::usage(format!(
                    "durable native prepared generation {local_generation} is corrupt: {error}"
                ))
            })?;
        if prepared.local_generation != local_generation || prepared.format_ver != 4 {
            return Err(CliError::usage(format!(
                "durable native prepared generation {local_generation} has an incompatible format"
            )));
        }
        let generation = local
            .generation(local_generation)
            .map_err(|error| CliError::usage(format!("reading native generation state: {error}")))?
            .ok_or_else(|| CliError::usage("durable native generation disappeared"))?;
        let (snapshot_id, files, push_ms) =
            if generation.state == super::local_state::GenerationState::Published {
                (
                    generation.published_snapshot.clone().ok_or_else(|| {
                        CliError::usage("published native generation has no snapshot id")
                    })?,
                    prepared.candidate.files,
                    0,
                )
            } else {
                let (user, token) = self.creds.lock().expect("creds lock").clone();
                eprintln!(
                    "seal: publishing prepared generation={} watermark={} files={} (metadata only)",
                    local_generation, prepared.watermark, prepared.candidate.files,
                );
                let journal_template = prepared.clone();
                let published = super::publish_durable_native_candidate(
                    &self.sdk,
                    &self.project,
                    &self.repo,
                    local,
                    local_generation,
                    &prepared_record.source_fingerprint,
                    prepared.candidate.clone(),
                    request.clone(),
                    &user,
                    &token,
                    Some(self.workspace.clone()),
                    None,
                    move |candidate| {
                        let mut journal = journal_template.clone();
                        journal.base_snapshot =
                            candidate.base_snapshot_id.clone().ok_or_else(|| {
                                CliError::usage("rebased mounted snapshot candidate has no base")
                            })?;
                        journal.candidate = candidate.clone();
                        serde_json::to_vec(&journal).map_err(Into::into)
                    },
                )
                .await?;
                let report = published.report;
                prepared.candidate = published.candidate;
                prepared.base_snapshot = prepared
                    .candidate
                    .base_snapshot_id
                    .clone()
                    .unwrap_or_else(|| prepared.base_snapshot.clone());
                request = published.request;
                let push_ms = published.publish_ms;
                tracing::info!(
                    operation_id = %report.operation_id,
                    local_generation,
                    transport = %report.transport,
                    logical_bytes = report.logical_bytes,
                    stored_bytes = report.stored_bytes,
                    push_ms,
                    client_total_ms = report.client_timings.total_ms,
                    publish_complete_ms = ?report.client_timings.publish_complete_ms,
                    prepared_watermark = prepared.watermark,
                    "native prepared journal publish"
                );
                (report.snapshot_id, report.files, push_ms)
            };

        if request.clear_after_publish && self.core.current_commit() != snapshot_id {
            let mut refreshed = false;
            for _ in 0..50 {
                match self.core.poll_ref().await {
                    Ok(Some(refresh)) => {
                        absorb_refresh(&self.overlay, &self.invalidate, &self.pending, &refresh);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return Err(CliError::usage(format!(
                            "snapshot {snapshot_id} published, but the mount could not refresh \
                             before generation-safe cleanup; cleanup remains durable and will \
                             retry: {error}"
                        )));
                    }
                }
                if self.core.current_commit() == snapshot_id {
                    refreshed = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            if !refreshed {
                return Err(CliError::usage(format!(
                    "snapshot {snapshot_id} published, but the mount has not observed it yet; \
                     generation-safe cleanup remains durable and will retry"
                )));
            }
        }

        let mut baselines =
            Vec::with_capacity(prepared.path_stats.len() + prepared.delete_paths.len());
        for (path, stat) in &prepared.path_stats {
            baselines.push(super::local_state::SealedBaseline::upsert(
                path.clone(),
                snapshot_id.clone(),
                super::local_state::FileIdentity {
                    device: stat.dev,
                    inode: stat.ino,
                    size: stat.size,
                    mtime_secs: stat.mtime_secs,
                    mtime_nanos: stat.mtime_nanos,
                    ctime_secs: stat.ctime_secs,
                    ctime_nanos: stat.ctime_nanos,
                    mode: stat.mode,
                },
                None,
            ));
        }
        baselines.extend(prepared.delete_paths.iter().map(|path| {
            super::local_state::SealedBaseline::delete(path.clone(), snapshot_id.clone())
        }));
        // A requested clear is part of the durable publication request. Complete it before
        // retiring that request so a crash can replay the idempotent trim. `trim_retained` skips
        // paths dirty in N+1 and therefore never deletes writes that raced this publication;
        // ignored/local-only content is intentionally retained because no published snapshot
        // vouches for it.
        let cleared = if request.clear_after_publish {
            // The just-published N entries are still present in the in-memory dirty mirror until
            // retirement. Drop only entries through N's exact watermark before trimming, so they
            // are eligible while writes recorded in N+1 remain protected. If retirement fails,
            // the durable Published generation remains authoritative and the retry repeats this
            // idempotently; a daemon restart seeds N again from redb.
            self.overlay.prune_dirty(prepared.watermark);
            let outcome = self
                .overlay
                .trim_retained(&prepared.sealed_paths, &prepared.delete_paths)
                .await;
            (self.invalidate)(outcome.invals);
            if !outcome.held_open.is_empty() {
                return Err(CliError::usage(format!(
                    "snapshot {snapshot_id} published, but {} retained path(s) are still open \
                     (first: {}); cleanup remains durable and will retry after handles close",
                    outcome.held_open.len(),
                    outcome.held_open[0],
                )));
            }
            Some(
                outcome
                    .trimmed
                    .into_iter()
                    .chain(outcome.tombstones_removed)
                    .collect(),
            )
        } else {
            None
        };
        let report = SealReport {
            completed_request_id: request.requires_ack.then(|| request.request_id.clone()),
            commit: snapshot_id.clone(),
            files,
            chunks_uploaded: 0,
            chunks_total: 0,
            sealed_paths: prepared.sealed_paths.clone(),
            push_ms,
            cleared: cleared.clone(),
        };
        let completed_response = serde_json::to_vec(&report).map_err(|error| {
            CliError::usage(format!(
                "serializing durable native snapshot response failed closed: {error}"
            ))
        })?;
        let baseline_removals = cleared.as_deref().unwrap_or(&[]);
        local
            .retire_published(
                local_generation,
                &snapshot_id,
                &baselines,
                baseline_removals,
                completed_response,
            )
            .map_err(|error| {
                CliError::usage(format!(
                    "adopting and retiring published native generation failed closed: {error}"
                ))
            })?;

        st.sealed_gen = prepared.watermark;
        self.overlay.prune_dirty(prepared.watermark);
        self.overlay.close_dirty_base_if_clean();
        st.recent_seals.push((
            snapshot_id.clone(),
            prepared.sealed_upserts.iter().cloned().collect(),
        ));
        st.chunk_cache.clear();
        if let Err(error) = super::generation_capture::retire_generation_capture(
            &self.state_dir,
            prepared.local_generation,
        ) {
            eprintln!(
                "seal: retiring local generation capture {} failed: {error}",
                prepared.local_generation
            );
        }
        self.publish_mirror(&st);

        let cleared_set: std::collections::HashSet<&str> =
            baseline_removals.iter().map(String::as_str).collect();
        for (path, stat) in &prepared.stats {
            if cleared_set.contains(path.as_str()) {
                st.sealed.upserts.remove(path);
                st.sealed.deletes.remove(path);
            } else {
                st.sealed.upserts.insert(path.clone(), *stat);
                st.sealed.deletes.remove(path);
            }
        }
        for path in &prepared.delete_paths {
            st.sealed.upserts.remove(path);
            if cleared_set.contains(path.as_str()) {
                st.sealed.deletes.remove(path);
            } else {
                st.sealed.deletes.insert(path.clone());
            }
        }
        st.sealed.commit = snapshot_id.clone();

        match self.core.poll_ref().await {
            Ok(Some(refresh)) => {
                absorb_refresh(&self.overlay, &self.invalidate, &self.pending, &refresh)
            }
            Ok(None) => {}
            Err(error) => {
                eprintln!("seal: post-seal refresh failed (follow poll catches up): {error}")
            }
        }
        if !prepared.redirects.is_empty() && self.core.current_commit() == snapshot_id {
            if let Err(error) = self.overlay.consume_redirect_entries(&prepared.redirects) {
                eprintln!("seal: consuming native rename journal failed: {error}");
            }
        }
        Ok(SealOutcome::Sealed(report))
    }

    /// Run ONE seal cycle: resolve the dirty delta since the sealed watermark (plus pending
    /// renames) against the on-disk overlay, push it as a snapshot commit carrying `message`,
    /// and advance the lower to the sealed commit. Errors are retryable — nothing was
    /// published, and the dirty set stays pending for the next cycle.
    ///
    /// For native filesystems, `clear` trims only retained paths owned by the published generation;
    /// later writes and ignored/local-only content survive. Repository mounts retain their legacy
    /// whole-overlay clear behavior.
    ///
    /// `progress` receives the push's `PushEvent`s (the `seal` op streams them to the CLI).
    async fn seal_once(
        &self,
        message: &str,
        clear: bool,
        progress: Option<tensorlake::artifact_storage::ingest::PushProgress>,
        request_id: Option<String>,
    ) -> Result<SealOutcome> {
        if self.native_filesystem {
            return self.seal_native_once(message, clear, request_id).await;
        }
        use tensorlake::artifact_storage::ingest::{PushFile, PushOptions, PushSource};
        let mut st = self.state.lock().await;
        if st.reindex_pending {
            // Restore is refilling the overlay out-of-band; the dirty index is not yet
            // rebuilt, so a cycle here would seal (or worse, no-op through) a half-restored
            // world. Retryable — the restore's reindex disarms this.
            return Err(CliError::usage(
                "the overlay is being restored (reindex pending); nothing was sealed",
            ));
        }
        let epoch = self.overlay.epoch();
        if epoch != st.seen_epoch {
            st.seen_epoch = epoch;
            st.chunk_cache.clear();
            st.recent_seals.clear();
            // The sealed index describes the pre-rewrite world too. Restore's own reindex
            // reconciles what genuinely survives; keeping records here would let a
            // stat-coincident future file absolve against dropped content.
            st.sealed = SealedIndex::default();
            SealedIndex::reset(&self.state_dir);
            self.publish_mirror(&st);
        }
        // Drop guard sets the lower has caught up with: the followed ref only moves along
        // this workspace's snapshots, so matching the current lower commit confirms it and
        // everything sealed before it.
        let lower = self.core.current_commit();
        if let Some(i) = st
            .recent_seals
            .iter()
            .rposition(|(commit, _)| *commit == lower)
        {
            st.recent_seals.drain(..=i);
            self.publish_mirror(&st);
        }
        // Renames a previous seal published but never consumed (crash, or the lower lagged
        // past our post-seal check) dangle once the lower advances; reap them before anything
        // resolves through the table.
        if self.overlay.has_redirects() {
            let consumed = self
                .overlay
                .reap_sealed_redirects()
                .await
                .map_err(|e| CliError::usage(format!("reaping sealed renames failed: {e}")))?;
            if !consumed.is_empty() {
                eprintln!("seal: reaped {} sealed rename(s)", consumed.len());
            }
        }
        let mut delta = self.overlay.dirty_since(st.sealed_gen);
        // Dir-over-file self-heal: a mkdir that raced another session's same-named FILE
        // landing has an upper directory shadowing a lower file with no whiteout. Write
        // the missing marker now so this seal publishes the replacement's delete half and
        // the merged view stays coherent (upper-first reads already shadow the file).
        let healed = self
            .overlay
            .heal_replaced_files(delta.upserts.iter().map(|(p, _)| p.as_str()))
            .await
            .map_err(|e| CliError::usage(format!("{e} (nothing was sealed; will retry)")))?;
        if !healed.is_empty() {
            for path in &healed {
                eprintln!("seal: healed dir-over-file at {path} (whiteout written)");
            }
            // Healing records each boundary as a dirty dir upsert whose generation is past
            // the delta just taken. Re-snapshot so THIS seal carries the boundary natively
            // (the dir-upsert arm publishes the whiteout-delete plus children) AND so the
            // watermark covers the heal record — pruning to the pre-heal watermark would
            // leave the boundary dirty and re-publish the same delete every following seal.
            delta = self.overlay.dirty_since(st.sealed_gen);
        }
        let watermark = delta.watermark;
        if delta.is_empty() && !self.overlay.has_redirects() {
            st.sealed_gen = watermark;
            // Nothing to seal: close the mutation clocks (and batch base) so the autosave
            // tick goes back to its idle path instead of re-entering every poll.
            self.overlay.close_dirty_base_if_clean();
            self.publish_mirror(&st);
            let cleared = clear.then(|| self.clear_overlay(&mut st)).transpose()?;
            return Ok(SealOutcome::Clean { cleared });
        }
        // Resolution reads ignore files through the mountpoint — FUSE round-trips served by
        // this very process. Run it on the blocking pool so it can never starve the runtime
        // workers serving it.
        let recently: std::collections::HashSet<String> = st
            .recent_seals
            .iter()
            .flat_map(|(_, set)| set)
            .cloned()
            .collect();
        let cached: std::collections::HashMap<String, ChunkList> = delta
            .upserts
            .iter()
            .filter_map(|(path, _)| {
                st.chunk_cache
                    .get(path)
                    .map(|chunks| (path.clone(), chunks.clone()))
            })
            .collect();
        let (sd, mp) = (self.state_dir.clone(), self.mountpoint.clone());
        let resolved =
            tokio::task::spawn_blocking(move || resolve_seal(&sd, &mp, &delta, &recently, &cached))
                .await;
        let mut resolved = match resolved {
            Ok(Ok(resolved)) => resolved,
            Ok(Err(e)) => {
                return Err(CliError::usage(format!(
                    "resolving the dirty delta failed: {e}"
                )));
            }
            Err(e) => return Err(CliError::usage(format!("resolution task failed: {e}"))),
        };
        if self.overlay.epoch() != st.seen_epoch {
            // clear_upper/restore raced this cycle: the resolution described a world that no
            // longer exists — publishing it would delete files that no longer answer. Undo
            // the whiteouts the tombstone arm wrote and bail (the next cycle's epoch check
            // clears the caches).
            for path in &resolved.tombstoned {
                let _ = std::fs::remove_file(self.state_dir.join("wh").join(path));
            }
            if !resolved.tombstoned.is_empty() {
                (self.invalidate)(self.overlay.invals_for(&resolved.tombstoned));
            }
            return Err(CliError::usage(
                "the overlay was rewritten while sealing; nothing was published",
            ));
        }
        if !resolved.tombstoned.is_empty() {
            // The on-disk merged view already flipped when resolve wrote the whiteouts; tell
            // the kernel now — deferring to push success would leave stale positive dentries
            // until TTL if the push fails (the retry routes through the plain-deletes arm and
            // never re-lists these).
            (self.invalidate)(self.overlay.invals_for(&resolved.tombstoned));
        }
        // Pending directory renames seal as by-oid references: every file the destination
        // serves from the lower commits by blob oid (nothing uploads), alongside the source
        // delete the whiteout already produced. Expansion failing leaves the whole delta
        // pending — publishing the source delete without the destination would lose the
        // subtree.
        let redirect_seals = if self.overlay.has_redirects() {
            self.overlay.expand_redirects().await.map_err(|e| {
                CliError::usage(format!(
                    "expanding pending renames failed (will retry): {e}"
                ))
            })?
        } else {
            Vec::new()
        };
        if resolved.files.is_empty() && redirect_seals.is_empty() {
            // The whole delta was ignored paths, bare directories, or files that were born
            // and died between seals: sealed through, nothing to publish.
            st.sealed_gen = watermark;
            self.overlay.prune_dirty(watermark);
            self.publish_mirror(&st);
            let cleared = clear.then(|| self.clear_overlay(&mut st)).transpose()?;
            return Ok(SealOutcome::Clean { cleared });
        }
        {
            // An upper copy-up under a renamed tree shadows the lower file; the resolved walk
            // already carries it.
            let have: std::collections::HashSet<String> =
                resolved.files.iter().map(|f| f.repo_path.clone()).collect();
            for seal in &redirect_seals {
                for file in &seal.files {
                    if !have.contains(&file.path) {
                        resolved.files.push(PushFile {
                            repo_path: file.path.clone(),
                            source: PushSource::KnownOid(file.oid.clone()),
                            mode: Some(file.mode),
                            delete: false,
                        });
                    }
                }
            }
        }
        // Final validity check on every stable prefix: a write below the boundary that landed
        // after the delta snapshot voids the stability claim — sealing it would publish a
        // prefix+tail chimera that never existed on disk. Demote to a full read; the racing
        // write's entry stays pending for the next cycle.
        for file in &mut resolved.files {
            let PushSource::StablePrefix {
                path,
                stable_chunks,
            } = &file.source
            else {
                continue;
            };
            let stable_len: u64 = stable_chunks.iter().map(|(_, s)| *s as u64).sum();
            if self.overlay.min_write_offset(&file.repo_path).unwrap_or(0) < stable_len {
                file.source = PushSource::Path(path.clone());
            }
        }
        let (user, token) = self.creds.lock().expect("creds lock").clone();
        let delete_paths: Vec<String> = resolved
            .files
            .iter()
            .filter(|f| f.delete)
            .map(|f| f.repo_path.clone())
            .collect();
        let sealed_paths: Vec<String> =
            resolved.files.iter().map(|f| f.repo_path.clone()).collect();
        let push_started = std::time::Instant::now();
        let report = self
            .sdk
            .push_files(
                &self.project,
                &self.repo,
                &user,
                &token,
                resolved.files,
                PushOptions {
                    message: message.to_string(),
                    workspace_snapshot: Some(self.workspace.clone()),
                    // The view this delta's edits were made against: the lower at the
                    // batch's FIRST write (not at seal time — the branch can move during
                    // the quiet window, and claiming the later view would turn a genuine
                    // concurrent write into a silent overwrite). The server parents the
                    // snapshot here, so the shared-rw application three-ways against what
                    // the writer saw: overwriting another session's file is a clean write,
                    // concurrent writes collide visibly, and resolving a collision
                    // converges instead of nesting markers. Servers that predate the field
                    // ignore it (old behavior).
                    base: Some(
                        self.overlay
                            .dirty_base()
                            .unwrap_or_else(|| lower.to_string()),
                    ),
                    collect_file_chunks: true,
                    path_prefix: self.subtree.clone(),
                    progress,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| CliError::usage(format!("snapshot push failed (will retry): {e}")))?;
        let push_ms = push_started.elapsed().as_millis() as u64;
        st.sealed_gen = watermark;
        self.overlay.prune_dirty(watermark);
        let report = report.into_inner();
        let sealed_oids = report
            .file_blob_oids
            .iter()
            .map(|(path, oid)| Ok((self.mount_relative_path(path)?.to_string(), oid.clone())))
            .collect::<Result<Vec<_>>>()?;
        self.overlay.record_sealed_oids(sealed_oids);
        self.overlay.close_dirty_base_if_clean();
        st.recent_seals
            .push((report.commit.clone(), resolved.sealed_upserts));
        // Remember what each file's content chunked to; a blunt cap bounds daemon memory (a
        // full re-learn is just one full-cost seal per file).
        for (path, chunks) in &report.file_chunks {
            let local_path = self.mount_relative_path(path)?;
            st.chunk_cache
                .insert(local_path.to_string(), chunks.clone());
        }
        for path in &delete_paths {
            st.chunk_cache.remove(path);
        }
        if st.chunk_cache.len() > 8192 {
            st.chunk_cache.clear();
        }
        self.publish_mirror(&st);
        if self.overlay.epoch() != st.seen_epoch {
            // clear_upper/restore raced the push window: the snapshot itself published fine,
            // but every resolve-time record describes the dropped world — saving it would
            // resurrect a sealed.json the clear just reset, and a stat-coincident future file
            // could absolve against dead content. The next cycle's epoch check retires the
            // remaining caches.
            st.sealed = SealedIndex::default();
            SealedIndex::reset(&self.state_dir);
        } else {
            // Record what this seal vouched for: each pushed upper file under its
            // resolve-time stat, each published delete as an inert whiteout. The save failing
            // costs nothing but a pessimistic restart.
            for (path, stat) in &resolved.stats {
                st.sealed.upserts.insert(path.clone(), *stat);
                st.sealed.deletes.remove(path);
            }
            for path in &delete_paths {
                st.sealed.upserts.remove(path);
                st.sealed.deletes.insert(path.clone());
            }
            st.sealed.commit = report.commit.clone();
            if let Err(e) = st.sealed.save(&self.state_dir) {
                eprintln!("seal: persisting sealed index failed: {e}");
            }
        }
        // Advance the lower to the sealed commit now instead of waiting out the follow poll:
        // from here on, a delete of a just-sealed path sees lower presence and whiteouts
        // normally — and the mount serves the new commit before a manual `seal` replies.
        // Best-effort — the guard above holds every unconfirmed seal, however long the lower
        // lags.
        match self.core.poll_ref().await {
            Ok(Some(refresh)) => {
                absorb_refresh(&self.overlay, &self.invalidate, &self.pending, &refresh)
            }
            Ok(None) => {}
            Err(e) => eprintln!("seal: post-seal refresh failed (follow poll catches up): {e}"),
        }
        // Published renames are consumed only once the lower serves the sealed commit: the
        // new tree carries their destinations directly and drops their sources, so remapping
        // through the entry would dangle from here on — and conversely, consuming against an
        // older commit would make the destinations unreachable.
        if !redirect_seals.is_empty() {
            if self.core.current_commit() == report.commit {
                let dsts: Vec<String> = redirect_seals.iter().map(|s| s.dst.clone()).collect();
                if let Err(e) = self.overlay.consume_redirects(&dsts) {
                    eprintln!("seal: consuming sealed renames failed: {e}");
                }
            } else {
                eprintln!(
                    "seal: lower has not reached sealed snapshot {}; pending renames stay \
                     recorded (next seal republishes them idempotently)",
                    report.commit
                );
            }
        }
        // Tombstone hygiene: once the lower serves the sealed commit, every published
        // whiteout is inert (the tree it hides the path from no longer carries the path).
        // Dropping the markers costs nothing kernel-visible and keeps wh/ from accumulating
        // the workspace's whole deletion history. Retained upper FILES are deliberately NOT
        // trimmed here: they double as the local byte cache — the next write to a sealed
        // path copies up from the local file instead of re-reading pushed bytes back off the
        // server. `tl fs sync` is the flow that needs them gone, and it asks via `trim`.
        if self.core.current_commit() == report.commit && !st.sealed.deletes.is_empty() {
            let tombstones: Vec<String> = st.sealed.deletes.iter().cloned().collect();
            // try, not wait: hygiene must never park the seal (and, transitively, every new
            // mutating op queued behind the write-preferring fence) behind one slow in-flight
            // copy-up. A contended fence just leaves the markers for the next seal.
            if let Some(outcome) =
                self.overlay
                    .try_trim_retained(&[], &tombstones, TrimBoundaries::RetireInert)
                && !outcome.tombstones_removed.is_empty()
            {
                for path in &outcome.tombstones_removed {
                    st.sealed.deletes.remove(path);
                }
                if let Err(e) = st.sealed.save(&self.state_dir) {
                    eprintln!("seal: persisting sealed index failed: {e}");
                }
            }
        }
        // The destructive opt-in, last: only after the seal published AND the lower refresh
        // above ran does dropping the upper leave the mount serving the sealed content. A
        // failed clear does not un-seal — name the commit so the caller knows it exists.
        let cleared = clear
            .then(|| {
                self.clear_overlay(&mut st).map_err(|e| {
                    CliError::usage(format!(
                        "snapshot {} sealed, but clearing the overlay failed: {e}; the \
                         overlay is kept — retry with `tl fs snapshot --clear`",
                        report.commit
                    ))
                })
            })
            .transpose()?;
        Ok(SealOutcome::Sealed(SealReport {
            completed_request_id: None,
            commit: report.commit,
            files: report.files,
            chunks_uploaded: report.chunks_uploaded,
            chunks_total: report.chunks_total,
            sealed_paths,
            push_ms,
            cleared,
        }))
    }

    /// Drop the whole overlay for legacy repository-mount clear/reset, returning the FULL list
    /// of repo paths the drop removed. [`OverlayFs::clear_upper`]'s inval list only covers
    /// paths the kernel ever interned, so the complete answer comes from walking the raw
    /// upper/wh trees first (ignored files and whiteouts included — exactly the state that
    /// never enumerates as dirty but dies here), plus any pending rename destinations whose
    /// resolution flips from remap to direct lower service.
    ///
    /// Runs under the sealer state lock; the caches describing the dropped world die with it
    /// (clear_upper bumps the overlay epoch, which is re-adopted here so the next cycle does
    /// not double-clear).
    fn clear_overlay(&self, st: &mut SealerState) -> Result<Vec<String>> {
        let mut cleared = std::collections::BTreeSet::new();
        collect_raw_overlay_paths(&self.state_dir.join("upper"), &mut cleared);
        collect_raw_overlay_paths(&self.state_dir.join("wh"), &mut cleared);
        for (dst, _) in self.overlay.redirect_entries() {
            cleared.insert(dst);
        }
        let affected = self
            .overlay
            .clear_upper()
            .map_err(|e| CliError::usage(format!("clearing the overlay failed: {e}")))?;
        (self.invalidate)(affected);
        st.seen_epoch = self.overlay.epoch();
        st.chunk_cache.clear();
        st.recent_seals.clear();
        // Nothing is retained anymore; a stale record would absolve a future upper file that
        // happens to stat-match dropped content.
        st.sealed = SealedIndex::default();
        SealedIndex::reset(&self.state_dir);
        self.publish_mirror(&st);
        Ok(cleared.into_iter().collect())
    }

    /// Republish the dirty-op mirror from the authoritative state. Call after every mutation
    /// of `sealed_gen` or `recent_seals`.
    fn publish_mirror(&self, st: &SealerState) {
        let mut mirror = self.mirror.lock().expect("mirror lock");
        mirror.sealed_gen = st.sealed_gen;
        mirror.reindex_pending = st.reindex_pending;
        mirror.recently = st
            .recent_seals
            .iter()
            .flat_map(|(_, set)| set)
            .cloned()
            .collect();
    }

    fn begin_native_restore(
        &self,
        target_snapshot_id: &str,
        discard_local: bool,
    ) -> Result<super::local_state::RestoreOperation> {
        if !self.native_filesystem {
            return Err(CliError::usage(
                "begin_native_restore is only valid for native filesystems",
            ));
        }
        self.local_state
            .as_ref()
            .expect("writable native sealer has durable local state")
            .begin_restore(
                target_snapshot_id,
                discard_local,
                chrono::Utc::now().timestamp_millis().max(0) as u64,
            )
            .map_err(|error| {
                CliError::usage(format!(
                    "persisting native restore before server mutation failed closed: {error}"
                ))
            })
    }

    async fn adopt_native_restore(
        &self,
        request_id: &str,
        snapshot_id: &str,
    ) -> Result<Vec<crate::commands::fs::overlay::OverlayInval>> {
        if !self.native_filesystem {
            return Err(CliError::usage(
                "adopt_native_restore is only valid for native filesystems",
            ));
        }
        let local = self
            .local_state
            .as_ref()
            .expect("writable native sealer has durable local state");
        local
            .record_restore_server_result(request_id, snapshot_id)
            .map_err(|error| {
                CliError::usage(format!(
                    "recording native restore result before local adoption failed closed: {error}"
                ))
            })?;
        let mut st = self.state.lock().await;
        let affected = self
            .overlay
            .with_mutations_frozen(|| self.overlay.clear_upper())
            .await
            .map_err(|error| CliError::usage(format!("adopting restored snapshot: {error}")))?;
        let mut refreshed = self.core.current_commit() == snapshot_id;
        for _ in 0..50 {
            if refreshed {
                break;
            }
            match self.core.poll_ref().await {
                Ok(Some(refresh)) => {
                    absorb_refresh(&self.overlay, &self.invalidate, &self.pending, &refresh);
                    refreshed = self.core.current_commit() == snapshot_id;
                }
                Ok(None) => {
                    refreshed = self.core.current_commit() == snapshot_id;
                }
                Err(error) => {
                    return Err(CliError::usage(format!(
                        "server restore {snapshot_id} is durable, but refreshing the mount failed; \
                         the restore remains fenced and will resume: {error}"
                    )));
                }
            }
            if !refreshed {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        if !refreshed {
            return Err(CliError::usage(format!(
                "server restore {snapshot_id} is durable, but the mount has not observed it yet; \
                 the restore remains fenced and will resume"
            )));
        }
        local.finish_restore(request_id).map_err(|error| {
            CliError::usage(format!(
                "resetting durable local state after server restore failed closed: {error}"
            ))
        })?;
        st.seen_epoch = self.overlay.epoch();
        st.sealed_gen = self.overlay.current_generation();
        st.chunk_cache.clear();
        st.recent_seals.clear();
        st.sealed = SealedIndex::default();
        st.sealed.commit = snapshot_id.to_string();
        st.reindex_pending = false;
        self.publish_mirror(&st);
        Ok(affected)
    }

    fn acknowledge_restore_response(&self, request_id: &str) -> Result<()> {
        let local = self.local_state.as_ref().ok_or_else(|| {
            CliError::usage("legacy repository mounts do not use native restore receipts")
        })?;
        local
            .acknowledge_restore(request_id)
            .map_err(|error| CliError::usage(format!("acknowledging restore response: {error}")))
    }

    fn fail_native_restore(&self, request_id: &str, reason: &str) -> Result<()> {
        let local = self.local_state.as_ref().ok_or_else(|| {
            CliError::usage("legacy repository mounts do not use native restore lifecycle")
        })?;
        local.fail_restore(request_id, reason).map_err(|error| {
            CliError::usage(format!("dead-lettering failed native restore: {error}"))
        })
    }

    fn active_restore(&self) -> Result<Option<super::local_state::RestoreOperation>> {
        match self.local_state.as_ref() {
            Some(local) => local
                .active_restore()
                .map_err(|error| CliError::usage(format!("reading active restore: {error}"))),
            None => Ok(None),
        }
    }

    /// `clear_upper` routed through the sealer (adopted from the parallel #840 draft): the
    /// state lock serializes the drop against in-flight seals — a clear can no longer land
    /// inside a push window — and `reindex_pending` arms the fail-closed guard for the
    /// out-of-band refill that follows.
    async fn clear_upper_control(&self) -> Result<Vec<crate::commands::fs::overlay::OverlayInval>> {
        let mut st = self.state.lock().await;
        let affected = self
            .overlay
            .clear_upper()
            .map_err(|e| CliError::usage(format!("clearing the overlay failed: {e}")))?;
        // The caches (and the sealed index) describe the dropped world; adopt the new epoch
        // here so the next cycle doesn't double-clear.
        st.seen_epoch = self.overlay.epoch();
        st.chunk_cache.clear();
        st.recent_seals.clear();
        st.sealed = SealedIndex::default();
        SealedIndex::reset(&self.state_dir);
        st.reindex_pending = true;
        self.publish_mirror(&st);
        Ok(affected)
    }

    /// `reindex` routed through the sealer: rebuild the dirty index from the refilled
    /// overlay, reconcile retained state, and disarm the fail-closed guard — under the same
    /// lock that serializes seals, so no cycle observes the half-rebuilt index.
    async fn reindex_control(&self) -> Result<()> {
        let mut st = self.state.lock().await;
        self.overlay
            .rebuild_dirty_index()
            .map_err(|e| CliError::usage(format!("rebuilding the dirty index failed: {e}")))?;
        reconcile_sealed(&self.state_dir, &self.overlay);
        st.seen_epoch = self.overlay.epoch();
        st.chunk_cache.clear();
        st.recent_seals.clear();
        st.reindex_pending = false;
        self.publish_mirror(&st);
        Ok(())
    }

    /// The truthful dirty view — exactly what the next seal would publish, resolved by the
    /// dry-run twin of the seal walk. Reads the mirror, not the state lock, so it answers
    /// instantly while a push is in flight.
    async fn dirty_view(&self) -> Result<DirtyReply> {
        let (sealed_gen, recently) = {
            let mirror = self.mirror.lock().expect("mirror lock");
            if mirror.reindex_pending {
                // Between restore's clear and its reindex the dirty index is empty while the
                // upper is being refilled — answering would claim a half-restored workspace
                // is clean.
                return Err(CliError::usage(
                    "the overlay is being restored (reindex pending); retry when the restore \
                     completes",
                ));
            }
            (mirror.sealed_gen, mirror.recently.clone())
        };
        let delta = self.overlay.dirty_since(sealed_gen);
        let renames: Vec<(String, String)> = self
            .overlay
            .redirect_entries()
            .into_iter()
            .map(|(dst, src)| (src, dst))
            .collect();
        let commit = self.core.current_commit();
        if delta.is_empty() {
            return Ok(DirtyReply {
                upserts: Vec::new(),
                deletes: Vec::new(),
                renames,
                commit,
            });
        }
        let (sd, mp) = (self.state_dir.clone(), self.mountpoint.clone());
        let include_directories = self.native_filesystem;
        // Same blocking-pool rule as resolve_seal: the ignore rules read through the
        // mountpoint this very process serves.
        let (upserts, deletes) = tokio::task::spawn_blocking(move || {
            resolve_dirty(&sd, &mp, &delta, &recently, include_directories)
        })
        .await
        .map_err(|e| CliError::usage(format!("dirty resolution task failed: {e}")))?
        .map_err(|e| CliError::usage(format!("resolving the dirty view failed: {e}")))?;
        Ok(DirtyReply {
            upserts,
            deletes,
            renames,
            commit,
        })
    }

    async fn overlay_safety_view(&self) -> Result<OverlaySafetyReply> {
        let sealed = self.state.lock().await.sealed.clone();
        let dirty = if let Some(local) = &self.local_state {
            // Unlike the user-facing dirty view, the destructive gate must include frozen and
            // publish-requested generations too. They no longer sit in the overlay's active
            // in-memory generation, but deleting the state directory would still discard their
            // captured work and retry authority.
            let recovery = local.recovery_dirty_state().map_err(|error| {
                CliError::usage(format!(
                    "reading durable local overlay safety state failed closed: {error}"
                ))
            })?;
            let mut upserts = std::collections::BTreeSet::new();
            let mut deletes = std::collections::BTreeSet::new();
            for path in recovery.paths {
                match path.kind {
                    super::local_state::DirtyKind::Upsert => {
                        upserts.insert(path.path);
                    }
                    super::local_state::DirtyKind::Delete => {
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
                        "reading durable local overlay base failed closed: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    CliError::usage(format!(
                        "durable local overlay active generation {} is missing",
                        recovery.active_generation
                    ))
                })?;
            DirtyReply {
                upserts: upserts.into_iter().collect(),
                deletes: deletes.into_iter().collect(),
                renames: renames.into_iter().collect(),
                commit: active.base_snapshot.unwrap_or_default(),
            }
        } else {
            self.dirty_view().await?
        };
        Ok(OverlaySafetyReply { dirty, sealed })
    }

    /// Drop ALL retained overlay state (`trim` control op — `tl fs sync`'s pre-flight).
    /// Everything dropped is sealed into workspace history, so nothing is lost; the mount just
    /// stops shadowing the lower, which is exactly what a sync needs before it pulls content
    /// the retained copies would mask. Dirty and ignored files are untouched.
    async fn trim_all(&self) -> Result<TrimReply> {
        let mut st = self.state.lock().await;
        if st.reindex_pending {
            return Err(CliError::usage(
                "the overlay is being restored (reindex pending); retry when the restore \
                 completes",
            ));
        }
        let epoch = self.overlay.epoch();
        if epoch != st.seen_epoch {
            // clear_upper/restore rewrote the world; the sealed index describes the old one.
            if let Some(local) = &self.local_state {
                let stale = local
                    .sealed_baselines()
                    .map_err(|error| {
                        CliError::usage(format!("reading stale native retained baselines: {error}"))
                    })?
                    .into_iter()
                    .map(|baseline| baseline.path)
                    .collect::<Vec<_>>();
                local.remove_sealed_baselines(&stale).map_err(|error| {
                    CliError::usage(format!("retiring stale native retained baselines: {error}"))
                })?;
            }
            st.seen_epoch = epoch;
            st.chunk_cache.clear();
            st.recent_seals.clear();
            st.sealed = SealedIndex::default();
            SealedIndex::reset(&self.state_dir);
            self.publish_mirror(&st);
            return Ok(TrimReply::default());
        }
        let candidates: Vec<String> = st.sealed.upserts.keys().cloned().collect();
        // Retained UPSERTS may drop regardless of where the lower ref sits — their bytes are
        // in workspace history and the caller (sync) is about to move the view anyway. A
        // whiteout is different: until the lower serves the commit that published its delete,
        // the marker is still actively hiding the path — removing it early resurrects the
        // deleted file. Leave tombstones for a later seal/trim when the lower lags.
        let tombstones: Vec<String> = if self.core.current_commit() == st.sealed.commit {
            st.sealed.deletes.iter().cloned().collect()
        } else {
            Vec::new()
        };
        if candidates.is_empty() && tombstones.is_empty() {
            return Ok(TrimReply::default());
        }
        let outcome = self.overlay.trim_retained(&candidates, &tombstones).await;
        (self.invalidate)(outcome.invals);
        for path in &outcome.trimmed {
            st.sealed.upserts.remove(path);
        }
        for path in &outcome.tombstones_removed {
            st.sealed.deletes.remove(path);
        }
        if let Some(local) = &self.local_state {
            let removed = outcome
                .trimmed
                .iter()
                .chain(outcome.tombstones_removed.iter())
                .cloned()
                .collect::<Vec<_>>();
            local.remove_sealed_baselines(&removed).map_err(|error| {
                CliError::usage(format!(
                    "recording native retained-path trim durably: {error}"
                ))
            })?;
        }
        if !self.native_filesystem
            && let Err(e) = st.sealed.save(&self.state_dir)
        {
            eprintln!("trim: persisting sealed index failed: {e}");
        }
        Ok(TrimReply {
            trimmed: (outcome.trimmed.len() + outcome.tombstones_removed.len()) as u64,
            held_open: outcome.held_open,
        })
    }
}

/// Collect every file and symlink under an overlay tree as repo-relative paths — raw, no
/// ignore filtering (whiteout markers in `wh` are plain files, so one walk serves both trees).
#[cfg(unix)]
fn collect_raw_overlay_paths(root: &Path, out: &mut std::collections::BTreeSet<String>) {
    fn walk(root: &Path, dir: &Path, out: &mut std::collections::BTreeSet<String>) {
        let Ok(read) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in read.flatten() {
            let abs = entry.path();
            let Ok(meta) = abs.symlink_metadata() else {
                continue;
            };
            if meta.is_dir() && !meta.file_type().is_symlink() {
                walk(root, &abs, out);
                continue;
            }
            let rel = abs
                .strip_prefix(root)
                .expect("under root")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.insert(rel);
        }
    }
    walk(root, root, out)
}

#[cfg(unix)]
fn native_change_signature(
    changes: &tensorlake::artifact_storage::native_fs::NativeChangeSet,
) -> Vec<String> {
    let mut signature: Vec<String> = changes
        .upserts
        .iter()
        .map(|upsert| format!("u:{}", upsert.path))
        .chain(changes.deletes.iter().map(|path| format!("d:{path}")))
        .chain(
            changes
                .renames
                .iter()
                .map(|rename| format!("r:{}\0{}", rename.from, rename.to)),
        )
        .collect();
    signature.sort();
    signature
}

/// One tick's seal work, resolved from the overlay's event delta against the on-disk overlay
/// state. Produced by [`resolve_seal`].
#[cfg(unix)]
struct ResolvedSeal {
    files: Vec<tensorlake::artifact_storage::ingest::PushFile>,
    /// Upper-backed directories touched by the delta, including empty directories and the
    /// descendants of a directory upsert. Native snapshots preserve these; Git ignores them.
    directories: Vec<(String, PathBuf)>,
    /// Paths whose content this seal publishes — the next ticks' resurrection guard.
    sealed_upserts: std::collections::HashSet<String>,
    /// Vanished-but-recently-sealed paths that got a whiteout written here; their merged view
    /// flipped without a kernel-visible operation, so the kernel needs invalidations.
    tombstoned: Vec<String>,
    /// Resolve-time [`SealedStat`] of every upper-backed upsert — what the sealed index
    /// records for each path once the push succeeds.
    stats: std::collections::HashMap<String, SealedStat>,
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
    let mut directories: Vec<(String, PathBuf)> = Vec::new();
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
            //
            // A whiteout at the same name is the other half of a file→directory replacement
            // (`rm file; mkdir file`): the mkdir's dirty record overwrote the rm's, but the
            // marker survives. Publish the delete alongside the children — without it the
            // application reads the adds as a kind conflict against the still-present file
            // and keeps the file.
            if whited_out_on_disk(&wh, path) {
                deletes.push(path.clone());
            }
            collect_dir_upserts(&upper, path, &mut ignore, &mut upserts, &mut directories)?;
            continue;
        }
        if ignore.is_ignored(path, false)? {
            continue;
        }
        upserts.push((path.clone(), abs, git_mode(&meta)));
    }
    for path in delta.deletes.iter().chain(vanished.iter()) {
        if upper
            .join(path)
            .symlink_metadata()
            .is_ok_and(|m| !m.is_dir() || m.file_type().is_symlink())
        {
            // Re-created as CONTENT since the event; its own upsert event covers it. An
            // upper DIRECTORY over the deleted name is different — a file→directory
            // replacement, whose children ride their own upserts while this delete is the
            // other half — and falls through to publish via its whiteout.
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
    directories.sort_by(|a, b| a.0.cmp(&b.0));
    directories.dedup_by(|a, b| a.0 == b.0);
    deletes.sort();
    deletes.dedup();
    // The sealed index's identity capture, stat'd here — before the push reads the bytes —
    // so any later write mismatches the record (see [`SealedStat`]).
    let stats: std::collections::HashMap<String, SealedStat> = upserts
        .iter()
        .filter_map(|(rel, abs, _)| {
            std::fs::symlink_metadata(abs)
                .ok()
                .map(|meta| (rel.clone(), SealedStat::of(&meta)))
        })
        .collect();
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
        sealed_upserts: upserts
            .iter()
            .map(|(p, _, _)| p.clone())
            .chain(directories.iter().map(|(p, _)| p.clone()))
            .collect(),
        files,
        directories,
        tombstoned,
        stats,
    })
}

/// The dry-run twin of [`resolve_seal`]: what WOULD the next seal publish. Same delta, same
/// ignore rules, same directory-event expansion, but NO side effects — the tombstone arm
/// reports the delete without writing the whiteout, and nothing chunk-related runs. This is
/// what the `dirty` control op serves, so `tl fs status` and `tl fs snapshot` cannot disagree:
/// they resolve the same state through the same walk.
///
/// Runs on the blocking pool for the same reason as [`resolve_seal`] (ignore files are read
/// through the mountpoint this daemon serves).
#[cfg(unix)]
fn resolve_dirty(
    state_dir: &Path,
    mount_root: &Path,
    delta: &super::overlay::DirtyDelta,
    recently_sealed: &std::collections::HashSet<String>,
    include_directories: bool,
) -> crate::error::Result<(Vec<String>, Vec<String>)> {
    let mut ignore = super::SnapshotIgnore::new(mount_root);
    let upper = state_dir.join("upper");
    let wh = state_dir.join("wh");
    let mut upserts: super::OverlayUpserts = Vec::new();
    let mut directories = Vec::new();
    let mut deletes: Vec<String> = Vec::new();
    let mut vanished: Vec<String> = Vec::new();
    for (path, _) in &delta.upserts {
        let abs = upper.join(path);
        let Ok(meta) = std::fs::symlink_metadata(&abs) else {
            vanished.push(path.clone());
            continue;
        };
        if meta.is_dir() && !meta.file_type().is_symlink() {
            // Parity with resolve_seal's replacement arm: a whiteout under a directory
            // upsert is the delete half of a file→directory replacement, and status must
            // report what the next seal will publish.
            if whited_out_on_disk(&wh, path) {
                deletes.push(path.clone());
            }
            collect_dir_upserts(&upper, path, &mut ignore, &mut upserts, &mut directories)?;
            continue;
        }
        if ignore.is_ignored(path, false)? {
            continue;
        }
        upserts.push((path.clone(), abs, git_mode(&meta)));
    }
    for path in delta.deletes.iter().chain(vanished.iter()) {
        // Parity with resolve_seal: an upper DIRECTORY over the deleted name is a
        // file→directory replacement whose delete still publishes; only re-created
        // CONTENT covers the event via its own upsert.
        if upper
            .join(path)
            .symlink_metadata()
            .is_ok_and(|m| !m.is_dir() || m.file_type().is_symlink())
        {
            continue;
        }
        if ignore.is_ignored(path, false)? {
            continue;
        }
        if whited_out_on_disk(&wh, path) || recently_sealed.contains(path) {
            deletes.push(path.clone());
        }
    }
    let mut upserts: Vec<String> = upserts.into_iter().map(|(rel, _, _)| rel).collect();
    if include_directories {
        upserts.extend(directories.into_iter().map(|(rel, _)| rel));
    }
    upserts.sort();
    upserts.dedup();
    deletes.sort();
    deletes.dedup();
    Ok((upserts, deletes))
}

/// The git mode a local file publishes as (same policy as `enumerate_overlay`'s walk).
#[cfg(unix)]
fn git_mode(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;
    git_mode_from_raw(meta.mode())
}

#[cfg(unix)]
fn git_mode_from_raw(mode: u32) -> u32 {
    match mode & libc::S_IFMT as u32 {
        value if value == libc::S_IFDIR as u32 => 0o040000,
        value if value == libc::S_IFLNK as u32 => 0o120000,
        _ if mode & 0o111 != 0 => 0o100755,
        _ => 0o100644,
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
    directories: &mut Vec<(String, PathBuf)>,
) -> crate::error::Result<()> {
    let abs_dir = upper.join(dir_rel);
    if ignore.is_ignored(dir_rel, true)? {
        return Ok(());
    }
    directories.push((dir_rel.to_string(), abs_dir.clone()));
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
                collect_dir_upserts(upper, &rel, ignore, upserts, directories)?;
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

    fn recovery_with_renames(
        active_generation: u64,
        renames: Vec<super::super::local_state::RenameIntent>,
    ) -> super::super::local_state::RecoveryDirtyState {
        super::super::local_state::RecoveryDirtyState {
            active_generation,
            maximum_mutation_sequence: renames
                .iter()
                .map(|rename| rename.sequence)
                .max()
                .unwrap_or(0),
            paths: Vec::new(),
            renames,
            intents: Vec::new(),
        }
    }

    fn recovery_rename(
        generation: u64,
        sequence: u64,
        from: &str,
        local_from: &str,
        to: &str,
        applied: bool,
    ) -> super::super::local_state::RenameIntent {
        super::super::local_state::RenameIntent {
            format_ver: 1,
            generation,
            sequence,
            from: from.to_string(),
            local_from: local_from.to_string(),
            to: to.to_string(),
            applied,
        }
    }

    #[test]
    fn recovery_ignores_write_ahead_rename_until_namespace_application_commits() {
        let recovery = recovery_with_renames(
            1,
            vec![recovery_rename(
                1,
                1,
                "source",
                "source",
                "destination",
                false,
            )],
        );
        assert!(
            overlay_recovery_renames(&recovery).is_empty(),
            "a crash after intent persistence but before rename(2) must not move the namespace"
        );

        let recovery = recovery_with_renames(
            1,
            vec![recovery_rename(
                1,
                1,
                "source",
                "source",
                "destination",
                true,
            )],
        );
        assert_eq!(
            overlay_recovery_renames(&recovery),
            vec![(
                "destination".to_string(),
                "source".to_string(),
                "source".to_string(),
                1,
            )],
            "the separately committed applied bit makes the redirect crash-visible"
        );
    }

    #[test]
    fn recovery_composes_rename_chains_across_generations_and_rebases_after_retirement() {
        // Generation N moves lower-backed a -> b. While N is publishing, N+1 moves the live
        // destination b -> c. Recovery before N retires must still serve c from the original
        // lower coordinate a.
        let before_retirement = recovery_with_renames(
            2,
            vec![
                recovery_rename(1, 1, "a", "a", "b", true),
                recovery_rename(2, 2, "b", "b", "c", true),
            ],
        );
        assert_eq!(
            overlay_recovery_renames(&before_retirement),
            vec![("c".to_string(), "a".to_string(), "b".to_string(), 2)],
            "the live redirect is one hop to the pre-publication lower coordinate"
        );

        // Once N is published, the refreshed lower serves b directly and retirement removes N's
        // row. N+1 must therefore recover as c -> b, not retain the obsolete c -> a mapping.
        let after_retirement =
            recovery_with_renames(2, vec![recovery_rename(2, 2, "b", "b", "c", true)]);
        assert_eq!(
            overlay_recovery_renames(&after_retirement),
            vec![("c".to_string(), "b".to_string(), "b".to_string(), 2)],
            "surviving intent rebases to the coordinate provided by the advanced lower"
        );
    }

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

/// The dirty view (dry-run resolution) and the sealed index: what `tl fs status` shows must
/// be what a seal would publish, asking must never mutate overlay state, and seal records
/// must survive restarts by exact stat identity.
#[cfg(all(test, unix))]
mod seal_tracking_tests {
    use super::super::overlay::DirtyDelta;
    use super::tests::state_with;
    use super::*;

    fn delta(upserts: &[&str], deletes: &[&str]) -> DirtyDelta {
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
    fn resolve_dirty_matches_what_a_seal_would_publish() {
        let state = state_with(
            &[("kept.txt", "hi"), ("dir/nested.txt", "deep")],
            &["gone.txt"],
        );
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join(".gitignore"), "*.tmp\n").unwrap();
        std::fs::write(state.path().join("upper/junk.tmp"), "x").unwrap();

        let d = delta(&["kept.txt", "dir/nested.txt", "junk.tmp"], &["gone.txt"]);
        let (upserts, deletes) = resolve_dirty(
            state.path(),
            mount.path(),
            &d,
            &std::collections::HashSet::new(),
            false,
        )
        .unwrap();
        let sealed = resolve_seal(
            state.path(),
            mount.path(),
            &d,
            &std::collections::HashSet::new(),
            &no_cache(),
        )
        .unwrap();

        let mut published: Vec<String> = sealed
            .files
            .iter()
            .filter(|f| !f.delete)
            .map(|f| f.repo_path.clone())
            .collect();
        published.sort();
        assert_eq!(upserts, published, "dry run and seal agree on upserts");
        let mut sealed_deletes: Vec<String> = sealed
            .files
            .iter()
            .filter(|f| f.delete)
            .map(|f| f.repo_path.clone())
            .collect();
        sealed_deletes.sort();
        assert_eq!(deletes, sealed_deletes, "dry run and seal agree on deletes");
        assert!(
            !upserts.contains(&"junk.tmp".to_string()),
            "ignored paths never show"
        );
    }

    #[test]
    fn resolve_dirty_expands_directory_events() {
        let state = state_with(
            &[("moved/a.txt", "alpha"), ("moved/sub/b.txt", "beta")],
            &[],
        );
        let mount = tempfile::tempdir().unwrap();
        let (upserts, deletes) = resolve_dirty(
            state.path(),
            mount.path(),
            &delta(&["moved"], &[]),
            &std::collections::HashSet::new(),
            false,
        )
        .unwrap();
        assert_eq!(upserts, vec!["moved/a.txt", "moved/sub/b.txt"]);
        assert!(deletes.is_empty());
    }

    #[test]
    fn native_dirty_view_preserves_empty_directories() {
        let state = state_with(&[], &[]);
        std::fs::create_dir_all(state.path().join("upper/empty")).unwrap();
        let mount = tempfile::tempdir().unwrap();
        let (upserts, deletes) = resolve_dirty(
            state.path(),
            mount.path(),
            &delta(&["empty"], &[]),
            &std::collections::HashSet::new(),
            true,
        )
        .unwrap();
        assert_eq!(upserts, vec!["empty"]);
        assert!(deletes.is_empty());
    }

    #[test]
    fn resolve_dirty_reports_recently_sealed_vanished_without_writing_the_whiteout() {
        // The seal's tombstone arm WRITES a whiteout for a vanished-but-recently-sealed path.
        // The dry run must report the same delete but leave the overlay untouched — status
        // runs must be idempotent and side-effect-free.
        let state = state_with(&[], &[]);
        let mount = tempfile::tempdir().unwrap();
        let recently: std::collections::HashSet<String> =
            [String::from("vanished.txt")].into_iter().collect();
        let (upserts, deletes) = resolve_dirty(
            state.path(),
            mount.path(),
            &delta(&["vanished.txt"], &[]),
            &recently,
            false,
        )
        .unwrap();
        assert!(upserts.is_empty());
        assert_eq!(deletes, vec!["vanished.txt"]);
        assert!(
            !state.path().join("wh/vanished.txt").exists(),
            "the dry run must not write the whiteout the real seal would"
        );
    }

    // ---------------------------------------------------------------------------------------
    // The sealed index: per-path seal records that survive daemon restarts.
    // ---------------------------------------------------------------------------------------

    #[test]
    fn sealed_index_roundtrips_and_tolerates_corruption() {
        let state = state_with(&[("a.txt", "alpha")], &[]);
        let meta = std::fs::symlink_metadata(state.path().join("upper/a.txt")).unwrap();
        let mut index = SealedIndex {
            commit: "c1".into(),
            ..Default::default()
        };
        index.upserts.insert("a.txt".into(), SealedStat::of(&meta));
        index.deletes.insert("gone.txt".into());
        index.save(state.path()).unwrap();

        let loaded = SealedIndex::load(state.path());
        assert_eq!(loaded.commit, "c1");
        assert_eq!(loaded.upserts.get("a.txt"), Some(&SealedStat::of(&meta)));
        assert!(loaded.deletes.contains("gone.txt"));

        std::fs::write(state.path().join("sealed.json"), b"{not json").unwrap();
        let corrupt = SealedIndex::load(state.path());
        assert!(
            corrupt.upserts.is_empty() && corrupt.deletes.is_empty(),
            "corruption degrades to the pessimistic empty index, never an error"
        );
    }

    #[test]
    fn sealed_index_uses_active_generation_base_not_an_arbitrary_path_baseline() {
        let state = tempfile::tempdir().unwrap();
        let identity = super::super::local_state::LocalStateIdentity::fresh(
            "project",
            "filesystem",
            "workspace",
        );
        let local = super::super::local_state::LocalState::open(
            state
                .path()
                .join(super::super::local_state::LOCAL_STATE_FILE),
            identity,
        )
        .unwrap();
        local
            .import_legacy_once(super::super::local_state::LegacyImport {
                base_snapshot: Some("old-base".to_string()),
                mutations: Vec::new(),
            })
            .unwrap();
        local.reset_after_restore("current-base").unwrap();

        let index = sealed_index_from_local_state(&local).unwrap();
        assert_eq!(index.commit, "current-base");
    }

    #[test]
    fn sealed_mode_preserves_directory_kind() {
        let state = tempfile::tempdir().unwrap();
        let directory = std::fs::symlink_metadata(state.path()).unwrap();
        assert_eq!(git_mode(&directory), 0o040000);
        assert_eq!(git_mode_from_raw(libc::S_IFDIR as u32 | 0o755), 0o040000);
    }

    #[test]
    fn legacy_snapshot_metadata_cleanup_removes_every_superseded_file() {
        let state = tempfile::tempdir().unwrap();
        let names = [
            "sealed.json",
            "sealed.json.tmp",
            "native-prepared.json",
            "native-prepared.json.tmp",
            "native-seal-request.json",
            "native-seal-request.json.tmp",
        ];
        for name in names {
            std::fs::write(state.path().join(name), b"legacy").unwrap();
        }

        remove_legacy_native_snapshot_state(state.path()).unwrap();
        remove_legacy_native_snapshot_state(state.path()).unwrap();

        for name in names {
            assert!(!state.path().join(name).exists(), "{name} was not removed");
        }
    }

    #[test]
    fn sealed_survivors_matches_by_exact_stat_identity() {
        let state = state_with(
            &[("same.txt", "stable"), ("changed.txt", "old")],
            &["dead.txt"],
        );
        let stat_of = |p: &str| {
            SealedStat::of(&std::fs::symlink_metadata(state.path().join("upper").join(p)).unwrap())
        };
        let mut index = SealedIndex::default();
        index.upserts.insert("same.txt".into(), stat_of("same.txt"));
        index
            .upserts
            .insert("changed.txt".into(), stat_of("changed.txt"));
        index
            .upserts
            .insert("missing.txt".into(), stat_of("same.txt"));
        index.deletes.insert("dead.txt".into());
        index.deletes.insert("reaped.txt".into());

        // Rewrite one file with different content (size changes → identity breaks even if the
        // filesystem's mtime granularity is coarse).
        std::fs::write(state.path().join("upper/changed.txt"), "newer-bytes").unwrap();

        let (upserts, deletes) = sealed_survivors(state.path(), &index);
        assert_eq!(
            upserts,
            vec!["same.txt"],
            "only the untouched file survives"
        );
        assert_eq!(
            deletes,
            vec!["dead.txt"],
            "only the still-present whiteout survives"
        );
    }

    #[test]
    fn resolve_seal_captures_stats_for_every_upper_upsert() {
        let state = state_with(&[("a.txt", "alpha"), ("dir/b.txt", "beta")], &[]);
        let mount = tempfile::tempdir().unwrap();
        let resolved = resolve_seal(
            state.path(),
            mount.path(),
            &delta(&["a.txt", "dir/b.txt"], &[]),
            &std::collections::HashSet::new(),
            &no_cache(),
        )
        .unwrap();
        for path in ["a.txt", "dir/b.txt"] {
            let meta = std::fs::symlink_metadata(state.path().join("upper").join(path)).unwrap();
            assert_eq!(
                resolved.stats.get(path),
                Some(&SealedStat::of(&meta)),
                "resolve-time stat recorded for {path}"
            );
        }
    }
}
