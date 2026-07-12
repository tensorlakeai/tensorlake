//! Plain-directory workspace snapshots: `tl fs init` binds an ordinary directory to a
//! workspace, and `tl fs snapshot` on that binding scans + pushes it — no FUSE mount, no
//! overlay, no kernel coherence tax (plan "decouple snapshot capture", phase 1 v1).
//!
//! The design constraint everything here serves: **the server delta is computed from a local
//! stat index, and a wrong index turns into silent server-side deletion or stale content.**
//! Hence:
//!
//! - Two kinds of state per path, never conflated: the *remote baseline* (`server_oid`,
//!   `server_mode` at `indexed_head` — what a delete or an unchanged path means server-side)
//!   and the *local cleanliness proof* (`clean_fingerprint` — permission to skip rehashing).
//!   Losing the proof costs a rehash; losing the baseline costs correctness, so a missing or
//!   corrupt index fails closed instead of rescanning into a mass delete.
//! - Scanning is strict: an unreadable directory or ignore file aborts the snapshot (a
//!   silently skipped subtree would enumerate as deletion of everything under it).
//! - Snapshots are crash-safe: a write-ahead journal (idempotency key + the exact candidate
//!   per-path oids, hashed locally *before* `push_files` runs) is durably on disk before any
//!   commit can publish, and recovery on the next snapshot decides fresh-retry vs adopt vs
//!   fail-closed from the observed workspace head.
//! - Scope is v1-narrow: empty-base workspaces only (`init` verifies the base commit's root
//!   tree is empty), single writer (a dedicated flock'd lock file serializes snapshots),
//!   `sync`/`restore`/`promote` rejected.
//!
//! Known v1 costs, accepted deliberately: changed files are hashed twice (once here to
//! journal their oids before the push, once inside `push_files` — content-addressed, so the
//! second pass uploads nothing extra), and the workspace lease is only heartbeaten when a
//! command runs (no daemon exists to re-arm it in the background).

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use console::style;
use serde::{Deserialize, Serialize};
use tensorlake::artifact_storage::ingest::{BlobOidHasher, PushFile, PushOptions, PushSource};
use tensorlake::artifact_storage::workspaces::CreateWorkspaceRequest;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

use super::{FsSession, SnapshotIgnore, canonical_mountpoint, fmt_dur, short_id};

// ---------------------------------------------------------------------------------------------
// Binding registry: canonical directory -> binding state dir. Deliberately a *typed* registry
// file separate from the untyped mounts.toml map — bindings and mounts have different
// lifecycles (no daemon, no kernel volume) and different commands (unbind vs unmount), and
// collision checks need to see both without either registry guessing at the other's shape.
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
struct BindingRegistry {
    /// Canonical bound directory -> binding state dir.
    #[serde(default)]
    bindings: BTreeMap<String, PathBuf>,
}

fn registry_path() -> PathBuf {
    crate::config::files::config_dir().join("bindings.json")
}

/// Where binding state dirs live (sibling of the mounts state root).
fn state_dir_root() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("share")
        .join("tensorlake")
        .join("bindings")
}

/// Fail-closed load: unlike the mounts registry (where a bad parse degrades to "not
/// mounted"), an unreadable binding registry must not read as empty — `init` would happily
/// double-bind and `snapshot` would claim the directory is unbound.
fn registry_load() -> Result<BindingRegistry> {
    registry_load_at(&registry_path())
}

/// [`registry_load`] against an explicit path (the unit-testable core).
fn registry_load_at(path: &Path) -> Result<BindingRegistry> {
    match std::fs::read(path) {
        Ok(raw) => serde_json::from_slice(&raw).map_err(|e| {
            CliError::usage(format!(
                "the binding registry {} is corrupt ({e}); refusing to guess. Repair or move \
                 the file aside — binding state dirs under {} are untouched.",
                path.display(),
                state_dir_root().display(),
            ))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BindingRegistry::default()),
        Err(e) => Err(CliError::usage(format!(
            "cannot read the binding registry {}: {e}",
            path.display()
        ))),
    }
}

/// Warn-once state for [`registry_lenient`], hoisted to module level on purpose: a static
/// inside the generic fn is monomorphized per `T`, i.e. "once per instantiation" — several
/// warnings per process instead of one.
static REGISTRY_WARNED: std::sync::Once = std::sync::Once::new();

/// Set (to the registry file's path) the first time a lenient load hit corruption. Error
/// messages on the mount arm consult it: when binding dispatch silently degraded, the
/// resulting "is not a tl fs mount" must carry the real cause on the ERROR path too (a
/// `--json`/CI consumer never sees the stderr warning).
static REGISTRY_CORRUPT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// The note mount-arm errors append once the lenient loaders have degraded past a corrupt
/// registry (see [`REGISTRY_CORRUPT`]); `None` while the registry reads fine.
pub(crate) fn registry_corruption_note() -> Option<String> {
    REGISTRY_CORRUPT.get().map(|path| {
        format!(
            "note: the binding registry at {path} is unreadable; if this directory is a \
             plain-directory binding, repair or remove that file"
        )
    })
}

/// The lenient read policy for callers that are NOT binding-owned: warn once (naming the
/// file, and that binding commands stay fail-closed) and degrade to "nothing bound". A
/// mount command asks the registry only to route AWAY from bindings; failing all mount
/// commands because bindings.json is corrupt would brick the whole `tl fs` surface.
fn registry_lenient<T>(result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(e) => {
            let _ = REGISTRY_CORRUPT.set(registry_path().display().to_string());
            REGISTRY_WARNED.call_once(|| {
                eprintln!(
                    "{} {e}\n         (treating no directory as bound so mount commands keep \
                     working; binding commands fail closed until the registry is repaired)",
                    style("warning:").yellow(),
                );
            });
            None
        }
    }
}

fn registry_save(registry: &BindingRegistry) -> Result<()> {
    write_atomic(&registry_path(), &serde_json::to_vec_pretty(registry)?)
}

/// Serialize a registry read-modify-write under a dedicated `bindings.lock` flock (blocking:
/// mutations are tiny). Without it two concurrent `tl fs init`/`unbind` runs interleave
/// load→save and one binding silently vanishes from the registry. Dangling entries (state
/// dir gone — the debris a crashed `unbind` can leave, since it removes the state dir first)
/// are pruned on every mutation; the closure may return `Err` to abort with nothing written.
fn registry_mutate(mutate: impl FnOnce(&mut BindingRegistry) -> Result<()>) -> Result<()> {
    let dir = crate::config::files::config_dir();
    std::fs::create_dir_all(&dir)?;
    let _lock = flock_exclusive(&dir.join("bindings.lock"), true)?.ok_or_else(|| {
        CliError::usage("could not lock the binding registry (flock unsupported here)")
    })?;
    let mut registry = registry_load()?;
    prune_dangling(&mut registry.bindings);
    mutate(&mut registry)?;
    registry_save(&registry)
}

/// Whether a registry entry's state dir still holds a binding. An entry pointing at nothing
/// is self-healing debris (`unbind` removes the state dir FIRST, registry second, so a crash
/// between the two leaves exactly this shape): lookups treat it as unbound and
/// [`registry_mutate`] prunes it.
fn binding_state_live(state_dir: &Path) -> bool {
    state_dir.join("binding.json").exists()
}

/// Drop registry entries whose state dir no longer holds a binding (see
/// [`binding_state_live`]).
fn prune_dangling(bindings: &mut BTreeMap<String, PathBuf>) {
    bindings.retain(|_, state_dir| binding_state_live(state_dir));
}

/// The binding registered exactly at `path` (same exact-match semantics as mounts:
/// path-addressed commands name the root, `*_containing_cwd` handles the inside-of case).
/// Fail-closed on registry corruption — but note that command DISPATCH does not come through
/// here: the fs command surface routes via the lenient twin ([`binding_for_lenient`]), which
/// degrades a corrupt registry to "not a binding" (falling through to the mount arm, whose
/// errors then append [`registry_corruption_note`]). The fail-closed form serves the
/// binding-owned mutations themselves (`init`'s double-bind check, `unbind`).
pub fn binding_for(path: &Path) -> Result<Option<(String, PathBuf)>> {
    let root = canonical_mountpoint(path)?;
    Ok(registry_load()?
        .bindings
        .get(&root)
        // A dangling entry (crashed unbind) is already unbound; the next mutation prunes it.
        .filter(|state_dir| binding_state_live(state_dir))
        .map(|state_dir| (root, state_dir.clone())))
}

/// Lenient twin of [`binding_for`] for mount-command dispatch and CWD resolution: on registry
/// corruption it warns once and answers "not a binding" instead of failing every mount
/// command (see [`registry_lenient`]).
pub fn binding_for_lenient(path: &Path) -> Option<(String, PathBuf)> {
    registry_lenient(binding_for(path)).flatten()
}

/// Every bound directory, for CWD-containment resolution alongside mount roots. Lenient —
/// same policy (and warning) as [`binding_for_lenient`].
pub fn binding_roots_lenient() -> Vec<String> {
    registry_lenient(registry_load())
        .map(|registry| registry.bindings.keys().cloned().collect())
        .unwrap_or_default()
}

/// The deepest binding whose root contains `path` — how `tl fs unbind` with no argument
/// walks ancestors, like every other path-addressed command. Fail-closed (binding-owned).
pub(crate) fn binding_containing(path: &Path) -> Result<Option<(String, PathBuf)>> {
    let registry = registry_load()?;
    let live = registry
        .bindings
        .iter()
        .filter(|(_, state_dir)| binding_state_live(state_dir))
        .map(|(root, _)| root);
    Ok(deepest_containing(live, path).map(|root| {
        let state_dir = registry.bindings[&root].clone();
        (root, state_dir)
    }))
}

/// The deepest of `roots` that contains `path` (roots are canonical absolute paths, but the
/// leaf component may spell a symlink — compare the stored AND canonicalized forms, same as
/// `mount_containing_cwd`, so a bare `tl fs unbind` from a symlinked CWD still matches).
fn deepest_containing<'a>(roots: impl Iterator<Item = &'a String>, path: &Path) -> Option<String> {
    roots
        .filter(|root| {
            let root_path = Path::new(root);
            path.starts_with(root_path)
                || root_path
                    .canonicalize()
                    .is_ok_and(|canonical| path.starts_with(canonical))
        })
        .max_by_key(|root| Path::new(root).components().count())
        .cloned()
}

/// `(workspace id, bound directory)` for every readable binding — `tl fs ls` visibility.
/// Listing-only, so a corrupt registry degrades (with the one warning) and individual
/// unreadable state dirs are skipped rather than fatal.
pub(crate) fn bound_workspaces() -> Vec<(String, String)> {
    let Some(registry) = registry_lenient(registry_load()) else {
        return Vec::new();
    };
    registry
        .bindings
        .iter()
        .filter_map(|(root, state_dir)| {
            let binding = load_binding(state_dir).ok()?;
            Some((binding.workspace_id, root.clone()))
        })
        .collect()
}

/// Every binding as (backing repo, bound root) — the filesystem listing's attachment column.
pub(crate) fn bound_binding_repos() -> Vec<(String, String)> {
    let Some(registry) = registry_lenient(registry_load()) else {
        return Vec::new();
    };
    registry
        .bindings
        .iter()
        .filter_map(|(root, state_dir)| {
            let binding = load_binding(state_dir).ok()?;
            Some((binding.repo, root.clone()))
        })
        .collect()
}

/// The bound directory attached to `workspace_id`, if any. Scans the binding state dirs
/// themselves (not the registry): this guards destructive/attachment decisions (`tl fs rm`,
/// mount write policy), and a binding missing from a damaged registry still owns its
/// workspace.
///
/// Fail-closed per state dir: `tl fs rm` deletes the WORKSPACE, so a binding whose
/// `binding.json` cannot be read or parsed must abort the question (it may be exactly the
/// binding that owns this workspace) rather than be skipped as if unbound. Only a state dir
/// with no `binding.json` at all (not a binding) skips.
pub(crate) fn binding_using_workspace(workspace_id: &str) -> Result<Option<String>> {
    binding_using_workspace_in(&state_dir_root(), workspace_id)
}

/// [`binding_using_workspace`] against an explicit state root (the unit-testable core).
fn binding_using_workspace_in(state_root: &Path, workspace_id: &str) -> Result<Option<String>> {
    let read = match std::fs::read_dir(state_root) {
        Ok(read) => read,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(CliError::usage(format!(
                "cannot read the binding state root {}: {e}",
                state_root.display()
            )));
        }
    };
    for entry in read {
        let entry = entry.map_err(|e| {
            CliError::usage(format!(
                "cannot read the binding state root {}: {e}",
                state_root.display()
            ))
        })?;
        let path = entry.path().join("binding.json");
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            // Not a binding state dir (stray file/dir under the root): honestly not bound.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(CliError::usage(format!(
                    "cannot read binding state {}: {e}; refusing to treat the workspace as \
                     unbound — fix or remove the state dir first",
                    path.display()
                )));
            }
        };
        let binding: Binding = serde_json::from_slice(&raw).map_err(|e| {
            CliError::usage(format!(
                "binding state {} is corrupt ({e}); refusing to treat the workspace as \
                 unbound — repair or remove the state dir first",
                path.display()
            ))
        })?;
        if binding.workspace_id == workspace_id {
            return Ok(Some(binding.root.to_string_lossy().into_owned()));
        }
    }
    Ok(None)
}

/// Refuse path overlap between a candidate root and every registered mount and binding, in
/// both directions — a mount inside a bound tree would be scanned (and its kernel volume
/// stat'd) by snapshots; a binding inside a mount would double-track the overlay's files.
///
/// This is `init`'s preflight only; the authoritative check re-runs inside the
/// `registry_mutate` closure against the freshly locked registry (see [`init`]) — two
/// concurrent inits racing this preflight otherwise both pass and double-bind.
fn assert_no_overlap(root: &str) -> Result<()> {
    if let Some(msg) = mount_overlap_error(root) {
        return Err(CliError::usage(msg));
    }
    if let Some(msg) = binding_overlap_error(root, &registry_load()?.bindings) {
        return Err(CliError::usage(msg));
    }
    Ok(())
}

/// The complaint for `root` overlapping a registered mount, if any (reads the mounts
/// registry fresh on every call).
fn mount_overlap_error(root: &str) -> Option<String> {
    let candidate = Path::new(root);
    super::registry_load().keys().find_map(|mountpoint| {
        let mount_path = Path::new(mountpoint);
        (candidate.starts_with(mount_path) || mount_path.starts_with(candidate)).then(|| {
            format!(
                "{root} overlaps the tl fs mount at {mountpoint}; a directory cannot be both \
                 mounted and bound"
            )
        })
    })
}

/// The complaint for `root` overlapping (or equalling — the double-bind case) an existing
/// binding, if any. Pure over the given binding set so `init`'s in-closure re-check runs it
/// against the locked registry and tests drive it directly.
fn binding_overlap_error(root: &str, bindings: &BTreeMap<String, PathBuf>) -> Option<String> {
    let candidate = Path::new(root);
    bindings.keys().find_map(|bound| {
        let bound_path = Path::new(bound);
        (candidate.starts_with(bound_path) || bound_path.starts_with(candidate)).then(|| {
            format!("{root} overlaps the existing binding at {bound} (see `tl fs unbind {bound}`)")
        })
    })
}

/// Guard for `tl fs mount`: the converse of [`assert_no_overlap`]'s mount check.
pub fn assert_no_binding_overlap(mountpoint: &str) -> Result<()> {
    let candidate = Path::new(mountpoint);
    for bound in registry_load()?.bindings.keys() {
        let bound_path = Path::new(bound);
        if candidate.starts_with(bound_path) || bound_path.starts_with(candidate) {
            return Err(CliError::usage(format!(
                "{mountpoint} overlaps the plain-directory binding at {bound}; unbind it \
                 first (tl fs unbind {bound}) or mount elsewhere"
            )));
        }
    }
    Ok(())
}

/// Persisted binding identity (`binding.json` in the state dir). No credentials — commands
/// mint their own from the CLI auth context, exactly like mounts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub project_id: String,
    #[serde(default)]
    pub organization_id: Option<String>,
    pub repo: String,
    pub workspace_id: String,
    pub ref_name: String,
    /// Canonical bound directory.
    pub root: PathBuf,
    pub created_at_secs: u64,
}

pub(crate) fn load_binding(state_dir: &Path) -> Result<Binding> {
    let path = state_dir.join("binding.json");
    let raw = std::fs::read(&path).map_err(|e| {
        CliError::usage(format!("cannot read binding state {}: {e}", path.display()))
    })?;
    serde_json::from_slice(&raw).map_err(|e| {
        CliError::usage(format!(
            "binding state {} is corrupt ({e}); refusing to guess",
            path.display()
        ))
    })
}

// ---------------------------------------------------------------------------------------------
// Atomic file replacement + the snapshot lock.
// ---------------------------------------------------------------------------------------------

/// Replace `path` atomically: temp file in the same directory, fsync, rename, fsync the
/// parent directory (the rename itself is not durable until the directory is). Readers see
/// either the old or the new content, never a torn write — the index and journal both hang
/// correctness off this. The temp name is unique per process+call: with a fixed name, two
/// concurrent writers (e.g. two `tl fs init`s racing on the registry, or the push progress
/// hook rewriting the journal) could interleave create/write/rename on the same temp inode
/// and publish a torn file through the "atomic" path.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let parent = path
        .parent()
        .ok_or_else(|| CliError::usage(format!("{} has no parent directory", path.display())))?;
    let tmp = unique_temp_path(path)?;
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.into());
    }
    // Durability of the rename itself. Some platforms refuse fsync on a directory handle;
    // treat that as best-effort there (unix, the only supported target, allows it).
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// A collision-free sibling temp name for [`write_atomic`]: pid disambiguates processes, the
/// counter disambiguates threads/calls within one.
fn unique_temp_path(path: &Path) -> Result<PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let parent = path
        .parent()
        .ok_or_else(|| CliError::usage(format!("{} has no parent directory", path.display())))?;
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| CliError::usage(format!("{} has no file name", path.display())))?;
    Ok(parent.join(format!(
        "{file_name}.{}.{}.tmp",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    )))
}

fn remove_durably(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    if let Some(parent) = path.parent()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Whole-snapshot exclusion via flock(2) on a dedicated `lock` file — dedicated because the
/// index itself is replaced by rename, and a lock on a replaced inode guards nothing. The
/// lock releases when the fd closes (drop), including on crash (kernel-owned), so there is
/// no stale-lock cleanup to get wrong.
struct BindingLock {
    _file: std::fs::File,
}

/// flock(2) `path` exclusively. `block: false` returns `Ok(None)` when another process holds
/// it; `block: true` waits. The one flock implementation — the per-binding snapshot lock and
/// the registry mutation lock both go through here.
#[cfg(unix)]
fn flock_exclusive(path: &Path, block: bool) -> Result<Option<std::fs::File>> {
    use std::os::unix::io::AsRawFd as _;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    let flags = libc::LOCK_EX | if block { 0 } else { libc::LOCK_NB };
    if unsafe { libc::flock(file.as_raw_fd(), flags) } != 0 {
        if block {
            return Err(CliError::usage(format!(
                "could not lock {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )));
        }
        return Ok(None);
    }
    Ok(Some(file))
}

#[cfg(not(unix))]
fn flock_exclusive(_path: &Path, _block: bool) -> Result<Option<std::fs::File>> {
    Err(CliError::usage(
        "plain-directory bindings are supported on unix only in v1",
    ))
}

fn acquire_lock(state_dir: &Path) -> Result<BindingLock> {
    let path = state_dir.join("lock");
    match flock_exclusive(&path, false)? {
        Some(file) => Ok(BindingLock { _file: file }),
        None => Err(CliError::usage(format!(
            "another tl fs command holds this binding's snapshot lock ({}); wait for it to \
             finish and retry",
            path.display()
        ))),
    }
}

// ---------------------------------------------------------------------------------------------
// Fingerprints + the two-layer index.
// ---------------------------------------------------------------------------------------------

/// `(seconds, nanoseconds)` wall-clock / inode timestamp, ordered lexicographically.
type Stamp = (i64, u32);

fn now_stamp() -> Stamp {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(_) => (0, 0),
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Everything stat(2) offers about a path's identity at filesystem resolution. Equality of
/// fingerprints is the *only* license to skip rehashing a file, and even that is subject to
/// the racy-window rule below — never to timestamps alone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Fingerprint {
    /// Git mode (`0o100644` / `0o100755` / `0o120000`) — encodes the file type, so a
    /// file-replaced-by-symlink at identical size/times still reads as changed.
    mode: u32,
    size: u64,
    mtime: Stamp,
    ctime: Stamp,
    dev: u64,
    ino: u64,
}

impl Fingerprint {
    #[cfg(unix)]
    fn of(meta: &std::fs::Metadata) -> Fingerprint {
        use std::os::unix::fs::MetadataExt as _;
        Fingerprint {
            mode: git_mode(meta),
            size: meta.size(),
            mtime: (meta.mtime(), meta.mtime_nsec() as u32),
            ctime: (meta.ctime(), meta.ctime_nsec() as u32),
            dev: meta.dev(),
            ino: meta.ino(),
        }
    }

    // Non-unix: no ctime/dev/ino at std resolution. Compile-only — the commands reject
    // non-unix use before any fingerprint is taken.
    #[cfg(not(unix))]
    fn of(meta: &std::fs::Metadata) -> Fingerprint {
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| (d.as_secs() as i64, d.subsec_nanos()))
            .unwrap_or((0, 0));
        Fingerprint {
            mode: git_mode(meta),
            size: meta.len(),
            mtime,
            ctime: (0, 0),
            dev: 0,
            ino: 0,
        }
    }

    /// The racy-window rule: a fingerprint whose timestamps are not strictly older than the
    /// scan that recorded it cannot prove cleanliness — a write inside that window can leave
    /// timestamps identical at the filesystem's clock resolution. Such entries are
    /// conservatively rehashed on the next snapshot (git's index plays the same trick).
    fn overlaps_scan(&self, scan_started_at: Stamp) -> bool {
        self.mtime >= scan_started_at || self.ctime >= scan_started_at
    }
}

#[cfg(unix)]
fn git_mode(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    if meta.file_type().is_symlink() {
        0o120000
    } else if meta.permissions().mode() & 0o111 != 0 {
        0o100755
    } else {
        0o100644
    }
}

#[cfg(not(unix))]
fn git_mode(meta: &std::fs::Metadata) -> u32 {
    if meta.file_type().is_symlink() {
        0o120000
    } else {
        0o100644
    }
}

/// One indexed path: the remote baseline plus (optionally) the local cleanliness proof.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexEntry {
    /// Blob oid this path has at `indexed_head` — the fact a server-side delete or an
    /// unchanged-path claim is derived from. Never dropped while the path exists remotely.
    server_oid: String,
    server_mode: u32,
    /// Proof that the local bytes still equal `server_oid`. `None` = untrusted: the path is
    /// rehashed next snapshot (costs time, never correctness).
    clean_fingerprint: Option<Fingerprint>,
}

/// The whole baseline in one atomically-replaced file: `indexed_head` and every entry must
/// change together or a crash could pair head A with entries of head B.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Index {
    /// The workspace snapshot commit the baseline reflects.
    indexed_head: String,
    /// When the scan that produced this index's fingerprints *started* — the lower bound of
    /// the racy window (see [`Fingerprint::overlaps_scan`]). Entries carried forward from an
    /// older index already passed the racy check against their own scan, so their
    /// timestamps are strictly older than that older start, and a fortiori older than this.
    scan_started_at: Stamp,
    entries: BTreeMap<String, IndexEntry>,
}

fn index_path(state_dir: &Path) -> PathBuf {
    state_dir.join("index.json")
}

/// Fail closed on both absence and corruption: `init` always writes the initial (empty)
/// index, so from then on "no index" means lost state, and a rebuilt-from-nothing baseline
/// would translate every unknown server path into a delete on the next snapshot.
fn load_index(state_dir: &Path) -> Result<Index> {
    let path = index_path(state_dir);
    let raw = std::fs::read(&path).map_err(|e| {
        CliError::usage(format!(
            "cannot read the snapshot index {}: {e}. Without the index the binding cannot \
             prove its server baseline, and snapshotting anyway could publish spurious \
             deletes — refusing. (The workspace and its snapshots are safe on the server.)",
            path.display()
        ))
    })?;
    serde_json::from_slice(&raw).map_err(|e| {
        CliError::usage(format!(
            "the snapshot index {} is corrupt ({e}); refusing to snapshot from a guessed \
             baseline. The workspace and its snapshots are safe on the server.",
            path.display()
        ))
    })
}

fn save_index(state_dir: &Path, index: &Index) -> Result<()> {
    write_atomic(&index_path(state_dir), &serde_json::to_vec_pretty(index)?)
}

// ---------------------------------------------------------------------------------------------
// Write-ahead attempt journal.
// ---------------------------------------------------------------------------------------------

/// One journaled path of a snapshot attempt: exactly what the commit will publish (oid/mode/
/// delete, all hashed locally before submission) plus the fingerprint the content was proven
/// stable under — the ingredients recovery and the post-push race rule both need.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct JournalEntry {
    path: String,
    /// Blob oid the commit publishes; `None` for deletes.
    oid: Option<String>,
    mode: Option<u32>,
    delete: bool,
    /// Stat taken around the local hash (stat-before == stat-after, else `None`): the tuple
    /// a post-push re-stat must still equal for the oid to be recorded as clean.
    pre_fingerprint: Option<Fingerprint>,
}

/// Durable record of an in-flight snapshot attempt, written *before* `push_files` (which is
/// the point of no return: once submitted, the commit can publish server-side even if this
/// process dies). Its presence at the start of a later snapshot triggers recovery.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Journal {
    idempotency_key: String,
    /// The head the attempt CAS'd against (`expect_oid`); our commit can only have landed
    /// directly on top of this.
    expected_head: String,
    started_at_secs: u64,
    message: String,
    /// The server commit job the attempt submitted. This is recovery's authorship oracle:
    /// the id names OUR attempt, so polling the job to terminal proves whether (and as what
    /// commit) the interrupted attempt published — evidence the head alone can never give.
    /// Absent when the crash predates submission (or the commit was synchronous), where the
    /// head-comparison table is sound on its own.
    ///
    /// Kept for wire/forward compatibility, but no longer WRITTEN mid-push: the id lands in
    /// the tiny `journal.job` sidecar instead (see [`journal_job_path`]) — rewriting the
    /// whole journal from inside the push progress hook serialized an entry-proportional
    /// fsync into the hot path. Recovery reads journal + sidecar ([`journal_job_id`]).
    #[serde(default)]
    job_id: Option<String>,
    entries: Vec<JournalEntry>,
}

fn journal_path(state_dir: &Path) -> PathBuf {
    state_dir.join("journal.json")
}

/// Sidecar holding just the submitted commit job's id (written the moment the 202 names it,
/// off the progress hook's thread). Meaningful only next to `journal.json`; removed with it.
fn journal_job_path(state_dir: &Path) -> PathBuf {
    state_dir.join("journal.job")
}

/// The journaled attempt's commit-job id: the journal's own field when present (older
/// journals were rewritten mid-push), else the `journal.job` sidecar.
fn journal_job_id(state_dir: &Path, journal: &Journal) -> Option<String> {
    journal.job_id.clone().or_else(|| {
        let raw = std::fs::read_to_string(journal_job_path(state_dir)).ok()?;
        let id = raw.trim().to_string();
        (!id.is_empty()).then_some(id)
    })
}

/// Remove the journal AND its job-id sidecar. Sidecar first: a crash between the two leaves
/// journal-without-sidecar (recovery just re-runs), never a stale sidecar that a LATER
/// attempt's journal would pick up as its own job id.
fn remove_journal_durably(state_dir: &Path) -> Result<()> {
    remove_durably(&journal_job_path(state_dir))?;
    remove_durably(&journal_path(state_dir))
}

fn load_journal(state_dir: &Path) -> Result<Option<Journal>> {
    let path = journal_path(state_dir);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(CliError::usage(format!(
                "cannot read the snapshot journal {}: {e}",
                path.display()
            )));
        }
    };
    // A torn journal write means the attempt never reached push_files (the journal lands
    // durably before submission) — nothing can have published, so it is safe to discard.
    // But `write_atomic` makes torn contents impossible; parseable-but-wrong is the only
    // corrupt shape left, and that we refuse to guess about.
    serde_json::from_slice(&raw).map(Some).map_err(|e| {
        CliError::usage(format!(
            "the snapshot journal {} is corrupt ({e}); refusing to guess whether the \
             journaled attempt published. Verify the workspace head (tl fs status) and \
             remove the file once resolved.",
            path.display()
        ))
    })
}

fn save_journal(state_dir: &Path, journal: &Journal) -> Result<()> {
    // A new journal means a new attempt with no submitted job yet: clear any stale sidecar
    // so a crash later in THIS attempt can never resolve against a previous attempt's job.
    remove_durably(&journal_job_path(state_dir))?;
    write_atomic(
        &journal_path(state_dir),
        &serde_json::to_vec_pretty(journal)?,
    )
}

// ---------------------------------------------------------------------------------------------
// Recovery: what a journal found at snapshot start means, as a pure decision.
// ---------------------------------------------------------------------------------------------

/// Whether the advanced workspace head has been proven to *be* the journaled attempt.
/// Without proof, a tree that merely matches the candidate is not proof of authorship — so
/// head-advanced recovery fails closed rather than guessing. `Verified` is produced in
/// production by the journaled commit-job id: polling OUR job (the id was journaled the
/// moment the 202 named it) to a committed terminal state is authorship by construction.
/// Journals without a job id (crash before submission, synchronous commit) still pass
/// `Unavailable` and keep the fail-closed table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdoptionEvidence {
    Unavailable,
    /// The observed head is proven to be the journaled attempt's own commit (today: the
    /// journaled job id reached `committed` with exactly this commit oid).
    Verified,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryDecision {
    /// The crash happened after the index was installed but before the journal was removed:
    /// the index already reflects the published commit. Drop the journal, nothing else.
    AlreadyInstalled,
    /// The journaled attempt never published (head still equals `expected_head`, or its
    /// commit job terminally failed): drop the journal and snapshot fresh. The CAS
    /// (`expect_oid = expected_head`) still guards the case where the old attempt's detached
    /// server job races the fresh one.
    Fresh,
    /// The head is the journaled attempt: install the journaled candidate at exactly this
    /// head as the baseline with every `clean_fingerprint = None` (local bytes unverified
    /// since the crash). Carrying the head here — instead of a caller-side variable that
    /// defaults to something — is what makes an empty-string head unrepresentable.
    Adopt { head: String },
    /// The head advanced and authorship could not be proven — fail closed with this message.
    Fail(String),
}

pub(crate) fn recovery_decision(
    journal_expected_head: &str,
    observed_head: &str,
    indexed_head: &str,
    evidence: AdoptionEvidence,
) -> RecoveryDecision {
    if observed_head == journal_expected_head {
        return RecoveryDecision::Fresh;
    }
    if observed_head == indexed_head {
        return RecoveryDecision::AlreadyInstalled;
    }
    match evidence {
        AdoptionEvidence::Verified => RecoveryDecision::Adopt {
            head: observed_head.to_string(),
        },
        AdoptionEvidence::Unavailable => RecoveryDecision::Fail(format!(
            "a snapshot journal from an interrupted attempt exists, and the workspace head \
             advanced past the journaled base (head {observed_head}, journal expected \
             {journal_expected_head}). The advanced head may be the interrupted snapshot \
             itself or someone else's — the client cannot prove which, so nothing is \
             changed. Inspect the workspace (tl fs ls / the server UI); if the head is your \
             interrupted snapshot, this binding's index is stale and v1 has no rebind path — \
             recover manually before removing the journal.",
        )),
    }
}

/// A journaled commit job's terminal fate, as observed by recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JobFate {
    /// The job reached `committed`, publishing this commit oid.
    Committed(String),
    /// The job reached `failed`: nothing published.
    Failed,
}

/// What polling the journaled commit job established.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JobPoll {
    /// The job reached a terminal state; its fate decides recovery outright.
    Fate(JobFate),
    /// The server definitively no longer knows the job (404 / not-found / expired record):
    /// the authorship oracle is gone, but the head-comparison table still decides safely —
    /// head == expected proves nothing published; anything else fails closed.
    RecordGone,
}

/// Recovery when the journal carries the attempt's commit-job id: the job's terminal state
/// overrides the head table entirely. A committed job IS `Verified` adoption evidence — its
/// commit oid plays the observed head, so the decision (and the adopted `indexed_head`) is
/// the job's commit even if the workspace head has since advanced further (the stale-head
/// preflight then reports that separately, as it should). A failed job published nothing:
/// fresh retry, CAS-guarded as ever.
pub(crate) fn recovery_decision_from_job(
    fate: &JobFate,
    journal_expected_head: &str,
    indexed_head: &str,
) -> RecoveryDecision {
    match fate {
        JobFate::Failed => RecoveryDecision::Fresh,
        JobFate::Committed(commit) => recovery_decision(
            journal_expected_head,
            commit,
            indexed_head,
            AdoptionEvidence::Verified,
        ),
    }
}

/// The recovery decision once the job poll settled: a terminal fate decides outright
/// ([`recovery_decision_from_job`]); a gone record falls back to the plain head table with
/// no authorship evidence — the pre-job-id decision procedure, which was always safe, just
/// stricter than a live oracle.
pub(crate) fn recovery_decision_after_poll(
    poll: &JobPoll,
    journal_expected_head: &str,
    workspace_head: &str,
    indexed_head: &str,
) -> RecoveryDecision {
    match poll {
        JobPoll::Fate(fate) => {
            recovery_decision_from_job(fate, journal_expected_head, indexed_head)
        }
        JobPoll::RecordGone => recovery_decision(
            journal_expected_head,
            workspace_head,
            indexed_head,
            AdoptionEvidence::Unavailable,
        ),
    }
}

/// The fail-closed recovery error, extended with the manual escape hatch: the journal file
/// is the interrupted attempt's tracking, and deleting it is the documented way out once the
/// user has verified the advanced head by hand.
fn recovery_fail_error(msg: &str, state_dir: &Path) -> CliError {
    CliError::usage(format!(
        "{msg}\nManual escape hatch: the interrupted attempt's tracking lives at {}; \
         deleting that file discards it, and the next snapshot proceeds against the \
         existing index — only do this after verifying the advanced head yourself.",
        journal_path(state_dir).display()
    ))
}

/// Install the journaled candidate over the current baseline: journaled upserts become
/// baseline entries at their journaled oids with `clean_fingerprint = None` (the bytes were
/// last seen before a crash — rehash before trusting), journaled deletes leave the baseline.
/// Untouched entries keep their fingerprints: the adopted commit did not change them, and
/// fingerprint comparison (not the crash) is what detects any local edits since.
pub(crate) fn apply_adopted_journal(index: &mut Index, journal: &Journal, observed_head: &str) {
    for entry in &journal.entries {
        if entry.delete {
            index.entries.remove(&entry.path);
            continue;
        }
        let Some(oid) = &entry.oid else { continue };
        index.entries.insert(
            entry.path.clone(),
            IndexEntry {
                server_oid: oid.clone(),
                server_mode: entry.mode.unwrap_or(0o100644),
                clean_fingerprint: None,
            },
        );
    }
    index.indexed_head = observed_head.to_string();
}

/// Whether an SDK error definitively means the job record is gone server-side (aged out or
/// never durably recorded) — as opposed to the server being temporarily unable to answer.
fn job_record_gone(e: &tensorlake::error::SdkError) -> bool {
    use tensorlake::error::SdkError;
    if let SdkError::ServerError { status, .. } = e {
        if status.as_u16() == 404 {
            return true;
        }
        // 5xx never proves absence.
        if status.is_server_error() {
            return false;
        }
    }
    let msg = e.to_string().to_ascii_lowercase();
    msg.contains("not found") || msg.contains("expired")
}

/// Transient poll failures worth retrying in place (mirrors the SDK's internal
/// `with_transient_retries` predicate, which is private to its ingest module).
fn job_poll_transient(e: &tensorlake::error::SdkError) -> bool {
    use tensorlake::error::SdkError;
    match e {
        SdkError::ServerError { status, .. } => status.is_server_error() || status.as_u16() == 429,
        SdkError::Http(_) | SdkError::Middleware(_) => true,
        _ => false,
    }
}

/// Poll a journaled commit job through the SDK's out-of-band job API (`commit_job_status`,
/// the same state machine `push_files` itself polls) and classify the outcome:
///
/// - terminal `committed`/`failed` → [`JobPoll::Fate`];
/// - the record is definitively gone (404/not-found/expired) → [`JobPoll::RecordGone`], and
///   the caller falls back to the head-comparison table (this is NOT a wedge);
/// - transient errors retry in place (3 extra attempts with backoff), then abort recovery
///   with a retry-later error — the journal stays, nothing is decided;
/// - an unrecognized state string fails closed naming the state (a new server state must
///   not be mistaken for "still running" forever);
/// - still `queued`/`running` at the deadline (10 minutes, 500ms→5s exponential cadence) →
///   a clear error: the journal keeps tracking the job, re-running resumes the wait.
async fn poll_job_fate(session: &FsSession, binding: &Binding, job_id: &str) -> Result<JobPoll> {
    let (user, token) = session.creds();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(600);
    let mut delay = std::time::Duration::from_millis(500);
    loop {
        let job = {
            let mut attempts = 0usize;
            let mut retry_delay = std::time::Duration::from_millis(500);
            loop {
                match session
                    .client
                    .commit_job_status(&session.project_id, &binding.repo, user, token, job_id)
                    .await
                {
                    Ok(job) => break job.into_inner(),
                    Err(e) if job_record_gone(&e) => return Ok(JobPoll::RecordGone),
                    Err(e) if job_poll_transient(&e) && attempts < 3 => {
                        attempts += 1;
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(4));
                    }
                    Err(e) => {
                        return Err(CliError::usage(format!(
                            "could not poll the interrupted attempt's commit job {job_id}: \
                             {e}; refusing to guess whether it published — retry when the \
                             server answers"
                        )));
                    }
                }
            }
        };
        match job.state.as_str() {
            "committed" => {
                return job
                    .commit
                    .map(|commit| JobPoll::Fate(JobFate::Committed(commit)))
                    .ok_or_else(|| {
                        CliError::usage(format!(
                            "commit job {job_id} is committed but reports no commit oid; \
                             refusing to guess the published head"
                        ))
                    });
            }
            "failed" => return Ok(JobPoll::Fate(JobFate::Failed)),
            "queued" | "running" => {
                if std::time::Instant::now() >= deadline {
                    return Err(CliError::usage(format!(
                        "the interrupted attempt's commit job {job_id} is still running \
                         server-side; nothing is wedged — the journal keeps tracking it, \
                         re-run `tl fs snapshot` to resume waiting"
                    )));
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(5));
            }
            other => {
                return Err(CliError::usage(format!(
                    "commit job {job_id} reports the unrecognized state {other:?}; refusing \
                     to guess whether the interrupted attempt published (a newer server may \
                     have added states — upgrade tl, or inspect the job out-of-band)"
                )));
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Strict scanner.
// ---------------------------------------------------------------------------------------------

/// One scanned path: repo-relative, with the fingerprint stat'd during the walk.
#[derive(Clone, Debug)]
struct ScanEntry {
    rel: String,
    fingerprint: Fingerprint,
}

/// A strict ignore matcher: same semantics as mount snapshot enumeration (built-ins + root
/// `.tlignore` names + nested `.gitignore`s via [`SnapshotIgnore`]), except that an
/// unreadable `.tlignore` aborts instead of silently ignoring nothing — a vanished ignore
/// rule would widen the tracked set and start uploading (or deleting!) paths the user
/// declared workspace-local. `SnapshotIgnore` already aborts on unreadable `.gitignore`s.
fn strict_ignore(root: &Path) -> Result<SnapshotIgnore> {
    let tlignore = root.join(".tlignore");
    if std::fs::symlink_metadata(&tlignore).is_ok() && std::fs::read_to_string(&tlignore).is_err() {
        return Err(CliError::usage(format!(
            "cannot read {}; aborting the scan (unreadable ignore rules would silently widen \
             what gets uploaded)",
            tlignore.display()
        )));
    }
    Ok(SnapshotIgnore::new(root))
}

/// Walk the whole bound directory. Strictness is the contract: any unreadable directory or
/// entry aborts with an error naming the path — a skipped subtree is indistinguishable from
/// a deleted one and would snapshot as mass deletion. Non-UTF-8 names and special files
/// (sockets, fifos, devices) are rejected by name; ignored paths are skipped silently;
/// empty directories are unrepresentable in a git tree and skip without error.
fn scan(root: &Path, ignore: &mut SnapshotIgnore) -> Result<Vec<ScanEntry>> {
    let mut out = Vec::new();
    scan_dir(root, "", ignore, &mut out)?;
    out.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(out)
}

fn scan_dir(
    root: &Path,
    rel_dir: &str,
    ignore: &mut SnapshotIgnore,
    out: &mut Vec<ScanEntry>,
) -> Result<()> {
    let abs_dir = if rel_dir.is_empty() {
        root.to_path_buf()
    } else {
        root.join(rel_dir)
    };
    let entries = std::fs::read_dir(&abs_dir).map_err(|e| {
        CliError::usage(format!(
            "cannot read directory {}: {e}; aborting the scan (a skipped directory would \
             snapshot as deletion of everything under it)",
            abs_dir.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            CliError::usage(format!(
                "cannot read an entry of {}: {e}; aborting the scan",
                abs_dir.display()
            ))
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(CliError::usage(format!(
                "{} has a non-UTF-8 name; repository paths are UTF-8 — rename or ignore it",
                abs_dir.join(&name).display()
            )));
        };
        let rel = if rel_dir.is_empty() {
            name.to_string()
        } else {
            format!("{rel_dir}/{name}")
        };
        let abs = entry.path();
        let meta = std::fs::symlink_metadata(&abs).map_err(|e| {
            CliError::usage(format!(
                "cannot stat {}: {e}; aborting the scan",
                abs.display()
            ))
        })?;
        let file_type = meta.file_type();
        if ignore.is_ignored(&rel, file_type.is_dir())? {
            continue;
        }
        if file_type.is_dir() {
            scan_dir(root, &rel, ignore, out)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            out.push(ScanEntry {
                rel,
                fingerprint: Fingerprint::of(&meta),
            });
        } else {
            return Err(CliError::usage(format!(
                "{} is not a regular file, directory, or symlink (socket/fifo/device); \
                 snapshots cannot represent it — remove or ignore it",
                abs.display()
            )));
        }
    }
    Ok(())
}

/// The scan's verdict against the index: paths to (re)hash, paths to delete server-side, and
/// paths proven clean (fingerprint hit outside the racy window — never opened, never read).
struct Delta {
    /// New paths, changed fingerprints, and untrusted (`clean_fingerprint: None`) entries.
    upserts: Vec<ScanEntry>,
    deletes: Vec<String>,
    clean: usize,
}

fn compute_delta(
    root: &Path,
    index: &Index,
    scanned: &[ScanEntry],
    ignore: &mut SnapshotIgnore,
) -> Result<Delta> {
    let mut upserts = Vec::new();
    let mut clean = 0usize;
    let seen: std::collections::BTreeSet<&str> =
        scanned.iter().map(|entry| entry.rel.as_str()).collect();
    for entry in scanned {
        let proven_clean = index.entries.get(&entry.rel).is_some_and(|indexed| {
            indexed.clean_fingerprint.as_ref().is_some_and(|fp| {
                fp == &entry.fingerprint && !fp.overlaps_scan(index.scan_started_at)
            })
        });
        if proven_clean {
            clean += 1;
        } else {
            upserts.push(entry.clone());
        }
    }
    // Baseline paths the scan did not produce. Ignored is NOT missing: a path newly covered
    // by an ignore rule keeps its server baseline (never silently deleted server-side); only
    // genuinely-absent paths become deletes. Any other stat failure aborts — same rationale
    // as the walk.
    let mut deletes = Vec::new();
    for path in index.entries.keys() {
        if seen.contains(path.as_str()) {
            continue;
        }
        if ignore.is_ignored(path, false)? {
            continue;
        }
        // A scanned file/symlink at an ancestor path occludes everything the baseline had
        // beneath it (a directory replaced by a symlink or file): the old children are out
        // of the tree even though the present-stat below — which follows intermediate
        // symlinks — may still "see" them through the symlink's target. Decide from the
        // scan, not the stat. (Scanned entries are only ever files/symlinks; directories
        // never enumerate, so ancestor membership in `seen` is exactly non-directory.)
        let occluded = {
            let mut prefix = path.as_str();
            let mut hit = false;
            while let Some((parent, _)) = prefix.rsplit_once('/') {
                if seen.contains(parent) {
                    hit = true;
                    break;
                }
                prefix = parent;
            }
            hit
        };
        if occluded {
            deletes.push(path.clone());
            continue;
        }
        match std::fs::symlink_metadata(root.join(path)) {
            // ENOTDIR: a parent directory was replaced by a file — the old child is gone.
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.raw_os_error() == Some(libc_enotdir()) =>
            {
                deletes.push(path.clone());
            }
            // Present but unscanned can only be a scan/check race (created mid-command);
            // leave the baseline alone — the next snapshot sees it.
            Ok(_) => {}
            Err(e) => {
                return Err(CliError::usage(format!(
                    "cannot stat {} (baseline path): {e}; aborting (silently treating it as \
                     deleted would delete it from the workspace)",
                    root.join(path).display()
                )));
            }
        }
    }
    deletes.sort();
    Ok(Delta {
        upserts,
        deletes,
        clean,
    })
}

#[cfg(unix)]
fn libc_enotdir() -> i32 {
    libc::ENOTDIR
}

#[cfg(not(unix))]
fn libc_enotdir() -> i32 {
    // No errno mapping off-unix; pick a value no io::Error will carry.
    i32::MIN
}

// ---------------------------------------------------------------------------------------------
// Local hashing: bind an oid to a fingerprint before push_files runs.
// ---------------------------------------------------------------------------------------------

/// One upsert with its locally computed blob oid. `stable_fingerprint` is `Some` only when a
/// stat immediately before the content read equals a stat immediately after it — the oid is
/// then bound to that fingerprint (plan's stat-before-hash / re-stat-after rule). `None`
/// (the file mutated mid-hash) never blocks the push; it only forfeits the cleanliness proof.
struct HashedUpsert {
    rel: String,
    oid: String,
    mode: u32,
    stable_fingerprint: Option<Fingerprint>,
    source: PushSource,
}

/// Hash one upsert exactly as the SDK will (`sha1("blob <len>\0" + bytes)`), reading
/// symlink targets as raw bytes (never through the link). This pre-hash is the accepted v1
/// double-read of changed files: `push_files` re-hashes internally, but only a locally-owned
/// oid can be journaled *before* the push becomes able to publish, and `on_prepared` cannot
/// abort a push after the fact. The second read moves no extra bytes — uploads are
/// content-addressed and negotiated after the SDK's own hash pass.
fn hash_upsert(root: &Path, entry: &ScanEntry) -> Result<HashedUpsert> {
    let abs = root.join(&entry.rel);
    let pre = std::fs::symlink_metadata(&abs)
        .map_err(|e| CliError::usage(format!("cannot stat {}: {e}", abs.display())))?;
    let pre_fp = Fingerprint::of(&pre);
    let (oid, source) = if pre_fp.mode == 0o120000 {
        let target = std::fs::read_link(&abs)?;
        let bytes = symlink_target_bytes(&target);
        let mut hasher = BlobOidHasher::new(bytes.len() as u64);
        hasher.update(&bytes);
        (hasher.finalize_hex(), PushSource::Bytes(bytes))
    } else {
        let mut file = std::fs::File::open(&abs)?;
        let len = file.metadata()?.len();
        let mut hasher = BlobOidHasher::new(len);
        let mut buf = vec![0u8; 128 * 1024];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        (hasher.finalize_hex(), PushSource::Path(abs.clone()))
    };
    let post = std::fs::symlink_metadata(&abs)
        .ok()
        .map(|m| Fingerprint::of(&m));
    // Mode from the same stat the content decision came from (pre_fp), not the earlier scan
    // stat: a chmod (or file->symlink flip) between scan and hash must not pair one
    // instant's bytes with another instant's mode.
    let mode = pre_fp.mode;
    let stable_fingerprint = (post.as_ref() == Some(&pre_fp)).then_some(pre_fp);
    Ok(HashedUpsert {
        rel: entry.rel.clone(),
        oid,
        mode,
        stable_fingerprint,
        source,
    })
}

/// Raw symlink target bytes. `to_string_lossy` would corrupt non-UTF-8 targets; on unix the
/// OS bytes pass through verbatim (git stores exactly these bytes as the blob).
#[cfg(unix)]
fn symlink_target_bytes(target: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;
    target.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn symlink_target_bytes(target: &Path) -> Vec<u8> {
    target.to_string_lossy().into_owned().into_bytes()
}

// ---------------------------------------------------------------------------------------------
// tl fs init — bind a plain directory to a new (verified empty-base) workspace.
// ---------------------------------------------------------------------------------------------

pub async fn init(
    ctx: &CliContext,
    path: Option<PathBuf>,
    file_system: Option<&str>,
) -> Result<()> {
    let (root, _state_dir, repo, ws_id) = bind(ctx, path, file_system, false).await?;
    println!(
        "Bound {root} to new workspace {} (file system {repo}).",
        short_id(&ws_id)
    );
    println!("Work in the directory, then: tl fs snapshot {root}");
    Ok(())
}

/// `tl fs push <dir> <filesystem>` — upload a directory into a filesystem as one save, no
/// mount. First push binds the directory (publish-on-save workspace); later pushes reuse the
/// binding's stat index, so only changed files upload.
pub async fn push(
    ctx: &CliContext,
    dir: &Path,
    file_system: &str,
    message: Option<&str>,
) -> Result<()> {
    let (root, state_dir) = match binding_for_lenient(dir) {
        Some((root, state_dir)) => {
            let binding = load_binding(&state_dir)?;
            if binding.repo != file_system {
                return Err(CliError::usage(format!(
                    "{root} is already bound to filesystem {} — push there, or pick another \
                     directory for {file_system}",
                    binding.repo
                )));
            }
            (root, state_dir)
        }
        None => {
            let (root, state_dir, _, _) =
                bind(ctx, Some(dir.to_path_buf()), Some(file_system), true).await?;
            (root, state_dir)
        }
    };
    snapshot(ctx, &root, &state_dir, message).await
}

/// Bind a directory to a fresh workspace on a filesystem. `publish` arms shared-rw: every
/// snapshot of the binding lands on the filesystem's default branch (the `tl fs push`
/// contract). Returns (root, state dir, repo, workspace id).
async fn bind(
    ctx: &CliContext,
    path: Option<PathBuf>,
    file_system: Option<&str>,
    publish: bool,
) -> Result<(String, PathBuf, String, String)> {
    if cfg!(not(unix)) {
        return Err(CliError::usage(
            "plain-directory bindings are supported on unix only in v1",
        ));
    }
    let dir = match path {
        Some(path) => path,
        None => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&dir)?;
    let root = canonical_mountpoint(&dir)?;
    assert_no_overlap(&root)?;
    // Binding a directory that contains our own state root would make snapshots upload (and
    // fingerprint-track) every mount's and binding's local state. Nothing good lives there.
    let tl_state = state_dir_root();
    let tl_state = tl_state.parent().unwrap_or(&tl_state);
    if tl_state.starts_with(&root) {
        return Err(CliError::usage(format!(
            "{root} contains the tensorlake state directory ({}); bind a project directory, \
             not a home/system tree",
            tl_state.display()
        )));
    }

    let session = FsSession::open(ctx, file_system).await?;
    let (user, token) = session.creds();
    let file_systems = session
        .client
        .list_repos_with_credential(&session.project_id, None, user, token)
        .await?
        .into_inner();
    let repo = match file_system {
        Some(name) => {
            if !file_systems.repos.iter().any(|r| r.name == name) {
                return Err(CliError::usage(format!(
                    "no file system named {name:?}; create it first: tl git create {name}"
                )));
            }
            name.to_string()
        }
        // No --file-system: unambiguous only when the project has exactly one.
        None => match file_systems.repos.as_slice() {
            [only] => only.name.clone(),
            [] => {
                return Err(CliError::usage(
                    "this project has no file systems; create one first: tl git create <name>",
                ));
            }
            many => {
                return Err(CliError::usage(format!(
                    "this project has {} file systems; pick one with --file-system ({})",
                    many.len(),
                    many.iter()
                        .map(|r| r.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                )));
            }
        },
    };
    let default_branch = file_systems
        .repos
        .iter()
        .find(|r| r.name == repo)
        .expect("validated above")
        .default_branch
        .clone();
    // A fresh `tl git create` repo has an unborn default branch; seed it with an empty
    // initial commit (empty root tree — exactly the base v1 requires) so init just works.
    super::ensure_seeded(&session, &default_branch, &repo).await?;
    let ws = session
        .client
        .create_workspace(
            &session.project_id,
            &repo,
            user,
            token,
            &CreateWorkspaceRequest {
                shared_target: publish.then(|| default_branch.clone()),
                ..Default::default()
            },
        )
        .await?
        .into_inner();
    // v1 verifies — not assumes — the empty base: workspace creation defaults to the repo
    // HEAD, which is only empty on a freshly seeded repo. One root-tree page of one entry is
    // the cheapest proof either way. (425: the base commit's index can still be
    // materializing right after the seed push.)
    let deadline = std::time::Instant::now() + super::TOO_EARLY_DEADLINE;
    let base_tree_empty = loop {
        match session
            .client
            .list_tree_page(
                &session.project_id,
                &repo,
                user,
                token,
                &ws.head,
                "",
                None,
                1,
            )
            .await
        {
            Ok(page) => break page.into_inner().entries.is_empty(),
            Err(tensorlake::error::SdkError::ServerError { status, .. })
                if status.as_u16() == 425 && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => {
                // The workspace was created for this binding alone; don't leak it.
                let _ = session
                    .client
                    .delete_workspace(&session.project_id, &repo, user, token, &ws.id)
                    .await;
                return Err(e.into());
            }
        }
    };
    if !base_tree_empty {
        let _ = session
            .client
            .delete_workspace(&session.project_id, &repo, user, token, &ws.id)
            .await;
        return Err(CliError::usage(format!(
            "file system {repo} already has content at its head; v1 binds only empty-base \
             workspaces — use `tl fs mount {repo} <path>` for existing content"
        )));
    }

    let state_dir = state_dir_root().join(&ws.id);
    std::fs::create_dir_all(&state_dir)?;
    let binding = Binding {
        project_id: session.project_id.clone(),
        organization_id: ctx.effective_organization_id(),
        repo: repo.clone(),
        workspace_id: ws.id.clone(),
        ref_name: ws.ref_name.clone(),
        root: PathBuf::from(&root),
        created_at_secs: now_secs(),
    };
    write_atomic(
        &state_dir.join("binding.json"),
        &serde_json::to_vec_pretty(&binding)?,
    )?;
    // The initial index IS the empty-base proof persisted: head = the verified-empty base,
    // zero baseline entries. From here on, a missing index is lost state (fail closed).
    save_index(
        &state_dir,
        &Index {
            indexed_head: ws.head.clone(),
            scan_started_at: now_stamp(),
            entries: BTreeMap::new(),
        },
    )?;
    let registered = registry_mutate(|registry| {
        // TOCTOU re-check under the registry lock: the preflight `assert_no_overlap` ran
        // before the workspace round-trips, and another init (or mount) can have claimed an
        // overlapping root since. The closure aborting leaves the registry untouched.
        if let Some(msg) =
            mount_overlap_error(&root).or_else(|| binding_overlap_error(&root, &registry.bindings))
        {
            return Err(CliError::usage(msg));
        }
        registry.bindings.insert(root.clone(), state_dir.clone());
        Ok(())
    });
    if let Err(e) = registered {
        // Same cleanup as the empty-base failure: the workspace and state dir were created
        // for this binding alone — a lost registry race must leak neither.
        let _ = std::fs::remove_dir_all(&state_dir);
        let _ = session
            .client
            .delete_workspace(&session.project_id, &repo, user, token, &ws.id)
            .await;
        return Err(e);
    }
    Ok((root, state_dir, repo, ws.id))
}

// ---------------------------------------------------------------------------------------------
// tl fs snapshot — on a binding.
// ---------------------------------------------------------------------------------------------

pub async fn snapshot(
    ctx: &CliContext,
    root: &str,
    state_dir: &Path,
    message: Option<&str>,
) -> Result<()> {
    let binding = load_binding(state_dir)?;
    let _lock = acquire_lock(state_dir)?;
    let session = FsSession::open(ctx, Some(&binding.repo)).await?;
    let (user, token) = session.creds();
    // No daemon exists to re-arm the workspace lease in the background; every command is a
    // heartbeat so actively-used bindings stay alive.
    session
        .client
        .workspace_heartbeat(
            &session.project_id,
            &binding.repo,
            user,
            token,
            &binding.workspace_id,
        )
        .await?;

    let started = std::time::Instant::now();
    let bar = indicatif::ProgressBar::new_spinner();
    bar.enable_steady_tick(std::time::Duration::from_millis(120));

    // Recovery first: a journal means an earlier attempt may have published. Nothing —
    // preflight included — is trustworthy until its fate is settled.
    if let Some(journal) = load_journal(state_dir)? {
        bar.set_message("found an interrupted snapshot attempt; recovering...");
        let mut index = load_index(state_dir)?;
        // The head is the fallback decider (and the sole one when no job id was recorded).
        let head = session
            .client
            .get_workspace(
                &session.project_id,
                &binding.repo,
                user,
                token,
                &binding.workspace_id,
            )
            .await?
            .into_inner()
            .head;
        let decision = match journal_job_id(state_dir, &journal) {
            // The recorded job id names OUR attempt's server job: poll it to terminal FIRST
            // and let its fate decide. Without it, the old attempt's detached job could land
            // *after* the head comparison declared Fresh, CAS-failing the retry and wedging
            // the binding behind a fail-closed error. A job record the server no longer
            // knows (aged out) degrades to the head table — safe, just stricter.
            Some(job_id) => {
                bar.set_message("checking the interrupted attempt's commit job...");
                let poll = poll_job_fate(&session, &binding, &job_id).await?;
                recovery_decision_after_poll(
                    &poll,
                    &journal.expected_head,
                    &head,
                    &index.indexed_head,
                )
            }
            // No job id recorded (crash before submission): the head table is sound.
            None => recovery_decision(
                &journal.expected_head,
                &head,
                &index.indexed_head,
                AdoptionEvidence::Unavailable,
            ),
        };
        match decision {
            RecoveryDecision::Fresh | RecoveryDecision::AlreadyInstalled => {
                remove_journal_durably(state_dir)?;
            }
            RecoveryDecision::Adopt { head } => {
                apply_adopted_journal(&mut index, &journal, &head);
                save_index(state_dir, &index)?;
                remove_journal_durably(state_dir)?;
            }
            RecoveryDecision::Fail(msg) => {
                bar.finish_and_clear();
                return Err(recovery_fail_error(&msg, state_dir));
            }
        }
    }

    let index = load_index(state_dir)?;
    // Preflight: fail before hashing gigabytes when the workspace moved under us. The CAS
    // (`expect_oid`) still guards the preflight-to-commit window.
    bar.set_message("checking the workspace head...");
    let head = session
        .client
        .get_workspace(
            &session.project_id,
            &binding.repo,
            user,
            token,
            &binding.workspace_id,
        )
        .await?
        .into_inner()
        .head;
    if head != index.indexed_head {
        bar.finish_and_clear();
        return Err(CliError::usage(format!(
            "workspace advanced remotely (head {head}, index has {}); this binding's \
             baseline is stale and v1 cannot rebase it — snapshot from wherever advanced it",
            index.indexed_head
        )));
    }

    bar.set_message("scanning...");
    let scan_started_at = now_stamp();
    let root_path = Path::new(root);
    let mut ignore = strict_ignore(root_path)?;
    let scanned = scan(root_path, &mut ignore)?;
    let delta = compute_delta(root_path, &index, &scanned, &mut ignore)?;
    let t_scan = started.elapsed();

    // Hash the candidates locally. Some "upserts" are only untrusted (clean_fingerprint was
    // None or racy): when their bytes still hash to the server oid they leave the push and
    // merely re-earn their cleanliness proof.
    bar.set_message(format!(
        "hashing {} candidate file(s)...",
        delta.upserts.len()
    ));
    // Hash on the blocking pool, a bounded batch at a time: the pre-hash is CPU+IO bound and
    // was fully serial. Deterministic output order (and so a deterministic journal) is kept
    // by indexing the input and sorting the collected results.
    let hashed_upserts: Vec<HashedUpsert> = {
        use futures::StreamExt as _;
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(8);
        let results: Vec<Result<(usize, HashedUpsert)>> =
            futures::stream::iter(delta.upserts.iter().cloned().enumerate().map(
                |(position, entry)| {
                    let root = root_path.to_path_buf();
                    async move {
                        tokio::task::spawn_blocking(move || {
                            hash_upsert(&root, &entry).map(|hashed| (position, hashed))
                        })
                        .await
                        .map_err(|e| CliError::usage(format!("hashing task failed: {e}")))?
                    }
                },
            ))
            .buffer_unordered(parallelism)
            .collect()
            .await;
        let mut indexed = results.into_iter().collect::<Result<Vec<_>>>()?;
        indexed.sort_by_key(|(position, _)| *position);
        indexed.into_iter().map(|(_, hashed)| hashed).collect()
    };
    let mut pushes: Vec<HashedUpsert> = Vec::new();
    let mut reproven: Vec<(String, Fingerprint)> = Vec::new();
    for hashed in hashed_upserts {
        let unchanged = index.entries.get(&hashed.rel).is_some_and(|indexed| {
            indexed.server_oid == hashed.oid && indexed.server_mode == hashed.mode
        });
        match (unchanged, &hashed.stable_fingerprint) {
            (true, Some(fp)) => reproven.push((hashed.rel, fp.clone())),
            // Unchanged but unstable mid-hash: nothing to push (the server already has this
            // oid) and nothing to prove — leave the entry untrusted for the next snapshot.
            (true, None) => {}
            (false, _) => pushes.push(hashed),
        }
    }
    let t_hash = started.elapsed() - t_scan;

    if pushes.is_empty() && delta.deletes.is_empty() {
        // An empty push_files call would still create a (contentless) commit — skip it. The
        // re-proven fingerprints are still worth persisting: they spare the next scan the
        // same rehash.
        let mut index = index;
        if !reproven.is_empty() {
            for (rel, fp) in reproven {
                if let Some(entry) = index.entries.get_mut(&rel) {
                    entry.clean_fingerprint = Some(fp);
                }
            }
            index.scan_started_at = scan_started_at;
            save_index(state_dir, &index)?;
        }
        bar.finish_and_clear();
        println!("Nothing to snapshot: directory matches the last snapshot.");
        return Ok(());
    }

    // Journal the attempt durably BEFORE push_files can publish anything: caller-owned
    // idempotency key, the CAS base, and the exact candidate (locally hashed oids +
    // stability fingerprints).
    let idempotency_key = {
        let bytes: [u8; 16] = rand::random();
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    let message = message.unwrap_or("tl fs snapshot").to_string();
    let journal = Journal {
        idempotency_key: idempotency_key.clone(),
        expected_head: index.indexed_head.clone(),
        started_at_secs: now_secs(),
        message: message.clone(),
        job_id: None,
        entries: pushes
            .iter()
            .map(|hashed| JournalEntry {
                path: hashed.rel.clone(),
                oid: Some(hashed.oid.clone()),
                mode: Some(hashed.mode),
                delete: false,
                pre_fingerprint: hashed.stable_fingerprint.clone(),
            })
            .chain(delta.deletes.iter().map(|path| JournalEntry {
                path: path.clone(),
                oid: None,
                mode: None,
                delete: true,
                pre_fingerprint: None,
            }))
            .collect(),
    };
    save_journal(state_dir, &journal)?;

    let files: Vec<PushFile> = pushes
        .iter()
        .map(|hashed| PushFile {
            repo_path: hashed.rel.clone(),
            source: hashed.source.clone(),
            mode: Some(hashed.mode),
            delete: false,
        })
        .chain(delta.deletes.iter().map(|path| PushFile {
            repo_path: path.clone(),
            source: PushSource::Bytes(Vec::new()),
            mode: None,
            delete: true,
        }))
        .collect();

    // on_prepared is the cross-check on the accepted double-hash: the SDK re-reads and
    // re-hashes each file, and a path whose prepared oid differs from the journaled one
    // mutated between our hash and the SDK's. The *prepared* oid is what the server
    // publishes, so raced paths take it as their new baseline — with no cleanliness proof.
    let journaled_oids: Arc<BTreeMap<String, String>> = Arc::new(
        pushes
            .iter()
            .map(|hashed| (hashed.rel.clone(), hashed.oid.clone()))
            .collect(),
    );
    let raced: Arc<Mutex<BTreeMap<String, String>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let hook_raced = raced.clone();
    let hook_journaled = journaled_oids.clone();
    // Record the server job id the instant the 202 names it (`CommitDetached` rides the
    // progress stream): from that moment the commit can publish without this process, and
    // recovery's job poll is the only oracle that can prove whose commit the head is. The id
    // goes to the tiny `journal.job` SIDECAR, written off-thread — rewriting the whole
    // journal here would serialize an entry-proportional fsync into the progress callback
    // (which runs inline in the push). The handle is joined right after push_files returns;
    // a failed write costs recovery evidence, never correctness.
    let sidecar_writer: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> =
        Arc::new(Mutex::new(None));
    let progress = {
        let inner = crate::commands::push_progress_spinner(&bar);
        let journal_state_dir = state_dir.to_path_buf();
        let sidecar_writer = sidecar_writer.clone();
        let hook: Arc<dyn Fn(tensorlake::artifact_storage::ingest::PushEvent) + Send + Sync> =
            Arc::new(move |ev| {
                if let tensorlake::artifact_storage::ingest::PushEvent::CommitDetached { job_id } =
                    &ev
                {
                    let state_dir = journal_state_dir.clone();
                    let job_id = job_id.clone();
                    let handle = std::thread::spawn(move || {
                        if let Err(e) =
                            write_atomic(&journal_job_path(&state_dir), job_id.as_bytes())
                        {
                            eprintln!(
                                "{} could not record the commit job id: {e}",
                                style("warning:").yellow()
                            );
                        }
                    });
                    *sidecar_writer
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
                }
                inner(ev);
            });
        hook
    };
    let push_result = session
        .client
        .push_files(
            &session.project_id,
            &binding.repo,
            user,
            token,
            files,
            PushOptions {
                message,
                workspace_snapshot: Some(binding.workspace_id.clone()),
                expect_oid: Some(index.indexed_head.clone()),
                idempotency_key: Some(idempotency_key),
                on_prepared: Some(Arc::new(move |prepared| {
                    // Runs inside the SDK: stay panic-free, record and move on.
                    let mut raced = hook_raced
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    for p in prepared {
                        if p.delete {
                            continue;
                        }
                        if let (Some(prepared_oid), Some(journaled_oid)) =
                            (&p.oid, hook_journaled.get(&p.path))
                            && prepared_oid != journaled_oid
                        {
                            raced.insert(p.path.clone(), prepared_oid.clone());
                        }
                    }
                })),
                progress: Some(progress),
                ..Default::default()
            },
        )
        .await;
    // The sidecar write must be settled (either way) before recovery could ever need it —
    // and before the success path deletes journal + sidecar below.
    if let Some(handle) = sidecar_writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        let _ = handle.join();
    }
    let report = push_result?.into_inner();
    let t_push = started.elapsed() - t_hash - t_scan;

    // Commit confirmed: build the post-push baseline under the race rule. A pushed path is
    // clean only if (a) the SDK hashed the same bytes we journaled AND (b) a re-stat now
    // still equals the journaled fingerprint. Anything else keeps the entry (its oid is what
    // the server published) with clean_fingerprint = None — never a stale-clean tuple, never
    // a dropped entry (a dropped new file could not translate a later local delete into a
    // server delete).
    bar.set_message("updating the local index...");
    let raced = raced
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // `index` is done answering questions; take its entries instead of cloning the map.
    let mut entries = index.entries;
    for (rel, fp) in reproven {
        if let Some(entry) = entries.get_mut(&rel) {
            entry.clean_fingerprint = Some(fp);
        }
    }
    for hashed in &pushes {
        let committed_oid = raced
            .get(&hashed.rel)
            .cloned()
            .unwrap_or_else(|| hashed.oid.clone());
        let clean_fingerprint = if raced.contains_key(&hashed.rel) {
            None
        } else {
            hashed.stable_fingerprint.as_ref().and_then(|fp| {
                let now = std::fs::symlink_metadata(root_path.join(&hashed.rel))
                    .ok()
                    .map(|m| Fingerprint::of(&m));
                (now.as_ref() == Some(fp)).then(|| fp.clone())
            })
        };
        entries.insert(
            hashed.rel.clone(),
            IndexEntry {
                server_oid: committed_oid,
                server_mode: hashed.mode,
                clean_fingerprint,
            },
        );
    }
    for path in &delta.deletes {
        entries.remove(path);
    }
    save_index(
        state_dir,
        &Index {
            indexed_head: report.commit.clone(),
            scan_started_at,
            entries,
        },
    )?;
    remove_journal_durably(state_dir)?;

    let total = started.elapsed();
    bar.finish_and_clear();
    println!(
        "Snapshot {} ({} file(s), {} delete(s), {} of {} chunks uploaded in {})",
        report.commit,
        pushes.len(),
        delta.deletes.len(),
        report.chunks_uploaded,
        report.chunks_total.max(report.chunks_uploaded),
        fmt_dur(total),
    );
    println!(
        "  scan {} ({} clean skipped)  hash {}  push {}{}",
        fmt_dur(t_scan),
        delta.clean,
        fmt_dur(t_hash),
        fmt_dur(t_push),
        if raced.is_empty() {
            String::new()
        } else {
            format!(
                "  ({} path(s) changed mid-push; they rehash next time)",
                raced.len()
            )
        },
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// tl fs status / unbind — on a binding.
// ---------------------------------------------------------------------------------------------

pub async fn status(
    ctx: &CliContext,
    root: &str,
    state_dir: &Path,
    output_json: bool,
) -> Result<()> {
    let binding = load_binding(state_dir)?;
    let index = load_index(state_dir)?;
    let session = FsSession::open(ctx, Some(&binding.repo)).await?;
    let (user, token) = session.creds();
    let ws = session
        .client
        .get_workspace(
            &session.project_id,
            &binding.repo,
            user,
            token,
            &binding.workspace_id,
        )
        .await?
        .into_inner();
    let _ = session
        .client
        .workspace_heartbeat(
            &session.project_id,
            &binding.repo,
            user,
            token,
            &binding.workspace_id,
        )
        .await;
    // Fingerprints only — status never reads file contents. "Changed" therefore includes
    // untrusted entries a snapshot may re-prove clean without pushing.
    let root_path = Path::new(root);
    let mut ignore = strict_ignore(root_path)?;
    let scanned = scan(root_path, &mut ignore)?;
    let delta = compute_delta(root_path, &index, &scanned, &mut ignore)?;
    let journal_pending = journal_path(state_dir).exists();

    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "workspace": ws,
                "binding": root,
                "indexed_head": index.indexed_head,
                "changed": delta.upserts.iter().map(|e| e.rel.clone()).collect::<Vec<_>>(),
                "deleted": delta.deletes,
                "clean": delta.clean,
                "journal_pending": journal_pending,
            }))?
        );
        return Ok(());
    }
    println!("{} {}", style("file system:").dim(), binding.repo);
    println!(
        "{} {} (created {} ago)",
        style("workspace:").dim(),
        short_id(&ws.id),
        super::age_display(ws.created_at_secs)
    );
    println!(
        "{} plain directory at {root} (no mount)",
        style("binding:").dim()
    );
    println!(
        "{} {}{}",
        style("indexed head:").dim(),
        short_id(&index.indexed_head),
        if ws.head == index.indexed_head {
            String::new()
        } else {
            format!(
                " ({} — the workspace advanced remotely to {}; snapshots here will refuse)",
                style("stale").red(),
                short_id(&ws.head)
            )
        }
    );
    if journal_pending {
        println!(
            "{} an interrupted snapshot attempt is journaled; the next snapshot recovers it",
            style("note:").yellow()
        );
    }
    if delta.upserts.is_empty() && delta.deletes.is_empty() {
        println!(
            "{} clean ({} file(s) verified by stat)",
            style("local:").dim(),
            delta.clean
        );
    } else {
        println!(
            "{} {} changed / {} deleted vs the index ({} clean):",
            style("local:").dim(),
            delta.upserts.len(),
            delta.deletes.len(),
            delta.clean,
        );
        for entry in delta.upserts.iter().take(20) {
            println!("  {} {}", style("M").yellow(), entry.rel);
        }
        for path in delta.deletes.iter().take(20) {
            println!("  {} {}", style("D").red(), path);
        }
        if delta.upserts.len() + delta.deletes.len() > 40 {
            println!("  … and more");
        }
        println!("Snapshot with: tl fs snapshot {root}");
    }
    Ok(())
}

/// The state dir whose `binding.json` names `root` when the registry has lost track of it —
/// the orphan a crashed pre-reorder unbind (or a lost registry) leaves behind. Such a state
/// dir still claims its workspace (`binding_using_workspace` scans state dirs, blocking
/// `tl fs rm`), so `tl fs unbind <root>` must be able to reap it or the rm guard's advice
/// becomes unfollowable.
fn orphan_state_dir_for(root: &str, state_root: &Path) -> Option<PathBuf> {
    let read = std::fs::read_dir(state_root).ok()?;
    for entry in read.flatten() {
        if let Ok(binding) = load_binding(&entry.path())
            && binding.root == Path::new(root)
        {
            return Some(entry.path());
        }
    }
    None
}

/// `tl fs unbind` — remove the local binding and its state. Deliberately NOT `rm`: `tl fs rm`
/// deletes the *workspace*; unbinding only forgets this directory's link to it, and the
/// workspace (with every snapshot) survives on the server.
pub async fn unbind(path: Option<PathBuf>) -> Result<()> {
    let (root, state_dir) = match path {
        Some(path) => match binding_for(&path)? {
            Some(found) => found,
            None => {
                // The registry doesn't know the root, but an orphaned state dir may still
                // claim it (crashed unbind, lost registry). Reap it here so the binding's
                // workspace is deletable again.
                let root = canonical_mountpoint(&path)?;
                if let Some(state_dir) = orphan_state_dir_for(&root, &state_dir_root()) {
                    let workspace = load_binding(&state_dir)
                        .map(|b| b.workspace_id)
                        .unwrap_or_default();
                    std::fs::remove_dir_all(&state_dir)?;
                    println!(
                        "Removed the orphaned binding state for {root} (the registry had \
                         already lost track of it). Workspace {} survives on the server.",
                        short_id(&workspace),
                    );
                    return Ok(());
                }
                return Err(CliError::usage(format!(
                    "{} is not a bound directory (see `tl fs status`, or bind one with \
                     `tl fs init`)",
                    path.display()
                )));
            }
        },
        None => {
            // Walk ancestors like every other path-addressed command: `tl fs unbind` from
            // anywhere inside the bound tree names its binding.
            let cwd = std::env::current_dir()?;
            let cwd = cwd.canonicalize().unwrap_or(cwd);
            binding_containing(&cwd)?.ok_or_else(|| {
                CliError::usage(format!(
                    "{} is not inside a bound directory; pass the bound directory explicitly",
                    cwd.display()
                ))
            })?
        }
    };
    let workspace = load_binding(&state_dir)
        .map(|b| b.workspace_id)
        .unwrap_or_default();
    if journal_path(&state_dir).exists() {
        eprintln!(
            "{} an interrupted snapshot attempt was journaled for this binding; its fate on \
             the server was never verified (check the workspace head before trusting it)",
            style("warning:").yellow()
        );
    }
    // State dir FIRST, registry second. A crash between the two then leaves only a registry
    // entry pointing at nothing — self-healing debris (`binding_for` treats it as unbound,
    // the next `registry_mutate` prunes it). The reverse order left an orphaned state dir
    // that still claimed the workspace with no registry entry naming it.
    std::fs::remove_dir_all(&state_dir)?;
    registry_mutate(|registry| {
        registry.bindings.remove(&root);
        Ok(())
    })?;
    if workspace.is_empty() {
        println!("Unbound {root}.");
    } else {
        println!(
            "Unbound {root}. Workspace {} and its snapshots survive on the server (delete \
             with: tl fs rm {}).",
            short_id(&workspace),
            short_id(&workspace),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(mode: u32, size: u64, mtime: Stamp) -> Fingerprint {
        Fingerprint {
            mode,
            size,
            mtime,
            ctime: mtime,
            dev: 1,
            ino: 42,
        }
    }

    fn index_with(entries: &[(&str, &str, u32, Option<Fingerprint>)]) -> Index {
        Index {
            indexed_head: "head0".to_string(),
            scan_started_at: (1000, 0),
            entries: entries
                .iter()
                .map(|(path, oid, mode, clean)| {
                    (
                        path.to_string(),
                        IndexEntry {
                            server_oid: oid.to_string(),
                            server_mode: *mode,
                            clean_fingerprint: clean.clone(),
                        },
                    )
                })
                .collect(),
        }
    }

    // -- index persistence ---------------------------------------------------------------

    #[test]
    fn index_round_trips_through_atomic_replace() {
        let dir = tempfile::tempdir().unwrap();
        let index = index_with(&[("a.txt", "oid-a", 0o100644, Some(fp(0o100644, 3, (900, 0))))]);
        save_index(dir.path(), &index).unwrap();
        let loaded = load_index(dir.path()).unwrap();
        assert_eq!(loaded.indexed_head, "head0");
        assert_eq!(loaded.entries.len(), 1);
        let entry = &loaded.entries["a.txt"];
        assert_eq!(entry.server_oid, "oid-a");
        assert_eq!(entry.clean_fingerprint, Some(fp(0o100644, 3, (900, 0))));
        // Replacing keeps exactly one durable file; no temp remains.
        save_index(dir.path(), &index).unwrap();
        assert!(!dir.path().join("index.json.tmp").exists());
    }

    /// A crash mid-write leaves a stray temp next to the last durable index; the loader must
    /// serve the durable file and never the temp — and with only a temp present (crash on
    /// the very first write), it must fail closed, not adopt the partial file.
    #[test]
    fn index_loader_ignores_stray_temp_and_fails_closed_without_index() {
        let dir = tempfile::tempdir().unwrap();
        let index = index_with(&[("a.txt", "oid-a", 0o100644, None)]);
        save_index(dir.path(), &index).unwrap();
        std::fs::write(dir.path().join("index.json.tmp"), b"{\"partial").unwrap();
        assert_eq!(load_index(dir.path()).unwrap().indexed_head, "head0");

        let fresh = tempfile::tempdir().unwrap();
        std::fs::write(fresh.path().join("index.json.tmp"), b"{\"partial").unwrap();
        let err = load_index(fresh.path()).unwrap_err().to_string();
        assert!(err.contains("index.json"), "names the file: {err}");
    }

    #[test]
    fn corrupt_index_fails_closed_naming_the_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(index_path(dir.path()), b"not json").unwrap();
        let err = load_index(dir.path()).unwrap_err().to_string();
        assert!(
            err.contains("index.json") && err.contains("corrupt"),
            "{err}"
        );
        // Suggests nothing destructive.
        assert!(!err.contains("delete") && !err.contains("remove"), "{err}");
    }

    // -- registry policy: strict for binding owners, lenient for mount dispatch -------------

    #[test]
    fn registry_load_is_strict_and_lenient_wrapper_degrades() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bindings.json");
        std::fs::write(&path, b"{ definitely not json").unwrap();
        let err = registry_load_at(&path).unwrap_err().to_string();
        assert!(
            err.contains("bindings.json") && err.contains("corrupt"),
            "strict load names the file: {err}"
        );
        // The lenient policy turns the same failure into "nothing bound" (mount commands
        // keep working); a healthy result passes through untouched.
        assert_eq!(registry_lenient::<u32>(Err(CliError::usage(err))), None);
        assert_eq!(registry_lenient(Ok(7u32)), Some(7));
        // Absence is not corruption: strict load treats a missing registry as empty.
        assert!(
            registry_load_at(&dir.path().join("missing.json"))
                .unwrap()
                .bindings
                .is_empty()
        );
    }

    // -- write_atomic: unique temps --------------------------------------------------------

    #[test]
    fn write_atomic_temps_are_unique_and_never_linger() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("index.json");
        // Two concurrent (or consecutive) writers must never share a temp inode.
        let a = unique_temp_path(&target).unwrap();
        let b = unique_temp_path(&target).unwrap();
        assert_ne!(a, b, "temp names must be call-unique");

        // Simulated race: two threads replace the same target repeatedly; the final state is
        // one writer's complete content, and no temp survives.
        let t1 = {
            let target = target.clone();
            std::thread::spawn(move || {
                for _ in 0..50 {
                    write_atomic(&target, b"{\"writer\":1}").unwrap();
                }
            })
        };
        let t2 = {
            let target = target.clone();
            std::thread::spawn(move || {
                for _ in 0..50 {
                    write_atomic(&target, b"{\"writer\":2}").unwrap();
                }
            })
        };
        t1.join().unwrap();
        t2.join().unwrap();
        let final_bytes = std::fs::read(&target).unwrap();
        assert!(
            final_bytes == b"{\"writer\":1}" || final_bytes == b"{\"writer\":2}",
            "torn write: {final_bytes:?}"
        );
        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temps must not linger: {leftovers:?}");
    }

    // -- unbind ancestor walk ----------------------------------------------------------------

    #[test]
    fn deepest_containing_walks_ancestors() {
        let roots = vec![
            "/w/project".to_string(),
            "/w/project/nested".to_string(),
            "/elsewhere".to_string(),
        ];
        assert_eq!(
            deepest_containing(roots.iter(), Path::new("/w/project/nested/src/lib.rs")),
            Some("/w/project/nested".to_string()),
            "the deepest containing root wins"
        );
        assert_eq!(
            deepest_containing(roots.iter(), Path::new("/w/project/src")),
            Some("/w/project".to_string())
        );
        assert_eq!(
            deepest_containing(roots.iter(), Path::new("/w/projectile")),
            None,
            "prefix matching is component-wise, not string-wise"
        );
        assert_eq!(deepest_containing(roots.iter(), Path::new("/tmp")), None);
    }

    // -- fingerprint dirty rules -----------------------------------------------------------

    #[test]
    fn fingerprint_dirty_rules() {
        let clean = fp(0o100644, 10, (500, 0));
        let index = index_with(&[
            ("clean.txt", "oid", 0o100644, Some(clean.clone())),
            ("untrusted.txt", "oid", 0o100644, None),
            ("changed.txt", "oid", 0o100644, Some(clean.clone())),
        ]);
        let scan_hits = |entry: &ScanEntry| {
            index.entries.get(&entry.rel).is_some_and(|indexed| {
                indexed.clean_fingerprint.as_ref().is_some_and(|f| {
                    f == &entry.fingerprint && !f.overlaps_scan(index.scan_started_at)
                })
            })
        };
        // Identical fingerprint outside the racy window: clean.
        assert!(scan_hits(&ScanEntry {
            rel: "clean.txt".into(),
            fingerprint: clean.clone()
        }));
        // clean_fingerprint = None: always rehash.
        assert!(!scan_hits(&ScanEntry {
            rel: "untrusted.txt".into(),
            fingerprint: clean.clone()
        }));
        // Any stat drift (size here): dirty.
        assert!(!scan_hits(&ScanEntry {
            rel: "changed.txt".into(),
            fingerprint: fp(0o100644, 11, (500, 0))
        }));
        // Mode drift (chmod +x, or file->symlink): dirty.
        assert!(!scan_hits(&ScanEntry {
            rel: "changed.txt".into(),
            fingerprint: fp(0o100755, 10, (500, 0))
        }));
    }

    /// The racy-window rule: a fingerprint recorded with timestamps at/after the recording
    /// scan's start cannot prove cleanliness — a same-resolution write inside the window
    /// would be invisible to stat. It must read as dirty even though the fingerprints match.
    #[test]
    fn fingerprint_racy_window_dirties() {
        let racy = fp(0o100644, 10, (1000, 5)); // >= scan_started_at (1000, 0)
        assert!(racy.overlaps_scan((1000, 0)));
        let settled = fp(0o100644, 10, (999, 999_999_999));
        assert!(!settled.overlaps_scan((1000, 0)));
        // ctime alone in the window also dirties (metadata-only change, e.g. chmod raced).
        let ctime_racy = Fingerprint {
            ctime: (1000, 0),
            ..fp(0o100644, 10, (900, 0))
        };
        assert!(ctime_racy.overlaps_scan((1000, 0)));
    }

    // -- scanner ---------------------------------------------------------------------------

    #[cfg(unix)]
    #[test]
    fn scanner_distinguishes_ignored_from_missing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".gitignore"), "*.tmp\n").unwrap();
        std::fs::write(dir.path().join("kept.txt"), "x").unwrap();
        std::fs::write(dir.path().join("scratch.tmp"), "x").unwrap();

        let index = index_with(&[
            // Baseline path now covered by an ignore rule (and still on disk): NOT a delete.
            ("scratch.tmp", "oid-tmp", 0o100644, None),
            // Baseline path genuinely gone from disk: a delete.
            ("vanished.txt", "oid-gone", 0o100644, None),
        ]);
        let mut ignore = strict_ignore(dir.path()).unwrap();
        let scanned = scan(dir.path(), &mut ignore).unwrap();
        let rels: Vec<&str> = scanned.iter().map(|e| e.rel.as_str()).collect();
        assert_eq!(
            rels,
            vec![".gitignore", "kept.txt"],
            "ignored paths never enumerate"
        );

        let delta = compute_delta(dir.path(), &index, &scanned, &mut ignore).unwrap();
        assert_eq!(delta.deletes, vec!["vanished.txt".to_string()]);
        let upserts: Vec<&str> = delta.upserts.iter().map(|e| e.rel.as_str()).collect();
        assert_eq!(upserts, vec![".gitignore", "kept.txt"]);
    }

    /// A tracked directory replaced by a symlink: the baseline children are gone from the
    /// tree, but a present-check that stats THROUGH the symlink still finds them at the
    /// target and used to skip the delete — the scan's upsert set, not the stat, must
    /// decide. (Same shape for a directory replaced by a regular file, minus the stat trap.)
    #[cfg(unix)]
    #[test]
    fn dir_replaced_by_symlink_deletes_baseline_children() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("child.txt"), "still here via symlink").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("d")).unwrap();

        let index = index_with(&[("d/child.txt", "oid-child", 0o100644, None)]);
        let mut ignore = strict_ignore(dir.path()).unwrap();
        let scanned = scan(dir.path(), &mut ignore).unwrap();
        let rels: Vec<&str> = scanned.iter().map(|e| e.rel.as_str()).collect();
        assert_eq!(rels, vec!["d"], "the symlink scans; nothing under it does");
        // The stat trap this guards against: the naive present-check DOES see the old path.
        assert!(dir.path().join("d/child.txt").metadata().is_ok());

        let delta = compute_delta(dir.path(), &index, &scanned, &mut ignore).unwrap();
        assert_eq!(
            delta.deletes,
            vec!["d/child.txt".to_string()],
            "occluded baseline children delete"
        );
    }

    /// The strictness contract: an unreadable directory ABORTS — silently skipping it would
    /// make every baseline path under it look missing, i.e. a server-side mass deletion.
    #[cfg(unix)]
    #[test]
    fn scanner_unreadable_directory_aborts() {
        use std::os::unix::fs::PermissionsExt as _;
        if unsafe { libc::geteuid() } == 0 {
            return; // root ignores permission bits; the scenario cannot be staged
        }
        let dir = tempfile::tempdir().unwrap();
        let sealed = dir.path().join("sealed");
        std::fs::create_dir(&sealed).unwrap();
        std::fs::write(sealed.join("inside.txt"), "x").unwrap();
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o000)).unwrap();

        let mut ignore = strict_ignore(dir.path()).unwrap();
        let err = scan(dir.path(), &mut ignore).unwrap_err().to_string();
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(err.contains("sealed"), "names the unreadable path: {err}");
        assert!(err.contains("aborting"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn scanner_rejects_non_utf8_names() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt as _;
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join(OsStr::from_bytes(b"bad-\xFF-name"));
        // APFS (macOS) refuses to create non-UTF-8 names at all (EILSEQ) — the scanner can
        // then never meet one locally, and the rejection path is only stageable on ext4 &co.
        if std::fs::write(&bad, "x").is_err() {
            return;
        }
        let mut ignore = strict_ignore(dir.path()).unwrap();
        let err = scan(dir.path(), &mut ignore).unwrap_err().to_string();
        assert!(err.contains("non-UTF-8"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn scanner_rejects_special_files_by_name() {
        use std::os::unix::ffi::OsStrExt as _;
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("pipe");
        let c = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o644) }, 0);
        let mut ignore = strict_ignore(dir.path()).unwrap();
        let err = scan(dir.path(), &mut ignore).unwrap_err().to_string();
        assert!(err.contains("pipe"), "names the special file: {err}");
    }

    #[cfg(unix)]
    #[test]
    fn hashing_preserves_symlink_target_bytes() {
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink("../target/file", dir.path().join("link")).unwrap();
        let mut ignore = strict_ignore(dir.path()).unwrap();
        let scanned = scan(dir.path(), &mut ignore).unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].fingerprint.mode, 0o120000);
        let hashed = hash_upsert(dir.path(), &scanned[0]).unwrap();
        // The blob content is the raw target path — not the target file's bytes.
        match &hashed.source {
            PushSource::Bytes(bytes) => assert_eq!(bytes.as_slice(), b"../target/file"),
            other => panic!("symlink must push its target bytes, got {other:?}"),
        }
        // Oid matches git's blob oid of the target string.
        let mut h = BlobOidHasher::new(14);
        h.update(b"../target/file");
        assert_eq!(hashed.oid, h.finalize_hex());
        assert!(hashed.stable_fingerprint.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_tlignore_aborts() {
        use std::os::unix::fs::PermissionsExt as _;
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let tlignore = dir.path().join(".tlignore");
        std::fs::write(&tlignore, "scratch\n").unwrap();
        std::fs::set_permissions(&tlignore, std::fs::Permissions::from_mode(0o000)).unwrap();
        // (`unwrap_err` needs Debug on the Ok side; SnapshotIgnore has none.)
        let err = match strict_ignore(dir.path()) {
            Ok(_) => panic!("unreadable .tlignore must abort"),
            Err(e) => e.to_string(),
        };
        std::fs::set_permissions(&tlignore, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(err.contains(".tlignore"), "{err}");
    }

    // -- journal recovery decision table ----------------------------------------------------

    #[test]
    fn recovery_decision_table() {
        // Head never moved: the attempt did not publish — drop the journal, snapshot fresh.
        assert_eq!(
            recovery_decision("h0", "h0", "h0", AdoptionEvidence::Unavailable),
            RecoveryDecision::Fresh
        );
        // Head moved and already equals the installed index: the crash was between index
        // install and journal removal — only the stale journal needs dropping.
        assert_eq!(
            recovery_decision("h0", "h1", "h1", AdoptionEvidence::Unavailable),
            RecoveryDecision::AlreadyInstalled
        );
        // Head moved with no authorship proof: hard error, change nothing.
        match recovery_decision("h0", "h1", "h0", AdoptionEvidence::Unavailable) {
            RecoveryDecision::Fail(msg) => {
                assert!(msg.contains("h1") && msg.contains("h0"), "{msg}");
            }
            other => panic!("expected Fail, got {other:?}"),
        }
        // Head moved and proven to be our attempt: adopt — the decision carries the head it
        // proved, so no caller-side sentinel can ever reach the index.
        assert_eq!(
            recovery_decision("h0", "h1", "h0", AdoptionEvidence::Verified),
            RecoveryDecision::Adopt { head: "h1".into() }
        );
    }

    /// With a journaled commit-job id, the job's terminal fate decides — the head comparison
    /// never runs, so the interrupted attempt's detached job landing *after* a head check can
    /// no longer produce a wrong Fresh (which CAS-failed the retry and wedged the binding).
    #[test]
    fn recovery_decision_from_job_table() {
        // Job failed terminally: nothing published, retry fresh (CAS still guards).
        assert_eq!(
            recovery_decision_from_job(&JobFate::Failed, "h0", "h0"),
            RecoveryDecision::Fresh
        );
        // Job committed and the index already reflects it (crash between index install and
        // journal removal): drop the journal only.
        assert_eq!(
            recovery_decision_from_job(&JobFate::Committed("h1".into()), "h0", "h1"),
            RecoveryDecision::AlreadyInstalled
        );
        // Job committed and the index has not caught up: the commit is proven ours
        // (Verified evidence by construction) — adopt it as the new baseline head, carried
        // in the decision itself.
        assert_eq!(
            recovery_decision_from_job(&JobFate::Committed("h1".into()), "h0", "h0"),
            RecoveryDecision::Adopt { head: "h1".into() }
        );
    }

    /// A commit job the server no longer knows (404/expired) is NOT a wedge: recovery falls
    /// back to the plain head-comparison table with no authorship evidence — head still at
    /// the journaled base proves nothing published (Fresh); an advanced head stays
    /// fail-closed (and the caller appends the journal-file escape hatch).
    #[test]
    fn job_record_gone_falls_back_to_head_table() {
        // Terminal fates keep deciding outright.
        assert_eq!(
            recovery_decision_after_poll(
                &JobPoll::Fate(JobFate::Committed("h1".into())),
                "h0",
                "h9",
                "h0"
            ),
            RecoveryDecision::Adopt { head: "h1".into() },
            "a committed job decides by its own commit, not the workspace head"
        );
        // Record gone, head never moved: the attempt cannot have published — fresh retry.
        assert_eq!(
            recovery_decision_after_poll(&JobPoll::RecordGone, "h0", "h0", "h0"),
            RecoveryDecision::Fresh
        );
        // Record gone, head advanced: fail closed (no authorship proof).
        match recovery_decision_after_poll(&JobPoll::RecordGone, "h0", "h1", "h0") {
            RecoveryDecision::Fail(msg) => {
                assert!(msg.contains("h1") && msg.contains("h0"), "{msg}");
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// The fail-closed error must name the manual escape: the journal file path, and that
    /// deleting it discards the interrupted attempt's tracking.
    #[test]
    fn recovery_fail_error_names_the_journal_escape_hatch() {
        let dir = tempfile::tempdir().unwrap();
        let err = recovery_fail_error("head advanced", dir.path()).to_string();
        assert!(err.contains("head advanced"), "{err}");
        assert!(
            err.contains("journal.json"),
            "names the journal file: {err}"
        );
        assert!(
            err.contains("discards"),
            "says what deleting it does: {err}"
        );
    }

    /// Adoption installs the journaled candidate as baseline with untrusted fingerprints:
    /// upserts take their journaled oids with `clean_fingerprint = None` (bytes unseen since
    /// the crash), deletes leave, untouched entries keep their proofs.
    #[test]
    fn adoption_installs_untrusted_candidate() {
        let mut index = index_with(&[
            (
                "kept.txt",
                "oid-kept",
                0o100644,
                Some(fp(0o100644, 1, (1, 0))),
            ),
            ("gone.txt", "oid-gone", 0o100644, None),
            (
                "edited.txt",
                "oid-old",
                0o100644,
                Some(fp(0o100644, 2, (2, 0))),
            ),
        ]);
        let journal = Journal {
            idempotency_key: "k".into(),
            expected_head: "h0".into(),
            started_at_secs: 0,
            message: "m".into(),
            job_id: None,
            entries: vec![
                JournalEntry {
                    path: "edited.txt".into(),
                    oid: Some("oid-new".into()),
                    mode: Some(0o100755),
                    delete: false,
                    pre_fingerprint: Some(fp(0o100755, 3, (3, 0))),
                },
                JournalEntry {
                    path: "created.txt".into(),
                    oid: Some("oid-created".into()),
                    mode: Some(0o100644),
                    delete: false,
                    pre_fingerprint: None,
                },
                JournalEntry {
                    path: "gone.txt".into(),
                    oid: None,
                    mode: None,
                    delete: true,
                    pre_fingerprint: None,
                },
            ],
        };
        apply_adopted_journal(&mut index, &journal, "h1");
        assert_eq!(index.indexed_head, "h1");
        assert!(
            !index.entries.contains_key("gone.txt"),
            "journaled delete leaves"
        );
        let edited = &index.entries["edited.txt"];
        assert_eq!(edited.server_oid, "oid-new");
        assert_eq!(edited.server_mode, 0o100755);
        assert_eq!(
            edited.clean_fingerprint, None,
            "bytes unverified since the crash"
        );
        let created = &index.entries["created.txt"];
        assert_eq!(created.server_oid, "oid-created");
        assert_eq!(created.clean_fingerprint, None);
        // A raced-in new file staying in the baseline is what turns its later local
        // deletion into a server delete.
        assert!(index.entries.contains_key("created.txt"));
        let kept = &index.entries["kept.txt"];
        assert!(
            kept.clean_fingerprint.is_some(),
            "untouched entries keep their proof"
        );
    }

    #[test]
    fn journal_round_trips_and_absence_is_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_journal(dir.path()).unwrap().is_none());
        let journal = Journal {
            idempotency_key: "abcd".into(),
            expected_head: "h0".into(),
            started_at_secs: 7,
            message: "m".into(),
            job_id: None,
            entries: vec![JournalEntry {
                path: "a".into(),
                oid: Some("o".into()),
                mode: Some(0o100644),
                delete: false,
                pre_fingerprint: Some(fp(0o100644, 1, (1, 1))),
            }],
        };
        save_journal(dir.path(), &journal).unwrap();
        let loaded = load_journal(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.idempotency_key, "abcd");
        assert_eq!(loaded.entries.len(), 1);
        remove_durably(&journal_path(dir.path())).unwrap();
        assert!(load_journal(dir.path()).unwrap().is_none());
        // Corrupt journal fails closed.
        std::fs::write(journal_path(dir.path()), b"{oops").unwrap();
        let err = load_journal(dir.path()).unwrap_err().to_string();
        assert!(err.contains("journal.json"), "{err}");
    }

    // -- job-id sidecar ----------------------------------------------------------------------

    /// The commit-job id lives in the `journal.job` sidecar (written off the progress hook's
    /// thread), not in a mid-push journal rewrite: recovery composes journal + sidecar, an
    /// embedded `job_id` (older journals) still wins, removal drops both files, and saving a
    /// NEW journal clears any stale sidecar so a later attempt can never adopt a previous
    /// attempt's job.
    #[test]
    fn job_id_sidecar_read_write_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal {
            idempotency_key: "k".into(),
            expected_head: "h0".into(),
            started_at_secs: 0,
            message: "m".into(),
            job_id: None,
            entries: Vec::new(),
        };
        save_journal(dir.path(), &journal).unwrap();
        assert_eq!(journal_job_id(dir.path(), &journal), None);

        // The sidecar supplies the id when the journal lacks it.
        write_atomic(&journal_job_path(dir.path()), b"job-123").unwrap();
        assert_eq!(
            journal_job_id(dir.path(), &journal),
            Some("job-123".to_string())
        );

        // An embedded id (journal written by an older tl that rewrote mid-push) wins.
        let mut with_embedded = journal.clone();
        with_embedded.job_id = Some("job-embedded".into());
        assert_eq!(
            journal_job_id(dir.path(), &with_embedded),
            Some("job-embedded".to_string())
        );

        // Saving a fresh journal clears the stale sidecar.
        save_journal(dir.path(), &journal).unwrap();
        assert_eq!(journal_job_id(dir.path(), &journal), None);

        // Removal drops both files.
        write_atomic(&journal_job_path(dir.path()), b"job-456").unwrap();
        remove_journal_durably(dir.path()).unwrap();
        assert!(!journal_path(dir.path()).exists());
        assert!(!journal_job_path(dir.path()).exists());
    }

    // -- binding lifecycle -------------------------------------------------------------------

    fn write_binding(state_dir: &Path, workspace_id: &str, root: &Path) {
        std::fs::create_dir_all(state_dir).unwrap();
        let binding = Binding {
            project_id: "p".into(),
            organization_id: None,
            repo: "fs".into(),
            workspace_id: workspace_id.into(),
            ref_name: format!("workspaces/{workspace_id}"),
            root: root.to_path_buf(),
            created_at_secs: 0,
        };
        std::fs::write(
            state_dir.join("binding.json"),
            serde_json::to_vec_pretty(&binding).unwrap(),
        )
        .unwrap();
    }

    /// A registry entry whose state dir vanished (the debris a crash between unbind's two
    /// steps leaves, in the new state-dir-first order) reads as unbound and is pruned by the
    /// next mutation — the self-healing half of the reversed ordering.
    #[test]
    fn dangling_registry_entries_read_unbound_and_prune() {
        let state_root = tempfile::tempdir().unwrap();
        let bound = tempfile::tempdir().unwrap();
        let live_dir = state_root.path().join("ws-live");
        write_binding(&live_dir, "ws-live", bound.path());

        let mut bindings: BTreeMap<String, PathBuf> = BTreeMap::new();
        bindings.insert("/bound/live".into(), live_dir.clone());
        bindings.insert(
            "/bound/dangling".into(),
            state_root.path().join("ws-vanished"),
        );

        assert!(binding_state_live(&live_dir));
        assert!(!binding_state_live(&state_root.path().join("ws-vanished")));
        prune_dangling(&mut bindings);
        assert_eq!(
            bindings.keys().collect::<Vec<_>>(),
            vec!["/bound/live"],
            "the dangling entry pruned; the live one kept"
        );
    }

    /// `tl fs unbind <root>` must also clear an orphaned state dir the registry lost track
    /// of (the shape older crashed unbinds left): the rm guard points users at unbind, so
    /// unbind has to work on exactly what the guard sees.
    #[test]
    fn orphan_state_dir_is_found_by_root() {
        let state_root = tempfile::tempdir().unwrap();
        let bound = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        write_binding(&state_root.path().join("ws-a"), "ws-a", bound.path());
        write_binding(&state_root.path().join("ws-b"), "ws-b", other.path());

        let found = orphan_state_dir_for(bound.path().to_str().unwrap(), state_root.path())
            .expect("orphan found");
        assert_eq!(found, state_root.path().join("ws-a"));
        assert_eq!(
            orphan_state_dir_for("/not/bound/anywhere", state_root.path()),
            None
        );
    }

    /// The init TOCTOU re-check runs INSIDE the registry_mutate closure against the freshly
    /// locked registry; this drives the same pure check with a pre-populated registry (the
    /// state a racing init would have committed between preflight and lock).
    #[test]
    fn binding_overlap_recheck_catches_a_raced_registry() {
        let mut bindings: BTreeMap<String, PathBuf> = BTreeMap::new();
        assert_eq!(binding_overlap_error("/work/project", &bindings), None);

        // A racing init bound the exact root: the re-check refuses the double-bind.
        bindings.insert("/work/project".into(), PathBuf::from("/state/a"));
        let msg = binding_overlap_error("/work/project", &bindings).expect("double-bind refused");
        assert!(msg.contains("/work/project"), "{msg}");

        // Overlap in either direction refuses too.
        bindings.clear();
        bindings.insert("/work".into(), PathBuf::from("/state/b"));
        assert!(binding_overlap_error("/work/project", &bindings).is_some());
        bindings.clear();
        bindings.insert("/work/project/sub".into(), PathBuf::from("/state/c"));
        assert!(binding_overlap_error("/work/project", &bindings).is_some());

        // Disjoint roots pass.
        bindings.clear();
        bindings.insert("/elsewhere".into(), PathBuf::from("/state/d"));
        assert_eq!(binding_overlap_error("/work/project", &bindings), None);
    }

    /// `tl fs rm` asks "does a binding own this workspace?" — a corrupt binding.json must
    /// abort that question (it may be exactly the owner), never read as "unbound".
    #[test]
    fn corrupt_binding_state_fails_the_workspace_ownership_check_closed() {
        let state_root = tempfile::tempdir().unwrap();
        let bound = tempfile::tempdir().unwrap();
        write_binding(&state_root.path().join("ws-ok"), "ws-ok", bound.path());

        // Healthy scan answers both ways.
        assert_eq!(
            binding_using_workspace_in(state_root.path(), "ws-ok").unwrap(),
            Some(bound.path().to_string_lossy().into_owned())
        );
        assert_eq!(
            binding_using_workspace_in(state_root.path(), "ws-other").unwrap(),
            None
        );
        // A stray non-binding dir under the root is skipped, not fatal.
        std::fs::create_dir_all(state_root.path().join("not-a-binding")).unwrap();
        assert!(
            binding_using_workspace_in(state_root.path(), "ws-other")
                .unwrap()
                .is_none()
        );

        // Corruption fails the whole question closed, naming the file.
        let corrupt = state_root.path().join("ws-corrupt");
        std::fs::create_dir_all(&corrupt).unwrap();
        std::fs::write(corrupt.join("binding.json"), b"{oops").unwrap();
        let err = binding_using_workspace_in(state_root.path(), "ws-other")
            .unwrap_err()
            .to_string();
        assert!(err.contains("binding.json"), "names the file: {err}");
        assert!(err.contains("corrupt"), "{err}");
    }

    /// Bare `tl fs unbind` (CWD containment) must work from a symlinked spelling of the
    /// bound root: containment compares the stored AND canonicalized forms, like
    /// `mount_containing_cwd`.
    #[cfg(unix)]
    #[test]
    fn deepest_containing_matches_through_symlinked_roots() {
        let real = tempfile::tempdir().unwrap();
        let links = tempfile::tempdir().unwrap();
        let link = links.path().join("via-link");
        std::os::unix::fs::symlink(real.path(), &link).unwrap();

        // The registry stores the symlink spelling; the caller canonicalized its CWD.
        let stored = vec![link.to_string_lossy().into_owned()];
        let cwd = real.path().canonicalize().unwrap().join("subdir");
        assert_eq!(
            deepest_containing(stored.iter(), &cwd),
            Some(stored[0].clone()),
            "canonicalized CWD matches the symlink-spelled root"
        );
    }

    // -- manual e2e ------------------------------------------------------------------------

    /// Manual end-to-end of the full binding flow against a live server (same precedent as
    /// the overlay e2e tests). Steps:
    ///
    /// 1. `tl git create <fs>` on a dev server, then `tl fs init <dir> --file-system <fs>`
    ///    (with TENSORLAKE_GIT_TOKEN etc. pointed at the local server).
    /// 2. Populate <dir> (files, an executable, a symlink, a .gitignore'd build dir),
    ///    `tl fs snapshot <dir>` — verify commit contents via `tl git`/the tree API, and
    ///    that a second immediate snapshot prints "Nothing to snapshot" without a commit.
    /// 3. Edit one file, snapshot, verify only it re-hashed (timing line) and uploaded.
    /// 4. Kill the CLI between journal write and index install (breakpoint or SIGKILL in
    ///    push), re-run snapshot: head unchanged → fresh retry; head advanced → the
    ///    fail-closed recovery error.
    /// 5. `tl fs status` (dirty counts), `tl fs unbind` (workspace survives, `tl fs ls`
    ///    still lists it).
    #[test]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080; see steps in the doc comment"]
    fn plaindir_snapshot_e2e_manual() {}
}
