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
    let path = registry_path();
    match std::fs::read(&path) {
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

fn registry_save(registry: &BindingRegistry) -> Result<()> {
    std::fs::create_dir_all(crate::config::files::config_dir())?;
    write_atomic(&registry_path(), &serde_json::to_vec_pretty(registry)?)
}

/// The binding registered exactly at `path` (same exact-match semantics as mounts:
/// path-addressed commands name the root, `*_containing_cwd` handles the inside-of case).
pub fn binding_for(path: &Path) -> Result<Option<(String, PathBuf)>> {
    let root = canonical_mountpoint(path)?;
    Ok(registry_load()?
        .bindings
        .get(&root)
        .map(|state_dir| (root, state_dir.clone())))
}

/// Every bound directory, for CWD-containment resolution alongside mount roots.
pub fn binding_roots() -> Result<Vec<String>> {
    Ok(registry_load()?.bindings.keys().cloned().collect())
}

/// `(workspace id, bound directory)` for every readable binding — `tl fs ls` visibility.
/// Listing-only, so individual unreadable state dirs are skipped rather than fatal.
pub(crate) fn bound_workspaces() -> Vec<(String, String)> {
    let Ok(registry) = registry_load() else {
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

/// Refuse path overlap between a candidate root and every registered mount and binding, in
/// both directions — a mount inside a bound tree would be scanned (and its kernel volume
/// stat'd) by snapshots; a binding inside a mount would double-track the overlay's files.
fn assert_no_overlap(root: &str) -> Result<()> {
    let candidate = Path::new(root);
    let overlaps = |other: &Path| candidate.starts_with(other) || other.starts_with(candidate);
    for mountpoint in super::registry_load().keys() {
        if overlaps(Path::new(mountpoint)) {
            return Err(CliError::usage(format!(
                "{root} overlaps the tl fs mount at {mountpoint}; a directory cannot be both \
                 mounted and bound"
            )));
        }
    }
    for bound in registry_load()?.bindings.keys() {
        if overlaps(Path::new(bound)) {
            return Err(CliError::usage(format!(
                "{root} overlaps the existing binding at {bound} (see `tl fs unbind {bound}`)"
            )));
        }
    }
    Ok(())
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
/// correctness off this.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let parent = path
        .parent()
        .ok_or_else(|| CliError::usage(format!("{} has no parent directory", path.display())))?;
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| CliError::usage(format!("{} has no file name", path.display())))?;
    let tmp = parent.join(format!("{file_name}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    // Durability of the rename itself. Some platforms refuse fsync on a directory handle;
    // treat that as best-effort there (unix, the only supported target, allows it).
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
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

#[cfg(unix)]
fn acquire_lock(state_dir: &Path) -> Result<BindingLock> {
    use std::os::unix::io::AsRawFd as _;
    let path = state_dir.join("lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(CliError::usage(format!(
            "another tl fs command holds this binding's snapshot lock ({}); wait for it to \
             finish and retry",
            path.display()
        )));
    }
    Ok(BindingLock { _file: file })
}

#[cfg(not(unix))]
fn acquire_lock(_state_dir: &Path) -> Result<BindingLock> {
    Err(CliError::usage(
        "plain-directory bindings are supported on unix only in v1",
    ))
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
    entries: Vec<JournalEntry>,
}

fn journal_path(state_dir: &Path) -> PathBuf {
    state_dir.join("journal.json")
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
    write_atomic(
        &journal_path(state_dir),
        &serde_json::to_vec_pretty(journal)?,
    )
}

// ---------------------------------------------------------------------------------------------
// Recovery: what a journal found at snapshot start means, as a pure decision.
// ---------------------------------------------------------------------------------------------

/// Whether the advanced workspace head has been proven to *be* the journaled attempt.
/// Today production always passes `Unavailable`: the client has no cheap API for a commit's
/// parent, and without `parent == expected_head` a tree that merely matches the candidate is
/// not proof of authorship — so head-advanced recovery fails closed rather than guessing.
/// The `Verified` arm is the seam a richer commit-info API (phase 1.5) plugs into; the
/// adopt path it drives is fully implemented and tested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdoptionEvidence {
    Unavailable,
    /// The head's parent is the journaled `expected_head` and its tree matches the
    /// journaled candidate at every journaled path.
    // Constructed only by tests today (see the enum docs); kept so the adopt path stays
    // implemented and exercised until a commit-parent API exists to feed it in production.
    #[allow(dead_code)]
    Verified,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryDecision {
    /// The crash happened after the index was installed but before the journal was removed:
    /// the index already reflects the published commit. Drop the journal, nothing else.
    AlreadyInstalled,
    /// The journaled attempt never published (head still equals `expected_head`): drop the
    /// journal and snapshot fresh. The CAS (`expect_oid = expected_head`) still guards the
    /// case where the old attempt's detached server job races the fresh one.
    Fresh,
    /// The head is the journaled attempt: install the journaled candidate as the baseline
    /// with every `clean_fingerprint = None` (local bytes unverified since the crash).
    Adopt,
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
        AdoptionEvidence::Verified => RecoveryDecision::Adopt,
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
        .list_repos_with_credential(&session.project_id, user, token)
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
            &CreateWorkspaceRequest::default(),
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
    let mut registry = registry_load()?;
    registry.bindings.insert(root.clone(), state_dir);
    registry_save(&registry)?;
    println!(
        "Bound {root} to new workspace {} (file system {repo}).",
        short_id(&ws.id)
    );
    println!("Work in the directory, then: tl fs snapshot {root}");
    Ok(())
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
        let mut index = load_index(state_dir)?;
        // Production evidence is always Unavailable today — see `AdoptionEvidence`.
        match recovery_decision(
            &journal.expected_head,
            &head,
            &index.indexed_head,
            AdoptionEvidence::Unavailable,
        ) {
            RecoveryDecision::Fresh | RecoveryDecision::AlreadyInstalled => {
                remove_durably(&journal_path(state_dir))?;
            }
            RecoveryDecision::Adopt => {
                apply_adopted_journal(&mut index, &journal, &head);
                save_index(state_dir, &index)?;
                remove_durably(&journal_path(state_dir))?;
            }
            RecoveryDecision::Fail(msg) => {
                bar.finish_and_clear();
                return Err(CliError::usage(msg));
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
    let mut pushes: Vec<HashedUpsert> = Vec::new();
    let mut reproven: Vec<(String, Fingerprint)> = Vec::new();
    for entry in &delta.upserts {
        let hashed = hash_upsert(root_path, entry)?;
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
    let progress = crate::commands::push_progress_spinner(&bar);
    let report = session
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
        .await?
        .into_inner();
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
    let mut entries = index.entries.clone();
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
    remove_durably(&journal_path(state_dir))?;

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

/// `tl fs unbind` — remove the local binding and its state. Deliberately NOT `rm`: `tl fs rm`
/// deletes the *workspace*; unbinding only forgets this directory's link to it, and the
/// workspace (with every snapshot) survives on the server.
pub async fn unbind(path: Option<PathBuf>) -> Result<()> {
    let (root, state_dir) = match path {
        Some(path) => binding_for(&path)?.ok_or_else(|| {
            CliError::usage(format!(
                "{} is not a bound directory (see `tl fs status`, or bind one with `tl fs init`)",
                path.display()
            ))
        })?,
        None => {
            let cwd = std::env::current_dir()?;
            let root = canonical_mountpoint(&cwd)?;
            binding_for(Path::new(&root))?.ok_or_else(|| {
                CliError::usage(format!(
                    "{root} is not a bound directory; pass the bound directory explicitly"
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
    let mut registry = registry_load()?;
    registry.bindings.remove(&root);
    registry_save(&registry)?;
    std::fs::remove_dir_all(&state_dir)?;
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
        // Head moved and proven to be our attempt: adopt.
        assert_eq!(
            recovery_decision("h0", "h1", "h0", AdoptionEvidence::Verified),
            RecoveryDecision::Adopt
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
