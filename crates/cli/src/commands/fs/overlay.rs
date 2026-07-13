//! Writable overlay over a read-only [`gsvc_mount::MountCore`].
//!
//! The lower layer is the workspace's snapshot commit, served by the mount core (immutable,
//! cached, branch-following). The upper layer is a plain local directory of real files; a
//! parallel `wh/` tree of marker files records deletions of lower paths (whiteouts). The merged
//! view is what a FUSE binding exposes:
//!
//! - reads route to the upper file when one exists, else to the core;
//! - the first write to a lower file copies it up, then all IO is local;
//! - `unlink`/`rmdir` remove upper state and whiteout any lower presence;
//! - the upper tree **is** the dirty set: snapshot enumerates `upper/` + `wh/` and nothing else.
//!
//! Everything here is VFS-agnostic and directly testable without a kernel; the `fuser` glue is a
//! thin translation layer. Write operations are synchronous local filesystem work; only
//! lower-layer access is async (delegated to the core).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use gsvc_mount::{MountCore, MountError, NodeAttr, NodeKind, ROOT_INO};

/// One merged-namespace node. Path-keyed: the same repo-relative path keeps the same ino for as
/// long as the kernel references it, regardless of which layer currently backs it.
struct ONode {
    path: String,
    /// The core's ino for the lower node at this path, when one exists. Holds exactly one
    /// counted core lookup reference (released on forget). Mutable because the followed ref
    /// moves: path-level operations re-resolve a stale binding against the current commit
    /// (see [`OverlayFs::lower_binding`]), while open file handles keep the commit they
    /// opened through their own pinned core handle.
    core_ino: Mutex<Option<u64>>,
    /// The core ref-generation this binding was made at; a mismatch with the core's current
    /// generation means the followed ref advanced and the binding must be re-walked.
    bound_gen: std::sync::atomic::AtomicU64,
    /// The redirect-table generation this binding was made at (see
    /// [`OverlayFs::redirect_gen`]); a mismatch means a pending directory rename changed how
    /// this path maps onto the lower and the binding must be re-walked.
    bound_rgen: std::sync::atomic::AtomicU64,
    /// Content identity at the last `open` of this node (the lower blob oid; `None` for
    /// upper-backed opens). An open whose identity matches gets `FOPEN_KEEP_CACHE`: same oid
    /// means byte-identical content, so the kernel page cache from earlier opens is still
    /// valid — including across branch refreshes that never touched the path.
    last_open_oid: Mutex<Option<String>>,
}

impl ONode {
    fn core_ino(&self) -> Option<u64> {
        *self.core_ino.lock().expect("core ino lock")
    }

    fn bound_gen(&self) -> u64 {
        self.bound_gen.load(Ordering::SeqCst)
    }

    fn bound_rgen(&self) -> u64 {
        self.bound_rgen.load(Ordering::SeqCst)
    }
}

struct InodeTable {
    nodes: HashMap<u64, (Arc<ONode>, u64)>,
    index: HashMap<String, u64>,
    next: u64,
}

impl InodeTable {
    fn new() -> Self {
        let mut nodes = HashMap::new();
        let mut index = HashMap::new();
        nodes.insert(
            ROOT_INO,
            (
                Arc::new(ONode {
                    path: String::new(),
                    core_ino: Mutex::new(Some(ROOT_INO)),
                    bound_gen: std::sync::atomic::AtomicU64::new(0),
                    bound_rgen: std::sync::atomic::AtomicU64::new(0),
                    last_open_oid: Mutex::new(None),
                }),
                1,
            ),
        );
        index.insert(String::new(), ROOT_INO);
        InodeTable {
            nodes,
            index,
            next: ROOT_INO + 1,
        }
    }

    /// Intern a path, bumping its lookup count. `fresh_core` is a just-acquired counted core
    /// reference (or `None`): the table keeps exactly one core reference per node, so the return
    /// value is a core ino the **caller must release** — either the duplicate fresh one, or the
    /// stale one this lookup replaced after the workspace ref moved.
    fn intern(&mut self, path: String, fresh_core: Option<u64>) -> (u64, Arc<ONode>, Option<u64>) {
        if let Some(&ino) = self.index.get(&path) {
            let (node, count) = self.nodes.get_mut(&ino).expect("indexed node");
            *count += 1;
            let mut stored = node.core_ino.lock().expect("core ino lock");
            let release = match (*stored, fresh_core) {
                (Some(old), Some(fresh)) if old == fresh => Some(fresh),
                (Some(old), Some(fresh)) => {
                    *stored = Some(fresh);
                    Some(old)
                }
                (None, Some(fresh)) => {
                    *stored = Some(fresh);
                    None
                }
                _ => None,
            };
            drop(stored);
            return (ino, node.clone(), release);
        }
        let ino = self.next;
        self.next += 1;
        let node = Arc::new(ONode {
            path: path.clone(),
            core_ino: Mutex::new(fresh_core),
            bound_gen: std::sync::atomic::AtomicU64::new(0),
            bound_rgen: std::sync::atomic::AtomicU64::new(0),
            last_open_oid: Mutex::new(None),
        });
        self.nodes.insert(ino, (node.clone(), 1));
        self.index.insert(path, ino);
        (ino, node, None)
    }

    fn get(&self, ino: u64) -> Option<Arc<ONode>> {
        self.nodes.get(&ino).map(|(n, _)| n.clone())
    }

    /// Decrement and drop at zero, returning the node so the caller can release its core ref.
    fn forget(&mut self, ino: u64, nlookups: u64) -> Option<Arc<ONode>> {
        if ino == ROOT_INO {
            return None;
        }
        let (node, count) = self.nodes.get_mut(&ino)?;
        *count = count.saturating_sub(nlookups);
        if *count == 0 {
            let node = node.clone();
            self.nodes.remove(&ino);
            // Only evict the index entry if it still points here. A rename may have re-keyed
            // this path onto a different ino (overwrite: the old occupant lingers by ino until
            // forgotten while its path now resolves to the moved node) — evicting blindly would
            // strand the live entry.
            if self.index.get(&node.path).copied() == Some(ino) {
                self.index.remove(&node.path);
            }
            Some(node)
        } else {
            None
        }
    }

    /// Re-key interned nodes after a successful upper rename `src` -> `dst`.
    ///
    /// A FUSE `rename` keeps the source's nodeid and re-points its dentry at the destination,
    /// then issues ino-based ops (`getattr`, `open`, `read`) against it. This table is path-keyed,
    /// so unless the moved node's path follows, resolution walks the now-vacated source path and
    /// returns `ENOENT` — the file lists in `readdir` (a fresh directory read) yet stats as gone.
    /// (That is exactly what breaks `git init`/`git clone`, which write `HEAD`, `config`, refs and
    /// every lockfile via write-temp-then-rename.)
    ///
    /// Handles subtree moves — a renamed directory carries its interned descendants — and
    /// overwrite: a node already at `dst` is dropped from the index (it survives by ino until the
    /// kernel forgets it, guarded by the path-conditional eviction in [`Self::forget`]). The moved
    /// node's lower binding is dropped and re-resolved lazily against the destination path; any
    /// counted core reference it held is returned for the caller to release.
    #[must_use = "returned core inos must be released via MountCore::forget"]
    fn rename(&mut self, src: &str, dst: &str) -> Vec<u64> {
        let prefix = format!("{src}/");
        let affected: Vec<String> = self
            .index
            .keys()
            .filter(|p| p.as_str() == src || p.starts_with(&prefix))
            .cloned()
            .collect();
        let mut released = Vec::new();
        for old in affected {
            let ino = self.index.remove(&old).expect("just enumerated from index");
            let new = if old == src {
                dst.to_string()
            } else {
                format!("{dst}/{}", &old[prefix.len()..])
            };
            let (node, count) = self.nodes.remove(&ino).expect("indexed node");
            if let Some(core) = *node.core_ino.lock().expect("core ino lock") {
                released.push(core);
            }
            let renamed = Arc::new(ONode {
                path: new.clone(),
                core_ino: Mutex::new(None),
                bound_gen: AtomicU64::new(0),
                bound_rgen: AtomicU64::new(0),
                last_open_oid: Mutex::new(node.last_open_oid.lock().expect("oid lock").clone()),
            });
            self.index.insert(new, ino);
            self.nodes.insert(ino, (renamed, count));
        }
        released
    }
}

/// The true-lower path serving `path` under a pending-rename table: the longest matching
/// destination prefix rewritten to its recorded source. Entries store true-lower coordinates
/// (composed at insert), so one hop resolves chains.
fn remap_through_redirects(redirects: &HashMap<String, String>, path: &str) -> String {
    if redirects.is_empty() {
        return path.to_string();
    }
    let mut probe = path;
    loop {
        if let Some(src) = redirects.get(probe) {
            return format!("{src}{}", &path[probe.len()..]);
        }
        match probe.rfind('/') {
            Some(i) => probe = &probe[..i],
            None => return path.to_string(),
        }
    }
}

/// Record a committed-directory move `src` -> `dst` in the pending-rename table. `true_src`
/// is `src` already resolved to true-lower coordinates. The destination is replaced
/// wholesale (a pending rename previously rooted there dies), pending renames nested inside
/// the moved subtree follow their new ancestor name (their recorded sources are already in
/// true-lower coordinates and don't change), and a source that is itself pending re-keys
/// instead of chaining.
fn record_committed_rename(
    redirects: &mut HashMap<String, String>,
    src: &str,
    dst: &str,
    true_src: String,
) {
    let dst_prefix = format!("{dst}/");
    redirects.retain(|k, _| k != dst && !k.starts_with(&dst_prefix));
    let src_prefix = format!("{src}/");
    let nested: Vec<String> = redirects
        .keys()
        .filter(|k| k.starts_with(&src_prefix))
        .cloned()
        .collect();
    for key in nested {
        let value = redirects.remove(&key).expect("just enumerated");
        redirects.insert(format!("{dst}/{}", &key[src_prefix.len()..]), value);
    }
    redirects.remove(src);
    redirects.insert(dst.to_string(), true_src);
}

/// Whether `path` is hidden by a whiteout marker under `wh_root`, honoring pending-rename
/// shielding. Markers are files; a directory at a wh path is only the container for child
/// markers (wh/dir/b.txt marks dir/b.txt, not dir). A marker on an ancestor whiteouts the
/// whole subtree — descendants must test as whited out too, or a node cached by inode keeps
/// serving content from under a deleted directory.
///
/// A pending rename recorded *under* a marker shields its subtree: dropping redirects when
/// their destination is removed (rmdir/overwrite) means a live entry below an ancestor
/// marker can only postdate it — the destination was created inside a deleted-then-recreated
/// directory, and its content must show. Deeper markers (deletions inside the renamed tree)
/// still apply on the rest of the walk.
fn whited_out_under(wh_root: &Path, redirects: &HashMap<String, String>, path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    let mut probe = String::with_capacity(path.len());
    for component in path.split('/') {
        if !probe.is_empty() {
            probe.push('/');
        }
        probe.push_str(component);
        let marker_is_file = wh_root
            .join(&probe)
            .symlink_metadata()
            .map(|m| m.is_file())
            .unwrap_or(false);
        if marker_is_file {
            let shielded = redirects.keys().any(|root| {
                (path == root.as_str() || path.starts_with(&format!("{root}/")))
                    && root.starts_with(&format!("{probe}/"))
            });
            if !shielded {
                return true;
            }
        }
    }
    false
}

/// An open handle in the merged namespace.
enum OHandle {
    /// Backed by a real upper file (reads and writes are positional on this descriptor). The
    /// path is carried so writes can feed the dirty index — a write only knows its handle.
    Upper { file: std::fs::File, path: String },
    /// Backed by the read-only core.
    Lower { core_fh: u64 },
    /// A merged directory listing, fixed at opendir time.
    Dir {
        /// The directory's own overlay ino, so `readdir_plus` can resolve entries through the
        /// counted lookup path. Only read by the Linux `readdir_plus` path.
        #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
        ino: u64,
        entries: Vec<(String, NodeKind)>,
    },
}

/// Attributes of a merged node, plus which layer answered.
#[derive(Clone, Debug)]
pub struct OverlayAttr {
    pub ino: u64,
    pub kind: NodeKind,
    pub size: u64,
    pub perm: u16,
    /// True when the upper layer backs this node (i.e. it is locally dirty).
    // Serialized into the macOS vfsserver wire protocol and asserted in tests; the Linux FUSE
    // path doesn't read it, so allow it to be unread off macOS rather than warn.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub upper: bool,
    /// Content timestamp. Upper-backed nodes report the real file mtime; lower-backed nodes
    /// report when this mount first saw their pinned commit (lower content can only change
    /// with the commit). The kernel's cache revalidation compares this across getattrs, so it
    /// must change exactly when content can have changed — and not otherwise.
    pub mtime: std::time::SystemTime,
}

#[derive(Clone, Debug)]
pub struct OverlayDirEntry {
    pub next_offset: u64,
    pub name: String,
    pub kind: NodeKind,
}

pub struct OverlayFs {
    core: Arc<MountCore>,
    upper: PathBuf,
    wh: PathBuf,
    /// Shared-ro mode: reject every mutation with [`MountError::ReadOnly`].
    read_only: bool,
    inodes: Mutex<InodeTable>,
    handles: Mutex<HashMap<u64, OHandle>>,
    next_fh: AtomicU64,
    /// When each lower commit was first served by this mount — the stable mtime for its nodes.
    lower_times: Mutex<HashMap<String, std::time::SystemTime>>,
    /// Global mutation clock: bumped by every recorded namespace/content change. The dirty
    /// index below is keyed to it, so "anything new since generation G?" is one atomic load.
    write_gen: AtomicU64,
    /// Event-driven dirty index: `path -> (generation, kind of last mutation)`. Every mutating
    /// op records here as it happens, so the auto-commit sealer never walks the upper tree —
    /// it asks [`OverlayFs::dirty_since`] for the exact delta. Rebuilt from the on-disk overlay
    /// at startup (and after out-of-band upper refills — restore) and pruned after each seal.
    dirty: Mutex<HashMap<String, DirtyEntry>>,
    /// Out-of-band mutation epoch: see [`OverlayFs::epoch`].
    epoch: AtomicU64,
    /// Monotonic clock origin for the mutation timestamps below (process-local millis).
    started: std::time::Instant,
    /// When the current dirty batch began / when the overlay was last mutated, in
    /// [`OverlayFs::clock_ms`] millis; `0` = clean. Stamped by every write hook (content,
    /// namespace, committed-dir renames), which makes the autosave quiet test a pair of
    /// atomic loads instead of resolving the dirty set and statting files every tick — and
    /// strictly more accurate: deletes and renames have no mtime to consult, and same-second
    /// writes hide inside filesystem timestamp granularity.
    first_dirty_ms: AtomicU64,
    last_mutation_ms: AtomicU64,
    /// Published blob oid per retained upper path, recorded after each seal from the push
    /// report. A branch refresh compares these against the delta's new oids: equal means the
    /// change is this mount's own save echoing back (the retained copy stays — it IS the
    /// byte cache), different means another session moved the path and the retained copy
    /// must stop shadowing the lower. Unknown (never recorded, or the push skipped hashing
    /// on the stable-prefix fast path) reads as divergent — dropping costs a refetch,
    /// keeping costs serving stale bytes. `None` values mark retained paths whose oid is
    /// unknown (seeded from the persisted seal index after a restart, or stable-prefix
    /// fast-path files that never hashed) — divergent by default. Only paths in this map
    /// are ever nominated for eviction: an upper file without a seal record is dirty or
    /// ignored content the trim must never touch.
    sealed_oids: Mutex<HashMap<String, Option<String>>>,
    /// Divergent retained paths a trim could not drop yet (open handle, contended fence).
    /// Retried on every subsequent refresh absorption.
    divergent_pending: Mutex<std::collections::HashSet<String>>,
    /// The lower commit the CURRENT dirty batch's edits were made against: stamped when the
    /// dirty set goes empty→nonempty (under the dirty lock), cleared only when a seal leaves
    /// the set empty. This — not the lower at seal time — is the merge base a seal declares:
    /// the lower can advance during the quiet window between a write and its autosave, and a
    /// base captured at seal time would claim the writer saw content that landed AFTER their
    /// edit, turning a genuine concurrent write into a silent overwrite. A too-old base costs
    /// at worst a spurious conflict (both sides kept, visibly); a too-new base loses a write
    /// invisibly — always err old.
    dirty_base: Mutex<Option<String>>,
    /// The trim fence. Mutating entry points hold it shared for their whole span (copy-up
    /// through dirty-index record); [`OverlayFs::trim_retained`] holds it exclusively while it
    /// drops sealed upper files. Without it, a trim could unlink an upper file between a
    /// writer's copy-up and its open — the writer would then feed an unlinked inode whose
    /// bytes no seal can ever read. Entry points take it exactly once and no private helper
    /// re-acquires it (tokio's RwLock is write-preferring: a nested read behind a queued
    /// writer deadlocks).
    mutate: tokio::sync::RwLock<()>,
    /// Pending committed-directory renames: merged-namespace destination path -> lower source
    /// path in **true lower coordinates** (composed through existing entries at insert, so
    /// resolution is always one hop). A `rename(2)` of a lower-backed directory records here
    /// instead of materializing the subtree; reads under the destination resolve through the
    /// remap, and the seal reconciles each entry into by-oid upserts plus the source delete.
    /// Persisted to `<state dir>/redirects.json` alongside `upper/` and `wh/`.
    redirects: Mutex<HashMap<String, String>>,
    redirects_path: PathBuf,
    /// Bumped on every redirect-table mutation. Lower bindings record it
    /// ([`ONode::bound_rgen`]) so cached path walks re-resolve when the remap changes.
    redirect_gen: AtomicU64,
}

/// What the last recorded mutation of a path was. `Upsert` covers create/write/copy-up/rename
/// destinations; `Delete` covers unlink/rmdir/rename sources. Last event wins — the sealer
/// re-resolves against the on-disk overlay anyway, the kind is only a routing hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirtyKind {
    Upsert,
    Delete,
}

struct DirtyEntry {
    generation: u64,
    kind: DirtyKind,
    /// Lowest byte offset any write touched over this entry's lifetime: bytes below it are
    /// unchanged since the entry was born (i.e. since the last sealed-and-pruned state), which
    /// is what lets a sealer reuse a previous push's chunk list for the untouched prefix.
    /// Structural mutations (create, truncate, rename, mode) pin it to 0 — nothing stable.
    min_write_offset: u64,
}

/// Everything that changed after a given generation, plus the clock value the snapshot was
/// taken at. Sealing through `watermark` and pruning to it leaves exactly the mutations that
/// raced the seal pending for the next tick. Upserts carry their entry's
/// [`min write offset`](DirtyEntry::min_write_offset).
pub struct DirtyDelta {
    pub upserts: Vec<(String, u64)>,
    pub deletes: Vec<String>,
    pub watermark: u64,
}

impl DirtyDelta {
    pub fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.deletes.is_empty()
    }
}

fn not_found() -> MountError {
    MountError::NotFound("no such file or directory".to_string())
}

/// How long lower reads poll out `IndexNotReady` before surfacing it. Right after a snapshot
/// the workspace ref moves to a commit whose derived index is still materializing server-side;
/// surfacing EAGAIN breaks tools that don't retry (`ls` aborts mid-listing with `fts_read`).
/// Bounded, so a genuinely broken index still errors.
const INDEX_SETTLE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

macro_rules! settle_lower {
    ($expr:expr) => {{
        let deadline = std::time::Instant::now() + INDEX_SETTLE_DEADLINE;
        loop {
            match $expr {
                Err(MountError::IndexNotReady(_)) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                other => break other,
            }
        }
    }};
}

fn io_err(e: std::io::Error) -> MountError {
    MountError::Protocol(format!("overlay io: {e}"))
}

impl OverlayFs {
    pub fn new(
        core: Arc<MountCore>,
        state_dir: &Path,
        read_only: bool,
    ) -> Result<Arc<OverlayFs>, MountError> {
        let upper = state_dir.join("upper");
        let wh = state_dir.join("wh");
        std::fs::create_dir_all(&upper).map_err(io_err)?;
        std::fs::create_dir_all(&wh).map_err(io_err)?;
        let redirects_path = state_dir.join("redirects.json");
        // Pending renames from a previous daemon (re-mount, crash) still gate the merged
        // namespace; an unreadable file is corrupt state worth failing loudly on.
        let redirects: HashMap<String, String> = match std::fs::read(&redirects_path) {
            Ok(raw) => serde_json::from_slice(&raw)
                .map_err(|e| MountError::Protocol(format!("corrupt redirects.json: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(io_err(e)),
        };
        let fs = Arc::new(OverlayFs {
            core,
            upper,
            wh,
            read_only,
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            lower_times: Mutex::new(HashMap::new()),
            write_gen: AtomicU64::new(0),
            dirty: Mutex::new(HashMap::new()),
            sealed_oids: Mutex::new(HashMap::new()),
            divergent_pending: Mutex::new(std::collections::HashSet::new()),
            dirty_base: Mutex::new(None),
            epoch: AtomicU64::new(0),
            started: std::time::Instant::now(),
            first_dirty_ms: AtomicU64::new(0),
            last_mutation_ms: AtomicU64::new(0),
            mutate: tokio::sync::RwLock::new(()),
            redirects: Mutex::new(redirects),
            redirects_path,
            redirect_gen: AtomicU64::new(0),
        });
        // Baseline: dirt left by a previous daemon (re-mount, crash) predates any events this
        // process will see; seed the index from disk so the first seal covers it.
        fs.rebuild_dirty_index()?;
        Ok(fs)
    }

    // -------------------------------------------------------------------------------------
    // Dirty tracking: the event feed behind auto-commit. Every mutating op below records the
    // path it touched, so sealing never scans — and an idle tick is one atomic load.
    // -------------------------------------------------------------------------------------

    fn record(&self, path: &str, kind: DirtyKind) {
        self.record_at(path, kind, 0);
    }

    /// A content write at `offset`: like [`OverlayFs::record`], but bytes below the offset are
    /// left claimable as stable.
    fn record_write(&self, path: &str, offset: u64) {
        self.record_at(path, DirtyKind::Upsert, offset);
    }

    fn record_at(&self, path: &str, kind: DirtyKind, offset: u64) {
        let mut dirty = self.dirty.lock().expect("dirty lock");
        if dirty.is_empty() {
            // First write of a new batch: remember which view these edits are against.
            // Stamped under the dirty lock so a sealer that empties the set and clears the
            // base can never interleave mid-transition.
            *self.dirty_base.lock().expect("dirty base lock") = Some(self.core.current_commit());
        }
        // The clock bump happens UNDER the map lock: a sealer that loaded watermark W is then
        // guaranteed to either see this entry (we inserted before it acquired the lock) or to
        // have loaded W < generation (we bumped after its load) — a generation can never be
        // both covered by a watermark and invisible to that watermark's delta, which is what
        // made a racing write silently unsealable forever.
        let generation = self.write_gen.fetch_add(1, Ordering::SeqCst) + 1;
        self.stamp_mutation();
        match dirty.get_mut(path) {
            Some(entry) => {
                entry.generation = generation;
                // A kind flip (delete then re-create, or vice versa) restarts content
                // identity: nothing before this event is stable.
                if entry.kind != kind {
                    entry.min_write_offset = 0;
                }
                entry.kind = kind;
                entry.min_write_offset = entry.min_write_offset.min(offset);
            }
            None => {
                dirty.insert(
                    path.to_string(),
                    DirtyEntry {
                        generation,
                        kind,
                        min_write_offset: offset,
                    },
                );
            }
        }
    }

    /// Every path mutated after `since`, split by the kind of its last mutation. The watermark
    /// is the clock at snapshot time: a caller that seals this delta records the watermark and
    /// prunes to it — mutations racing the seal carry higher generations and stay pending.
    pub fn dirty_since(&self, since: u64) -> DirtyDelta {
        let watermark = self.write_gen.load(Ordering::SeqCst);
        // The idle fast path: nothing was recorded since the caller's last seal, so don't
        // lock or scan (with failing pushes the map can hold a large unsealed backlog).
        if watermark == since {
            return DirtyDelta {
                upserts: Vec::new(),
                deletes: Vec::new(),
                watermark,
            };
        }
        let dirty = self.dirty.lock().expect("dirty lock");
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        for (path, entry) in dirty.iter() {
            if entry.generation <= since {
                continue;
            }
            match entry.kind {
                DirtyKind::Upsert => upserts.push((path.clone(), entry.min_write_offset)),
                DirtyKind::Delete => deletes.push(path.clone()),
            }
        }
        upserts.sort();
        deletes.sort();
        DirtyDelta {
            upserts,
            deletes,
            watermark,
        }
    }

    /// Drop index entries sealed through `upto` so the map holds only unsealed dirt.
    pub fn prune_dirty(&self, upto: u64) {
        self.dirty
            .lock()
            .expect("dirty lock")
            .retain(|_, entry| entry.generation > upto);
    }

    /// The mutation clock's current value — the generation ceiling for
    /// [`OverlayFs::absolve_clean`].
    pub fn current_generation(&self) -> u64 {
        self.write_gen.load(Ordering::SeqCst)
    }

    /// Drop dirty entries whose on-disk state a persisted seal record has vouched for —
    /// startup/reindex reconciliation, after [`OverlayFs::rebuild_dirty_index`] pessimistically
    /// marked every upper file an upsert and every whiteout a delete. Only entries whose kind
    /// matches are absolved: a path the sealed index calls an upsert but the rebuild calls a
    /// delete (or vice versa) changed shape while the daemon was down and stays dirty.
    ///
    /// `upto` is the caller's clock snapshot from BEFORE it stat-matched the paths: an entry
    /// with a newer generation was mutated by live kernel traffic after that snapshot, so the
    /// stat match proves nothing about it and it stays dirty (reindex reconciliation runs on a
    /// live mount).
    pub fn absolve_clean(&self, upserts: &[String], deletes: &[String], upto: u64) {
        let mut dirty = self.dirty.lock().expect("dirty lock");
        for path in upserts {
            if dirty
                .get(path)
                .is_some_and(|e| e.kind == DirtyKind::Upsert && e.generation <= upto)
            {
                dirty.remove(path);
            }
        }
        for path in deletes {
            if dirty
                .get(path)
                .is_some_and(|e| e.kind == DirtyKind::Delete && e.generation <= upto)
            {
                dirty.remove(path);
            }
        }
    }

    /// The path's current lowest written offset, if it is dirty. A sealer that built a stable
    /// prefix from an earlier snapshot of this value re-checks it here just before pushing: a
    /// racing write below the prefix invalidates the claim.
    pub fn min_write_offset(&self, path: &str) -> Option<u64> {
        self.dirty
            .lock()
            .expect("dirty lock")
            .get(path)
            .map(|entry| entry.min_write_offset)
    }

    /// The out-of-band mutation epoch: bumped whenever something other than kernel ops rewrote
    /// the overlay's state wholesale (`clear_upper`, `rebuild_dirty_index`). Sealer-side caches
    /// (chunk lists, recently-sealed guards, in-flight resolutions) describe the previous
    /// epoch's world and must be discarded when this moves.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Rebuild the dirty index from the on-disk overlay: every upper file/symlink is an
    /// upsert, every whiteout marker a delete. For dirt this process did not witness as
    /// events — startup, and `tl fs restore` refilling the upper out-of-band (the CLI writes
    /// straight into the state dir and then asks for a reindex).
    pub fn rebuild_dirty_index(&self) -> Result<(), MountError> {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.first_dirty_ms.store(0, Ordering::SeqCst);
        self.last_mutation_ms.store(0, Ordering::SeqCst);
        self.sealed_oids.lock().expect("sealed oid lock").clear();
        self.divergent_pending
            .lock()
            .expect("divergence lock")
            .clear();
        fn walk(root: &Path, dir: &Path, out: &mut dyn FnMut(String)) -> std::io::Result<()> {
            let Ok(read) = std::fs::read_dir(dir) else {
                return Ok(());
            };
            for entry in read.flatten() {
                let abs = entry.path();
                let meta = std::fs::symlink_metadata(&abs)?;
                if meta.is_dir() && !meta.file_type().is_symlink() {
                    walk(root, &abs, out)?;
                } else {
                    let rel = abs
                        .strip_prefix(root)
                        .expect("under root")
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    out(rel);
                }
            }
            Ok(())
        }
        self.dirty.lock().expect("dirty lock").clear();
        walk(&self.upper, &self.upper, &mut |rel| {
            self.record(&rel, DirtyKind::Upsert)
        })
        .map_err(io_err)?;
        walk(&self.wh, &self.wh, &mut |rel| {
            self.record(&rel, DirtyKind::Delete)
        })
        .map_err(io_err)?;
        Ok(())
    }

    /// Kernel invalidations for paths whose merged view changed without a kernel-visible
    /// operation (the sealer writing a whiteout for a vanished-but-sealed path). Paths the
    /// kernel never interned need nothing.
    pub(super) fn invals_for(&self, paths: &[String]) -> Vec<OverlayInval> {
        let inodes = self.inodes.lock().expect("inode lock");
        paths
            .iter()
            .filter_map(|path| {
                let &ino = inodes.index.get(path)?;
                let (parent_ino, name) = match path.rfind('/') {
                    Some(i) => (
                        inodes.index.get(&path[..i]).copied(),
                        path[i + 1..].to_string(),
                    ),
                    None => (Some(ROOT_INO), path.clone()),
                };
                Some(OverlayInval {
                    ino,
                    parent_ino,
                    name,
                    staled: true,
                })
            })
            .collect()
    }

    /// Every mutating entry point funnels through this guard: a shared-ro mount is a window
    /// onto a branch, and the kernel answer for any write attempt is `EROFS`.
    fn write_guard(&self) -> Result<(), MountError> {
        if self.read_only {
            return Err(MountError::ReadOnly);
        }
        Ok(())
    }

    fn upper_path(&self, path: &str) -> PathBuf {
        if path.is_empty() {
            self.upper.clone()
        } else {
            self.upper.join(path)
        }
    }

    fn wh_path(&self, path: &str) -> PathBuf {
        self.wh.join(path)
    }

    fn whited_out(&self, path: &str) -> bool {
        whited_out_under(
            &self.wh,
            &self.redirects.lock().expect("redirect lock"),
            path,
        )
    }

    // -------------------------------------------------------------------------------------
    // Pending directory renames (redirects)
    // -------------------------------------------------------------------------------------

    /// The true-lower path serving `path`: the longest pending-rename prefix rewritten to its
    /// recorded source (entries store true-lower coordinates, so one hop resolves chains).
    fn remap_lower(&self, path: &str) -> String {
        remap_through_redirects(&self.redirects.lock().expect("redirect lock"), path)
    }

    /// The recorded source when `path` is itself a pending-rename destination root.
    fn redirect_source(&self, path: &str) -> Option<String> {
        self.redirects
            .lock()
            .expect("redirect lock")
            .get(path)
            .cloned()
    }

    /// Whether `path` is a pending-rename destination root or sits under one.
    fn redirect_covers(&self, path: &str) -> bool {
        let redirects = self.redirects.lock().expect("redirect lock");
        redirects
            .keys()
            .any(|root| path == root.as_str() || path.starts_with(&format!("{root}/")))
    }

    /// Child names of pending-rename destination roots directly inside `dir`.
    fn redirect_roots_under(&self, dir: &str) -> Vec<String> {
        let redirects = self.redirects.lock().expect("redirect lock");
        redirects
            .keys()
            .filter_map(|root| {
                let rest = if dir.is_empty() {
                    root.as_str()
                } else {
                    root.strip_prefix(&format!("{dir}/"))?
                };
                (!rest.is_empty() && !rest.contains('/')).then(|| rest.to_string())
            })
            .collect()
    }

    /// Write the table through to `redirects.json` (tmp + rename: a crash never leaves a
    /// torn file — pending renames gate the merged namespace and the seal).
    fn persist_redirects(&self, redirects: &HashMap<String, String>) -> Result<(), MountError> {
        let tmp = self.redirects_path.with_extension("json.tmp");
        std::fs::write(
            &tmp,
            serde_json::to_vec_pretty(redirects)
                .map_err(|e| MountError::Protocol(format!("encode redirects: {e}")))?,
        )
        .map_err(io_err)?;
        std::fs::rename(&tmp, &self.redirects_path).map_err(io_err)?;
        Ok(())
    }

    fn upper_meta(&self, path: &str) -> Option<std::fs::Metadata> {
        self.upper_path(path).symlink_metadata().ok()
    }

    fn attr_from_meta(&self, ino: u64, meta: &std::fs::Metadata) -> OverlayAttr {
        use std::os::unix::fs::PermissionsExt;
        let kind = if meta.file_type().is_symlink() {
            NodeKind::Symlink
        } else if meta.is_dir() {
            NodeKind::Dir
        } else {
            NodeKind::File
        };
        OverlayAttr {
            ino,
            kind,
            size: meta.len(),
            perm: (meta.permissions().mode() & 0o777) as u16,
            upper: true,
            mtime: meta.modified().unwrap_or(std::time::UNIX_EPOCH),
        }
    }

    fn attr_from_core(&self, ino: u64, attr: &NodeAttr) -> OverlayAttr {
        OverlayAttr {
            ino,
            kind: attr.kind,
            size: attr.size,
            perm: attr.perm,
            upper: false,
            mtime: self.lower_mtime(&attr.commit),
        }
    }

    /// Stable mtime for lower-backed nodes: the wall time this mount first served their
    /// pinned commit. Advancing the lower (snapshot, ref follow) therefore moves every lower
    /// node's mtime forward exactly once, which is what kernel cache revalidation needs.
    fn lower_mtime(&self, commit: &str) -> std::time::SystemTime {
        let mut times = self.lower_times.lock().expect("lower times lock");
        *times
            .entry(commit.to_string())
            .or_insert_with(std::time::SystemTime::now)
    }

    fn node(&self, ino: u64) -> Result<Arc<ONode>, MountError> {
        self.inodes
            .lock()
            .expect("inode lock")
            .get(ino)
            .ok_or_else(not_found)
    }

    fn child_path(parent: &str, name: &str) -> String {
        if parent.is_empty() {
            name.to_string()
        } else {
            format!("{parent}/{name}")
        }
    }

    // -------------------------------------------------------------------------------------
    // Read side
    // -------------------------------------------------------------------------------------

    pub async fn lookup(&self, parent: u64, name: &str) -> Result<OverlayAttr, MountError> {
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);

        // Upper wins; a whiteout without upper presence is a hard miss.
        if let Some(meta) = self.upper_meta(&path) {
            let (ino, _, release) = self
                .inodes
                .lock()
                .expect("inode lock")
                .intern(path.clone(), None);
            debug_assert!(release.is_none());
            return Ok(self.attr_from_meta(ino, &meta));
        }
        // A pending rename serves its destination root here: the name exists in neither the
        // upper nor the parent's lower listing — resolve through the remapped walk.
        if self.redirect_source(&path).is_some() {
            let attr = self.walk_lower(&path).await?;
            let (ino, release) = {
                let mut inodes = self.inodes.lock().expect("inode lock");
                let (ino, _, release) = inodes.intern(path, Some(attr.ino));
                (ino, release)
            };
            if let Some(stale) = release {
                self.core.forget(stale, 1);
            }
            return Ok(self.attr_from_core(ino, &attr));
        }
        if self.whited_out(&path) {
            return Err(not_found());
        }
        let Ok(parent_core) = self.lower_dir_binding(&parent_node).await else {
            return Err(not_found());
        };
        // Always a fresh core lookup: after a snapshot the workspace ref moved, and the node's
        // lower backing must follow it (intern swaps and hands back the stale reference).
        let attr = settle_lower!(self.core.lookup(parent_core, name).await)?;
        let (ino, release) = {
            let mut inodes = self.inodes.lock().expect("inode lock");
            let (ino, _, release) = inodes.intern(path, Some(attr.ino));
            (ino, release)
        };
        if let Some(stale) = release {
            self.core.forget(stale, 1);
        }
        Ok(self.attr_from_core(ino, &attr))
    }

    /// The node's lower backing, resolving it lazily when absent. A node born in the upper
    /// (create/mkdir) has no core binding; once a snapshot seals it the advanced lower serves
    /// the same path, and the kernel keeps using the cached inode — ino-based ops must follow
    /// the path into the lower rather than report the node gone.
    async fn lower_binding(&self, node: &ONode) -> Result<u64, MountError> {
        // The core's root always tracks the followed ref internally.
        if node.path.is_empty() {
            return Ok(ROOT_INO);
        }
        // Capture the generations BEFORE resolving: an advance racing the walk just means one
        // redundant re-walk later, never a stale binding recorded as fresh.
        let generation = self.core.current_generation();
        let rgeneration = self.redirect_gen.load(Ordering::SeqCst);
        if let Some(ino) = node.core_ino()
            && node.bound_gen() == generation
            && node.bound_rgen() == rgeneration
        {
            return Ok(ino);
        }
        let attr = self.walk_lower(&node.path).await?;
        let leaf = attr.ino;
        let mut stored = node.core_ino.lock().expect("core ino lock");
        let old = stored.replace(leaf);
        node.bound_gen.store(generation, Ordering::SeqCst);
        node.bound_rgen.store(rgeneration, Ordering::SeqCst);
        drop(stored);
        if let Some(old) = old {
            if old != leaf {
                self.core.forget(old, 1);
            } else {
                // Same core node re-resolved: repay the duplicate reference.
                self.core.forget(leaf, 1);
            }
        }
        Ok(leaf)
    }

    /// A node's lower binding FOR CHILD RESOLUTION: only a lower directory can have lower
    /// children. An upper directory shadowing a lower non-directory — a concurrent
    /// dir-vs-file collision (mkdir raced another session's file landing), or the window
    /// between `rm file; mkdir file` and its seal — resolves as "no lower children"
    /// instead of probing a file as a tree, which reads as `IndexNotReady` in the core and
    /// spins the settle loop for its full deadline (the silently-stalled seals).
    async fn lower_dir_binding(&self, node: &ONode) -> Result<u64, MountError> {
        // The reverse shadow: an upper NON-directory at this name hides the lower entirely,
        // so a lower directory gained by refresh underneath it must not leak children.
        if let Some(meta) = self.upper_meta(&node.path)
            && !(meta.is_dir() && !meta.file_type().is_symlink())
        {
            return Err(not_found());
        }
        let ino = self.lower_binding(node).await?;
        match self.core.getattr(ino) {
            Ok(attr) if attr.kind == NodeKind::Dir => Ok(ino),
            _ => Err(not_found()),
        }
    }

    /// Walk a merged path component-by-component through the core, following any pending
    /// rename remap. Returns the leaf's attributes holding **one counted core reference**
    /// (on `attr.ino`) that the caller must own or repay.
    async fn walk_lower(&self, merged_path: &str) -> Result<NodeAttr, MountError> {
        let lower = self.remap_lower(merged_path);
        self.walk_true_lower(&lower).await
    }

    /// [`Self::walk_lower`] without the pending-rename remap: resolve a path exactly as the
    /// current lower commit spells it.
    async fn walk_true_lower(&self, lower: &str) -> Result<NodeAttr, MountError> {
        let mut cur = ROOT_INO;
        let mut chain: Vec<NodeAttr> = Vec::new();
        for component in lower.split('/') {
            // A non-directory intermediate means the path does not exist as spelled —
            // answer that directly instead of asking the core to list a file as a tree
            // (which reads as IndexNotReady and spins the settle loop).
            if let Some(parent) = chain.last()
                && parent.kind != NodeKind::Dir
            {
                for attr in chain {
                    self.core.forget(attr.ino, 1);
                }
                return Err(not_found());
            }
            match settle_lower!(self.core.lookup(cur, component).await) {
                Ok(attr) => {
                    cur = attr.ino;
                    chain.push(attr);
                }
                Err(e) => {
                    for attr in chain {
                        self.core.forget(attr.ino, 1);
                    }
                    return Err(e);
                }
            }
        }
        // Keep the reference on the leaf; the intermediate lookups are repaid immediately.
        let leaf = chain.pop().expect("path is never empty here");
        for attr in chain {
            self.core.forget(attr.ino, 1);
        }
        Ok(leaf)
    }

    pub async fn getattr(&self, ino: u64) -> Result<OverlayAttr, MountError> {
        let node = self.node(ino)?;
        if let Some(meta) = self.upper_meta(&node.path) {
            return Ok(self.attr_from_meta(ino, &meta));
        }
        if self.whited_out(&node.path) {
            return Err(not_found());
        }
        let core_ino = self.lower_binding(&node).await?;
        let attr = self.core.getattr(core_ino)?;
        Ok(self.attr_from_core(ino, &attr))
    }

    pub fn forget(&self, ino: u64, nlookups: u64) {
        let dropped = self
            .inodes
            .lock()
            .expect("inode lock")
            .forget(ino, nlookups);
        if let Some(node) = dropped
            && let Some(core_ino) = node.core_ino()
        {
            self.core.forget(core_ino, 1);
        }
    }

    pub async fn readlink(&self, ino: u64) -> Result<Bytes, MountError> {
        let node = self.node(ino)?;
        if self.upper_meta(&node.path).is_some() {
            let target = std::fs::read_link(self.upper_path(&node.path)).map_err(io_err)?;
            return Ok(Bytes::from(
                target.to_string_lossy().into_owned().into_bytes(),
            ));
        }
        if self.whited_out(&node.path) {
            return Err(not_found());
        }
        let core_ino = self.lower_binding(&node).await?;
        settle_lower!(self.core.readlink(core_ino).await)
    }

    /// Open a directory handle over the merged listing: lower entries (paged to exhaustion via
    /// the core) merged with upper entries, minus whiteouts, upper shadowing lower duplicates.
    pub async fn opendir(&self, ino: u64) -> Result<u64, MountError> {
        let node = self.node(ino)?;
        let mut merged: Vec<(String, NodeKind)> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

        let upper_dir = self.upper_path(&node.path);
        if let Ok(read) = std::fs::read_dir(&upper_dir) {
            for entry in read.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let Ok(meta) = entry
                    .metadata()
                    .or_else(|_| entry.path().symlink_metadata())
                else {
                    continue;
                };
                let kind = if entry
                    .path()
                    .symlink_metadata()
                    .map(|m| m.file_type().is_symlink())
                    .unwrap_or(false)
                {
                    NodeKind::Symlink
                } else if meta.is_dir() {
                    NodeKind::Dir
                } else {
                    NodeKind::File
                };
                seen.insert(name.clone());
                merged.push((name, kind));
            }
        }

        // Pending-rename destinations rooted here: physically absent from both the upper and
        // this directory's lower listing, but part of the merged namespace. An upper dir of
        // the same name (child copy-ups) already listed above; `seen` dedups.
        for name in self.redirect_roots_under(&node.path) {
            if seen.insert(name.clone()) {
                merged.push((name, NodeKind::Dir));
            }
        }

        let lower = if !self.whited_out(&node.path) || node.path.is_empty() {
            self.lower_dir_binding(&node).await.ok()
        } else {
            None
        };
        if let Some(core_ino) = lower {
            let fh = self.core.opendir(core_ino)?;
            let mut offset = 0u64;
            loop {
                let page = settle_lower!(self.core.readdir(fh, offset, 4096).await);
                let page = match page {
                    Ok(page) => page,
                    Err(e) => {
                        self.core.releasedir(fh);
                        return Err(e);
                    }
                };
                if page.is_empty() {
                    break;
                }
                offset = page.last().expect("nonempty page").next_offset;
                for entry in page {
                    let child = Self::child_path(&node.path, &entry.name);
                    if seen.contains(&entry.name) || self.whited_out(&child) {
                        continue;
                    }
                    merged.push((entry.name, entry.kind));
                }
            }
            self.core.releasedir(fh);
        }

        merged.sort_by(|a, b| a.0.cmp(&b.0));
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().expect("handle lock").insert(
            fh,
            OHandle::Dir {
                ino,
                entries: merged,
            },
        );
        Ok(fh)
    }

    pub fn readdir(
        &self,
        fh: u64,
        offset: u64,
        max: usize,
    ) -> Result<Vec<OverlayDirEntry>, MountError> {
        let handles = self.handles.lock().expect("handle lock");
        let Some(OHandle::Dir { entries, .. }) = handles.get(&fh) else {
            return Err(not_found());
        };
        Ok(entries
            .iter()
            .enumerate()
            .skip(offset as usize)
            .take(max)
            .map(|(i, (name, kind))| OverlayDirEntry {
                next_offset: i as u64 + 1,
                name: name.clone(),
                kind: *kind,
            })
            .collect())
    }

    /// `readdir` with attributes — the FUSE readdirplus shape. Every returned entry resolves
    /// through [`OverlayFs::lookup`], so it carries **one counted reference** on its overlay
    /// ino, which the kernel balances with later `forget`s; a binding that fails to deliver an
    /// entry to the kernel (reply buffer full) must call [`OverlayFs::forget`] with 1 for it.
    // Driven by the Linux FUSE readdirplus op; the macOS vfsserver transport doesn't use it.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub async fn readdir_plus(
        &self,
        fh: u64,
        offset: u64,
        max: usize,
    ) -> Result<Vec<(OverlayDirEntry, OverlayAttr)>, MountError> {
        let (dir_ino, window) = {
            let handles = self.handles.lock().expect("handle lock");
            let Some(OHandle::Dir { ino, entries }) = handles.get(&fh) else {
                return Err(not_found());
            };
            (
                *ino,
                entries
                    .iter()
                    .enumerate()
                    .skip(offset as usize)
                    .take(max)
                    .map(|(i, (name, kind))| OverlayDirEntry {
                        next_offset: i as u64 + 1,
                        name: name.clone(),
                        kind: *kind,
                    })
                    .collect::<Vec<_>>(),
            )
        };
        let mut out = Vec::with_capacity(window.len());
        for entry in window {
            let attr = self.lookup(dir_ino, &entry.name).await?;
            out.push((entry, attr));
        }
        Ok(out)
    }

    pub fn releasedir(&self, fh: u64) {
        self.handles.lock().expect("handle lock").remove(&fh);
    }

    // -------------------------------------------------------------------------------------
    // File handles / IO
    // -------------------------------------------------------------------------------------

    /// Open for read or write. Writing to a lower-backed file copies it up first; after that,
    /// all IO on the path is local.
    ///
    /// The returned flag is the keep-cache decision for the binding (`FOPEN_KEEP_CACHE`): true
    /// when this open serves byte-identical content to the node's previous open (same lower
    /// blob oid), so the kernel page cache survives — including across branch refreshes that
    /// never touched the path. Upper-backed opens always report false: local writes own the
    /// path from then on and the identity chain restarts.
    pub async fn open(&self, ino: u64, write: bool) -> Result<(u64, bool), MountError> {
        let _fence = if write {
            self.write_guard()?;
            // Held through handle registration: once the handle is in the table, trim skips
            // the path; before that, the fence is what keeps trim from unlinking the upper
            // file between copy-up and this open.
            Some(self.mutate.read().await)
        } else {
            None
        };
        let node = self.node(ino)?;
        // The kernel can open by cached inode without a fresh lookup; a node under a whiteout
        // (restore deleted it out-of-band) must not hand out its stale lower backing.
        if self.upper_meta(&node.path).is_none() && self.whited_out(&node.path) {
            return Err(not_found());
        }
        if write {
            self.copy_up(&node).await?;
        }
        let (handle, ident) = if self.upper_meta(&node.path).is_some() {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(write)
                .open(self.upper_path(&node.path))
                .map_err(io_err)?;
            (
                OHandle::Upper {
                    file,
                    path: node.path.clone(),
                },
                None,
            )
        } else {
            let core_ino = self.lower_binding(&node).await?;
            let ident = self
                .core
                .getattr(core_ino)
                .ok()
                .map(|attr| attr.oid)
                .filter(|oid| !oid.is_empty());
            (
                OHandle::Lower {
                    core_fh: self.core.open(core_ino)?,
                },
                ident,
            )
        };
        let keep_cache = {
            let mut last = node.last_open_oid.lock().expect("last open oid lock");
            let keep = ident.is_some() && *last == ident;
            *last = ident;
            keep
        };
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().expect("handle lock").insert(fh, handle);
        Ok((fh, keep_cache))
    }

    pub async fn read(&self, fh: u64, offset: u64, size: u64) -> Result<Bytes, MountError> {
        let core_fh = {
            let handles = self.handles.lock().expect("handle lock");
            match handles.get(&fh) {
                Some(OHandle::Upper { file, .. }) => {
                    use std::os::unix::fs::FileExt;
                    let mut buf = vec![0u8; size as usize];
                    let n = file.read_at(&mut buf, offset).map_err(io_err)?;
                    buf.truncate(n);
                    return Ok(Bytes::from(buf));
                }
                Some(OHandle::Lower { core_fh }) => *core_fh,
                _ => return Err(not_found()),
            }
        };
        settle_lower!(self.core.read(core_fh, offset, size).await)
    }

    pub fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32, MountError> {
        self.write_guard()?;
        use std::os::unix::fs::FileExt;
        let path = {
            let handles = self.handles.lock().expect("handle lock");
            match handles.get(&fh) {
                Some(OHandle::Upper { file, path }) => {
                    file.write_all_at(data, offset).map_err(io_err)?;
                    path.clone()
                }
                // open(write=true) always yields an upper handle; a write on a lower handle
                // means the kernel opened read-only, which it won't for writes.
                Some(OHandle::Lower { .. }) => {
                    return Err(MountError::Protocol(
                        "write on read-only handle".to_string(),
                    ));
                }
                _ => return Err(not_found()),
            }
        };
        self.record_write(&path, offset);
        Ok(data.len() as u32)
    }

    pub fn fsync(&self, fh: u64) -> Result<(), MountError> {
        let handles = self.handles.lock().expect("handle lock");
        if let Some(OHandle::Upper { file, .. }) = handles.get(&fh) {
            file.sync_all().map_err(io_err)?;
        }
        Ok(())
    }

    pub fn release(&self, fh: u64) {
        let handle = self.handles.lock().expect("handle lock").remove(&fh);
        if let Some(OHandle::Lower { core_fh }) = handle {
            self.core.release(core_fh);
        }
    }

    /// Materialize a lower file into the upper layer (no-op when upper already has the path).
    async fn copy_up(&self, node: &ONode) -> Result<(), MountError> {
        if self.upper_meta(&node.path).is_some() {
            return Ok(());
        }
        let core_ino = self.lower_binding(node).await?;
        let attr = self.core.getattr(core_ino)?;
        match attr.kind {
            NodeKind::Dir => {
                std::fs::create_dir_all(self.upper_path(&node.path)).map_err(io_err)?;
                return Ok(());
            }
            NodeKind::Symlink => {
                let target = self.core.readlink(core_ino).await?;
                let dest = self.upper_path(&node.path);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent).map_err(io_err)?;
                }
                std::os::unix::fs::symlink(String::from_utf8_lossy(&target).as_ref(), &dest)
                    .map_err(io_err)?;
                self.record(&node.path, DirtyKind::Upsert);
                return Ok(());
            }
            NodeKind::File => {}
        }
        let dest = self.upper_path(&node.path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(io_err)?;
        }
        let tmp = dest.with_extension("tlfs-copyup");
        {
            use std::io::Write;
            let mut out = std::fs::File::create(&tmp).map_err(io_err)?;
            let fh = self.core.open(core_ino)?;
            let mut offset = 0u64;
            loop {
                let chunk = match settle_lower!(self.core.read(fh, offset, 4 * 1024 * 1024).await) {
                    Ok(chunk) => chunk,
                    Err(e) => {
                        self.core.release(fh);
                        let _ = std::fs::remove_file(&tmp);
                        return Err(e);
                    }
                };
                if chunk.is_empty() {
                    break;
                }
                offset += chunk.len() as u64;
                out.write_all(&chunk).map_err(io_err)?;
            }
            self.core.release(fh);
            if attr.perm & 0o111 != 0 {
                use std::os::unix::fs::PermissionsExt;
                out.set_permissions(std::fs::Permissions::from_mode(0o755))
                    .map_err(io_err)?;
            }
        }
        std::fs::rename(&tmp, &dest).map_err(io_err)?;
        // The upper backs this path from here on, so the path is dirty NOW — not at the first
        // write() through the handle. An open-for-write that never writes (or a `touch`, whose
        // setattr carries neither size nor mode) would otherwise leave an upper file no dirty
        // index, seal, or trim ever learns about: invisible to `status`, unpublishable, and
        // silently shadowing every later sync of the path. Sealing it costs nothing — the
        // bytes are lower-identical, so content dedup makes the push a by-reference no-op.
        self.record(&node.path, DirtyKind::Upsert);
        Ok(())
    }

    // -------------------------------------------------------------------------------------
    // Write side (namespace mutations)
    // -------------------------------------------------------------------------------------

    /// Whether the lower layer has a node at `path` (via a counted lookup that is immediately
    /// balanced — used to decide whether a whiteout is needed).
    async fn lower_has(&self, parent_core: Option<u64>, name: &str) -> bool {
        let Some(parent_core) = parent_core else {
            return false;
        };
        match self.core.lookup(parent_core, name).await {
            Ok(attr) => {
                self.core.forget(attr.ino, 1);
                true
            }
            Err(_) => false,
        }
    }

    fn clear_whiteout(&self, path: &str) {
        let _ = std::fs::remove_file(self.wh_path(path));
    }

    fn set_whiteout(&self, path: &str) -> Result<(), MountError> {
        let marker = self.wh_path(path);
        if let Some(parent) = marker.parent() {
            std::fs::create_dir_all(parent).map_err(io_err)?;
        }
        // A container of child markers may sit where a directory whiteout goes; the dir-level
        // marker supersedes them.
        if marker.is_dir() {
            std::fs::remove_dir_all(&marker).map_err(io_err)?;
        }
        std::fs::write(&marker, b"").map_err(io_err)?;
        Ok(())
    }

    /// Whether the merged view already has an entry at `path` (upper presence, or lower
    /// presence not hidden by a whiteout). Create-style ops must enforce this themselves: the
    /// kernel skips its LOOKUP when it trusts a (possibly stale) negative name-cache entry, so
    /// a create arriving here may target a name that already exists.
    async fn merged_exists(&self, parent_core: Option<u64>, path: &str, name: &str) -> bool {
        if self.upper_path(path).symlink_metadata().is_ok() {
            return true;
        }
        // A pending rename occupies its destination root.
        if self.redirect_source(path).is_some() {
            return true;
        }
        if self.whited_out(path) {
            return false;
        }
        let Some(core_parent) = parent_core else {
            return false;
        };
        // A cached parent binding can point at a lower non-directory (the upper dir shadows
        // it): no lower children exist, and probing would spin the settle loop.
        if !matches!(self.core.getattr(core_parent), Ok(attr) if attr.kind == NodeKind::Dir) {
            return false;
        }
        match self.core.lookup(core_parent, name).await {
            Ok(attr) => {
                self.core.forget(attr.ino, 1);
                true
            }
            Err(_) => false,
        }
    }

    pub async fn create(
        &self,
        parent: u64,
        name: &str,
        exec: bool,
    ) -> Result<(OverlayAttr, u64), MountError> {
        self.write_guard()?;
        let _fence = self.mutate.read().await;
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        if self
            .merged_exists(parent_node.core_ino(), &path, name)
            .await
        {
            return Err(MountError::Exists);
        }
        let dest = self.upper_path(&path);
        if let Some(dir) = dest.parent() {
            std::fs::create_dir_all(dir).map_err(io_err)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&dest)
            .map_err(io_err)?;
        if exec {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o755))
                .map_err(io_err)?;
        }
        self.clear_whiteout(&path);
        self.record(&path, DirtyKind::Upsert);
        let meta = dest.symlink_metadata().map_err(io_err)?;
        let (ino, _, _) = self
            .inodes
            .lock()
            .expect("inode lock")
            .intern(path.clone(), None);
        let attr = self.attr_from_meta(ino, &meta);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles
            .lock()
            .expect("handle lock")
            .insert(fh, OHandle::Upper { file, path });
        Ok((attr, fh))
    }

    pub async fn mkdir(&self, parent: u64, name: &str) -> Result<OverlayAttr, MountError> {
        self.write_guard()?;
        let _fence = self.mutate.read().await;
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        if self
            .merged_exists(parent_node.core_ino(), &path, name)
            .await
        {
            return Err(MountError::Exists);
        }
        let dest = self.upper_path(&path);
        std::fs::create_dir_all(&dest).map_err(io_err)?;
        // A whiteout here can be load-bearing: `rm file; mkdir file` replaces a lower FILE
        // with a directory, and clearing the marker would resurface the file underneath the
        // new dir — child lookups then resolve against the file and fail, and the merged
        // view flip-flops. Every read path is upper-first ("a whiteout without upper
        // presence is a hard miss"), so the kept marker never hides the new directory; it
        // only keeps hiding what the delete deleted, and the seal reads the pair as exactly
        // the right change set (delete the file, add the dir's children). Clear it only
        // when the lower holds nothing or a directory (the recreate case, where the child
        // markers own the hiding).
        let lower_non_dir = match parent_node.core_ino() {
            Some(core_parent) => match self.core.lookup(core_parent, name).await {
                Ok(attr) => {
                    let non_dir = attr.kind != gsvc_mount::NodeKind::Dir;
                    self.core.forget(attr.ino, 1);
                    non_dir
                }
                // Only a definite "nothing below" clears the marker: a transient probe
                // failure (stale binding mid-refresh, index lag) must KEEP a possibly
                // load-bearing whiteout — keeping is safe in every case (it hides only
                // deleted-or-replaced lower state), clearing can resurface a replaced
                // file underneath the new directory.
                Err(MountError::NotFound(_)) => false,
                Err(_) => true,
            },
            None => false,
        };
        if !lower_non_dir {
            self.clear_whiteout(&path);
        }
        self.record(&path, DirtyKind::Upsert);
        let meta = dest.symlink_metadata().map_err(io_err)?;
        let (ino, _, _) = self.inodes.lock().expect("inode lock").intern(path, None);
        Ok(self.attr_from_meta(ino, &meta))
    }

    pub async fn symlink(
        &self,
        parent: u64,
        name: &str,
        target: &str,
    ) -> Result<OverlayAttr, MountError> {
        self.write_guard()?;
        let _fence = self.mutate.read().await;
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        if self
            .merged_exists(parent_node.core_ino(), &path, name)
            .await
        {
            return Err(MountError::Exists);
        }
        let dest = self.upper_path(&path);
        if let Some(dir) = dest.parent() {
            std::fs::create_dir_all(dir).map_err(io_err)?;
        }
        let _ = std::fs::remove_file(&dest);
        std::os::unix::fs::symlink(target, &dest).map_err(io_err)?;
        self.clear_whiteout(&path);
        self.record(&path, DirtyKind::Upsert);
        let meta = dest.symlink_metadata().map_err(io_err)?;
        let (ino, _, _) = self.inodes.lock().expect("inode lock").intern(path, None);
        Ok(self.attr_from_meta(ino, &meta))
    }

    pub async fn unlink(&self, parent: u64, name: &str) -> Result<(), MountError> {
        self.write_guard()?;
        let _fence = self.mutate.read().await;
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        let dest = self.upper_path(&path);
        let had_upper = dest.symlink_metadata().is_ok();
        if had_upper {
            std::fs::remove_file(&dest).map_err(io_err)?;
        }
        if self.lower_has(parent_node.core_ino(), name).await {
            self.set_whiteout(&path)?;
        } else if !had_upper {
            return Err(not_found());
        }
        self.record(&path, DirtyKind::Delete);
        Ok(())
    }

    pub async fn rmdir(&self, parent: u64, name: &str) -> Result<(), MountError> {
        self.write_guard()?;
        let _fence = self.mutate.read().await;
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        // Merged-empty check: opendir the directory and require zero entries.
        let attr = self.lookup(parent, name).await?;
        let fh = self.opendir(attr.ino).await?;
        let empty = self.readdir(fh, 0, 1)?.is_empty();
        self.releasedir(fh);
        // lookup() above took a reference; balance it.
        self.forget(attr.ino, 1);
        if !empty {
            // A real errno, not the Protocol->EIO catch-all: `rm`/`rmdir` on a non-empty
            // directory must see ENOTEMPTY, or it reports a bewildering "Input/output error".
            return Err(MountError::NotEmpty);
        }
        let dest = self.upper_path(&path);
        if dest.symlink_metadata().is_ok() {
            std::fs::remove_dir_all(&dest).map_err(io_err)?;
        }
        // A pending rename rooted here dies with the directory: its true source was
        // whiteouted when the rename was recorded, so nothing further hides.
        self.drop_redirect_tree(&path)?;
        if self.lower_has(parent_node.core_ino(), name).await {
            self.set_whiteout(&path)?;
        }
        self.record(&path, DirtyKind::Delete);
        Ok(())
    }

    /// Move a committed (lower-backed) directory by recording a pending rename: the table
    /// entry re-points the merged destination at the true-lower source, local state under the
    /// source (child copy-ups in the upper, deletion markers in `wh/`) rides along, and no
    /// content moves. The seal reconciles the entry into by-oid upserts plus the source
    /// delete. Moving a destination that is itself pending re-keys the existing entry, so
    /// chains stay one hop.
    fn rename_committed_dir(&self, src: &str, dst: &str) -> Result<(), MountError> {
        let true_src = self.remap_lower(src);
        {
            let mut redirects = self.redirects.lock().expect("redirect lock");
            record_committed_rename(&mut redirects, src, dst, true_src);
            self.persist_redirects(&redirects)?;
            // Under the redirects lock, so it serializes with the sealer's clock close —
            // see close_dirty_base_if_clean.
            self.stamp_mutation();
        }
        self.redirect_gen.fetch_add(1, Ordering::SeqCst);
        // Stale deletion markers at the destination belong to whatever the replace displaced.
        let dst_wh = self.wh_path(dst);
        if dst_wh
            .symlink_metadata()
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            std::fs::remove_dir_all(&dst_wh).map_err(io_err)?;
        }
        // Local state under the source rides along: child copy-ups in the upper...
        let src_upper = self.upper_path(src);
        if src_upper.symlink_metadata().is_ok() {
            let dst_upper = self.upper_path(dst);
            if dst_upper.symlink_metadata().is_ok() {
                std::fs::remove_dir_all(&dst_upper).map_err(io_err)?;
            }
            std::fs::rename(&src_upper, &dst_upper).map_err(io_err)?;
        }
        // ...and deletions of paths inside the moved subtree.
        let src_wh = self.wh_path(src);
        if src_wh
            .symlink_metadata()
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            if let Some(dir) = dst_wh.parent() {
                std::fs::create_dir_all(dir).map_err(io_err)?;
            }
            std::fs::rename(&src_wh, &dst_wh).map_err(io_err)?;
        }
        Ok(())
    }

    /// Drop the pending rename rooted at `path` (and defensively any nested under it) because
    /// its destination was removed or replaced. Stale deletion markers under the destination
    /// go with it — they described children of the dead entry and must not publish as deletes
    /// of paths that never existed. Returns whether an entry was dropped.
    fn drop_redirect_tree(&self, path: &str) -> Result<bool, MountError> {
        let dropped = {
            let mut redirects = self.redirects.lock().expect("redirect lock");
            let prefix = format!("{path}/");
            let before = redirects.len();
            redirects.retain(|k, _| k != path && !k.starts_with(&prefix));
            let dropped = redirects.len() != before;
            if dropped {
                self.persist_redirects(&redirects)?;
            }
            dropped
        };
        if dropped {
            self.redirect_gen.fetch_add(1, Ordering::SeqCst);
            let wh = self.wh_path(path);
            if wh.symlink_metadata().map(|m| m.is_dir()).unwrap_or(false) {
                std::fs::remove_dir_all(&wh).map_err(io_err)?;
            }
        }
        Ok(dropped)
    }

    /// Rename. Upper-only sources rename in place; a lower-backed file copies up, writes the
    /// destination, and whiteouts the source. A lower-backed (committed) directory moves as a
    /// pending rename — see [`OverlayFs::rename_committed_dir`].
    pub async fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(), MountError> {
        self.write_guard()?;
        let _fence = self.mutate.read().await;
        let parent_node = self.node(parent)?;
        let new_parent_node = self.node(new_parent)?;
        let src = Self::child_path(&parent_node.path, name);
        let dst = Self::child_path(&new_parent_node.path, new_name);
        let src_upper = self.upper_path(&src);
        let dst_upper = self.upper_path(&dst);
        if let Some(dir) = dst_upper.parent() {
            std::fs::create_dir_all(dir).map_err(io_err)?;
        }

        let src_meta = src_upper.symlink_metadata().ok();
        let src_redirect = self.redirect_source(&src).is_some();
        let lower_src = self.lower_has(parent_node.core_ino(), name).await;
        let upper_dir_over_redirect = src_redirect
            && src_meta
                .as_ref()
                .map(|m| m.is_dir() && !m.file_type().is_symlink())
                .unwrap_or(false);
        let mut committed_dir_move = false;
        if src_meta.is_some() && !upper_dir_over_redirect {
            std::fs::rename(&src_upper, &dst_upper).map_err(io_err)?;
        } else if lower_src || src_redirect {
            let attr = self.lookup(parent, name).await?;
            let node = self.node(attr.ino)?;
            let kind = attr.kind;
            self.forget(attr.ino, 1);
            if matches!(kind, NodeKind::Dir) {
                // A committed directory moves as a pending rename: metadata only, the seal
                // reconciles it into by-oid upserts plus the source delete. Materializing the
                // subtree here would put unbounded network I/O inside a syscall. An upper dir
                // of the same name (child copy-ups) rides along inside the move.
                self.rename_committed_dir(&src, &dst)?;
                committed_dir_move = true;
            } else {
                // Copy the lower file up directly at the destination.
                self.copy_up(&node).await?;
                std::fs::rename(self.upper_path(&src), &dst_upper).map_err(io_err)?;
            }
        } else {
            return Err(not_found());
        }
        if !committed_dir_move {
            // Whatever the destination held is replaced wholesale; a pending rename that was
            // rooted there dies with it.
            self.drop_redirect_tree(&dst)?;
        }
        if lower_src {
            self.set_whiteout(&src)?;
        }
        self.clear_whiteout(&dst);
        self.record(&src, DirtyKind::Delete);
        self.record(&dst, DirtyKind::Upsert);
        // A directory rename moved a whole subtree with the two events above naming only the
        // directory — but seals publish FILES: without per-child events the relocated files
        // never seal and previously-sealed old paths never tombstone. Walk the moved tree
        // (it's local upper state) and record both sides for every leaf.
        if dst_upper
            .symlink_metadata()
            .map(|m| m.is_dir() && !m.file_type().is_symlink())
            .unwrap_or(false)
        {
            fn leaves(root: &Path, dir: &Path, out: &mut Vec<String>) {
                let Ok(read) = std::fs::read_dir(dir) else {
                    return;
                };
                for entry in read.flatten() {
                    let abs = entry.path();
                    let Ok(meta) = std::fs::symlink_metadata(&abs) else {
                        continue;
                    };
                    if meta.is_dir() && !meta.file_type().is_symlink() {
                        leaves(root, &abs, out);
                    } else {
                        out.push(
                            abs.strip_prefix(root)
                                .expect("under root")
                                .components()
                                .map(|c| c.as_os_str().to_string_lossy())
                                .collect::<Vec<_>>()
                                .join("/"),
                        );
                    }
                }
            }
            let mut moved = Vec::new();
            leaves(&dst_upper, &dst_upper, &mut moved);
            for rel in moved {
                self.record(&format!("{src}/{rel}"), DirtyKind::Delete);
                self.record(&format!("{dst}/{rel}"), DirtyKind::Upsert);
            }
        }
        // Re-key the in-memory node table: the kernel keeps the source's nodeid and re-points its
        // dentry at the destination, then addresses it by ino — so the moved node's path must
        // follow it, or ino-based getattr/open resolve the vacated source path and return ENOENT.
        let released = {
            let mut inodes = self.inodes.lock().expect("inode lock");
            inodes.rename(&src, &dst)
        };
        for core in released {
            self.core.forget(core, 1);
        }
        Ok(())
    }

    /// Truncate and exec-bit changes; anything else is accepted and ignored (git versions
    /// neither timestamps nor ownership).
    pub async fn setattr(
        &self,
        ino: u64,
        size: Option<u64>,
        mode: Option<u32>,
    ) -> Result<OverlayAttr, MountError> {
        self.write_guard()?;
        let _fence = self.mutate.read().await;
        let node = self.node(ino)?;
        if size.is_some() || mode.is_some() {
            self.copy_up(&node).await?;
            let dest = self.upper_path(&node.path);
            if let Some(size) = size {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&dest)
                    .map_err(io_err)?;
                file.set_len(size).map_err(io_err)?;
            }
            if let Some(mode) = mode {
                use std::os::unix::fs::PermissionsExt;
                let perm = if mode & 0o111 != 0 { 0o755 } else { 0o644 };
                std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(perm))
                    .map_err(io_err)?;
            }
            self.record(&node.path, DirtyKind::Upsert);
        }
        self.getattr(ino).await
    }

    // -------------------------------------------------------------------------------------
    // Kernel-cache invalidation
    // -------------------------------------------------------------------------------------

    /// Translate a core refresh delta into everything the daemon needs to converge kernel
    /// caches, in one pass: FUSE invalidations in this overlay's ino space (Linux notify sink)
    /// and merged-coordinate probe expectations (macOS, where FSKit has no notify channel and
    /// `tl fs sync` probes from outside the mount instead).
    ///
    /// Entries whose kernel-visible content the lower change cannot affect — upper-shadowed or
    /// whited-out paths — are dropped, as are paths the kernel never looked up here (a rebound
    /// or staled path with no interned ino, or an appeared name whose parent was never
    /// interned, has no kernel cache entry — positive or negative — to converge). Appeared
    /// paths have no ino by definition; they translate to an entry invalidation on their live
    /// parent directory, which is what drops a kernel negative dentry cached from an `ENOENT`
    /// answered before the refresh.
    pub fn refresh_outputs(&self, delta: &gsvc_mount::RefreshDelta) -> RefreshOutputs {
        // Delta paths are lower coordinates; a pending rename serves that content under its
        // destination, so the kernel-visible path (and the interned ino) lives there. The
        // source coordinates are whiteouted and filter out below.
        //
        // Filter pass first, without the inode lock: the shadow checks stat the upper tree,
        // and a branch-jump delta can carry tens of thousands of paths — the lock is taken
        // once, after the filesystem work, for the pure ino mapping.
        enum Kind {
            Rebound { size: Option<u64> },
            Staled,
            Appeared,
        }
        let reverse = self.reverse_redirects();
        let tagged = delta
            .rebound
            .iter()
            .map(|e| (e.path.as_str(), Kind::Rebound { size: e.size }))
            .chain(delta.staled.iter().map(|e| (e.path.as_str(), Kind::Staled)))
            .chain(
                delta
                    .appeared
                    .iter()
                    .flatten()
                    .map(|p| (p.as_str(), Kind::Appeared)),
            );
        let mut visible: Vec<(String, Kind)> = Vec::new();
        for (lower, kind) in tagged {
            let path = merged_of(&reverse, lower);
            if self.upper_meta(&path).is_some() || self.whited_out(&path) {
                continue;
            }
            visible.push((path, kind));
        }

        let mut out = RefreshOutputs::default();
        let inodes = self.inodes.lock().expect("inode lock");
        for (path, kind) in visible {
            let (parent_ino, name) = match path.rfind('/') {
                Some(i) => (
                    inodes.index.get(&path[..i]).copied(),
                    path[i + 1..].to_string(),
                ),
                None => (Some(ROOT_INO), path.clone()),
            };
            match kind {
                // Rebound/staled: the kernel holds cache entries for this path only if it
                // looked it up here (which interned it) and has not since forgotten it.
                Kind::Rebound { size } => {
                    let Some(&ino) = inodes.index.get(&path) else {
                        continue;
                    };
                    out.invals.push(OverlayInval {
                        ino,
                        parent_ino,
                        name,
                        staled: false,
                    });
                    out.expectations.push(KernelExpectation {
                        path,
                        present: true,
                        size,
                    });
                }
                Kind::Staled => {
                    let Some(&ino) = inodes.index.get(&path) else {
                        continue;
                    };
                    out.invals.push(OverlayInval {
                        ino,
                        parent_ino,
                        name,
                        staled: true,
                    });
                    out.expectations.push(KernelExpectation {
                        path,
                        present: false,
                        size: None,
                    });
                }
                Kind::Appeared => {
                    // A parent the kernel never looked up holds no dentries (negative or
                    // otherwise) for children — nothing to drop or probe.
                    let Some(parent_ino) = parent_ino else {
                        continue;
                    };
                    // `ino` is the parent on purpose: the appeared name has no ino, and the
                    // parent's kernel attrs/readdir cache went stale the moment the name
                    // appeared under it.
                    out.invals.push(OverlayInval {
                        ino: parent_ino,
                        parent_ino: Some(parent_ino),
                        name,
                        staled: true,
                    });
                    out.expectations.push(KernelExpectation {
                        path,
                        present: true,
                        size: None,
                    });
                }
            }
        }
        out
    }

    /// The redirect table inverted for lower→merged translation (source path -> destination).
    fn reverse_redirects(&self) -> Vec<(String, String)> {
        let redirects = self.redirects.lock().expect("redirect lock");
        redirects
            .iter()
            .map(|(d, s)| (s.clone(), d.clone()))
            .collect()
    }

    // -------------------------------------------------------------------------------------
    // Snapshot support
    // -------------------------------------------------------------------------------------

    /// Drop all upper state (after a successful snapshot has sealed it into a commit, or a
    /// restore replaced it). Open upper handles keep their descriptors (unix semantics); new
    /// opens see the lower layer.
    ///
    /// Returns the kernel invalidations this implies: every interned node the upper layer or a
    /// whiteout was presenting flips to its merged-lower view without any kernel-visible
    /// operation, so a binding holding long TTLs must push these (`staled` here means "force a
    /// fresh lookup", not that the path is gone).
    pub fn clear_upper(&self) -> Result<Vec<OverlayInval>, MountError> {
        let affected: Vec<OverlayInval> = {
            let inodes = self.inodes.lock().expect("inode lock");
            inodes
                .nodes
                .iter()
                .filter(|(ino, _)| **ino != ROOT_INO)
                .filter(|(_, (node, _))| {
                    self.upper_meta(&node.path).is_some()
                        || self.whited_out(&node.path)
                        // Pending-rename paths flip from remapped resolution to the advanced
                        // lower serving them directly; force fresh lookups there too.
                        || self.redirect_covers(&node.path)
                })
                .map(|(&ino, (node, _))| {
                    let (parent_ino, name) = match node.path.rfind('/') {
                        Some(i) => (
                            inodes.index.get(&node.path[..i]).copied(),
                            node.path[i + 1..].to_string(),
                        ),
                        None => (Some(ROOT_INO), node.path.clone()),
                    };
                    OverlayInval {
                        ino,
                        parent_ino,
                        name,
                        staled: true,
                    }
                })
                .collect()
        };
        for dir in [&self.upper, &self.wh] {
            for entry in std::fs::read_dir(dir).map_err(io_err)?.flatten() {
                let path = entry.path();
                if path.is_dir() && !path.is_symlink() {
                    std::fs::remove_dir_all(&path).map_err(io_err)?;
                } else {
                    std::fs::remove_file(&path).map_err(io_err)?;
                }
            }
        }
        // Pending renames were sealed into the commit this drop follows (or replaced by a
        // restore); either way the advanced lower serves their destinations directly now.
        {
            let mut redirects = self.redirects.lock().expect("redirect lock");
            if !redirects.is_empty() {
                redirects.clear();
                self.persist_redirects(&redirects)?;
            }
        }
        self.redirect_gen.fetch_add(1, Ordering::SeqCst);
        // The dirty index described the dropped state; sealers fast-forward to the watermark.
        // The epoch bump tells them their other caches (chunk lists, sealed guards, in-flight
        // resolutions) describe a dead world too.
        self.dirty.lock().expect("dirty lock").clear();
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.first_dirty_ms.store(0, Ordering::SeqCst);
        self.last_mutation_ms.store(0, Ordering::SeqCst);
        self.sealed_oids.lock().expect("sealed oid lock").clear();
        self.divergent_pending
            .lock()
            .expect("divergence lock")
            .clear();
        Ok(affected)
    }

    /// Millis on this overlay's private monotonic clock (never 0 — 0 is the clean sentinel
    /// in the mutation stamps).
    pub fn clock_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64 + 1
    }

    fn stamp_mutation(&self) {
        let now = self.clock_ms();
        self.last_mutation_ms.store(now, Ordering::SeqCst);
        // First write of the batch; racing writers can both lose to an earlier stamp,
        // which is exactly right.
        let _ = self
            .first_dirty_ms
            .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// `(first dirty, last mutation)` clock stamps of the current dirty batch, or `None`
    /// when the overlay is clean — the autosave tick's entire cost on the idle path.
    pub fn dirty_clock(&self) -> Option<(u64, u64)> {
        let first = self.first_dirty_ms.load(Ordering::SeqCst);
        (first != 0).then(|| (first, self.last_mutation_ms.load(Ordering::SeqCst)))
    }

    /// The lower commit the current dirty batch was started against — the merge base a seal
    /// must declare. `None` when nothing has been written since the last clean seal (callers
    /// fall back to the current lower).
    pub fn dirty_base(&self) -> Option<String> {
        self.dirty_base.lock().expect("dirty base lock").clone()
    }

    /// After a successful seal: if no write raced in during the push (the dirty set is
    /// empty), the batch is closed — the next write stamps a fresh base. A non-empty set
    /// keeps the old (older-than-necessary, therefore safe) base for the writes it covers.
    pub fn close_dirty_base_if_clean(&self) {
        let dirty = self.dirty.lock().expect("dirty lock");
        if dirty.is_empty() {
            *self.dirty_base.lock().expect("dirty base lock") = None;
            // The emptiness check and the zeroing hold the REDIRECTS lock together:
            // rename_committed_dir stamps the clocks inside the same critical section, so
            // a rename either lands before this check (kept running) or stamps after the
            // zero (re-armed) — never a pending redirect with a zeroed clock, which the
            // event-driven autosave would sleep past forever.
            let redirects = self.redirects.lock().expect("redirect lock");
            if redirects.is_empty() {
                // Batch fully sealed: the clocks re-arm on the next write. Pending
                // redirects keep them running — they are unsealed work the dirty map
                // never sees.
                self.first_dirty_ms.store(0, Ordering::SeqCst);
                self.last_mutation_ms.store(0, Ordering::SeqCst);
            }
        }
    }

    /// Self-heal dir-over-file states before a seal: for every dirty upsert path (and its
    /// ancestors) whose UPPER is a directory while the LOWER holds a non-directory, write
    /// the missing whiteout AND record the path as a dirty upsert. The record is what makes
    /// the boundary durable: it rides every subsequent delta until a seal SUCCEEDS (a
    /// failed push prunes nothing), and the seal's directory-upsert arm then publishes the
    /// whiteout-delete plus the children — so retries re-derive nothing and probe nothing.
    /// Ancestors still scan (in-memory, cheap) because the mkdir's own record can be
    /// consumed by an earlier empty seal BEFORE the racing file lands, leaving only child
    /// upserts to name the boundary.
    ///
    /// Returns the paths healed THIS pass (for the daemon log and the current seal's
    /// forced delete — the fresh record's generation is past the in-flight delta's
    /// watermark, so this one seal injects it by hand).
    ///
    /// Errors abort the seal (retryable): a transient index error or a failed marker write
    /// swallowed here would let the seal publish the children WITHOUT the delete half.
    /// Only a genuinely absent lower is "nothing to heal".
    pub(crate) async fn heal_replaced_files<'a>(
        &self,
        upserts: impl Iterator<Item = &'a str>,
    ) -> Result<Vec<String>, MountError> {
        let mut candidates: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for path in upserts {
            let mut prefix = String::with_capacity(path.len());
            for component in path.split('/') {
                if !prefix.is_empty() {
                    prefix.push('/');
                }
                prefix.push_str(component);
                candidates.insert(prefix.clone());
            }
        }
        let mut healed = Vec::new();
        for path in candidates {
            let is_upper_dir = self
                .upper_path(&path)
                .symlink_metadata()
                .is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink());
            if !is_upper_dir || self.whited_out(&path) {
                // Already marked = already recorded (the heal that wrote the marker also
                // wrote the dirty record, which only a successful seal prunes): nothing to
                // probe, nothing to re-derive.
                continue;
            }
            match self.walk_lower(&path).await {
                Ok(attr) => {
                    let non_dir = attr.kind != NodeKind::Dir;
                    self.core.forget(attr.ino, 1);
                    if non_dir {
                        super::write_whiteout(&self.wh, &path).map_err(|e| {
                            MountError::Protocol(format!(
                                "healing dir-over-file at {path}: whiteout write failed: {e}"
                            ))
                        })?;
                        // TOCTOU re-verify: the follow poller can adopt a commit where the
                        // BRANCH itself made this path a directory between the probe and
                        // the marker write — that whiteout would suppress the merged dir's
                        // lower listing. Undo and skip; a genuine dir-over-file re-heals
                        // on the next seal.
                        match self.walk_lower(&path).await {
                            Ok(attr2) => {
                                let now_dir = attr2.kind == NodeKind::Dir;
                                self.core.forget(attr2.ino, 1);
                                if now_dir {
                                    self.clear_whiteout(&path);
                                    continue;
                                }
                            }
                            Err(MountError::NotFound(_)) => {}
                            Err(e) => {
                                self.clear_whiteout(&path);
                                return Err(MountError::Protocol(format!(
                                    "healing dir-over-file at {path}: re-verify failed: {e}"
                                )));
                            }
                        }
                        // Durable half: the boundary rides every future delta as a dirty
                        // dir upsert, whose seal arm publishes the delete AND the children.
                        self.record(&path, DirtyKind::Upsert);
                        healed.push(path);
                    }
                }
                Err(MountError::NotFound(_)) => {}
                Err(e) => {
                    return Err(MountError::Protocol(format!(
                        "healing dir-over-file at {path}: lower probe failed: {e}"
                    )));
                }
            }
        }
        Ok(healed)
    }

    /// Record the published blob oid for each sealed path (from the push report). Empty oids
    /// (deletes, stable-prefix fast-path files that never hashed) are dropped — those paths
    /// read as divergence-unknown and err toward refetching.
    pub fn record_sealed_oids(&self, oids: impl IntoIterator<Item = (String, String)>) {
        let mut map = self.sealed_oids.lock().expect("sealed oid lock");
        for (path, oid) in oids {
            map.insert(path, (!oid.is_empty()).then_some(oid));
        }
    }

    /// Seed retained paths whose seal oids are unknown (the persisted seal index after a
    /// daemon restart): they stay eligible for divergence eviction — treated as divergent
    /// whenever the branch touches them, and on a divergence-unknown fallback — without
    /// exposing dirty or ignored upper content to the trim.
    pub fn seed_retained(&self, paths: impl IntoIterator<Item = String>) {
        let mut map = self.sealed_oids.lock().expect("sealed oid lock");
        for path in paths {
            map.entry(path).or_insert(None);
        }
    }

    /// Drop retained upper copies the branch has moved past, so the merged view converges to
    /// what the branch actually says instead of serving this mount's stale byte cache. For
    /// every delta row shadowed by the upper: a dirty path is the user's in-flight edit and
    /// keeps shadowing (the next seal owns it); a retained path whose recorded seal oid
    /// equals the row's new oid is this mount's own save echoing back and stays (byte
    /// cache); anything else — different oid, unknown oid, branch-side deletion — stops
    /// shadowing via the trim fence (which pins open handles and skips redirects). Rows sit
    /// at the server's change boundary, so a removed directory (or a directory that became a
    /// file) expands against every retained path under it. A `None` delta (stat-walk
    /// fallback: older server, pruned base, transient diff failure) means divergence is
    /// UNKNOWN — every retained shadow is dropped rather than risk serving stale bytes; the
    /// cost is refetching content that usually matches. Paths a trim could not drop are
    /// retried on the next absorption. Returns kernel invalidations and probe expectations
    /// for everything dropped.
    pub fn absorb_divergence(&self, delta: &gsvc_mount::RefreshDelta) -> RefreshOutputs {
        let mut candidates: std::collections::HashSet<String> =
            std::mem::take(&mut *self.divergent_pending.lock().expect("divergence lock"));
        // Everything intersects the sealed-record map IN MEMORY first: rows that no seal of
        // ours ever touched (the overwhelming majority of a branch-jump delta) cost zero
        // syscalls — dirty and ignored upper content is never even considered, and the only
        // per-row filesystem work left is the whiteout probe for map hits.
        match &delta.changed {
            Some(rows) => {
                let sealed = self.sealed_oids.lock().expect("sealed oid lock");
                for row in rows {
                    // Boundary rows expand — the same rule the core applies to its own
                    // inodes: a removed directory row implies its subtree, and a modified
                    // non-directory row means whatever was under that name as a directory
                    // is gone.
                    let erases_subtree = match row.change {
                        gsvc_mount::ChangeKind::Removed => row.is_dir,
                        gsvc_mount::ChangeKind::Modified => !row.is_dir,
                        gsvc_mount::ChangeKind::Added => false,
                    };
                    if erases_subtree {
                        let prefix = format!("{}/", row.path);
                        for path in sealed.keys().filter(|p| p.starts_with(&prefix)) {
                            candidates.insert(path.clone());
                        }
                    }
                    let path = row.path.as_str();
                    let Some(sealed_oid) = sealed.get(path) else {
                        continue; // not sealed by this mount: never ours to evict
                    };
                    let own_echo = matches!((sealed_oid, row.oid.as_deref()),
                        (Some(sealed_oid), Some(new)) if sealed_oid == new);
                    if own_echo {
                        continue;
                    }
                    candidates.insert(path.to_string());
                }
            }
            None => {
                // Divergence unknown (stat-walk fallback): nominate every recorded retained
                // shadow AND sealed whiteout (trim skips dirty, pinned, and redirect-covered
                // ones). Restart-lost records are re-seeded from the persisted seal index at
                // daemon startup, so the map is the complete set — and an upper file OUTSIDE
                // it is dirty or ignored content that must never be nominated.
                let sealed = self.sealed_oids.lock().expect("sealed oid lock");
                candidates.extend(sealed.keys().cloned());
            }
        }
        if candidates.is_empty() {
            return RefreshOutputs::default();
        }
        // A divergent candidate that carries a whiteout is a sealed DELETE the branch moved
        // past — a remote (re-)add over it. The whiteout was masking lower content the
        // branch now carries; keeping it would hide the other session's file indefinitely.
        // Tombstoning requires EVIDENCE the branch carries the path (a delta row with an
        // oid): on the divergence-unknown fallback the sealed delete may simply not have
        // APPLIED yet (reconcile lag, a start-hint mount behind the branch), and removing
        // the marker would resurface the very files the user deleted. A whiteout under an
        // upper DIRECTORY is a live file→directory replacement boundary and is never
        // retired here — the seal owns it.
        let candidates: Vec<String> = candidates.into_iter().collect();
        let branch_carries: std::collections::HashSet<&str> = match &delta.changed {
            Some(rows) => rows
                .iter()
                .filter(|row| row.oid.is_some())
                .map(|row| row.path.as_str())
                .collect(),
            None => Default::default(),
        };
        let tombstones: Vec<String> = candidates
            .iter()
            .filter(|p| {
                branch_carries.contains(p.as_str())
                    && self.whited_out(p)
                    && !self
                        .upper_path(p)
                        .symlink_metadata()
                        .is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink())
            })
            .cloned()
            .collect();
        match self.try_trim_retained(&candidates, &tombstones) {
            Some(outcome) => {
                {
                    let mut sealed = self.sealed_oids.lock().expect("sealed oid lock");
                    for path in outcome.trimmed.iter().chain(&outcome.tombstones_removed) {
                        sealed.remove(path);
                    }
                }
                if !outcome.held_open.is_empty() {
                    self.divergent_pending
                        .lock()
                        .expect("divergence lock")
                        .extend(outcome.held_open.iter().cloned());
                }
                // Bindings without a notify channel (FSKit) converge via probe
                // expectations; a dropped shadow must purge cached pages and attrs. Rows
                // supply the lower's new size when known; boundary-expanded and
                // fallback-dropped paths get bare present-probes (the prober re-opens,
                // which revalidates content through the overlay).
                // A retired whiteout here is NOT inert — it was hiding content the branch
                // re-added — so the merged path needs the same kernel convergence as a
                // dropped shadow.
                let mut invals = outcome.invals;
                invals.extend(self.invals_for(&outcome.tombstones_removed));
                let trimmed: std::collections::HashSet<&str> = outcome
                    .trimmed
                    .iter()
                    .chain(&outcome.tombstones_removed)
                    .map(String::as_str)
                    .collect();
                let mut expectations: Vec<KernelExpectation> = Vec::new();
                if let Some(rows) = &delta.changed {
                    for row in rows {
                        if trimmed.contains(row.path.as_str()) {
                            expectations.push(KernelExpectation {
                                path: row.path.clone(),
                                present: row.oid.is_some(),
                                size: row.size,
                            });
                        }
                    }
                }
                // Trimmed paths WITHOUT a covering delta row (boundary-expanded children,
                // divergence-unknown drops) get no expectation: their branch-side presence
                // is unknown, and a wrong `present: true` makes the probe loop hammer a
                // genuinely absent path until its deadline — churning the overlay through
                // probe_negative_dentry's create/unlink pairs on the writable mount. The
                // Linux notify invals above cover them; macOS converges on the next
                // covered refresh or kernel TTL.
                RefreshOutputs {
                    invals,
                    expectations,
                }
            }
            // Fence contended (a copy-up mid-flight): retry the whole set next time (the
            // whiteout probe re-derives tombstones from these same paths).
            None => {
                self.divergent_pending
                    .lock()
                    .expect("divergence lock")
                    .extend(candidates.into_iter().chain(tombstones));
                RefreshOutputs::default()
            }
        }
    }

    /// Drop retained upper files (and inert whiteouts) whose content a seal has published and
    /// the lower now serves byte-identically — the incremental, non-destructive counterpart of
    /// [`OverlayFs::clear_upper`]. The caller nominates candidates from its sealed index; this
    /// method owns the liveness checks and skips, never deletes, anything it cannot prove
    /// quiescent:
    ///
    /// - a path with an open upper handle is reported in `held_open` (a live writer's
    ///   descriptor must keep resolving to the linked file);
    /// - a path with a dirty-index entry was mutated after its seal and is silently skipped
    ///   (it is not retained — the next seal owns it);
    /// - a path under a pending rename is skipped (the redirect table is the authority there).
    ///
    /// Runs under the exclusive side of the trim fence, so no copy-up or open can interleave
    /// with the unlinks. Empty parent directories are deliberately left in place: git has no
    /// empty trees, so a directory emptied by trim may exist nowhere in the lower — removing
    /// it would change the merged view.
    ///
    /// Dirty entries, sealer caches, and the epoch are untouched: logically nothing changed
    /// (the lower serves the same bytes), the kernel just needs fresh lookups where an upper
    /// inode was presenting (`invals`).
    pub async fn trim_retained(&self, candidates: &[String], tombstones: &[String]) -> TrimOutcome {
        let _fence = self.mutate.write().await;
        self.trim_fenced(candidates, tombstones)
    }

    /// Non-blocking [`OverlayFs::trim_retained`]: `None` when the fence is contended. The
    /// post-seal hygiene trim uses this — tokio's RwLock is write-preferring, so a BLOCKING
    /// write acquisition behind one slow in-flight copy-up (a large lower file streaming from
    /// the server) would stall every new mutating op mount-wide for the duration. Hygiene can
    /// always wait for the next seal; mount liveness cannot.
    pub fn try_trim_retained(
        &self,
        candidates: &[String],
        tombstones: &[String],
    ) -> Option<TrimOutcome> {
        let _fence = self.mutate.try_write().ok()?;
        Some(self.trim_fenced(candidates, tombstones))
    }

    fn trim_fenced(&self, candidates: &[String], tombstones: &[String]) -> TrimOutcome {
        let open_upper: std::collections::HashSet<String> = {
            let handles = self.handles.lock().expect("handle lock");
            handles
                .values()
                .filter_map(|h| match h {
                    OHandle::Upper { path, .. } => Some(path.clone()),
                    _ => None,
                })
                .collect()
        };
        let dirty: std::collections::HashSet<String> = {
            let dirty = self.dirty.lock().expect("dirty lock");
            dirty.keys().cloned().collect()
        };
        let mut out = TrimOutcome::default();
        for path in candidates {
            if dirty.contains(path) || self.redirect_covers(path) {
                continue;
            }
            if open_upper.contains(path) {
                out.held_open.push(path.clone());
                continue;
            }
            let abs = self.upper_path(path);
            if abs.symlink_metadata().is_err() {
                // Already gone (an earlier trim, or a stale seal record) — report it trimmed
                // so the caller drops the record.
                out.trimmed.push(path.clone());
                continue;
            }
            match std::fs::remove_file(&abs) {
                Ok(()) => out.trimmed.push(path.clone()),
                Err(e) => {
                    // A path that could not be dropped still shadows the lower; report it
                    // like a pinned file so a sync pre-flight refuses instead of proceeding
                    // over a silently retained copy.
                    eprintln!("trim: dropping retained {path} failed: {e}");
                    out.held_open.push(path.clone());
                }
            }
        }
        for path in tombstones {
            // A whiteout under a pending rename can be load-bearing (it hides remapped lower
            // content inside the renamed tree); the redirect table, not the sealed delete,
            // is the authority there.
            if dirty.contains(path) || self.redirect_covers(path) {
                continue;
            }
            // An inert whiteout: the sealed delete is in the lower's history, so the marker
            // no longer hides anything — removing it does not change the merged view and
            // needs no invalidation.
            match std::fs::remove_file(self.wh_path(path)) {
                Ok(()) => out.tombstones_removed.push(path.clone()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    out.tombstones_removed.push(path.clone());
                }
                Err(e) => eprintln!("trim: dropping whiteout {path} failed: {e}"),
            }
        }
        out.invals = self.invals_for(&out.trimmed);
        out
    }

    /// Whether any pending directory renames await the next seal.
    pub fn has_redirects(&self) -> bool {
        !self.redirects.lock().expect("redirect lock").is_empty()
    }

    /// The pending rename table (destination -> true-lower source), for status display.
    pub fn redirect_entries(&self) -> Vec<(String, String)> {
        let redirects = self.redirects.lock().expect("redirect lock");
        let mut entries: Vec<(String, String)> = redirects
            .iter()
            .map(|(d, s)| (d.clone(), s.clone()))
            .collect();
        entries.sort();
        entries
    }

    /// Expand every pending rename into the per-file upserts a seal must publish: each file
    /// the destination serves from the lower, as `(merged path, blob oid, git mode)` — the
    /// content already exists server-side, so a sealer commits these **by oid reference**,
    /// uploading nothing. Children the overlay already covers otherwise are skipped: upper
    /// copy-ups seal through the regular walk, whiteouts are deletions.
    pub async fn expand_redirects(&self) -> Result<Vec<RedirectSeal>, MountError> {
        let pending = self.redirect_entries();
        let mut out = Vec::new();
        for (dst, src) in pending {
            let mut files = Vec::new();
            let mut dirs = vec![dst.clone()];
            while let Some(dir) = dirs.pop() {
                let dir_attr = self.walk_lower(&dir).await?;
                let fh = match self.core.opendir(dir_attr.ino) {
                    Ok(fh) => fh,
                    Err(e) => {
                        self.core.forget(dir_attr.ino, 1);
                        return Err(e);
                    }
                };
                let mut offset = 0u64;
                let result: Result<(), MountError> = async {
                    loop {
                        let page = settle_lower!(self.core.readdir(fh, offset, 4096).await)?;
                        if page.is_empty() {
                            return Ok(());
                        }
                        offset = page.last().expect("nonempty page").next_offset;
                        for entry in page {
                            let child = Self::child_path(&dir, &entry.name);
                            if self.whited_out(&child) {
                                continue;
                            }
                            match entry.kind {
                                NodeKind::Dir => dirs.push(child),
                                NodeKind::File | NodeKind::Symlink => {
                                    // An upper file shadows the lower one; the regular seal
                                    // walk publishes it.
                                    if self.upper_meta(&child).is_some() {
                                        continue;
                                    }
                                    let attr = settle_lower!(
                                        self.core.lookup(dir_attr.ino, &entry.name).await
                                    )?;
                                    let oid = attr.oid.clone();
                                    let perm = attr.perm;
                                    self.core.forget(attr.ino, 1);
                                    if oid.is_empty() {
                                        // Sealing by reference needs the identity; publishing
                                        // without it would silently drop the file.
                                        return Err(MountError::Protocol(format!(
                                            "no oid for {child} under pending rename {dst}"
                                        )));
                                    }
                                    let mode = match entry.kind {
                                        NodeKind::Symlink => 0o120000,
                                        _ if perm & 0o111 != 0 => 0o100755,
                                        _ => 0o100644,
                                    };
                                    files.push(RedirectFile {
                                        path: child,
                                        oid,
                                        mode,
                                    });
                                }
                            }
                        }
                    }
                }
                .await;
                self.core.releasedir(fh);
                self.core.forget(dir_attr.ino, 1);
                result?;
            }
            out.push(RedirectSeal { dst, src, files });
        }
        Ok(out)
    }

    /// Drop pending renames that a previous seal already published: once the lower advances
    /// to the sealed commit, the tree serves each destination directly and the recorded
    /// source is gone — remapping through the entry would dangle. Detected, never assumed:
    /// an entry is reaped only when its source is absent **and** its destination present in
    /// the current lower. Heals the crash/race window between a seal landing and its
    /// [`Self::consume_redirects`]. Returns the consumed destinations.
    pub async fn reap_sealed_redirects(&self) -> Result<Vec<String>, MountError> {
        let mut consumed = Vec::new();
        for (dst, src) in self.redirect_entries() {
            match self.walk_true_lower(&src).await {
                Err(MountError::NotFound(_)) => {}
                Ok(attr) => {
                    self.core.forget(attr.ino, 1);
                    continue;
                }
                Err(e) => return Err(e),
            }
            match self.walk_true_lower(&dst).await {
                Ok(attr) => {
                    self.core.forget(attr.ino, 1);
                    consumed.push(dst);
                }
                Err(MountError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        if !consumed.is_empty() {
            self.consume_redirects(&consumed)?;
        }
        Ok(consumed)
    }

    /// Drop pending renames a seal just published: the commit the workspace ref now points at
    /// serves their destinations directly. Callers must refresh the lower **first** — dropping
    /// against the old commit would leave the destinations unresolvable.
    pub fn consume_redirects(&self, dsts: &[String]) -> Result<(), MountError> {
        {
            let mut redirects = self.redirects.lock().expect("redirect lock");
            let before = redirects.len();
            redirects.retain(|k, _| !dsts.contains(k));
            if redirects.len() == before {
                return Ok(());
            }
            self.persist_redirects(&redirects)?;
        }
        self.redirect_gen.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// One pending rename, expanded for sealing: every file the destination serves from the
/// lower, ready to commit by oid reference.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RedirectSeal {
    pub dst: String,
    pub src: String,
    pub files: Vec<RedirectFile>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RedirectFile {
    /// Merged-namespace path (under the destination root).
    pub path: String,
    /// Git blob oid (40-hex) of the content, already present server-side.
    pub oid: String,
    /// Git mode (`0o100644`, `0o100755`, `0o120000`).
    pub mode: u32,
}

/// Translate a lower-coordinate path to its merged (kernel-visible) coordinates through an
/// inverted redirect table (see [`OverlayFs::reverse_redirects`]).
fn merged_of(reverse: &[(String, String)], lower: &str) -> String {
    for (src, dst) in reverse {
        if lower == src {
            return dst.clone();
        }
        if let Some(rest) = lower.strip_prefix(&format!("{src}/")) {
            return format!("{dst}/{rest}");
        }
    }
    lower.to_string()
}

/// Everything a refresh delta implies for kernel-cache convergence, produced in one pass by
/// [`OverlayFs::refresh_outputs`]: push invalidations for the Linux FUSE notify sink, and probe
/// expectations for consumers without a notify channel (macOS — drained to `tl fs sync` through
/// the daemon's `refresh` control reply).
#[derive(Debug, Default)]
pub struct RefreshOutputs {
    pub invals: Vec<OverlayInval>,
    pub expectations: Vec<KernelExpectation>,
}

/// One merged-coordinate kernel-view expectation: after the refresh, `path` should resolve
/// (`present`, with `size` bytes when a file's content changed — the prober must purge cached
/// pages, not just confirm existence) or be gone. Serialized verbatim into the daemon's
/// `refresh` control reply (`"changed"`), and parsed back by `tl fs sync` — one shared type so
/// the wire shape cannot drift between the two sides.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KernelExpectation {
    pub path: String,
    pub present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

/// One kernel invalidation in the overlay's ino space, produced by [`OverlayFs::refresh_outputs`]
/// (branch refresh) or [`OverlayFs::clear_upper`] (post-snapshot upper drop). `staled` entries
/// need the `(parent_ino, name)` dentry invalidated in addition to the inode, so the next access
/// re-looks the path up; rebound entries only need the inode's attrs/data dropped. For a path
/// that newly appeared at a refresh there is no ino to invalidate: `ino` carries the parent
/// directory (its readdir cache went stale) and `(parent_ino, name)` drops any negative dentry
/// the kernel cached for the name before it existed.
///
/// Consumed by the Linux FUSE notify sink (`inval_entry`/`inval_inode`); the macOS FSKit path
/// uses a no-op sink, so these fields are unread there.
#[derive(Clone, Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct OverlayInval {
    pub ino: u64,
    pub parent_ino: Option<u64>,
    pub name: String,
    pub staled: bool,
}

/// What one [`OverlayFs::trim_retained`] pass did. `trimmed` includes candidates that were
/// already gone from the upper (the caller drops their seal records either way); `held_open`
/// names candidates that could NOT be dropped and still shadow the lower — pinned by a live
/// upper handle, or an unlink failure — retryable once the pin clears.
#[derive(Default)]
pub struct TrimOutcome {
    pub trimmed: Vec<String>,
    pub tombstones_removed: Vec<String>,
    pub held_open: Vec<String>,
    pub invals: Vec<OverlayInval>,
}

#[cfg(test)]
mod tests {
    // Overlay integration tests: drive `OverlayFs` — the exact surface the FUSE glue
    // translates — against a live local artifact-storage server, proving merged-namespace
    // semantics (copy-up, whiteouts, merged readdir, snapshot round-trip through the workspace
    // ref) without a kernel. Requires `gsvc-server` on 127.0.0.1:8080 in open-auth mode:
    // `cargo test -p tensorlake-cli overlay -- --ignored --nocapture`.

    use super::*;
    use gsvc_mount::NodeKind;

    // Integration tests for the `tl fs` overlay: the writable layer over the read-only mount core.
    //
    // These drive `OverlayFs` directly — the exact surface the FUSE glue translates — against a
    // live local artifact-storage server, so every merged-namespace semantic (copy-up, whiteouts,
    // merged readdir, snapshot round-trip through the workspace ref) is proven without a kernel.
    //
    // Requires `gsvc-server` on 127.0.0.1:8080 in open-auth mode (skips cleanly when absent):
    // `cargo run -p gsvc-server` in an artifact_storage checkout, then
    // `cargo test -p tensorlake-cli --test overlay_fs -- --ignored --nocapture`.
    use std::sync::Arc;

    use gsvc_mount::{FsClient, MountCore, MountOptions, ROOT_INO};
    use tensorlake::ClientBuilder;

    use tensorlake::artifact_storage::ArtifactStorageClient;
    use tensorlake::artifact_storage::ingest::{PushFile, PushOptions, PushSource};
    use tensorlake::artifact_storage::workspaces::CreateWorkspaceRequest;

    // ---------------------------------------------------------------------------------------
    // Inode-table re-keying (pure, no server): the rename fix that lets `git init`/`git clone`
    // work on the mount. A FUSE rename keeps the source nodeid and re-points its dentry at the
    // destination, so the node's path must follow or ino-based ops resolve the vacated source
    // path and return ENOENT.
    // ---------------------------------------------------------------------------------------

    #[test]
    fn inode_table_rename_rekeys_source_ino_to_destination() {
        let mut t = InodeTable::new();
        // A file created in the upper (git's `config.lock`) — no lower binding.
        let (ino, _, _) = t.intern("config.lock".to_string(), None);
        let released = t.rename("config.lock", "config");
        assert!(
            released.is_empty(),
            "pure-upper rename releases no core refs"
        );
        assert!(
            !t.index.contains_key("config.lock"),
            "source path is vacated"
        );
        assert_eq!(
            t.index.get("config").copied(),
            Some(ino),
            "the same nodeid now resolves to the destination"
        );
        assert_eq!(
            t.get(ino).unwrap().path,
            "config",
            "the node's path followed the rename"
        );
    }

    #[test]
    fn inode_table_rename_carries_interned_subtree() {
        let mut t = InodeTable::new();
        let (dir, _, _) = t.intern("d".to_string(), None);
        let (child, _, _) = t.intern("d/x".to_string(), None);
        let released = t.rename("d", "d2");
        assert!(released.is_empty());
        assert_eq!(t.index.get("d2").copied(), Some(dir));
        assert_eq!(
            t.index.get("d2/x").copied(),
            Some(child),
            "descendants move with the directory"
        );
        assert_eq!(t.get(child).unwrap().path, "d2/x");
        assert!(!t.index.contains_key("d/x"));
    }

    #[test]
    fn inode_table_rename_does_not_touch_prefix_siblings() {
        let mut t = InodeTable::new();
        let (_, _, _) = t.intern("d".to_string(), None);
        let (sibling, _, _) = t.intern("dxy".to_string(), None);
        let _ = t.rename("d", "d2");
        assert_eq!(
            t.index.get("dxy").copied(),
            Some(sibling),
            "a name that merely shares the prefix is not a descendant"
        );
    }

    #[test]
    fn inode_table_rename_overwrite_orphans_destination_until_forgotten() {
        let mut t = InodeTable::new();
        let (src, _, _) = t.intern("src".to_string(), None);
        let (dst, _, _) = t.intern("dst".to_string(), None);
        let _ = t.rename("src", "dst");
        assert_eq!(
            t.index.get("dst").copied(),
            Some(src),
            "destination path resolves to the moved (source) nodeid"
        );
        // The overwritten node lingers by ino until the kernel forgets it; forgetting it must not
        // evict the re-keyed entry that now owns its former path.
        let dropped = t.forget(dst, 1);
        assert!(dropped.is_some(), "orphan is dropped at zero lookups");
        assert_eq!(
            t.index.get("dst").copied(),
            Some(src),
            "forgetting the orphan preserves the live entry"
        );
    }

    fn table(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(d, s)| (d.to_string(), s.to_string()))
            .collect()
    }

    #[test]
    fn redirect_remap_follows_longest_prefix() {
        let t = table(&[("moved", "old"), ("moved/inner2", "elsewhere/inner")]);
        assert_eq!(remap_through_redirects(&t, "moved"), "old");
        assert_eq!(remap_through_redirects(&t, "moved/a/b.txt"), "old/a/b.txt");
        // The deeper entry wins for its own subtree.
        assert_eq!(
            remap_through_redirects(&t, "moved/inner2/x"),
            "elsewhere/inner/x"
        );
        // Prefix-shaped but not a path prefix: no remap.
        assert_eq!(remap_through_redirects(&t, "movedx"), "movedx");
        assert_eq!(remap_through_redirects(&t, "untouched/f"), "untouched/f");
    }

    #[test]
    fn record_committed_rename_composes_chains_and_carries_nested_entries() {
        let mut t = HashMap::new();
        // mv a b — first move records true coordinates.
        record_committed_rename(&mut t, "a", "b", "a".to_string());
        assert_eq!(t.get("b").map(String::as_str), Some("a"));
        // mv committed dir inside the pending tree: b/sub -> c (true src resolves through b).
        let true_src = remap_through_redirects(&t, "b/sub");
        record_committed_rename(&mut t, "b/sub", "c", true_src);
        assert_eq!(t.get("c").map(String::as_str), Some("a/sub"), "one hop");
        // mv b d — the root re-keys, nothing chains through the dead name.
        let true_src = remap_through_redirects(&t, "b");
        record_committed_rename(&mut t, "b", "d", true_src);
        assert!(!t.contains_key("b"));
        assert_eq!(t.get("d").map(String::as_str), Some("a"));
        // A nested pending rename rides an ancestor move: mv x d/y, then mv d e.
        record_committed_rename(&mut t, "x", "d/y", "x".to_string());
        let true_src = remap_through_redirects(&t, "d");
        record_committed_rename(&mut t, "d", "e", true_src);
        assert_eq!(t.get("e").map(String::as_str), Some("a"));
        assert_eq!(
            t.get("e/y").map(String::as_str),
            Some("x"),
            "nested entry follows its new ancestor name"
        );
        assert!(!t.contains_key("d/y"));
    }

    #[test]
    fn record_committed_rename_overwrite_drops_the_displaced_entry() {
        let mut t = table(&[("dst", "old1"), ("dst/nested", "old2")]);
        record_committed_rename(&mut t, "src", "dst", "src".to_string());
        assert_eq!(t.len(), 1);
        assert_eq!(
            t.get("dst").map(String::as_str),
            Some("src"),
            "the replaced destination's entries die with it"
        );
    }

    #[test]
    fn whiteout_shielding_shows_redirects_under_ancestor_markers() {
        // Defense-in-depth configuration: a subtree marker on an ancestor of a live pending
        // rename. Normal flows can't build it (recreating the ancestor clears its marker, and
        // a destination needs a visible parent), but if it ever arises the entry — which by
        // the drop-on-remove invariant postdates any marker above it — must win.
        let tmp = tempfile::tempdir().expect("tempdir");
        let wh = tmp.path();
        std::fs::write(wh.join("dir"), b"").unwrap();
        let t = table(&[("dir/moved", "old")]);
        assert!(!whited_out_under(wh, &t, "dir/moved"));
        assert!(
            !whited_out_under(wh, &t, "dir/moved/child.txt"),
            "content under the destination shows too"
        );
        assert!(
            whited_out_under(wh, &t, "dir/other"),
            "the marker still hides everything the rename does not cover"
        );
        // A marker AT the destination root itself is not shielded by its own entry.
        let t2 = table(&[("dir", "old")]);
        assert!(
            whited_out_under(wh, &t2, "dir"),
            "a marker at the root outranks the entry"
        );
    }

    #[test]
    fn whiteouts_inside_a_renamed_tree_still_apply() {
        // The normal case: files deleted under a pending-rename destination carry ordinary
        // child markers, and the walk past the (unmarked) ancestors must honor them.
        let tmp = tempfile::tempdir().expect("tempdir");
        let wh = tmp.path();
        std::fs::create_dir_all(wh.join("moved")).unwrap();
        std::fs::write(wh.join("moved/gone.txt"), b"").unwrap();
        let t = table(&[("moved", "old")]);
        assert!(whited_out_under(wh, &t, "moved/gone.txt"));
        assert!(!whited_out_under(wh, &t, "moved/kept.txt"));
        assert!(
            !whited_out_under(wh, &t, "moved"),
            "the container dir is not a marker"
        );
    }

    const BASE: &str = "http://127.0.0.1:8080";
    const PROJECT: &str = "overlaytest";
    const TOKEN: &str = "devtoken";

    fn server_up() -> bool {
        std::net::TcpStream::connect_timeout(
            &"127.0.0.1:8080".parse().unwrap(),
            std::time::Duration::from_millis(500),
        )
        .is_ok()
    }

    fn sdk() -> ArtifactStorageClient {
        let client = ClientBuilder::new(BASE)
            .bearer_token("dummy")
            .build()
            .unwrap();
        ArtifactStorageClient::new(client, BASE).unwrap()
    }

    fn push_file(path: &str, bytes: &[u8], mode: u32) -> PushFile {
        PushFile {
            repo_path: path.to_string(),
            source: PushSource::Bytes(bytes.to_vec()),
            mode: Some(mode),
            delete: false,
        }
    }

    async fn read_all(fs: &OverlayFs, parent: u64, name: &str) -> Vec<u8> {
        let attr = fs.lookup(parent, name).await.unwrap();
        let (fh, _) = fs.open(attr.ino, false).await.unwrap();
        let mut out = Vec::new();
        loop {
            let chunk = fs.read(fh, out.len() as u64, 1 << 20).await.unwrap();
            if chunk.is_empty() {
                break;
            }
            out.extend_from_slice(&chunk);
        }
        fs.release(fh);
        fs.forget(attr.ino, 1);
        out
    }

    async fn dir_names(fs: &OverlayFs, ino: u64) -> Vec<String> {
        let fh = fs.opendir(ino).await.unwrap();
        let entries = fs.readdir(fh, 0, 10_000).unwrap();
        fs.releasedir(fh);
        entries.into_iter().map(|e| e.name).collect()
    }

    /// The `tl fs` product surface's wire contract (server >= repo-kinds): filesystems are
    /// `kind=filesystem` repos, `?kind=` partitions the listing, and a fresh filesystem's
    /// genesis commit lets a publish-on-save workspace be created with no push ever made.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080"]
    async fn filesystem_kind_contract_roundtrips() {
        if !server_up() {
            eprintln!("skipping: no local artifact-storage server");
            return;
        }
        let sdk = sdk();
        let fs_name = format!(
            "fskind-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        sdk.create_repo_with_credential(PROJECT, &fs_name, None, Some("filesystem"), "t", TOKEN)
            .await
            .unwrap();
        let filesystems = sdk
            .list_repos_with_credential(PROJECT, Some("filesystem"), "t", TOKEN)
            .await
            .unwrap()
            .into_inner();
        let entry = filesystems
            .repos
            .iter()
            .find(|r| r.name == fs_name)
            .expect("created filesystem is in the kind=filesystem listing");
        assert_eq!(entry.kind, "filesystem");
        let repositories = sdk
            .list_repos_with_credential(PROJECT, Some("repository"), "t", TOKEN)
            .await
            .unwrap()
            .into_inner();
        assert!(
            !repositories.repos.iter().any(|r| r.name == fs_name),
            "a filesystem must not appear in the repository listing"
        );
        // The genesis commit is what makes `tl fs create` + mount work with no client push,
        // and `surface` is what makes it ONE round trip: the SERVER fills the publish target
        // from the repo's stored default branch — the client never reads a listing.
        let ws = sdk
            .create_workspace(
                PROJECT,
                &fs_name,
                "t",
                TOKEN,
                &CreateWorkspaceRequest {
                    surface: Some("filesystem".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(ws.base_ref.as_deref(), Some("refs/heads/main"));
        assert_eq!(ws.shared_target.as_deref(), Some("main"));

        // The authoritative meta point-read answers immediately after create — this is the
        // rm/validation path's read-your-writes guarantee.
        let meta = sdk
            .repo_meta_with_credential(PROJECT, &fs_name, "t", TOKEN)
            .await
            .unwrap()
            .into_inner();
        assert!(meta.is_filesystem());
        assert_eq!(meta.default_branch, "main");

        // An fs-surface session on a repository fails closed with the right command.
        let repo_name = format!("{fs_name}-repo");
        sdk.create_repo_with_credential(PROJECT, &repo_name, None, None, "t", TOKEN)
            .await
            .unwrap();
        let err = sdk
            .create_workspace(
                PROJECT,
                &repo_name,
                "t",
                TOKEN,
                &CreateWorkspaceRequest {
                    surface: Some("filesystem".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("tl git mount"),
            "kind mismatch must name the replacement, got: {err}"
        );
        sdk.delete_repo_with_credential(PROJECT, &repo_name, "t", TOKEN)
            .await
            .unwrap();
        // Pin `tl fs history`'s save definition to the server's operation vocabulary: the
        // genesis is a committed "push" op, i.e. exactly what the history filter
        // (kind in {push, reconcile, promote, merge} AND result == "committed") must match.
        // If the server ever renames these strings, this fails instead of history silently
        // showing "No saves yet" for a live filesystem.
        let ops = sdk
            .list_operations_with_credential(PROJECT, &fs_name, "t", TOKEN)
            .await
            .unwrap()
            .into_inner();
        assert!(
            ops.operations
                .iter()
                .any(|op| op.kind == "push" && op.result == "committed"),
            "genesis must appear as a committed push op; got kinds/results: {:?}",
            ops.operations
                .iter()
                .map(|op| (op.kind.clone(), op.result.clone()))
                .collect::<Vec<_>>()
        );
        sdk.delete_workspace(PROJECT, &fs_name, "t", TOKEN, &ws.id)
            .await
            .unwrap();
        sdk.delete_repo_with_credential(PROJECT, &fs_name, "t", TOKEN)
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080"]
    async fn overlay_merges_copies_up_whiteouts_and_snapshots() {
        if !server_up() {
            eprintln!("skipping: no local artifact-storage server");
            return;
        }
        let sdk = sdk();
        let repo = format!(
            "ofs-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let project_repo = format!("{PROJECT}/{repo}");
        let _ = project_repo;
        sdk.create_repo_with_credential(PROJECT, &repo, None, None, "t", TOKEN)
            .await
            .unwrap();
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![
                push_file("README.md", b"# seed\n", 0o100644),
                push_file("dir/a.txt", b"alpha\n", 0o100644),
                push_file("dir/b.txt", b"beta\n", 0o100644),
                push_file("bin/run.sh", b"#!/bin/sh\n", 0o100755),
            ],
            PushOptions {
                message: "seed".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let ws = sdk
            .create_workspace(
                PROJECT,
                &repo,
                "t",
                TOKEN,
                &CreateWorkspaceRequest::default(),
            )
            .await
            .unwrap()
            .into_inner();

        // The mount core follows the workspace ref; the overlay sits on a temp state dir.
        let client = FsClient::new(BASE, PROJECT, &repo, Some(TOKEN.to_string())).unwrap();
        let core = MountCore::new(
            client,
            MountOptions {
                reference: ws.ref_name.clone(),
                follow: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let state = tempfile::tempdir().unwrap();
        let fs: Arc<OverlayFs> = OverlayFs::new(core.clone(), state.path(), false).unwrap();

        // A read-only overlay over the same core answers EROFS for every mutation.
        let ro_state = tempfile::tempdir().unwrap();
        let ro: Arc<OverlayFs> = OverlayFs::new(core.clone(), ro_state.path(), true).unwrap();
        assert!(matches!(
            ro.mkdir(gsvc_mount::ROOT_INO, "nope").await,
            Err(MountError::ReadOnly)
        ));
        assert!(matches!(
            ro.create(gsvc_mount::ROOT_INO, "nope.txt", false).await,
            Err(MountError::ReadOnly)
        ));
        assert!(matches!(
            ro.open(gsvc_mount::ROOT_INO, true).await,
            Err(MountError::ReadOnly)
        ));

        // 1. Lower reads through the merged view.
        assert_eq!(read_all(&fs, ROOT_INO, "README.md").await, b"# seed\n");
        let names = dir_names(&fs, ROOT_INO).await;
        assert_eq!(names, vec!["README.md", "bin", "dir"]);
        let dir = fs.lookup(ROOT_INO, "dir").await.unwrap();
        assert_eq!(dir_names(&fs, dir.ino).await, vec!["a.txt", "b.txt"]);
        let run = fs.lookup(ROOT_INO, "bin").await.unwrap();
        let run_sh = fs.lookup(run.ino, "run.sh").await.unwrap();
        assert_eq!(run_sh.perm & 0o111, 0o111, "exec bit survives the lower");
        assert!(!run_sh.upper);

        // readdirplus: entries carry counted (ino, attr) pairs consistent with lookup.
        let root_fh = fs.opendir(ROOT_INO).await.unwrap();
        let plus = fs.readdir_plus(root_fh, 0, 100).await.unwrap();
        fs.releasedir(root_fh);
        assert_eq!(plus.len(), 3);
        let (_, dir_attr) = plus.iter().find(|(e, _)| e.name == "dir").unwrap();
        assert_eq!(dir_attr.ino, dir.ino, "readdirplus interns the same ino");
        let (_, readme_attr) = plus.iter().find(|(e, _)| e.name == "README.md").unwrap();
        assert_eq!(readme_attr.size, "# seed\n".len() as u64);
        for (_, attr) in &plus {
            fs.forget(attr.ino, 1); // balance the readdirplus references
        }

        // 2. Copy-up on first write; content merges; the upper layer holds exactly the dirty file.
        let readme = fs.lookup(ROOT_INO, "README.md").await.unwrap();
        let (fh, _) = fs.open(readme.ino, true).await.unwrap();
        fs.write(fh, 0, b"# edited\n").unwrap();
        fs.setattr(readme.ino, Some(9), None).await.unwrap();
        fs.release(fh);
        assert_eq!(read_all(&fs, ROOT_INO, "README.md").await, b"# edited\n");
        assert!(state.path().join("upper/README.md").is_file());
        assert!(
            !state.path().join("upper/dir").exists(),
            "reads never copy up"
        );
        let readme2 = fs.lookup(ROOT_INO, "README.md").await.unwrap();
        assert!(readme2.upper, "dirty file is upper-backed");

        // 3. Create / mkdir / symlink land in upper and merge into readdir.
        let (created, cfh) = fs.create(ROOT_INO, "new.txt", false).await.unwrap();
        fs.write(cfh, 0, b"fresh\n").unwrap();
        fs.release(cfh);
        fs.forget(created.ino, 1);
        fs.mkdir(ROOT_INO, "made").await.unwrap();
        fs.symlink(ROOT_INO, "lnk", "README.md").await.unwrap();
        let names = dir_names(&fs, ROOT_INO).await;
        assert_eq!(
            names,
            vec!["README.md", "bin", "dir", "lnk", "made", "new.txt"]
        );
        let lnk = fs.lookup(ROOT_INO, "lnk").await.unwrap();
        assert!(matches!(lnk.kind, NodeKind::Symlink));
        assert_eq!(fs.readlink(lnk.ino).await.unwrap().as_ref(), b"README.md");

        // 4. Whiteouts: unlink a lower file — gone from lookup and readdir, marker recorded.
        fs.unlink(dir.ino, "b.txt").await.unwrap();
        assert!(fs.lookup(dir.ino, "b.txt").await.is_err());
        assert_eq!(dir_names(&fs, dir.ino).await, vec!["a.txt"]);
        assert!(state.path().join("wh/dir/b.txt").is_file());

        // 5. rmdir refuses non-empty (as ENOTEMPTY, not the Protocol->EIO catch-all); empties
        //    then whiteouts a lower dir.
        assert!(matches!(
            fs.rmdir(ROOT_INO, "dir").await,
            Err(MountError::NotEmpty)
        ));
        fs.unlink(dir.ino, "a.txt").await.unwrap();
        fs.rmdir(ROOT_INO, "dir").await.unwrap();
        assert!(fs.lookup(ROOT_INO, "dir").await.is_err());

        // 6. Rename of a lower file: copy-up at destination + whiteout at source.
        fs.rename(run.ino, "run.sh", ROOT_INO, "run2.sh")
            .await
            .unwrap();
        assert!(fs.lookup(run.ino, "run.sh").await.is_err());
        assert_eq!(read_all(&fs, ROOT_INO, "run2.sh").await, b"#!/bin/sh\n");

        // 7. Snapshot round-trip: push the overlay's dirty set to the workspace ref, follow the
        //    ref, clear the overlay — the merged view must be unchanged, now served by the lower.
        let before_commit = core.current_commit();
        let mut files = vec![
            PushFile {
                repo_path: "README.md".into(),
                source: PushSource::Path(state.path().join("upper/README.md")),
                mode: Some(0o100644),
                delete: false,
            },
            push_file("new.txt", b"fresh\n", 0o100644),
            PushFile {
                repo_path: "lnk".into(),
                source: PushSource::Bytes(b"README.md".to_vec()),
                mode: Some(0o120000),
                delete: false,
            },
            push_file("run2.sh", b"#!/bin/sh\n", 0o100755),
        ];
        for gone in ["dir/a.txt", "dir/b.txt", "bin/run.sh"] {
            files.push(PushFile {
                repo_path: gone.into(),
                source: PushSource::Bytes(Vec::new()),
                mode: None,
                delete: true,
            });
        }
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            files,
            PushOptions {
                message: "overlay snapshot".into(),
                workspace_snapshot: Some(ws.id.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(
            core.poll_ref().await.unwrap().is_some(),
            "ref moved to the snapshot"
        );
        assert_ne!(core.current_commit(), before_commit);
        fs.clear_upper().unwrap();
        assert!(!state.path().join("upper/README.md").exists());

        // Fresh lookups now serve the snapshot from the lower layer, identically.
        assert_eq!(read_all(&fs, ROOT_INO, "README.md").await, b"# edited\n");
        assert_eq!(read_all(&fs, ROOT_INO, "new.txt").await, b"fresh\n");
        assert_eq!(read_all(&fs, ROOT_INO, "run2.sh").await, b"#!/bin/sh\n");
        let readme3 = fs.lookup(ROOT_INO, "README.md").await.unwrap();
        assert!(!readme3.upper, "post-snapshot content is lower-backed");
        assert!(
            fs.lookup(ROOT_INO, "dir").await.is_err(),
            "deletes committed"
        );
        // git drops empty trees: bin/ vanished with its only file.
        let names = dir_names(&fs, ROOT_INO).await;
        assert_eq!(names, vec!["README.md", "lnk", "new.txt", "run2.sh"]);

        sdk.delete_workspace(PROJECT, &repo, "t", TOKEN, &ws.id)
            .await
            .unwrap();
    }

    /// Committed-directory rename end to end: the pending-rename redirect serves reads at the
    /// destination without materializing anything, local edits and deletes inside the moved
    /// tree layer on top, the seal publishes by-oid references plus the source delete, and
    /// after the ref advances the reaped/cleared overlay serves the destination from the
    /// lower — byte-identical.
    #[tokio::test]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080"]
    async fn overlay_renames_committed_directory_and_seals_by_reference() {
        if !server_up() {
            eprintln!("skipping: no local artifact-storage server");
            return;
        }
        let sdk = sdk();
        let repo = format!(
            "ofsmv-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        sdk.create_repo_with_credential(PROJECT, &repo, None, None, "t", TOKEN)
            .await
            .unwrap();
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![
                push_file("keep.md", b"stay\n", 0o100644),
                push_file("pkg/lib.rs", b"pub fn lib() {}\n", 0o100644),
                push_file("pkg/sub/deep.txt", b"deep\n", 0o100644),
                push_file("pkg/tool.sh", b"#!/bin/sh\n", 0o100755),
                push_file("pkg/gone.txt", b"delete me\n", 0o100644),
            ],
            PushOptions {
                message: "seed".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let ws = sdk
            .create_workspace(
                PROJECT,
                &repo,
                "t",
                TOKEN,
                &CreateWorkspaceRequest::default(),
            )
            .await
            .unwrap()
            .into_inner();
        let client = FsClient::new(BASE, PROJECT, &repo, Some(TOKEN.to_string())).unwrap();
        let core = MountCore::new(
            client,
            MountOptions {
                reference: ws.ref_name.clone(),
                follow: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let state = tempfile::tempdir().unwrap();
        let fs: Arc<OverlayFs> = OverlayFs::new(core.clone(), state.path(), false).unwrap();

        // 1. mv pkg moved — instant, nothing materializes.
        fs.rename(ROOT_INO, "pkg", ROOT_INO, "moved").await.unwrap();
        assert!(fs.lookup(ROOT_INO, "pkg").await.is_err(), "source gone");
        assert!(
            !state.path().join("upper/moved").exists(),
            "no copy-up happened"
        );
        assert_eq!(dir_names(&fs, ROOT_INO).await, vec!["keep.md", "moved"]);
        let moved = fs.lookup(ROOT_INO, "moved").await.unwrap();
        assert!(matches!(moved.kind, NodeKind::Dir));
        assert_eq!(
            dir_names(&fs, moved.ino).await,
            vec!["gone.txt", "lib.rs", "sub", "tool.sh"]
        );
        assert_eq!(
            read_all(&fs, moved.ino, "lib.rs").await,
            b"pub fn lib() {}\n"
        );
        let tool = fs.lookup(moved.ino, "tool.sh").await.unwrap();
        assert_eq!(tool.perm & 0o111, 0o111, "exec bit survives the remap");
        fs.forget(tool.ino, 1);
        let sub = fs.lookup(moved.ino, "sub").await.unwrap();
        assert_eq!(read_all(&fs, sub.ino, "deep.txt").await, b"deep\n");

        // 2. Edits inside the moved tree: an upper copy-up shadows, a delete whiteouts.
        let lib = fs.lookup(moved.ino, "lib.rs").await.unwrap();
        let (fh, _) = fs.open(lib.ino, true).await.unwrap();
        fs.write(fh, 0, b"pub fn lib2() {}\n").unwrap();
        fs.setattr(lib.ino, Some(17), None).await.unwrap();
        fs.release(fh);
        fs.forget(lib.ino, 1);
        fs.unlink(moved.ino, "gone.txt").await.unwrap();
        assert_eq!(
            dir_names(&fs, moved.ino).await,
            vec!["lib.rs", "sub", "tool.sh"]
        );
        assert_eq!(
            read_all(&fs, moved.ino, "lib.rs").await,
            b"pub fn lib2() {}\n"
        );

        // 3. A second move re-keys the same entry (chain stays one hop).
        fs.rename(ROOT_INO, "moved", ROOT_INO, "moved2")
            .await
            .unwrap();
        assert!(fs.lookup(ROOT_INO, "moved").await.is_err());
        fs.forget(moved.ino, 1);
        fs.forget(sub.ino, 1);
        let moved2 = fs.lookup(ROOT_INO, "moved2").await.unwrap();
        assert_eq!(
            dir_names(&fs, moved2.ino).await,
            vec!["lib.rs", "sub", "tool.sh"]
        );
        assert_eq!(
            read_all(&fs, moved2.ino, "lib.rs").await,
            b"pub fn lib2() {}\n",
            "the copy-up rode the second move"
        );
        assert_eq!(fs.redirect_entries(), vec![("moved2".into(), "pkg".into())]);

        // 4. Seal exactly as `tl fs snapshot` would: the expansion's by-oid upserts (the
        //    upper-shadowed lib.rs and whiteouted gone.txt are excluded), the regular upsert
        //    for the copy-up, and the source delete the whiteout produced.
        let seals = fs.expand_redirects().await.unwrap();
        assert_eq!(seals.len(), 1);
        let mut sealed_paths: Vec<&str> = seals[0].files.iter().map(|f| f.path.as_str()).collect();
        sealed_paths.sort();
        assert_eq!(
            sealed_paths,
            vec!["moved2/sub/deep.txt", "moved2/tool.sh"],
            "shadowed and deleted children stay out of the by-oid set"
        );
        let tool_mode = seals[0]
            .files
            .iter()
            .find(|f| f.path == "moved2/tool.sh")
            .unwrap()
            .mode;
        assert_eq!(tool_mode, 0o100755, "exec mode carried by reference");
        let mut files: Vec<PushFile> = seals[0]
            .files
            .iter()
            .map(|f| PushFile {
                repo_path: f.path.clone(),
                source: PushSource::KnownOid(f.oid.clone()),
                mode: Some(f.mode),
                delete: false,
            })
            .collect();
        files.push(PushFile {
            repo_path: "moved2/lib.rs".into(),
            source: PushSource::Path(state.path().join("upper/moved2/lib.rs")),
            mode: Some(0o100644),
            delete: false,
        });
        files.push(PushFile {
            repo_path: "pkg".into(),
            source: PushSource::Bytes(Vec::new()),
            mode: None,
            delete: true,
        });
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            files,
            PushOptions {
                message: "rename snapshot".into(),
                workspace_snapshot: Some(ws.id.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // 5. Ref advance + reap: the entry is detected as sealed (src gone, dst present) and
        //    consumed; clear_upper drops the copy-up and whiteout.
        assert!(core.poll_ref().await.unwrap().is_some(), "ref moved");
        let consumed = fs.reap_sealed_redirects().await.unwrap();
        assert_eq!(consumed, vec!["moved2".to_string()]);
        assert!(!fs.has_redirects());
        fs.clear_upper().unwrap();

        // 6. The lower now serves the destination directly, byte-identical.
        fs.forget(moved2.ino, 1);
        assert!(fs.lookup(ROOT_INO, "pkg").await.is_err());
        let moved2 = fs.lookup(ROOT_INO, "moved2").await.unwrap();
        assert!(!moved2.upper);
        assert_eq!(
            dir_names(&fs, moved2.ino).await,
            vec!["lib.rs", "sub", "tool.sh"]
        );
        assert_eq!(
            read_all(&fs, moved2.ino, "lib.rs").await,
            b"pub fn lib2() {}\n"
        );
        let sub2 = fs.lookup(moved2.ino, "sub").await.unwrap();
        assert_eq!(read_all(&fs, sub2.ino, "deep.txt").await, b"deep\n");
        let tool2 = fs.lookup(moved2.ino, "tool.sh").await.unwrap();
        assert_eq!(tool2.perm & 0o111, 0o111);

        sdk.delete_workspace(PROJECT, &repo, "t", TOKEN, &ws.id)
            .await
            .unwrap();
    }

    /// Trim end to end: sealed-and-kept upper content drops in place — the merged view is
    /// unchanged, now lower-served — while a dirty path is silently skipped and a path with a
    /// live write handle is pinned and reported. This is `tl fs sync`'s pre-flight, so the
    /// liveness checks are the whole point: trim must never delete bytes no snapshot holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080"]
    async fn trim_drops_retained_keeps_dirty_and_pins_open_files() {
        if !server_up() {
            eprintln!("skipping: no local artifact-storage server");
            return;
        }
        let sdk = sdk();
        let repo = format!(
            "ofs-trim-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        sdk.create_repo_with_credential(PROJECT, &repo, None, None, "t", TOKEN)
            .await
            .unwrap();
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![push_file("README.md", b"# seed\n", 0o100644)],
            PushOptions {
                message: "seed".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let ws = sdk
            .create_workspace(
                PROJECT,
                &repo,
                "t",
                TOKEN,
                &CreateWorkspaceRequest::default(),
            )
            .await
            .unwrap()
            .into_inner();
        let client = FsClient::new(BASE, PROJECT, &repo, Some(TOKEN.to_string())).unwrap();
        let core = MountCore::new(
            client,
            MountOptions {
                reference: ws.ref_name.clone(),
                follow: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let state = tempfile::tempdir().unwrap();
        let fs: Arc<OverlayFs> = OverlayFs::new(core.clone(), state.path(), false).unwrap();

        // Three local files through the overlay.
        for (name, content) in [
            ("sealed.txt", &b"sealed\n"[..]),
            ("held.txt", b"held\n"),
            ("dirty.txt", b"dirty v1\n"),
        ] {
            let (attr, fh) = fs.create(ROOT_INO, name, false).await.unwrap();
            fs.write(fh, 0, content).unwrap();
            fs.release(fh);
            fs.forget(attr.ino, 1);
        }

        // "Seal" all three the way the daemon's sealer does: push to the workspace ref,
        // advance the lower, prune the dirty index to the watermark.
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![
                push_file("sealed.txt", b"sealed\n", 0o100644),
                push_file("held.txt", b"held\n", 0o100644),
                push_file("dirty.txt", b"dirty v1\n", 0o100644),
            ],
            PushOptions {
                message: "seal".into(),
                workspace_snapshot: Some(ws.id.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(core.poll_ref().await.unwrap().is_some());
        fs.prune_dirty(fs.current_generation());

        // Re-dirty one file and hold another open for write.
        let dirty = fs.lookup(ROOT_INO, "dirty.txt").await.unwrap();
        let (dfh, _) = fs.open(dirty.ino, true).await.unwrap();
        fs.write(dfh, 0, b"dirty v2\n").unwrap();
        fs.release(dfh);
        fs.forget(dirty.ino, 1);
        let held = fs.lookup(ROOT_INO, "held.txt").await.unwrap();
        let (held_fh, _) = fs.open(held.ino, true).await.unwrap();

        // Open a LOWER-backed file for write and close it without writing: the copy-up alone
        // must record dirt (an untracked upper copy would be invisible to every dirty view
        // and silently shadow later syncs of the path) — and being dirty, trim must skip it.
        let seeded = fs.lookup(ROOT_INO, "README.md").await.unwrap();
        let (sfh, _) = fs.open(seeded.ino, true).await.unwrap();
        fs.release(sfh);
        fs.forget(seeded.ino, 1);
        assert!(
            state.path().join("upper/README.md").is_file(),
            "copy-up materialized the upper copy"
        );

        let candidates: Vec<String> = ["sealed.txt", "held.txt", "dirty.txt", "README.md"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let outcome = fs.trim_retained(&candidates, &[]).await;
        assert_eq!(outcome.trimmed, vec!["sealed.txt"]);
        assert_eq!(outcome.held_open, vec!["held.txt"]);
        assert!(
            !state.path().join("upper/sealed.txt").exists(),
            "retained file dropped from the upper"
        );
        assert!(
            state.path().join("upper/dirty.txt").exists(),
            "dirty file untouched"
        );
        assert!(
            state.path().join("upper/held.txt").exists(),
            "open file untouched"
        );
        assert!(
            state.path().join("upper/README.md").exists(),
            "write-opened-but-never-written copy-up is dirty, not trimmable"
        );

        // The merged view is unchanged: the trimmed path now serves from the lower,
        // byte-identical; the dirty path still serves its unsealed content.
        assert_eq!(read_all(&fs, ROOT_INO, "sealed.txt").await, b"sealed\n");
        let sealed = fs.lookup(ROOT_INO, "sealed.txt").await.unwrap();
        assert!(!sealed.upper, "trimmed content is lower-backed");
        fs.forget(sealed.ino, 1);
        assert_eq!(read_all(&fs, ROOT_INO, "dirty.txt").await, b"dirty v2\n");

        // The pinned handle still reaches its linked file.
        fs.write(held_fh, 5, b"more\n").unwrap();
        fs.release(held_fh);
        fs.forget(held.ino, 1);
        assert_eq!(read_all(&fs, ROOT_INO, "held.txt").await, b"held\nmore\n");

        sdk.delete_workspace(PROJECT, &repo, "t", TOKEN, &ws.id)
            .await
            .unwrap();
    }

    /// The concurrent dir-vs-file collision: a `mkdir` lands while another writer's
    /// same-named FILE is being delivered to the followed ref (`merged_exists` answered
    /// before delivery, so no whiteout exists). The upper directory must consistently
    /// shadow the lower file — getattr, child create, child lookup, and readdir all answer
    /// upper-side, in bounded time (the old behavior probed the lower FILE as a tree,
    /// which reads as IndexNotReady and spins the settle loop 10s per probe: the silently
    /// stalled seals). The pre-seal heal then writes the missing whiteout, turning the
    /// state into a plain file→directory replacement. The reverse shape (upper FILE at a
    /// name where the lower gains a directory) stays upper-shadowed too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080"]
    async fn upper_dir_shadows_lower_file_gained_by_refresh() {
        if !server_up() {
            eprintln!("skipping: no local artifact-storage server");
            return;
        }
        let sdk = sdk();
        let repo = format!(
            "dirfile-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        sdk.create_repo_with_credential(PROJECT, &repo, None, None, "t", TOKEN)
            .await
            .unwrap();
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![push_file("README.md", b"# seed\n", 0o100644)],
            PushOptions {
                message: "seed".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let client = FsClient::new(BASE, PROJECT, &repo, Some(TOKEN.to_string())).unwrap();
        let core = MountCore::new(
            client,
            MountOptions {
                reference: "main".to_string(),
                follow: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let state = tempfile::tempdir().unwrap();
        let fs: Arc<OverlayFs> = OverlayFs::new(core.clone(), state.path(), false).unwrap();

        // The race: the local mkdir wins the local view before the remote file exists.
        let dir_attr = fs.mkdir(ROOT_INO, "swap").await.unwrap();
        assert_eq!(dir_attr.kind, NodeKind::Dir);

        // Another writer lands FILE `swap` on the followed branch; deliver it.
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![push_file("swap", b"remote file\n", 0o100644)],
            PushOptions {
                message: "remote file at the mkdir name".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut delivered = false;
        for _ in 0..100 {
            if core.poll_ref().await.unwrap().is_some() {
                delivered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(delivered, "the remote file's refresh never arrived");

        // Coherence, in bounded time — no settle-loop stalls.
        let bounded = std::time::Duration::from_secs(3);
        let attr = fs.getattr(dir_attr.ino).await.unwrap();
        assert_eq!(attr.kind, NodeKind::Dir, "upper dir keeps shadowing");
        let (inner, ifh) =
            tokio::time::timeout(bounded, fs.create(dir_attr.ino, "inner.txt", false))
                .await
                .expect("child create must not stall in the settle loop")
                .unwrap();
        fs.release(ifh);
        let looked = tokio::time::timeout(bounded, fs.lookup(dir_attr.ino, "inner.txt"))
            .await
            .expect("child lookup must not stall")
            .unwrap();
        assert_eq!(looked.kind, NodeKind::File);
        fs.forget(looked.ino, 1);
        fs.forget(inner.ino, 1);
        let names = tokio::time::timeout(bounded, dir_names(&fs, dir_attr.ino))
            .await
            .expect("readdir must not stall");
        assert_eq!(
            names,
            vec!["inner.txt".to_string()],
            "no lower leak-through"
        );
        let root_names = dir_names(&fs, ROOT_INO).await;
        assert_eq!(
            root_names.iter().filter(|n| n.as_str() == "swap").count(),
            1,
            "swap appears exactly once in the parent listing"
        );

        // The pre-seal heal writes the missing whiteout once AND records the boundary as a
        // dirty dir upsert — the durable half: a retry (a seal whose push failed after
        // healing) re-derives nothing because the record rides every subsequent delta, and
        // the seal's dir-upsert arm publishes the whiteout-delete plus the children.
        let gen_before = fs.dirty_since(0).watermark;
        let healed = fs.heal_replaced_files(["swap"].into_iter()).await.unwrap();
        assert_eq!(healed, vec!["swap".to_string()]);
        let delta = fs.dirty_since(gen_before);
        assert!(
            delta.upserts.iter().any(|(p, _)| p == "swap"),
            "the healed boundary is recorded in the dirty index: {:?}",
            delta.upserts
        );
        let retry = fs.heal_replaced_files(["swap"].into_iter()).await.unwrap();
        assert!(
            retry.is_empty(),
            "already marked and recorded: retries probe nothing"
        );
        // Ancestor derivation still discovers a boundary whose own record was consumed
        // before the racing file landed (child-only delta): clear the record's effect by
        // asking with only the child — the marker short-circuits, no re-heal needed.
        let via_child = fs
            .heal_replaced_files(["swap/inner.txt"].into_iter())
            .await
            .unwrap();
        assert!(via_child.is_empty(), "marker present: nothing to re-heal");
        let attr = fs.getattr(dir_attr.ino).await.unwrap();
        assert_eq!(
            attr.kind,
            NodeKind::Dir,
            "the marker never hides the upper dir"
        );

        // Reverse shape: upper FILE where the lower gains a directory.
        let (rev, rfh) = fs.create(ROOT_INO, "rev", false).await.unwrap();
        fs.release(rfh);
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![push_file("rev/child.txt", b"below\n", 0o100644)],
            PushOptions {
                message: "remote dir at the file name".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut delivered = false;
        for _ in 0..100 {
            if core.poll_ref().await.unwrap().is_some() {
                delivered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(delivered, "the remote dir's refresh never arrived");
        let attr = fs.getattr(rev.ino).await.unwrap();
        assert_eq!(attr.kind, NodeKind::File, "upper file keeps shadowing");
        let child = tokio::time::timeout(bounded, fs.lookup(rev.ino, "child.txt"))
            .await
            .expect("shadowed child lookup must not stall");
        assert!(
            matches!(child, Err(MountError::NotFound(_))),
            "the lower directory's children stay hidden under the upper file"
        );
        fs.forget(rev.ino, 1);
        fs.forget(dir_attr.ino, 1);
    }
}
