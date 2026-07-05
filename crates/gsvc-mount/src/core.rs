//! The VFS-agnostic mount core: inode table, lookup/readdir/read over the native API, and the
//! branch-refresh state machine.
//!
//! ## Coherence model: stable per-path inodes, close-to-open
//!
//! Inodes are allocated **per path** and survive a branch refresh (issue #24's shared-ro
//! decision): to `watchman`/`make`/`rsync`, a refresh looks like files being edited in place,
//! not a full tree replacement. Content under one commit is immutable, so each node records the
//! commit and object it currently serves; a refresh walks the live inode table against the new
//! commit and, per path:
//!
//! - **unchanged** (same oid and mode): the node is left exactly as it was — same ino, same
//!   attrs, and still keyed to the old commit so every metadata/block cache entry stays warm.
//!   An unchanged *directory* oid proves the whole subtree unchanged, so its descendants are
//!   skipped wholesale.
//! - **changed**: the node is rebound in place — same ino, new size/mode/oid under the new
//!   commit. New opens serve the new content.
//! - **gone**: the node is marked stale; lookups and new opens return `NotFound` while the ino
//!   lingers until the kernel forgets it (NFS-ish `ESTALE` shape). A later lookup of the same
//!   path revives the ino.
//!
//! Open *handles* are unaffected throughout: they snapshot their node at open time and keep
//! reading the commit they opened under — close-to-open coherence, stated as such in the design.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::{Bytes, BytesMut};

use crate::cache::{MountCaches, StatOutcome};
use crate::client::{FileStat, FsClient, TreeEntry};
use crate::{MountError, MountOptions};

/// What a node is, from its git mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    Dir,
    File,
    Symlink,
}

fn kind_of_mode(mode: u32) -> NodeKind {
    match mode {
        0o40000 => NodeKind::Dir,
        0o120000 => NodeKind::Symlink,
        _ => NodeKind::File,
    }
}

/// POSIX-ish attributes for a node, ready for a FUSE binding to translate.
#[derive(Clone, Debug)]
pub struct NodeAttr {
    pub ino: u64,
    pub kind: NodeKind,
    pub size: u64,
    /// Permission bits derived from the git mode (git stores no more than exec-or-not).
    pub perm: u16,
    /// The commit this node is pinned to (hex).
    pub commit: String,
}

/// One resolved view of a path. Immutable once created — the (commit, path) pair it names can
/// never change content — but the inode table may swap a fresh `Node` in behind the same ino
/// when a branch refresh rebinds the path. Handles clone the `Arc` at open time, which is what
/// pins their commit.
#[derive(Debug)]
struct Node {
    commit: Arc<str>,
    path: String, // repo-relative, "" for a root
    kind: NodeKind,
    mode: u32,
    size: u64,
    /// Object oid (hex). For directories this is the tree oid, which lets readdir descend by oid
    /// with no server-side path walk. Empty when unknown (the root, or a dir resolved through the
    /// tree-probe fallback) — those list by path instead.
    oid: String,
}

impl Node {
    fn attr(&self, ino: u64) -> NodeAttr {
        NodeAttr {
            ino,
            kind: self.kind,
            size: self.size,
            perm: match self.kind {
                NodeKind::Dir => 0o755,
                NodeKind::Symlink => 0o777,
                NodeKind::File => {
                    if self.mode == 0o100755 {
                        0o755
                    } else {
                        0o644
                    }
                }
            },
            commit: self.commit.to_string(),
        }
    }
}

/// One inode's slot: the node it currently serves, the kernel lookup count, and whether the
/// path vanished in a refresh (`stale` — served as `NotFound` until the kernel forgets the ino
/// or a lookup of the same path revives it).
struct NodeSlot {
    node: Arc<Node>,
    count: u64,
    stale: bool,
}

/// Inode table with kernel-style lookup counting: `lookup`-family calls increment a node's count,
/// `forget` decrements, and a node is dropped at zero so the table stays bounded by what the
/// kernel actually references. Inos are keyed **per path** and stable across branch refreshes;
/// a refresh rebinds or stales slots in place. Ino 1 is the root and never expires.
struct InodeTable {
    nodes: HashMap<u64, NodeSlot>,
    index: HashMap<String, u64>, // path -> ino
    next: u64,
}

pub const ROOT_INO: u64 = 1;

impl InodeTable {
    fn new() -> Self {
        InodeTable {
            nodes: HashMap::new(),
            index: HashMap::new(),
            next: ROOT_INO + 1,
        }
    }

    fn intern(&mut self, node: Node) -> (u64, Arc<Node>) {
        if let Some(&ino) = self.index.get(&node.path) {
            let slot = self.nodes.get_mut(&ino).expect("indexed node");
            slot.count += 1;
            // A fresh resolution revives stale slots and swaps the node when the *content*
            // (oid/mode/size) differs. A resolution that differs only by commit is the same
            // bytes proven by oid equality — keeping the existing node keeps every
            // (commit, path)-keyed cache entry warm, matching the refresh walk's
            // unchanged-path behavior.
            slot.stale = false;
            if slot.node.oid != node.oid
                || slot.node.mode != node.mode
                || slot.node.size != node.size
            {
                slot.node = Arc::new(node);
            }
            return (ino, slot.node.clone());
        }
        let ino = self.next;
        self.next += 1;
        let path = node.path.clone();
        let node = Arc::new(node);
        self.nodes.insert(
            ino,
            NodeSlot {
                node: node.clone(),
                count: 1,
                stale: false,
            },
        );
        self.index.insert(path, ino);
        (ino, node)
    }

    /// The node an ino currently serves; `None` for unknown or stale inos.
    fn get(&self, ino: u64) -> Option<Arc<Node>> {
        self.nodes
            .get(&ino)
            .filter(|slot| !slot.stale)
            .map(|slot| slot.node.clone())
    }

    /// All live (non-stale) inos with their nodes, for the refresh walk.
    fn live(&self) -> Vec<(u64, Arc<Node>)> {
        self.nodes
            .iter()
            .filter(|(_, slot)| !slot.stale)
            .map(|(ino, slot)| (*ino, slot.node.clone()))
            .collect()
    }

    /// Swap the node behind `ino` (same path, new commit/attrs). No-op if the ino was forgotten
    /// meanwhile.
    fn rebind(&mut self, ino: u64, node: Node) {
        if let Some(slot) = self.nodes.get_mut(&ino) {
            slot.node = Arc::new(node);
            slot.stale = false;
        }
    }

    /// Mark `ino` as vanished-in-refresh. No-op if forgotten meanwhile.
    fn mark_stale(&mut self, ino: u64) {
        if let Some(slot) = self.nodes.get_mut(&ino) {
            slot.stale = true;
        }
    }

    fn forget(&mut self, ino: u64, nlookups: u64) {
        if ino == ROOT_INO {
            return;
        }
        let Some(slot) = self.nodes.get_mut(&ino) else {
            return;
        };
        slot.count = slot.count.saturating_sub(nlookups);
        if slot.count == 0 {
            let path = slot.node.path.clone();
            self.nodes.remove(&ino);
            // Only unmap the path if it still points at this ino (a revive cannot remap, but
            // stay defensive against future re-keying).
            if self.index.get(&path) == Some(&ino) {
                self.index.remove(&path);
            }
        }
    }
}

/// The mount's mutable state: which commit the root currently resolves to.
struct RootState {
    commit: Arc<str>,
    generation: u64,
}

/// An open directory handle: incrementally paged entries, so a huge directory is never fetched
/// whole unless the reader actually walks it to the end (at which point it is cached whole).
struct DirHandle {
    node: Arc<Node>,
    entries: Vec<TreeEntry>,
    next_after: Option<String>,
    exhausted: bool,
    from_cache: bool,
}

/// An open file handle, pinning its node (and therefore its commit) for the handle's lifetime.
struct FileHandle {
    node: Arc<Node>,
}

enum Handle {
    Dir(DirHandle),
    File(FileHandle),
}

/// One entry as returned to a `readdir` caller. Plain-readdir semantics, matching the kernel's:
/// entries carry **no inode and no lookup reference** — a consumer resolves a name it cares about
/// through [`MountCore::lookup`], which is the counted-reference path that `forget` balances.
/// (Returning interned inos here would leak table entries: plain FUSE readdir never generates
/// matching forgets.)
#[derive(Clone, Debug)]
pub struct DirEntryOut {
    /// Offset of the *next* entry (pass back as the resume offset).
    pub next_offset: u64,
    pub name: String,
    pub kind: NodeKind,
}

/// The read-only mount core. All methods are async and thread-safe; a FUSE binding drives them
/// from its callbacks via a runtime handle.
pub struct MountCore {
    client: FsClient,
    caches: MountCaches,
    opts: MountOptions,
    root: RwLock<RootState>,
    inodes: Mutex<InodeTable>,
    handles: Mutex<HashMap<u64, Handle>>,
    next_fh: AtomicU64,
}

impl MountCore {
    /// Resolve the mount reference and build the core. `follow` mode requires the reference to be
    /// a live ref; pinned mode also accepts a raw commit hex.
    pub async fn new(client: FsClient, opts: MountOptions) -> Result<Arc<MountCore>, MountError> {
        let (commit, generation) = match client.ref_status(&opts.reference).await {
            Ok(status) => match status.oid {
                Some(oid) => (oid, status.generation),
                None if opts.follow => {
                    return Err(MountError::NotFound(format!(
                        "ref {} does not exist",
                        opts.reference
                    )));
                }
                None => (opts.reference.clone(), 0),
            },
            // A 40-hex pinned mount may name a commit, not a ref.
            Err(MountError::NotFound(_)) | Err(MountError::Status { status: 400, .. })
                if !opts.follow && opts.reference.len() == 40 =>
            {
                (opts.reference.clone(), 0)
            }
            Err(e) => return Err(e),
        };
        let core = Arc::new(MountCore {
            client,
            caches: MountCaches::new(opts.cache),
            root: RwLock::new(RootState {
                commit: Arc::from(commit.as_str()),
                generation,
            }),
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            opts,
        });
        // Validate the commit actually serves before reporting a successful mount.
        core.client
            .tree_page(&core.current_commit(), "", None, 1)
            .await?;
        Ok(core)
    }

    /// The commit the root currently resolves to (hex).
    pub fn current_commit(&self) -> String {
        self.root.read().unwrap().commit.to_string()
    }

    /// The last observed ref generation (0 for pinned-to-commit mounts).
    pub fn current_generation(&self) -> u64 {
        self.root.read().unwrap().generation
    }

    pub fn follow(&self) -> bool {
        self.opts.follow
    }

    pub fn poll_interval(&self) -> std::time::Duration {
        self.opts.poll_interval
    }

    fn root_commit(&self) -> Arc<str> {
        self.root.read().unwrap().commit.clone()
    }

    /// One poll of the followed ref. Returns `true` when the root moved to a new commit (after
    /// rebinding the live inode table against it). A deleted ref keeps serving the last commit
    /// (the mount is pinned-by-force and logs it).
    pub async fn poll_ref(&self) -> Result<bool, MountError> {
        if !self.opts.follow {
            return Ok(false);
        }
        let status = self.client.ref_status(&self.opts.reference).await?;
        let new_commit: Arc<str> = {
            let mut root = self.root.write().unwrap();
            if status.generation == root.generation {
                return Ok(false);
            }
            match status.oid {
                Some(oid) => {
                    tracing::info!(
                        reference = %self.opts.reference,
                        from = %root.commit,
                        to = %oid,
                        generation = status.generation,
                        "mount: ref advanced; refreshing root"
                    );
                    // The commit swaps now (new lookups must see the new tree immediately), but
                    // the generation is recorded only after the inode refresh completes cleanly:
                    // a partially-failed refresh leaves the generation stale so the next poll
                    // re-enters and retries the failed paths (already-rebound nodes are skipped
                    // as up to date, so the retry costs only the failures).
                    root.commit = Arc::from(oid.as_str());
                    root.commit.clone()
                }
                None => {
                    tracing::warn!(
                        reference = %self.opts.reference,
                        pinned = %root.commit,
                        "mount: followed ref deleted; serving last known commit"
                    );
                    root.generation = status.generation;
                    return Ok(false);
                }
            }
        };
        let failed = self.refresh_inodes(&new_commit).await;
        let mut root = self.root.write().unwrap();
        if failed == 0 {
            // Only mark the generation handled if the root hasn't moved again mid-refresh; a
            // newer target must keep the poll loop live.
            if root.commit.as_ref() == new_commit.as_ref() {
                root.generation = status.generation;
            }
        } else {
            tracing::warn!(
                reference = %self.opts.reference,
                failed,
                "mount: refresh left stale nodes; will retry on the next poll"
            );
        }
        Ok(true)
    }

    /// Rebind the live inode table against `new_commit`: unchanged paths keep their node (same
    /// ino, same attrs, caches stay warm on the old commit's immutable keys), changed paths get
    /// a fresh node behind the same ino, vanished paths go stale. An unchanged directory oid
    /// proves its whole subtree unchanged, so descendants are skipped without a stat.
    ///
    /// Per-path stat failures leave that node on its prior commit — it serves slightly stale
    /// (still immutable, still consistent) content — and are returned as a count so the caller
    /// keeps the poll generation stale and retries them on the next poll.
    async fn refresh_inodes(&self, new_commit: &Arc<str>) -> usize {
        let mut live = self.inodes.lock().unwrap().live();
        // Ancestors sort before their descendants (a path is lexicographically smaller than any
        // path it prefixes), which is all the pruning walk needs; sibling interleavings like
        // `a.txt` between `a` and `a/b` are irrelevant to it.
        live.sort_by(|a, b| a.1.path.cmp(&b.1.path));
        let mut pruned: Vec<String> = Vec::new();
        let mut failed = 0usize;
        for (ino, node) in live {
            if node.commit.as_ref() == new_commit.as_ref() {
                // Already resolved under the new commit by a racing lookup.
                continue;
            }
            if pruned.iter().any(|dir| under_dir(&node.path, dir)) {
                continue;
            }
            match self.stat_at(new_commit, &node.path).await {
                Ok(StatOutcome::Present(stat)) => {
                    let unchanged =
                        !stat.oid.is_empty() && stat.oid == node.oid && stat.mode == node.mode;
                    if unchanged {
                        if node.kind == NodeKind::Dir {
                            pruned.push(node.path.clone());
                        }
                        continue;
                    }
                    let fresh = Node {
                        commit: new_commit.clone(),
                        path: node.path.clone(),
                        kind: kind_of_mode(stat.mode),
                        mode: stat.mode,
                        size: stat.size,
                        oid: stat.oid,
                    };
                    self.inodes.lock().unwrap().rebind(ino, fresh);
                }
                Ok(StatOutcome::Absent) => {
                    self.inodes.lock().unwrap().mark_stale(ino);
                }
                Err(e) => {
                    failed += 1;
                    tracing::warn!(
                        path = %node.path,
                        error = %e,
                        "mount: refresh re-stat failed; node keeps its prior commit until the next poll"
                    );
                }
            }
        }
        failed
    }

    fn node_of(&self, ino: u64) -> Result<Arc<Node>, MountError> {
        if ino == ROOT_INO {
            return Ok(Arc::new(Node {
                commit: self.root_commit(),
                path: String::new(),
                kind: NodeKind::Dir,
                mode: 0o40000,
                size: 0,
                oid: String::new(),
            }));
        }
        self.inodes
            .lock()
            .unwrap()
            .get(ino)
            .ok_or_else(|| MountError::NotFound(format!("inode {ino}")))
    }

    pub fn getattr(&self, ino: u64) -> Result<NodeAttr, MountError> {
        Ok(self.node_of(ino)?.attr(ino))
    }

    /// Drop `nlookups` kernel references to `ino` (FUSE `forget`).
    pub fn forget(&self, ino: u64, nlookups: u64) {
        self.inodes.lock().unwrap().forget(ino, nlookups);
    }

    /// Resolve `name` under `parent`. The child inherits the parent's commit, which is what pins
    /// entire in-use subtrees across a branch refresh.
    pub async fn lookup(&self, parent: u64, name: &str) -> Result<NodeAttr, MountError> {
        let parent_node = self.node_of(parent)?;
        if parent_node.kind != NodeKind::Dir {
            return Err(MountError::NotADirectory);
        }
        let commit = parent_node.commit.clone();
        let child_path = join_path(&parent_node.path, name);

        match self.stat_at(&commit, &child_path).await? {
            StatOutcome::Present(stat) => {
                let node = Node {
                    commit: commit.clone(),
                    kind: kind_of_mode(stat.mode),
                    mode: stat.mode,
                    size: stat.size,
                    oid: stat.oid,
                    path: child_path,
                };
                let (ino, node) = self.inodes.lock().unwrap().intern(node);
                Ok(node.attr(ino))
            }
            StatOutcome::Absent => Err(MountError::NotFound(name.to_string())),
        }
    }

    /// Stat one `(commit, path)` with negative caching. Resolution order: stat cache, the parent
    /// directory's complete cached listing, `HEAD files/`, and finally a one-entry `tree` probe to
    /// distinguish "directory" from "absent" (the files endpoint serves blobs, not trees).
    async fn stat_at(&self, commit: &Arc<str>, path: &str) -> Result<StatOutcome, MountError> {
        if let Some(outcome) = self.caches.stat(commit, path) {
            return Ok(outcome);
        }
        if let Some((dir, name)) = split_path(path) {
            if let Some(entries) = self.caches.dir(commit, dir) {
                let outcome = match entries.iter().find(|e| e.name == name) {
                    Some(e) => StatOutcome::Present(FileStat {
                        oid: e.oid.clone(),
                        mode: e.mode,
                        // A tree entry without a cheap size resolves through HEAD below.
                        size: match (e.is_dir(), e.size) {
                            (true, _) => 0,
                            (false, Some(size)) => size,
                            (false, None) => {
                                return self.stat_via_http(commit, path).await;
                            }
                        },
                    }),
                    None => StatOutcome::Absent,
                };
                self.caches.put_stat(commit, path, outcome.clone());
                return Ok(outcome);
            }
        }
        self.stat_via_http(commit, path).await
    }

    async fn stat_via_http(
        &self,
        commit: &Arc<str>,
        path: &str,
    ) -> Result<StatOutcome, MountError> {
        let outcome = match self.client.stat(commit, path).await {
            Ok(stat) => StatOutcome::Present(stat),
            Err(MountError::NotFound(_)) => {
                // Not servable as a file: either a directory or truly absent. The probe page
                // carries the tree's own oid, which is what lets the refresh walk prove the
                // directory (and its subtree) unchanged across commits.
                match self.client.tree_page(commit, path, None, 1).await {
                    Ok(page) => StatOutcome::Present(FileStat {
                        oid: page.tree_oid.unwrap_or_default(),
                        mode: 0o40000,
                        size: 0,
                    }),
                    Err(MountError::NotFound(_)) => StatOutcome::Absent,
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        self.caches.put_stat(commit, path, outcome.clone());
        Ok(outcome)
    }

    /// Open a directory for reading; the handle pages incrementally.
    pub fn opendir(&self, ino: u64) -> Result<u64, MountError> {
        let node = self.node_of(ino)?;
        if node.kind != NodeKind::Dir {
            return Err(MountError::NotADirectory);
        }
        let (entries, exhausted, from_cache) = match self.caches.dir(&node.commit, &node.path) {
            Some(cached) => ((*cached).clone(), true, true),
            None => (Vec::new(), false, false),
        };
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().unwrap().insert(
            fh,
            Handle::Dir(DirHandle {
                node,
                entries,
                next_after: None,
                exhausted,
                from_cache,
            }),
        );
        Ok(fh)
    }

    /// Read directory entries starting at `offset` (0 for the first call; then the last returned
    /// `next_offset`). Synthetic `.`/`..` are the binding's concern, not the core's.
    pub async fn readdir(
        &self,
        fh: u64,
        offset: u64,
        max: usize,
    ) -> Result<Vec<DirEntryOut>, MountError> {
        // Take what we need under the lock, fetch pages outside it, then write back.
        loop {
            let (commit, path, dir_oid, have, next_after, exhausted) = {
                let handles = self.handles.lock().unwrap();
                let Some(Handle::Dir(dir)) = handles.get(&fh) else {
                    return Err(MountError::BadHandle);
                };
                (
                    dir.node.commit.clone(),
                    dir.node.path.clone(),
                    dir.node.oid.clone(),
                    dir.entries.len() as u64,
                    dir.next_after.clone(),
                    dir.exhausted,
                )
            };
            if exhausted || have > offset {
                break;
            }
            // Descend by tree oid when the parent listing gave us one — the server skips version
            // resolution and the O(depth) path walk. The root (and probe-resolved dirs) page by
            // path.
            let page = if dir_oid.is_empty() {
                self.client
                    .tree_page(&commit, &path, next_after.as_deref(), self.opts.page_limit)
                    .await?
            } else {
                self.client
                    .tree_page_by_oid(&dir_oid, next_after.as_deref(), self.opts.page_limit)
                    .await?
            };
            let mut handles = self.handles.lock().unwrap();
            let Some(Handle::Dir(dir)) = handles.get_mut(&fh) else {
                return Err(MountError::BadHandle);
            };
            // The page was fetched outside the lock: a concurrent readdir on the same handle may
            // already have appended it. Append only if the handle is exactly where we left it;
            // otherwise discard and re-evaluate.
            if dir.entries.len() as u64 != have || dir.next_after != next_after {
                continue;
            }
            dir.entries.extend(page.entries);
            dir.exhausted = !page.truncated;
            dir.next_after = page.next_after;
            if dir.exhausted && !dir.from_cache {
                // The full listing is now known; publish it so lookups and later opendirs skip
                // the network entirely.
                self.caches
                    .put_dir(&commit, &path, Arc::new(dir.entries.clone()));
            }
        }

        let handles = self.handles.lock().unwrap();
        let Some(Handle::Dir(dir)) = handles.get(&fh) else {
            return Err(MountError::BadHandle);
        };
        // Plain-readdir: names and kinds only. No inode is interned and no reference is taken —
        // `lookup` is the counted path (and resolves sizes there, from the cached listing this
        // very call populates).
        Ok(dir
            .entries
            .iter()
            .enumerate()
            .skip(offset as usize)
            .take(max)
            .map(|(i, entry)| DirEntryOut {
                next_offset: (i + 1) as u64,
                name: entry.name.clone(),
                kind: kind_of_mode(entry.mode),
            })
            .collect())
    }

    pub fn releasedir(&self, fh: u64) {
        self.handles.lock().unwrap().remove(&fh);
    }

    /// Open a file (or symlink) for reading; the handle pins the node's commit.
    pub fn open(&self, ino: u64) -> Result<u64, MountError> {
        let node = self.node_of(ino)?;
        if node.kind == NodeKind::Dir {
            return Err(MountError::IsADirectory);
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles
            .lock()
            .unwrap()
            .insert(fh, Handle::File(FileHandle { node }));
        Ok(fh)
    }

    pub fn release(&self, fh: u64) {
        self.handles.lock().unwrap().remove(&fh);
    }

    /// Read `[offset, offset+size)` of an open file through the block cache. Fetches are
    /// block-aligned so sequential FUSE reads coalesce into few range requests.
    pub async fn read(&self, fh: u64, offset: u64, size: u64) -> Result<Bytes, MountError> {
        let node = {
            let handles = self.handles.lock().unwrap();
            match handles.get(&fh) {
                Some(Handle::File(f)) => f.node.clone(),
                _ => return Err(MountError::BadHandle),
            }
        };
        self.read_node(&node, offset, size).await
    }

    /// Symlink target: git stores it as the blob's content.
    pub async fn readlink(&self, ino: u64) -> Result<Bytes, MountError> {
        let node = self.node_of(ino)?;
        if node.kind != NodeKind::Symlink {
            return Err(MountError::Protocol("not a symlink".to_string()));
        }
        self.read_node(&node, 0, node.size.max(4096)).await
    }

    async fn read_node(&self, node: &Node, offset: u64, size: u64) -> Result<Bytes, MountError> {
        if size == 0 || offset >= node.size {
            return Ok(Bytes::new());
        }
        let end = offset.saturating_add(size).min(node.size);
        let block_bytes = self.caches.config.block_bytes.max(1);
        let first_block = offset / block_bytes;
        let last_block = (end - 1) / block_bytes;

        let mut out = BytesMut::with_capacity((end - offset) as usize);
        for block in first_block..=last_block {
            let block_start = block * block_bytes;
            let block_end = (block_start + block_bytes).min(node.size);
            let bytes = match self.caches.block(&node.commit, &node.path, block) {
                Some(bytes) => bytes,
                None => {
                    let bytes = self
                        .client
                        .read_range(
                            &node.commit,
                            &node.path,
                            block_start,
                            block_end - block_start,
                        )
                        .await?;
                    self.caches
                        .put_block(&node.commit, &node.path, block, bytes.clone());
                    bytes
                }
            };
            let want_start = offset.max(block_start) - block_start;
            let want_end = (end.min(block_end) - block_start).min(bytes.len() as u64);
            if want_start < want_end {
                out.extend_from_slice(&bytes[want_start as usize..want_end as usize]);
            }
        }
        Ok(out.freeze())
    }
}

fn join_path(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// Whether `path` lies strictly inside directory `dir`.
fn under_dir(path: &str, dir: &str) -> bool {
    path.strip_prefix(dir)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// Split a repo-relative path into `(parent dir, name)`; `None` for the root itself.
fn split_path(path: &str) -> Option<(&str, &str)> {
    if path.is_empty() {
        return None;
    }
    match path.rfind('/') {
        Some(i) => Some((&path[..i], &path[i + 1..])),
        None => Some(("", path)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_helpers_round_trip() {
        assert_eq!(join_path("", "a"), "a");
        assert_eq!(join_path("a/b", "c"), "a/b/c");
        assert_eq!(split_path(""), None);
        assert_eq!(split_path("a"), Some(("", "a")));
        assert_eq!(split_path("a/b/c"), Some(("a/b", "c")));
    }

    fn node_at(commit: &Arc<str>, path: &str, oid: &str, size: u64) -> Node {
        Node {
            commit: commit.clone(),
            path: path.to_string(),
            kind: NodeKind::File,
            mode: 0o100644,
            size,
            oid: oid.to_string(),
        }
    }

    #[test]
    fn inode_table_interns_and_forgets() {
        let mut table = InodeTable::new();
        let commit: Arc<str> = Arc::from("c1");
        let (ino_a, _) = table.intern(node_at(&commit, "a", "o1", 1));
        let (ino_a2, _) = table.intern(node_at(&commit, "a", "o1", 1));
        assert_eq!(ino_a, ino_a2, "same path interns to one ino");
        let (ino_b, _) = table.intern(node_at(&commit, "b", "o2", 1));
        assert_ne!(ino_a, ino_b);
        // Two lookups recorded for a: one forget keeps it, the second drops it.
        table.forget(ino_a, 1);
        assert!(table.get(ino_a).is_some());
        table.forget(ino_a, 1);
        assert!(table.get(ino_a).is_none());
        // A new intern of the same path gets a fresh ino.
        let (ino_a3, _) = table.intern(node_at(&commit, "a", "o1", 1));
        assert_ne!(ino_a, ino_a3);
    }

    #[test]
    fn inode_survives_refresh_rebind_and_revive() {
        let mut table = InodeTable::new();
        let c1: Arc<str> = Arc::from("c1");
        let c2: Arc<str> = Arc::from("c2");
        let (ino, _) = table.intern(node_at(&c1, "a", "o1", 1));

        // Same path resolved under a new commit with new content: same ino, node swapped.
        let (ino2, node2) = table.intern(node_at(&c2, "a", "o2", 7));
        assert_eq!(ino, ino2, "path keeps its ino across commits");
        assert_eq!(node2.size, 7);
        assert_eq!(node2.commit, c2);

        // Rebind (the refresh walk's path) also swaps in place.
        table.rebind(ino, node_at(&c2, "a", "o3", 9));
        assert_eq!(table.get(ino).unwrap().size, 9);

        // Stale hides the ino from get() but a fresh intern of the path revives it.
        table.mark_stale(ino);
        assert!(table.get(ino).is_none(), "stale ino serves NotFound");
        let (ino3, node3) = table.intern(node_at(&c2, "a", "o4", 3));
        assert_eq!(ino, ino3, "revived path keeps its ino");
        assert_eq!(node3.oid, "o4");
        assert!(table.get(ino).is_some());
    }

    #[test]
    fn under_dir_matches_strict_descendants_only() {
        assert!(under_dir("a/b", "a"));
        assert!(under_dir("a/b/c", "a"));
        assert!(!under_dir("a", "a"));
        assert!(!under_dir("a.txt", "a"));
        assert!(!under_dir("ab/c", "a"));
    }
}
